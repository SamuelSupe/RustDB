use std::{ops::Deref, sync::Arc};

use arrow::{
    array::ArrayRef, compute::concat_batches, datatypes::SchemaRef, record_batch::RecordBatch,
};
use futures::StreamExt;

use crate::{
    Result,
    runtime::{
        BatchEnvelope, IntoMemoryBatchStream, MemoryBatchStream, QueryContext,
        boxed_memory_batch_stream,
    },
    sql::{BoundExpr, ExprKind, JoinType, ScalarValue},
};

use super::{
    expr::evaluate,
    runtime_filter::RuntimeFilterSlot,
    value::{CellValue, cell},
};

mod condition;
mod grace;
mod matched;
mod output;
mod parallel;
mod probe;
mod sort_merge;
mod spill;

#[cfg(test)]
mod correlation_tests;
#[cfg(test)]
mod global_membership_tests;
#[cfg(test)]
mod tests;
#[cfg(test)]
mod tracker_fallback_tests;

use condition::JoinPredicates;
use matched::BuildMatchTracker;
use output::build_unmatched_right_envelope;
use probe::{
    GlobalMembershipState, ProbeCursor, try_build_existence_hash_table_with_nulls,
    try_build_hash_table, try_build_hash_table_with_nulls,
};
use spill::{BuildPartition, Side};

// A streaming build starts spilling after only a bounded prefix is buffered.
// Reserve growth for the unseen suffix so common large scans do not begin
// with a tiny fanout and immediately rewrite the complete build side.
const STREAMING_BUILD_GROWTH_RESERVE: u64 = 4;

// The physical join boundary carries both input schemas, output schema, keys,
// execution state, and sizing. Keeping this explicit avoids a public options
// abstraction for a single internal call site.
#[allow(clippy::too_many_arguments)]
#[cfg(test)]
pub(crate) fn join_with_null_keys<L, R>(
    left: L,
    right: R,
    on: Vec<(BoundExpr, BoundExpr)>,
    null_equal_keys: bool,
    residual: Option<BoundExpr>,
    null_aware: Option<(BoundExpr, BoundExpr)>,
    left_schema: SchemaRef,
    right_schema: SchemaRef,
    join_type: JoinType,
    schema: SchemaRef,
    context: Arc<QueryContext>,
    batch_size: usize,
) -> MemoryBatchStream
where
    L: IntoMemoryBatchStream,
    R: IntoMemoryBatchStream,
{
    join_with_runtime_filter(
        left,
        right,
        on,
        null_equal_keys,
        residual,
        null_aware,
        left_schema,
        right_schema,
        join_type,
        schema,
        context,
        batch_size,
        None,
    )
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn join_with_runtime_filter<L, R>(
    left: L,
    right: R,
    on: Vec<(BoundExpr, BoundExpr)>,
    null_equal_keys: bool,
    residual: Option<BoundExpr>,
    null_aware: Option<(BoundExpr, BoundExpr)>,
    left_schema: SchemaRef,
    right_schema: SchemaRef,
    join_type: JoinType,
    schema: SchemaRef,
    context: Arc<QueryContext>,
    batch_size: usize,
    runtime_filter: Option<Arc<RuntimeFilterSlot>>,
) -> MemoryBatchStream
where
    L: IntoMemoryBatchStream,
    R: IntoMemoryBatchStream,
{
    let use_global_membership_hash = on.is_empty()
        && residual.is_none()
        && null_aware.is_some()
        && matches!(join_type, JoinType::Mark | JoinType::NullAwareAnti);
    let skip_right_build = residual
        .as_ref()
        .is_some_and(|expr| matches!(&expr.kind, ExprKind::Literal(ScalarValue::Boolean(false))))
        && matches!(
            join_type,
            JoinType::Left | JoinType::LeftSingle | JoinType::Mark
        );
    let predicates = JoinPredicates::new(residual, null_aware, &left_schema, &right_schema);
    let mut left = left.into_memory_batch_stream(Arc::clone(&context), "join left input");
    let mut right = right.into_memory_batch_stream(Arc::clone(&context), "join right input");
    if skip_right_build {
        // A guarded scalar/mark join with a constant-false residual cannot
        // have a qualifying RHS row. Do not even poll the lazy right plan:
        // evaluating an unreachable scalar projection could otherwise raise
        // a division/cardinality error before the join sees the guard.
        right = boxed_memory_batch_stream(futures::stream::empty());
    }
    boxed_memory_batch_stream(async_stream::try_stream! {
        let left_key_expressions = on.iter().map(|(left, _)| left.clone()).collect::<Vec<_>>();
        let right_key_expressions = on.iter().map(|(_, right)| right.clone()).collect::<Vec<_>>();
        if join_type == JoinType::NullAwareAnti && !predicates.is_null_aware() {
            Err(crate::Error::Internal(
                "NullAwareAnti join requires a null-aware membership comparison".into(),
            ))?;
        }
        let mut reservation = context.memory.reservation();
        let mut right_batches: Vec<RecordBatch> = Vec::new();
        let mut right_bytes = 0usize;
        let mut right_rows = 0usize;
        let mut right_partitions = None;
        let mut in_memory_build = None;
        // Retaining both source batches and the future concat buffer can use
        // roughly twice the logical build bytes. Cap the buffered side so
        // scan/kernel workspaces and bounded queues always retain headroom.
        let build_buffer_limit = context.memory.limit().checked_div(4).unwrap_or(0).max(1);

        while let Some(batch) = right.next().await {
            context.check_cancelled()?;
            let batch = batch?;
            let bytes = batch.memory_size();
            let projected_bytes = right_bytes.saturating_add(bytes);
            let projected_rows = right_rows.saturating_add(batch.batch().num_rows());
            // Reserve the future concat buffer while the source envelope still
            // accounts for the retained input batch.
            if projected_bytes > build_buffer_limit
                || reservation.try_grow(bytes).is_err()
            {
                reservation.shrink(right_bytes);
                let footprint = spill::estimated_build_footprint(
                    u64::try_from(projected_bytes).unwrap_or(u64::MAX),
                    u64::try_from(projected_rows).unwrap_or(u64::MAX),
                    right_key_expressions.len(),
                )
                .saturating_mul(STREAMING_BUILD_GROWTH_RESERVE);
                let partitions = spill::adaptive_partition_count(
                    &context,
                    usize::try_from(footprint).unwrap_or(usize::MAX),
                );
                let mut spiller = spill::PartitionSpiller::with_partitions(
                    &context,
                    "join-right",
                    partitions,
                );
                for buffered in right_batches.drain(..) {
                    let buffered_bytes = buffered.get_array_memory_size();
                    spill::spill_batch_with_null_keys(
                        buffered,
                        &right_key_expressions,
                        Side::Right,
                        join_type,
                        null_equal_keys,
                        &mut spiller,
                        0,
                    )?;
                    reservation.shrink(buffered_bytes);
                }
                let (batch, batch_memory) = batch.into_parts();
                spill::spill_batch_with_null_keys(
                    batch,
                    &right_key_expressions,
                    Side::Right,
                    join_type,
                    null_equal_keys,
                    &mut spiller,
                    0,
                )?;
                drop(batch_memory);
                while let Some(batch) = right.next().await {
                    let (batch, batch_memory) = batch?.into_parts();
                    spill::spill_batch_with_null_keys(
                        batch,
                        &right_key_expressions,
                        Side::Right,
                        join_type,
                        null_equal_keys,
                        &mut spiller,
                        0,
                    )?;
                    drop(batch_memory);
                }
                right_partitions = Some(spiller.finish_manifest()?);
                break;
            }
            let (batch, batch_memory) = batch.into_parts();
            reservation.absorb(batch_memory)?;
            right_bytes = right_bytes.saturating_add(bytes);
            right_rows = projected_rows;
            right_batches.push(batch);
        }

        if right_partitions.is_none() {
            let right_batch = if right_batches.is_empty() {
                RecordBatch::new_empty(Arc::clone(&right_schema))
            } else {
                concat_batches(&right_schema, &right_batches)?
            };
            drop(right_batches);
            reservation.shrink(right_bytes);
            let rows = right_batch.num_rows();
            let (hash_table, right_values, global_membership) = if use_global_membership_hash {
                let right_values = evaluate_optional_values(
                    predicates.right_value(),
                    &right_batch,
                    &context,
                    "join build membership value",
                )?;
                let right_array = optional_array(&right_values).ok_or_else(|| {
                    crate::Error::Internal(
                        "global membership hash is missing its right value array".into(),
                    )
                })?;
                let state = GlobalMembershipState::new(rows, right_array);
                let hash_table = try_build_hash_table(
                    std::slice::from_ref(right_array),
                    rows,
                    true,
                    &mut reservation,
                )?;
                (hash_table, right_values, Some(state))
            } else {
                let right_keys = evaluate_keys_accounted(
                    &right_key_expressions,
                    &right_batch,
                    &context,
                    "join build keys",
                )?;
                let inequality_value = predicates
                    .existence_inequality_right_value(join_type, left_schema.fields().len());
                let inequality_values = evaluate_optional_values(
                    inequality_value.as_ref(),
                    &right_batch,
                    &context,
                    "join build existence inequality value",
                )?;
                let hash_table = if let Some(values) = optional_array(&inequality_values) {
                    try_build_existence_hash_table_with_nulls(
                        &right_keys,
                        rows,
                        null_equal_keys,
                        values,
                        &mut reservation,
                    )?
                } else {
                    try_build_hash_table_with_nulls(
                        &right_keys,
                        rows,
                        can_deduplicate_build(join_type, &predicates),
                        null_equal_keys,
                        &mut reservation,
                    )?
                };
                drop(inequality_values);
                drop(right_keys);
                let right_values = evaluate_optional_values(
                    predicates.right_value(),
                    &right_batch,
                    &context,
                    "join build membership value",
                )?;
                (hash_table, right_values, None)
            };
            match hash_table {
                Some(hash_table) => {
                    if let Some(matched_build) =
                        try_build_match_tracker(join_type, rows, &mut reservation)
                    {
                        in_memory_build = Some((
                            right_batch,
                            hash_table,
                            right_values,
                            global_membership,
                            matched_build,
                        ));
                    } else {
                        drop(hash_table);
                        drop(right_values);
                        right_partitions = Some(spill_build_batch(
                            right_batch,
                            &right_key_expressions,
                            join_type,
                            null_equal_keys,
                            &context,
                            &mut reservation,
                        )?);
                    }
                }
                None => {
                    drop(right_values);
                    right_partitions = Some(spill_build_batch(
                        right_batch,
                        &right_key_expressions,
                        join_type,
                        null_equal_keys,
                        &context,
                        &mut reservation,
                    )?);
                }
            }
        }

        if let Some(runtime_filter) = &runtime_filter {
            if let Some((_, hash_table, _, _, _)) = &in_memory_build {
                runtime_filter.publish_hash(hash_table, &context);
            } else {
                runtime_filter.publish_none();
            }
        }

        if let Some(right_partitions) = right_partitions {
            let left_partitions = spill::spill_stream(
                &mut left,
                &left_key_expressions,
                Side::Left,
                join_type,
                null_equal_keys,
                &context,
                "join-left",
                right_partitions.len(),
            ).await?;
            let initial = spill::initial_tasks(left_partitions, right_partitions);
            context.metrics.record_spill(
                0,
                u64::try_from(initial.len()).unwrap_or(u64::MAX),
            );
            if grace::is_supported(&context, &initial) {
                let mut output = grace::join(
                    initial,
                    left_key_expressions.clone(),
                    right_key_expressions.clone(),
                    Arc::clone(&left_schema),
                    Arc::clone(&right_schema),
                    predicates.clone(),
                    null_equal_keys,
                    join_type,
                    Arc::clone(&schema),
                    Arc::clone(&context),
                    batch_size,
                );
                while let Some(batch) = output.next().await {
                    yield batch?;
                }
                return;
            }
            let mut pending = initial;
            while let Some(task) = spill::pop_largest_task(&mut pending) {
                context.check_cancelled()?;
                let build = match spill::load_build_partition(
                    &task.right,
                    &right_schema,
                    &context,
                    &mut reservation,
                    task.build,
                )? {
                    BuildPartition::Loaded(right_batch) => {
                        let right_keys = evaluate_keys_accounted(
                            &right_key_expressions,
                            &right_batch,
                            &context,
                            "join spill build keys",
                        )?;
                        let rows = right_batch.num_rows();
                        let inequality_value = predicates.existence_inequality_right_value(
                            join_type,
                            left_schema.fields().len(),
                        );
                        let inequality_values = evaluate_optional_values(
                            inequality_value.as_ref(),
                            &right_batch,
                            &context,
                            "join spill existence inequality value",
                        )?;
                        let hash_table = if let Some(values) = optional_array(&inequality_values) {
                            try_build_existence_hash_table_with_nulls(
                                &right_keys,
                                rows,
                                null_equal_keys,
                                values,
                                &mut reservation,
                            )?
                        } else {
                            try_build_hash_table_with_nulls(
                                &right_keys,
                                rows,
                                can_deduplicate_build(join_type, &predicates),
                                null_equal_keys,
                                &mut reservation,
                            )?
                        };
                        drop(inequality_values);
                        drop(right_keys);
                        let right_values = evaluate_optional_values(
                            predicates.right_value(),
                            &right_batch,
                            &context,
                            "join spill build membership value",
                        )?;
                        match hash_table {
                            Some(hash_table)
                                if let Some(matched_build) = try_build_match_tracker(
                                    join_type,
                                    rows,
                                    &mut reservation,
                                ) =>
                            {
                                PartitionHashBuild::Ready(
                                    right_batch,
                                    hash_table,
                                    right_values,
                                    matched_build,
                                )
                            }
                            Some(hash_table) => {
                                drop(hash_table);
                                drop(right_values);
                                PartitionHashBuild::TooLarge(rows)
                            }
                            None => {
                                drop(right_values);
                                PartitionHashBuild::TooLarge(rows)
                            }
                        }
                    }
                    BuildPartition::TooLarge { rows } => PartitionHashBuild::TooLarge(rows),
                };
                match build {
                    PartitionHashBuild::Ready(
                        right_batch,
                        hash_table,
                        right_values,
                        matched_build,
                    ) => {
                        for file in &task.left {
                            for left_batch in context.spill.read_file(file)? {
                                let left_batch = left_batch?;
                                let left_batch = BatchEnvelope::try_new(
                                    left_batch,
                                    &context.memory,
                                    "join spill probe",
                                )?;
                                let left_keys = evaluate_keys_accounted(
                                    &left_key_expressions,
                                    left_batch.batch(),
                                    &context,
                                    "join spill probe keys",
                                )?;
                                let left_values = evaluate_optional_values(
                                    predicates.left_value(),
                                    left_batch.batch(),
                                    &context,
                                    "join spill probe membership value",
                                )?;
                                let mut probe = ProbeCursor::new(
                                    left_batch.batch(),
                                    &right_batch,
                                    &left_keys,
                                    &hash_table,
                                    &predicates,
                                    optional_array(&left_values),
                                    optional_array(&right_values),
                                    None,
                                    null_equal_keys,
                                    matched_build.clone(),
                                    join_type,
                                    Arc::clone(&schema),
                                    batch_size,
                                    reservation
                                        .size()
                                        .saturating_add(left_batch.memory_size())
                                        .saturating_add(left_keys.memory_size())
                                        .saturating_add(optional_memory(&left_values))
                                        .saturating_add(optional_memory(&right_values)),
                                );
                                while let Some(output) = probe.next_batch(&context).await? {
                                    yield output;
                                }
                            }
                        }
                        if let Some(matched) = &matched_build {
                            let mut start = 0;
                            loop {
                                let indices = matched
                                    .unmatched_from(
                                        start,
                                        batch_size.max(1),
                                        &context,
                                        reservation.size(),
                                    )
                                    .await?;
                                let Some(last) = indices.last().copied() else { break };
                                start = last as usize + 1;
                                yield build_unmatched_right_envelope(
                                    &left_schema,
                                    &right_batch,
                                    &indices,
                                    Arc::clone(&schema),
                                    &context,
                                    reservation.size().saturating_add(indices.memory_size()),
                                ).await?;
                            }
                        }
                        spill::remove_task(&context, &task)?;
                        reservation.try_resize(0)?;
                    }
                    PartitionHashBuild::TooLarge(rows) => {
                        reservation.try_resize(0)?;
                        if task.depth < context.execution.max_repartition_depth {
                            let next_depth = task.depth + 1;
                            let repartitioned = spill::repartition(
                                &task,
                                &left_key_expressions,
                                &right_key_expressions,
                                join_type,
                                null_equal_keys,
                                next_depth,
                                &context,
                            )?;
                            let shrank = repartitioned.largest_build_rows < rows;
                            if shrank || task.stagnant_repartitions == 0 {
                                spill::remove_task(&context, &task)?;
                                let stagnant = if shrank {
                                    0
                                } else {
                                    task.stagnant_repartitions + 1
                                };
                                for mut child in repartitioned.tasks {
                                    child.stagnant_repartitions = stagnant;
                                    pending.push(child);
                                }
                                continue;
                            }
                            spill::remove_tasks(&context, &repartitioned.tasks)?;
                        }

                        let mut fallback = sort_merge::fallback_with_null_keys(
                            task,
                            left_key_expressions.clone(),
                            right_key_expressions.clone(),
                            Arc::clone(&left_schema),
                            Arc::clone(&right_schema),
                            predicates.clone(),
                            null_equal_keys,
                            join_type,
                            Arc::clone(&schema),
                            Arc::clone(&context),
                            batch_size,
                        );
                        while let Some(output) = fallback.next().await {
                            yield output?;
                        }
                        reservation.try_resize(0)?;
                    }
                }
            }
            return;
        }

        let (right_batch, hash_table, right_values, global_membership, matched_build) = in_memory_build
            .take()
            .expect("a non-spilling join has an in-memory build");
        if parallel::is_supported(&context, reservation.size()) {
            let build = parallel::FrozenBuild::new(
                right_batch,
                hash_table,
                right_values,
                global_membership,
                null_equal_keys,
                matched_build,
                reservation,
            );
            let mut output = parallel::probe(
                left,
                left_key_expressions,
                build,
                predicates.clone(),
                join_type,
                Arc::clone(&left_schema),
                Arc::clone(&schema),
                Arc::clone(&context),
                batch_size,
            );
            while let Some(batch) = output.next().await {
                yield batch?;
            }
            return;
        }
        while let Some(batch) = left.next().await {
            context.check_cancelled()?;
            let left_batch = batch?;
            let left_values = evaluate_optional_values(
                predicates.left_value(),
                left_batch.batch(),
                &context,
                "join probe membership value",
            )?;
            let left_keys = if global_membership.is_some() {
                None
            } else {
                Some(evaluate_keys_accounted(
                    &left_key_expressions,
                    left_batch.batch(),
                    &context,
                    "join probe keys",
                )?)
            };
            let probe_keys = if global_membership.is_some() {
                std::slice::from_ref(optional_array(&left_values).ok_or_else(|| {
                    crate::Error::Internal(
                        "global membership hash is missing its left value array".into(),
                    )
                })?)
            } else {
                left_keys.as_deref().expect("regular join evaluated its keys")
            };
            let mut probe = ProbeCursor::new(
                left_batch.batch(),
                &right_batch,
                probe_keys,
                &hash_table,
                &predicates,
                optional_array(&left_values),
                optional_array(&right_values),
                global_membership,
                null_equal_keys,
                matched_build.clone(),
                join_type,
                Arc::clone(&schema),
                batch_size,
                reservation
                    .size()
                    .saturating_add(left_batch.memory_size())
                    .saturating_add(optional_memory(&left_keys))
                    .saturating_add(optional_memory(&left_values))
                    .saturating_add(optional_memory(&right_values)),
            );
            while let Some(output) = probe.next_batch(&context).await? {
                yield output;
            }
        }
        if let Some(matched) = &matched_build {
            let mut start = 0;
            loop {
                let indices = matched
                    .unmatched_from(
                        start,
                        batch_size.max(1),
                        &context,
                        reservation.size(),
                    )
                    .await?;
                let Some(last) = indices.last().copied() else { break };
                start = last as usize + 1;
                yield build_unmatched_right_envelope(
                    &left_schema,
                    &right_batch,
                    &indices,
                    Arc::clone(&schema),
                    &context,
                    reservation.size().saturating_add(indices.memory_size()),
                ).await?;
            }
        }
    })
}

#[allow(clippy::too_many_arguments)]
#[cfg(test)]
pub(crate) fn join<L, R>(
    left: L,
    right: R,
    on: Vec<(BoundExpr, BoundExpr)>,
    residual: Option<BoundExpr>,
    null_aware: Option<(BoundExpr, BoundExpr)>,
    left_schema: SchemaRef,
    right_schema: SchemaRef,
    join_type: JoinType,
    schema: SchemaRef,
    context: Arc<QueryContext>,
    batch_size: usize,
) -> MemoryBatchStream
where
    L: IntoMemoryBatchStream,
    R: IntoMemoryBatchStream,
{
    join_with_null_keys(
        left,
        right,
        on,
        false,
        residual,
        null_aware,
        left_schema,
        right_schema,
        join_type,
        schema,
        context,
        batch_size,
    )
}

enum PartitionHashBuild {
    Ready(
        RecordBatch,
        std::collections::HashMap<Vec<CellValue>, Vec<u32>>,
        Option<EvaluatedKeys>,
        Option<BuildMatchTracker>,
    ),
    TooLarge(usize),
}

fn evaluate_keys(expressions: &[BoundExpr], batch: &RecordBatch) -> Result<Vec<ArrayRef>> {
    expressions
        .iter()
        .map(|expression| evaluate(expression, batch))
        .collect()
}

pub(super) struct EvaluatedKeys {
    arrays: Vec<ArrayRef>,
    _memory: crate::runtime::MemoryReservation,
}

impl Deref for EvaluatedKeys {
    type Target = [ArrayRef];

    fn deref(&self) -> &Self::Target {
        &self.arrays
    }
}

impl EvaluatedKeys {
    pub(super) fn memory_size(&self) -> usize {
        self._memory.size()
    }
}

fn evaluate_optional_values(
    expression: Option<&BoundExpr>,
    batch: &RecordBatch,
    context: &QueryContext,
    owner: &'static str,
) -> Result<Option<EvaluatedKeys>> {
    expression
        .map(|expression| {
            evaluate_keys_accounted(std::slice::from_ref(expression), batch, context, owner)
        })
        .transpose()
}

fn optional_array(values: &Option<EvaluatedKeys>) -> Option<&ArrayRef> {
    values.as_ref().map(|values| &values[0])
}

fn optional_memory(values: &Option<EvaluatedKeys>) -> usize {
    values.as_ref().map(EvaluatedKeys::memory_size).unwrap_or(0)
}

fn can_deduplicate_build(join_type: JoinType, predicates: &JoinPredicates) -> bool {
    predicates.residual().is_none()
        && !predicates.is_null_aware()
        && matches!(join_type, JoinType::Semi | JoinType::Anti | JoinType::Mark)
}

fn tracks_build_matches(join_type: JoinType) -> bool {
    matches!(join_type, JoinType::Right | JoinType::Full)
}

fn try_build_match_tracker(
    join_type: JoinType,
    rows: usize,
    reservation: &mut crate::runtime::MemoryReservation,
) -> Option<Option<BuildMatchTracker>> {
    if tracks_build_matches(join_type) {
        BuildMatchTracker::try_new(rows, reservation).map(Some)
    } else {
        Some(None)
    }
}

fn spill_build_batch(
    batch: RecordBatch,
    keys: &[BoundExpr],
    join_type: JoinType,
    null_equal_keys: bool,
    context: &QueryContext,
    reservation: &mut crate::runtime::MemoryReservation,
) -> Result<spill::PartitionManifest> {
    reservation.try_resize(batch.get_array_memory_size())?;
    let footprint = spill::estimated_build_footprint(
        u64::try_from(spill::batch_logical_buffer_bytes(&batch)).unwrap_or(u64::MAX),
        u64::try_from(batch.num_rows()).unwrap_or(u64::MAX),
        keys.len(),
    );
    let partitions =
        spill::adaptive_partition_count(context, usize::try_from(footprint).unwrap_or(usize::MAX));
    let mut spiller = spill::PartitionSpiller::with_partitions(context, "join-right", partitions);
    spill::spill_batch_with_null_keys(
        batch,
        keys,
        Side::Right,
        join_type,
        null_equal_keys,
        &mut spiller,
        0,
    )?;
    reservation.try_resize(0)?;
    spiller.finish_manifest()
}

pub(super) fn evaluate_keys_accounted(
    expressions: &[BoundExpr],
    batch: &RecordBatch,
    context: &QueryContext,
    owner: &'static str,
) -> Result<EvaluatedKeys> {
    let estimate = expressions
        .iter()
        .map(|expression| {
            if matches!(&expression.kind, crate::sql::ExprKind::Column(_)) {
                std::mem::size_of::<ArrayRef>().saturating_add(256)
            } else {
                super::expr::projection_workspace_bytes(std::slice::from_ref(expression), batch)
            }
        })
        .fold(1usize, usize::saturating_add);
    let mut memory = context.memory.try_reserve(estimate).map_err(|error| {
        crate::Error::ResourceExhausted(format!(
            "{owner} require up to {estimate} bytes of expression workspace: {error}"
        ))
    })?;
    let arrays = evaluate_keys(expressions, batch)?;
    let actual = arrays
        .iter()
        .filter(|array| {
            !batch
                .columns()
                .iter()
                .any(|column| Arc::ptr_eq(column, array))
        })
        .map(|array| array.get_array_memory_size())
        .fold(0usize, usize::saturating_add)
        .saturating_add(
            arrays
                .capacity()
                .saturating_mul(std::mem::size_of::<ArrayRef>()),
        )
        .saturating_add(256)
        .max(1);
    memory.try_resize(actual).map_err(|error| {
        crate::Error::ResourceExhausted(format!(
            "{owner} retain {actual} bytes of evaluated keys: {error}"
        ))
    })?;
    context.metrics.observe_memory(context.memory.used());
    Ok(EvaluatedKeys {
        arrays,
        _memory: memory,
    })
}

fn row_key(arrays: &[ArrayRef], row: usize) -> Result<Vec<CellValue>> {
    arrays.iter().map(|array| cell(array, row)).collect()
}
