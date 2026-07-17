use std::{ops::Deref, sync::Arc};

use arrow::{array::ArrayRef, datatypes::SchemaRef, record_batch::RecordBatch};
use futures::StreamExt;

use crate::{
    Result,
    runtime::{IntoMemoryBatchStream, MemoryBatchStream, QueryContext, boxed_memory_batch_stream},
    sql::{BoundExpr, ExprKind, JoinType, ScalarValue},
};

use super::{
    expr::evaluate,
    runtime_filter::RuntimeFilterSlot,
    value::{CellValue, cell},
};

mod aggregate_output;
mod build;
mod condition;
mod grace;
mod hash_table;
mod matched;
mod metrics;
mod multiplicity;
pub(in crate::execution) mod output;
mod parallel;
mod probe;
mod sort_merge;
mod spill;
mod spilled;

pub(in crate::execution) use aggregate_output::join_global_aggregate;

#[cfg(test)]
mod binary_tests;
#[cfg(test)]
mod correlation_tests;
#[cfg(test)]
mod global_membership_tests;
#[cfg(test)]
mod tests;
#[cfg(test)]
mod tracker_fallback_tests;

use condition::JoinPredicates;
use hash_table::JoinHashTable;
use matched::BuildMatchTracker;
use metrics::JoinPhaseMetrics;
use output::{BatchOutputTarget, JoinEmission, build_unmatched_right_envelope};
#[cfg(test)]
use probe::try_build_hash_table;
use probe::{
    GlobalMembershipState, ProbeCursor, try_build_existence_hash_table_with_nulls,
    try_build_hash_table_with_nulls,
};

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
    join_operator_id: Option<u64>,
) -> MemoryBatchStream
where
    L: IntoMemoryBatchStream,
    R: IntoMemoryBatchStream,
{
    let phases = JoinPhaseMetrics::register(&context, join_operator_id);
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
        let build_phase = phases.start_build();
        let mut reservation = context.memory.reservation();
        let outcome = build::build(
            &mut right,
            &right_key_expressions,
            &predicates,
            use_global_membership_hash,
            left_schema.fields().len(),
            &right_schema,
            join_type,
            null_equal_keys,
            &context,
            &mut reservation,
        ).await?;
        let (right_partitions, mut in_memory_build) = match outcome {
            build::BuildOutcome::InMemory(build) => (None, Some(build)),
            build::BuildOutcome::Spilled(partitions) => (Some(partitions), None),
        };

        if let Some(runtime_filter) = &runtime_filter {
            if let Some(build) = &in_memory_build {
                let _permit = context.acquire_compute().await?;
                let _active = context.scheduler.enter_lane();
                let hash_table = build.hash_table();
                if let Some(values) = hash_table.generic_values() {
                    runtime_filter.publish_hash(values, &context);
                } else if let Some(keys) = hash_table.fixed_keys() {
                    runtime_filter.publish_fixed_keys(keys, &context);
                } else if let Some(keys) = hash_table.utf8_keys() {
                    runtime_filter.publish_utf8_keys(keys, &context);
                } else if let Some(keys) = hash_table.binary_keys() {
                    runtime_filter.publish_binary_keys(keys, &context);
                } else {
                    runtime_filter.publish_none();
                }
            } else {
                runtime_filter.publish_none();
            }
        }
        if let Some(right_partitions) = right_partitions {
            drop(build_phase);
            let _spill_phase = phases.start_spill(&context);
            let mut output = spilled::execute(
                left,
                right_partitions,
                left_key_expressions,
                right_key_expressions,
                Arc::clone(&left_schema),
                Arc::clone(&right_schema),
                predicates,
                null_equal_keys,
                join_type,
                Arc::clone(&schema),
                Arc::clone(&context),
                batch_size,
                reservation,
            );
            while let Some(batch) = output.next().await {
                yield batch?;
            }
            return;
        }

        drop(build_phase);
        let _probe_phase = phases.start_probe(&context);

        let (right_batch, hash_table, right_values, global_membership, matched_build) = in_memory_build
            .take()
            .expect("a non-spilling join has an in-memory build")
            .into_parts();
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
            let (left_values, left_keys) = {
                let _permit = context.acquire_compute().await?;
                let _active = context.scheduler.enter_lane();
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
                (left_values, left_keys)
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
            let mut target = BatchOutputTarget;
            loop {
                match probe.next_output(&mut target, &context).await? {
                    JoinEmission::Batch(output) => yield output,
                    JoinEmission::Consumed { .. } => {}
                    JoinEmission::Exhausted => break,
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
