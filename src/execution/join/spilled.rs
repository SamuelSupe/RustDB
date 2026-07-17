use std::sync::Arc;

use arrow::{datatypes::SchemaRef, record_batch::RecordBatch};
use futures::StreamExt;

use crate::{
    Result,
    runtime::{
        BatchEnvelope, MemoryBatchStream, MemoryReservation, QueryContext,
        boxed_memory_batch_stream,
    },
    sql::{BoundExpr, JoinType},
};

use super::{
    EvaluatedKeys, JoinHashTable, ProbeCursor,
    condition::JoinPredicates,
    evaluate_keys_accounted, evaluate_optional_values, grace, optional_array, optional_memory,
    output::{BatchOutputTarget, JoinEmission, build_unmatched_right_envelope},
    sort_merge,
    spill::{self, BuildPartition, PartitionManifest, Side},
    try_build_existence_hash_table_with_nulls, try_build_hash_table_with_nulls,
    try_build_match_tracker,
};

#[allow(clippy::too_many_arguments)]
pub(super) fn execute(
    mut left: MemoryBatchStream,
    right_partitions: PartitionManifest,
    left_key_expressions: Vec<BoundExpr>,
    right_key_expressions: Vec<BoundExpr>,
    left_schema: SchemaRef,
    right_schema: SchemaRef,
    predicates: JoinPredicates,
    null_equal_keys: bool,
    join_type: JoinType,
    schema: SchemaRef,
    context: Arc<QueryContext>,
    batch_size: usize,
    mut reservation: MemoryReservation,
) -> MemoryBatchStream {
    boxed_memory_batch_stream(async_stream::try_stream! {
        let left_partitions = spill::spill_stream(
            &mut left,
            &left_key_expressions,
            Side::Left,
            join_type,
            null_equal_keys,
            &context,
            "join-left",
            right_partitions.len(),
        )
        .await?;
        let initial = spill::initial_tasks(left_partitions, right_partitions);
        context
            .metrics
            .record_spill(0, u64::try_from(initial.len()).unwrap_or(u64::MAX));
        if grace::is_supported(&context, &initial) {
            let mut output = grace::join(
                initial,
                left_key_expressions,
                right_key_expressions,
                left_schema,
                right_schema,
                predicates,
                null_equal_keys,
                join_type,
                schema,
                context,
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
            let build = load_hash_build(
                &task,
                &right_key_expressions,
                &left_schema,
                &right_schema,
                &predicates,
                null_equal_keys,
                join_type,
                &context,
                &mut reservation,
            )?;
            match build {
                PartitionHashBuild::Ready(
                    right_batch,
                    hash_table,
                    right_values,
                    matched_build,
                ) => {
                    for file in &task.left {
                        for left_batch in context.spill.read_file(file)? {
                            let left_batch = BatchEnvelope::try_new(
                                left_batch?,
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
                            let mut target = BatchOutputTarget;
                            loop {
                                match probe.next_output(&mut target, &context).await? {
                                    JoinEmission::Batch(output) => yield output,
                                    JoinEmission::Consumed { .. } => {}
                                    JoinEmission::Exhausted => break,
                                }
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
                            let Some(last) = indices.last().copied() else {
                                break;
                            };
                            start = last as usize + 1;
                            yield build_unmatched_right_envelope(
                                &left_schema,
                                &right_batch,
                                &indices,
                                Arc::clone(&schema),
                                &context,
                                reservation.size().saturating_add(indices.memory_size()),
                            )
                            .await?;
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
                        if let Some(repartitioned) = repartitioned {
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
    })
}

#[allow(clippy::too_many_arguments)]
fn load_hash_build(
    task: &spill::PartitionTask,
    right_key_expressions: &[BoundExpr],
    left_schema: &SchemaRef,
    right_schema: &SchemaRef,
    predicates: &JoinPredicates,
    null_equal_keys: bool,
    join_type: JoinType,
    context: &QueryContext,
    reservation: &mut MemoryReservation,
) -> Result<PartitionHashBuild> {
    let right_batch = match spill::load_build_partition(
        &task.right,
        right_schema,
        context,
        reservation,
        task.build,
    )? {
        BuildPartition::Loaded(batch) => batch,
        BuildPartition::TooLarge { rows } => return Ok(PartitionHashBuild::TooLarge(rows)),
    };
    let right_keys = evaluate_keys_accounted(
        right_key_expressions,
        &right_batch,
        context,
        "join spill build keys",
    )?;
    let rows = right_batch.num_rows();
    let inequality_value =
        predicates.existence_inequality_right_value(join_type, left_schema.fields().len());
    let inequality_values = evaluate_optional_values(
        inequality_value.as_ref(),
        &right_batch,
        context,
        "join spill existence inequality value",
    )?;
    let hash_table = if let Some(values) = optional_array(&inequality_values) {
        try_build_existence_hash_table_with_nulls(
            &right_keys,
            rows,
            null_equal_keys,
            values,
            reservation,
        )?
    } else {
        try_build_hash_table_with_nulls(
            &right_keys,
            rows,
            super::can_deduplicate_build(join_type, predicates),
            null_equal_keys,
            reservation,
        )?
    };
    drop(inequality_values);
    drop(right_keys);
    let right_values = evaluate_optional_values(
        predicates.right_value(),
        &right_batch,
        context,
        "join spill build membership value",
    )?;
    match hash_table {
        Some(hash_table)
            if let Some(matched_build) = try_build_match_tracker(join_type, rows, reservation) =>
        {
            Ok(PartitionHashBuild::Ready(
                right_batch,
                hash_table,
                right_values,
                matched_build,
            ))
        }
        Some(hash_table) => {
            drop(hash_table);
            drop(right_values);
            Ok(PartitionHashBuild::TooLarge(rows))
        }
        None => {
            drop(right_values);
            Ok(PartitionHashBuild::TooLarge(rows))
        }
    }
}

// A successfully loaded partition is consumed immediately, so keep its hash
// table inline instead of adding a heap allocation to the probe hot path.
#[allow(clippy::large_enum_variant)]
enum PartitionHashBuild {
    Ready(
        RecordBatch,
        JoinHashTable,
        Option<EvaluatedKeys>,
        Option<super::BuildMatchTracker>,
    ),
    TooLarge(usize),
}
