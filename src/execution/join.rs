use std::sync::Arc;

use arrow::{
    array::ArrayRef, compute::concat_batches, datatypes::SchemaRef, record_batch::RecordBatch,
};
use futures::StreamExt;

use crate::{
    Result,
    runtime::{QueryContext, RecordBatchStream, boxed_record_batch_stream},
    sql::{BoundExpr, JoinType},
};

use super::{
    expr::evaluate,
    value::{CellValue, cell},
};

mod probe;
mod skew;
mod spill;

#[cfg(test)]
mod tests;

use probe::{ProbeCursor, build_hash_table, build_output};
use spill::{BuildPartition, MAX_REPARTITION_DEPTH, Side};

// The physical join boundary carries both input schemas, output schema, keys,
// execution state, and sizing. Keeping this explicit avoids a public options
// abstraction for a single internal call site.
#[allow(clippy::too_many_arguments)]
pub(crate) fn join(
    mut left: RecordBatchStream,
    mut right: RecordBatchStream,
    on: Vec<(BoundExpr, BoundExpr)>,
    left_schema: SchemaRef,
    right_schema: SchemaRef,
    join_type: JoinType,
    schema: SchemaRef,
    context: Arc<QueryContext>,
    batch_size: usize,
) -> RecordBatchStream {
    boxed_record_batch_stream(async_stream::try_stream! {
        let left_key_expressions = on.iter().map(|(left, _)| left.clone()).collect::<Vec<_>>();
        let right_key_expressions = on.iter().map(|(_, right)| right.clone()).collect::<Vec<_>>();
        let mut reservation = context.memory.reservation();
        let mut right_batches = Vec::new();
        let mut right_bytes = 0usize;
        let mut right_rows = 0usize;
        let mut right_partitions = None;

        while let Some(batch) = right.next().await {
            context.check_cancelled()?;
            let batch = batch?;
            let bytes = batch.get_array_memory_size();
            if reservation.try_grow(bytes).is_err() {
                reservation.try_resize(0)?;
                let mut spiller = spill::PartitionSpiller::new(&context, "join-right");
                for buffered in right_batches.drain(..) {
                    spill::spill_batch(
                        buffered,
                        &right_key_expressions,
                        Side::Right,
                        join_type,
                        &mut spiller,
                        0,
                    )?;
                }
                spill::spill_batch(
                    batch,
                    &right_key_expressions,
                    Side::Right,
                    join_type,
                    &mut spiller,
                    0,
                )?;
                while let Some(batch) = right.next().await {
                    spill::spill_batch(
                        batch?,
                        &right_key_expressions,
                        Side::Right,
                        join_type,
                        &mut spiller,
                        0,
                    )?;
                }
                right_partitions = Some(spiller.finish()?);
                break;
            }
            right_bytes = right_bytes.saturating_add(bytes);
            right_rows = right_rows.saturating_add(batch.num_rows());
            right_batches.push(batch);
        }

        if right_partitions.is_none() {
            let build_overhead = right_rows.saturating_mul(128);
            if reservation
                .try_grow(right_bytes.saturating_add(build_overhead))
                .is_err()
            {
                reservation.try_resize(0)?;
                let mut spiller = spill::PartitionSpiller::new(&context, "join-right");
                for batch in right_batches.drain(..) {
                    spill::spill_batch(
                        batch,
                        &right_key_expressions,
                        Side::Right,
                        join_type,
                        &mut spiller,
                        0,
                    )?;
                }
                right_partitions = Some(spiller.finish()?);
            }
        }

        if let Some(right_partitions) = right_partitions {
            let left_partitions = spill::spill_stream(
                &mut left,
                &left_key_expressions,
                Side::Left,
                join_type,
                &context,
                "join-left",
            ).await?;
            let initial = spill::initial_tasks(left_partitions, right_partitions);
            context.metrics.record_spill(
                0,
                u64::try_from(initial.len()).unwrap_or(u64::MAX),
            );
            let mut pending = initial.into_iter().rev().collect::<Vec<_>>();
            while let Some(task) = pending.pop() {
                context.check_cancelled()?;
                match spill::load_build_partition(
                    &task.right,
                    &right_schema,
                    &context,
                    &mut reservation,
                )? {
                    BuildPartition::Loaded(right_batch) => {
                        let right_keys = evaluate_keys(&right_key_expressions, &right_batch)?;
                        let hash_table = build_hash_table(
                            &right_keys,
                            right_batch.num_rows(),
                            matches!(join_type, JoinType::Semi | JoinType::Anti),
                        )?;
                        for file in &task.left {
                            for left_batch in context.spill.read_file(file)? {
                                let left_batch = left_batch?;
                                let left_keys = evaluate_keys(&left_key_expressions, &left_batch)?;
                                let mut probe = ProbeCursor::new(
                                    &left_batch,
                                    &right_batch,
                                    &left_keys,
                                    &hash_table,
                                    join_type,
                                    Arc::clone(&schema),
                                    batch_size,
                                );
                                while let Some(output) = probe.next_batch(&context)? {
                                    yield output;
                                }
                            }
                        }
                        spill::remove_task(&context, &task);
                        reservation.try_resize(0)?;
                    }
                    BuildPartition::TooLarge { rows } => {
                        reservation.try_resize(0)?;
                        if task.depth < MAX_REPARTITION_DEPTH {
                            let next_depth = task.depth + 1;
                            let repartitioned = spill::repartition(
                                &task,
                                &left_key_expressions,
                                &right_key_expressions,
                                join_type,
                                next_depth,
                                &context,
                            )?;
                            if repartitioned.largest_build_rows < rows {
                                spill::remove_task(&context, &task);
                                for child in repartitioned.tasks.into_iter().rev() {
                                    pending.push(child);
                                }
                                continue;
                            }
                            spill::remove_tasks(&context, &repartitioned.tasks);
                        }

                        let mut fallback = skew::fallback(
                            task,
                            left_key_expressions.clone(),
                            right_key_expressions.clone(),
                            Arc::clone(&right_schema),
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

        let right_batch = if right_batches.is_empty() {
            RecordBatch::new_empty(Arc::clone(&right_schema))
        } else {
            concat_batches(&right_schema, &right_batches)?
        };
        drop(right_batches);
        reservation.shrink(right_bytes);
        let right_keys = evaluate_keys(&right_key_expressions, &right_batch)?;
        let hash_table = build_hash_table(
            &right_keys,
            right_batch.num_rows(),
            matches!(join_type, JoinType::Semi | JoinType::Anti),
        )?;
        while let Some(batch) = left.next().await {
            context.check_cancelled()?;
            let left_batch = batch?;
            let left_keys = evaluate_keys(&left_key_expressions, &left_batch)?;
            let mut probe = ProbeCursor::new(
                &left_batch,
                &right_batch,
                &left_keys,
                &hash_table,
                join_type,
                Arc::clone(&schema),
                batch_size,
            );
            while let Some(output) = probe.next_batch(&context)? {
                yield output;
            }
        }
        let _ = left_schema;
    })
}

fn evaluate_keys(expressions: &[BoundExpr], batch: &RecordBatch) -> Result<Vec<ArrayRef>> {
    expressions
        .iter()
        .map(|expression| evaluate(expression, batch))
        .collect()
}

fn row_key(arrays: &[ArrayRef], row: usize) -> Result<Vec<CellValue>> {
    arrays.iter().map(|array| cell(array, row)).collect()
}
