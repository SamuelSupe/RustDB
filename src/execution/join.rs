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
    sql::{BoundExpr, JoinType},
};

use super::{
    expr::evaluate,
    value::{CellValue, cell},
};

mod grace;
mod parallel;
mod probe;
mod sort_merge;
mod spill;

#[cfg(test)]
mod tests;

use probe::{ProbeCursor, try_build_hash_table};
use spill::{BuildPartition, MAX_REPARTITION_DEPTH, Side};

// The physical join boundary carries both input schemas, output schema, keys,
// execution state, and sizing. Keeping this explicit avoids a public options
// abstraction for a single internal call site.
#[allow(clippy::too_many_arguments)]
pub(crate) fn join<L, R>(
    left: L,
    right: R,
    on: Vec<(BoundExpr, BoundExpr)>,
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
    let mut left = left.into_memory_batch_stream(Arc::clone(&context), "join left input");
    let mut right = right.into_memory_batch_stream(Arc::clone(&context), "join right input");
    boxed_memory_batch_stream(async_stream::try_stream! {
        let left_key_expressions = on.iter().map(|(left, _)| left.clone()).collect::<Vec<_>>();
        let right_key_expressions = on.iter().map(|(_, right)| right.clone()).collect::<Vec<_>>();
        let mut reservation = context.memory.reservation();
        let mut right_batches: Vec<RecordBatch> = Vec::new();
        let mut right_bytes = 0usize;
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
            // Reserve the future concat buffer while the source envelope still
            // accounts for the retained input batch.
            if right_bytes.saturating_add(bytes) > build_buffer_limit
                || reservation.try_grow(bytes).is_err()
            {
                reservation.shrink(right_bytes);
                let mut spiller = spill::PartitionSpiller::new(&context, "join-right");
                for buffered in right_batches.drain(..) {
                    let buffered_bytes = buffered.get_array_memory_size();
                    spill::spill_batch(
                        buffered,
                        &right_key_expressions,
                        Side::Right,
                        join_type,
                        &mut spiller,
                        0,
                    )?;
                    reservation.shrink(buffered_bytes);
                }
                let (batch, batch_memory) = batch.into_parts();
                spill::spill_batch(
                    batch,
                    &right_key_expressions,
                    Side::Right,
                    join_type,
                    &mut spiller,
                    0,
                )?;
                drop(batch_memory);
                while let Some(batch) = right.next().await {
                    let (batch, batch_memory) = batch?.into_parts();
                    spill::spill_batch(
                        batch,
                        &right_key_expressions,
                        Side::Right,
                        join_type,
                        &mut spiller,
                        0,
                    )?;
                    drop(batch_memory);
                }
                right_partitions = Some(spiller.finish()?);
                break;
            }
            let (batch, batch_memory) = batch.into_parts();
            reservation.absorb(batch_memory)?;
            right_bytes = right_bytes.saturating_add(bytes);
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
            let right_keys = evaluate_keys_accounted(
                &right_key_expressions,
                &right_batch,
                &context,
                "join build keys",
            )?;
            let hash_table = try_build_hash_table(
                &right_keys,
                right_batch.num_rows(),
                matches!(join_type, JoinType::Semi | JoinType::Anti),
                &mut reservation,
            )?;
            drop(right_keys);
            match hash_table {
                Some(hash_table) => in_memory_build = Some((right_batch, hash_table)),
                None => {
                let mut spiller = spill::PartitionSpiller::new(&context, "join-right");
                    spill::spill_batch(
                        right_batch,
                        &right_key_expressions,
                        Side::Right,
                        join_type,
                        &mut spiller,
                        0,
                    )?;
                    reservation.try_resize(0)?;
                    right_partitions = Some(spiller.finish()?);
                }
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
            if grace::is_supported(&context, initial.len()) {
                let mut output = grace::join(
                    initial,
                    left_key_expressions.clone(),
                    right_key_expressions.clone(),
                    Arc::clone(&left_schema),
                    Arc::clone(&right_schema),
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
            let mut pending = initial.into_iter().rev().collect::<Vec<_>>();
            while let Some(task) = pending.pop() {
                context.check_cancelled()?;
                let build = match spill::load_build_partition(
                    &task.right,
                    &right_schema,
                    &context,
                    &mut reservation,
                )? {
                    BuildPartition::Loaded(right_batch) => {
                        let right_keys = evaluate_keys_accounted(
                            &right_key_expressions,
                            &right_batch,
                            &context,
                            "join spill build keys",
                        )?;
                        let rows = right_batch.num_rows();
                        let hash_table = try_build_hash_table(
                            &right_keys,
                            rows,
                            matches!(join_type, JoinType::Semi | JoinType::Anti),
                            &mut reservation,
                        )?;
                        drop(right_keys);
                        match hash_table {
                            Some(hash_table) => PartitionHashBuild::Ready(right_batch, hash_table),
                            None => PartitionHashBuild::TooLarge(rows),
                        }
                    }
                    BuildPartition::TooLarge { rows } => PartitionHashBuild::TooLarge(rows),
                };
                match build {
                    PartitionHashBuild::Ready(right_batch, hash_table) => {
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
                                let mut probe = ProbeCursor::new(
                                    left_batch.batch(),
                                    &right_batch,
                                    &left_keys,
                                    &hash_table,
                                    join_type,
                                    Arc::clone(&schema),
                                    batch_size,
                                    reservation
                                        .size()
                                        .saturating_add(left_batch.memory_size())
                                        .saturating_add(left_keys.memory_size()),
                                );
                                while let Some(output) = probe.next_batch(&context).await? {
                                    yield output;
                                }
                            }
                        }
                        spill::remove_task(&context, &task)?;
                        reservation.try_resize(0)?;
                    }
                    PartitionHashBuild::TooLarge(rows) => {
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
                            let shrank = repartitioned.largest_build_rows < rows;
                            if shrank || task.stagnant_repartitions == 0 {
                                spill::remove_task(&context, &task)?;
                                let stagnant = if shrank {
                                    0
                                } else {
                                    task.stagnant_repartitions + 1
                                };
                                for mut child in repartitioned.tasks.into_iter().rev() {
                                    child.stagnant_repartitions = stagnant;
                                    pending.push(child);
                                }
                                continue;
                            }
                            spill::remove_tasks(&context, &repartitioned.tasks)?;
                        }

                        let mut fallback = sort_merge::fallback(
                            task,
                            left_key_expressions.clone(),
                            right_key_expressions.clone(),
                            Arc::clone(&left_schema),
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

        let (right_batch, hash_table) = in_memory_build
            .take()
            .expect("a non-spilling join has an in-memory build");
        if parallel::is_supported(&context, reservation.size()) {
            let build = parallel::FrozenBuild::new(right_batch, hash_table, reservation);
            let mut output = parallel::probe(
                left,
                left_key_expressions,
                build,
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
        while let Some(batch) = left.next().await {
            context.check_cancelled()?;
            let left_batch = batch?;
            let left_keys = evaluate_keys_accounted(
                &left_key_expressions,
                left_batch.batch(),
                &context,
                "join probe keys",
            )?;
            let mut probe = ProbeCursor::new(
                left_batch.batch(),
                &right_batch,
                &left_keys,
                &hash_table,
                join_type,
                Arc::clone(&schema),
                batch_size,
                reservation
                    .size()
                    .saturating_add(left_batch.memory_size())
                    .saturating_add(left_keys.memory_size()),
            );
            while let Some(output) = probe.next_batch(&context).await? {
                yield output;
            }
        }
        let _ = left_schema;
    })
}

enum PartitionHashBuild {
    Ready(
        RecordBatch,
        std::collections::HashMap<Vec<CellValue>, Vec<u32>>,
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

pub(super) fn evaluate_keys_accounted(
    expressions: &[BoundExpr],
    batch: &RecordBatch,
    context: &QueryContext,
    owner: &'static str,
) -> Result<EvaluatedKeys> {
    let estimate = batch
        .get_array_memory_size()
        .saturating_mul(expressions.len().max(1))
        .max(1);
    let mut memory = context.memory.try_reserve(estimate).map_err(|error| {
        crate::Error::ResourceExhausted(format!(
            "{owner} require up to {estimate} bytes of expression workspace: {error}"
        ))
    })?;
    let arrays = evaluate_keys(expressions, batch)?;
    let actual = arrays
        .iter()
        .map(|array| array.get_array_memory_size())
        .fold(0usize, usize::saturating_add)
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
