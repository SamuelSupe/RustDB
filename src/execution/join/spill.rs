use std::{
    collections::hash_map::DefaultHasher,
    hash::{Hash, Hasher},
    sync::Arc,
};

use arrow::{
    array::UInt32Array, compute::take_record_batch, datatypes::SchemaRef, record_batch::RecordBatch,
};
use futures::StreamExt;

use crate::{
    Error, Result,
    runtime::{MemoryReservation, QueryContext, RecordBatchStream, SpillFile},
    sql::{BoundExpr, JoinType},
};

use super::{evaluate_keys, row_key};

pub(super) const PARTITIONS: usize = 64;
pub(super) const MAX_REPARTITION_DEPTH: usize = 4;
const SEED_STEP: u64 = 0x9e37_79b9_7f4a_7c15;

#[derive(Clone, Copy)]
pub(super) enum Side {
    Left,
    Right,
}

pub(super) struct PartitionTask {
    pub(super) left: Vec<SpillFile>,
    pub(super) right: Vec<SpillFile>,
    pub(super) depth: usize,
}

impl PartitionTask {
    fn new(left: Vec<SpillFile>, right: Vec<SpillFile>, depth: usize) -> Self {
        Self { left, right, depth }
    }
}

pub(super) enum BuildPartition {
    Loaded(RecordBatch),
    TooLarge { rows: usize },
}

pub(super) struct Repartitioned {
    pub(super) tasks: Vec<PartitionTask>,
    pub(super) largest_build_rows: usize,
}

pub(super) fn initial_tasks(
    left: Vec<Vec<SpillFile>>,
    right: Vec<Vec<SpillFile>>,
) -> Vec<PartitionTask> {
    left.into_iter()
        .zip(right)
        .filter(|(left, right)| !left.is_empty() || !right.is_empty())
        .map(|(left, right)| PartitionTask::new(left, right, 0))
        .collect()
}

pub(super) fn empty_partitions() -> Vec<Vec<SpillFile>> {
    (0..PARTITIONS).map(|_| Vec::new()).collect()
}

pub(super) async fn spill_stream(
    stream: &mut RecordBatchStream,
    expressions: &[BoundExpr],
    side: Side,
    join_type: JoinType,
    context: &QueryContext,
    label: &str,
) -> Result<Vec<Vec<SpillFile>>> {
    let mut partitions = empty_partitions();
    while let Some(batch) = stream.next().await {
        context.check_cancelled()?;
        spill_batch(
            batch?,
            expressions,
            side,
            join_type,
            &mut partitions,
            context,
            label,
            0,
        )?;
    }
    Ok(partitions)
}

#[allow(clippy::too_many_arguments)]
pub(super) fn spill_batch(
    batch: RecordBatch,
    expressions: &[BoundExpr],
    side: Side,
    join_type: JoinType,
    partitions: &mut [Vec<SpillFile>],
    context: &QueryContext,
    label: &str,
    seed: u64,
) -> Result<Vec<usize>> {
    let keys = evaluate_keys(expressions, &batch)?;
    let mut indices: Vec<Vec<u32>> = (0..PARTITIONS).map(|_| Vec::new()).collect();
    for row in 0..batch.num_rows() {
        let key = row_key(&keys, row)?;
        let partition = if key.iter().any(super::CellValue::is_null) {
            match side {
                Side::Right => continue,
                Side::Left if matches!(join_type, JoinType::Inner | JoinType::Semi) => continue,
                Side::Left => 0,
            }
        } else {
            partition_for_key(&key, seed)
        };
        indices[partition].push(u32::try_from(row).map_err(|_| {
            Error::ResourceExhausted("join spill batch exceeds UINT32_MAX rows".into())
        })?);
    }

    let rows_per_file = spill_chunk_rows(&batch, context);
    let counts = indices.iter().map(Vec::len).collect::<Vec<_>>();
    for (partition, indices) in indices.into_iter().enumerate() {
        for chunk in indices.chunks(rows_per_file) {
            let indices = UInt32Array::from(chunk.to_vec());
            let partition_batch = take_record_batch(&batch, &indices)?;
            let file = context.spill.write_record_batches(
                &format!("{label}-p{partition}"),
                partition_batch.schema(),
                [partition_batch],
            )?;
            partitions[partition].push(file);
        }
    }
    Ok(counts)
}

pub(super) fn load_build_partition(
    files: &[SpillFile],
    schema: &SchemaRef,
    context: &QueryContext,
    reservation: &mut MemoryReservation,
) -> Result<BuildPartition> {
    let mut bytes = 0usize;
    let mut rows = 0usize;
    for file in files {
        for batch in context.spill.read_batches(file)? {
            bytes = bytes.saturating_add(batch.get_array_memory_size());
            rows = rows.saturating_add(batch.num_rows());
        }
    }
    let required = estimated_build_bytes(bytes, rows);
    if reservation.try_resize(required).is_err() {
        return Ok(BuildPartition::TooLarge { rows });
    }

    let mut batches = Vec::new();
    for file in files {
        batches.extend(context.spill.read_batches(file)?);
    }
    if batches.is_empty() {
        Ok(BuildPartition::Loaded(RecordBatch::new_empty(Arc::clone(
            schema,
        ))))
    } else {
        Ok(BuildPartition::Loaded(arrow::compute::concat_batches(
            schema, &batches,
        )?))
    }
}

pub(super) fn repartition(
    task: &PartitionTask,
    left_expressions: &[BoundExpr],
    right_expressions: &[BoundExpr],
    join_type: JoinType,
    next_depth: usize,
    context: &QueryContext,
) -> Result<Repartitioned> {
    let seed = seed_for_depth(next_depth);
    let mut left = empty_partitions();
    let mut right = empty_partitions();
    let mut right_rows = vec![0usize; PARTITIONS];

    for file in &task.left {
        for batch in context.spill.read_batches(file)? {
            spill_batch(
                batch,
                left_expressions,
                Side::Left,
                join_type,
                &mut left,
                context,
                &format!("join-left-r{next_depth}"),
                seed,
            )?;
        }
    }
    for file in &task.right {
        for batch in context.spill.read_batches(file)? {
            let counts = spill_batch(
                batch,
                right_expressions,
                Side::Right,
                join_type,
                &mut right,
                context,
                &format!("join-right-r{next_depth}"),
                seed,
            )?;
            for (total, count) in right_rows.iter_mut().zip(counts) {
                *total = total.saturating_add(count);
            }
        }
    }

    let largest_build_rows = right_rows.into_iter().max().unwrap_or(0);
    let tasks = left
        .into_iter()
        .zip(right)
        .filter(|(left, right)| !left.is_empty() || !right.is_empty())
        .map(|(left, right)| PartitionTask::new(left, right, next_depth))
        .collect();
    Ok(Repartitioned {
        tasks,
        largest_build_rows,
    })
}

pub(super) fn remove_task(context: &QueryContext, task: &PartitionTask) {
    remove_files(context, &task.left);
    remove_files(context, &task.right);
}

pub(super) fn remove_tasks(context: &QueryContext, tasks: &[PartitionTask]) {
    for task in tasks {
        remove_task(context, task);
    }
}

pub(super) fn remove_files(context: &QueryContext, files: &[SpillFile]) {
    for file in files {
        context.spill.remove_file(file);
    }
}

pub(super) fn partition_for_key(key: &[super::CellValue], seed: u64) -> usize {
    let mut hasher = DefaultHasher::new();
    seed.hash(&mut hasher);
    key.hash(&mut hasher);
    (hasher.finish() as usize) % PARTITIONS
}

fn seed_for_depth(depth: usize) -> u64 {
    SEED_STEP.wrapping_mul(depth as u64)
}

fn spill_chunk_rows(batch: &RecordBatch, context: &QueryContext) -> usize {
    if batch.num_rows() == 0 {
        return 1;
    }
    let bytes_per_row = batch
        .get_array_memory_size()
        .div_ceil(batch.num_rows())
        .max(1);
    let estimated_per_row = bytes_per_row.saturating_mul(2).saturating_add(256);
    let budget = (context.memory.limit() / 4).max(estimated_per_row);
    (budget / estimated_per_row)
        .max(1)
        .min(context.batch_size.max(1))
}

fn estimated_build_bytes(bytes: usize, rows: usize) -> usize {
    bytes
        .saturating_mul(2)
        .saturating_add(rows.saturating_mul(128))
}
