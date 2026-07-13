use std::{
    collections::{HashMap, hash_map::DefaultHasher},
    hash::{Hash, Hasher},
    mem::size_of,
};

use arrow::record_batch::RecordBatch;

use crate::{
    Error, Result,
    runtime::{MemoryReservation, QueryContext, SpillFile},
    sql::{AggregateExpr, BoundExpr},
};

use super::{GroupState, estimate_group_bytes};

mod repartition;
mod write;

pub(super) use repartition::repartition_partition;
pub(super) use write::{StateSpiller, spill_largest_partition, spill_states};

pub(super) const SPILL_PARTITIONS: usize = 32;
const MAX_SPILL_PARTITIONS: usize = 256;

/// Files for one hash partition plus its estimated merge-side hash footprint.
///
/// Keeping this estimate with the Spill manifest lets merge scheduling choose
/// the largest partition without synchronously stat'ing compressed files.
#[derive(Debug)]
pub(super) struct SpillPartition {
    pub(super) files: Vec<SpillFile>,
    pub(super) estimated_bytes: u64,
}

pub(super) struct PartitionTask {
    pub(super) files: Vec<SpillFile>,
    depth: usize,
    estimated_bytes: u64,
}

impl PartitionTask {
    pub(super) fn initial(partition: SpillPartition) -> Self {
        Self {
            files: partition.files,
            depth: 0,
            estimated_bytes: partition.estimated_bytes,
        }
    }

    pub(super) fn child(partition: SpillPartition, depth: usize) -> Self {
        Self {
            files: partition.files,
            depth,
            estimated_bytes: partition.estimated_bytes,
        }
    }

    pub(super) fn next_depth(&self, max_depth: usize) -> Result<usize> {
        if self.depth >= max_depth {
            return Err(Error::ResourceExhausted(format!(
                "aggregate spill partition still exceeds available memory after {} seeded \
                 repartition levels; increase the memory limit or reduce group-key skew",
                max_depth
            )));
        }
        Ok(self.depth + 1)
    }

    pub(super) fn estimated_bytes(&self) -> usize {
        usize::try_from(self.estimated_bytes).unwrap_or(usize::MAX)
    }
}

pub(super) fn pop_largest_partition(tasks: &mut Vec<PartitionTask>) -> Option<PartitionTask> {
    let largest = tasks
        .iter()
        .enumerate()
        .max_by_key(|(_, task)| task.estimated_bytes)
        .map(|(index, _)| index)?;
    Some(tasks.swap_remove(largest))
}

pub(super) enum MergeOutcome {
    Merged(Vec<GroupState>),
    Repartition,
}

pub(super) fn merge_partition(
    files: &[SpillFile],
    groups: &[BoundExpr],
    aggregates: &[AggregateExpr],
    context: &QueryContext,
    reservation: &mut MemoryReservation,
) -> Result<MergeOutcome> {
    let mut group_index = HashMap::<Vec<super::CellValue>, usize>::new();
    let mut states = Vec::<GroupState>::new();
    for file in files {
        for batch in context.spill.read_file(file)? {
            context.check_cancelled()?;
            let batch = batch?;
            let batch_bytes = batch.get_array_memory_size().max(1);
            let _batch_reservation = match context.memory.try_reserve(batch_bytes) {
                Ok(reservation) => reservation,
                Err(_) if !states.is_empty() => return Ok(MergeOutcome::Repartition),
                Err(_) => return Err(merge_batch_error(batch_bytes, context)),
            };
            for row in 0..batch.num_rows() {
                let key = group_key(&batch, row, groups.len())?;
                let index = if let Some(index) = group_index.get(&key) {
                    *index
                } else {
                    let index_bytes = estimate_index_key_bytes(&key);
                    let state = GroupState::new(key.clone(), aggregates);
                    let bytes = estimate_group_bytes(&state).saturating_add(index_bytes);
                    if reservation.try_grow(bytes).is_err() {
                        if states.is_empty() {
                            return Err(single_group_error(
                                bytes,
                                context,
                                reservation.pool().limit(),
                            ));
                        }
                        return Ok(MergeOutcome::Repartition);
                    }
                    let index = states.len();
                    states.push(state);
                    group_index.insert(key, index);
                    index
                };
                let mut column = groups.len();
                for (aggregate_index, expression) in aggregates.iter().enumerate() {
                    states[index].aggregates[aggregate_index].merge_partial(
                        expression,
                        &batch,
                        row,
                        &mut column,
                    )?;
                }
            }
        }
    }
    Ok(MergeOutcome::Merged(states))
}

fn estimate_index_key_bytes(key: &Vec<super::CellValue>) -> usize {
    key.capacity()
        .saturating_mul(size_of::<super::CellValue>())
        .saturating_add(key.iter().fold(0usize, |bytes, value| {
            bytes.saturating_add(match value {
                super::CellValue::Utf8(value) => value.capacity(),
                super::CellValue::Binary(value) => value.capacity(),
                _ => 0,
            })
        }))
        // HashMap bucket, stored Vec header, and usize value.
        .saturating_add(64)
}

pub(super) fn remove_files(context: &QueryContext, files: &[SpillFile]) -> Result<()> {
    for file in files {
        context.spill.remove_file(file)?;
    }
    Ok(())
}

pub(super) fn single_group_error(
    bytes: usize,
    context: &QueryContext,
    state_limit: usize,
) -> Error {
    Error::ResourceExhausted(format!(
        "cannot reserve {bytes} bytes for one aggregate group (aggregate state budget \
         {state_limit} bytes, query limit {} bytes, currently available {} bytes)",
        context.memory.limit(),
        context.memory.available()
    ))
}

fn merge_batch_error(bytes: usize, context: &QueryContext) -> Error {
    Error::ResourceExhausted(format!(
        "aggregate spill merge requires at least {bytes} bytes for one IPC batch \
         (query limit {} bytes, currently available {} bytes); reduce the spill batch size \
         or increase the memory limit",
        context.memory.limit(),
        context.memory.available()
    ))
}

pub(super) fn group_key(
    batch: &RecordBatch,
    row: usize,
    group_count: usize,
) -> Result<Vec<super::CellValue>> {
    (0..group_count)
        .map(|index| super::cell(batch.column(index), row))
        .collect()
}

pub(super) fn partition_for_key(key: &[super::CellValue], partitions: usize, seed: u64) -> usize {
    let mut hasher = DefaultHasher::new();
    seed.hash(&mut hasher);
    key.hash(&mut hasher);
    (hasher.finish() as usize) % partitions
}

pub(super) fn adaptive_spill_partitions(context: &QueryContext, estimated_bytes: usize) -> usize {
    let lanes = context.scheduler.partitioning_lanes();
    let default_target = context
        .memory
        .limit()
        .checked_div(lanes.saturating_mul(2))
        .unwrap_or(0)
        .clamp(8 << 20, 64 << 20);
    let target = context
        .execution
        .spill_partition_target_bytes
        .unwrap_or(default_target)
        .max(1);
    estimated_bytes
        .max(1)
        .div_ceil(target)
        .checked_next_power_of_two()
        .unwrap_or(MAX_SPILL_PARTITIONS)
        .clamp(2, MAX_SPILL_PARTITIONS)
}

#[cfg(test)]
mod task_tests {
    use super::*;

    #[test]
    fn merge_pops_largest_partition_first() {
        let task = |estimated_bytes| {
            PartitionTask::initial(SpillPartition {
                files: Vec::new(),
                estimated_bytes,
            })
        };
        let mut tasks = vec![task(4), task(32), task(16)];
        assert_eq!(
            pop_largest_partition(&mut tasks).unwrap().estimated_bytes,
            32
        );
        assert_eq!(
            pop_largest_partition(&mut tasks).unwrap().estimated_bytes,
            16
        );
        assert_eq!(
            pop_largest_partition(&mut tasks).unwrap().estimated_bytes,
            4
        );
    }
}
