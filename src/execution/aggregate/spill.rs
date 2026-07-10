use std::{
    collections::{HashMap, hash_map::DefaultHasher},
    hash::{Hash, Hasher},
    sync::Arc,
};

use arrow::{array::UInt32Array, compute::take, datatypes::SchemaRef, record_batch::RecordBatch};

use crate::{
    Error, Result,
    runtime::{MemoryReservation, QueryContext, SpillFile},
    sql::{AggregateExpr, BoundExpr},
};

use super::{GroupState, build_partial_batch, estimate_group_bytes};

pub(super) const SPILL_PARTITIONS: usize = 32;
const MAX_REPARTITION_DEPTH: usize = 4;
const SEED_STEP: u64 = 0x9e37_79b9_7f4a_7c15;

pub(super) struct PartitionTask {
    pub(super) files: Vec<SpillFile>,
    depth: usize,
}

impl PartitionTask {
    pub(super) fn initial(files: Vec<SpillFile>) -> Self {
        Self { files, depth: 0 }
    }

    pub(super) fn child(files: Vec<SpillFile>, depth: usize) -> Self {
        Self { files, depth }
    }

    pub(super) fn next_depth(&self) -> Result<usize> {
        if self.depth >= MAX_REPARTITION_DEPTH {
            return Err(Error::ResourceExhausted(format!(
                "aggregate spill partition still exceeds available memory after {} seeded \
                 repartition levels; increase the memory limit or reduce group-key skew",
                MAX_REPARTITION_DEPTH
            )));
        }
        Ok(self.depth + 1)
    }
}

pub(super) enum MergeOutcome {
    Merged(Vec<GroupState>),
    Repartition,
}

pub(super) fn spill_states(
    states: &mut Vec<GroupState>,
    group_index: &mut HashMap<Vec<super::CellValue>, usize>,
    groups: &[BoundExpr],
    aggregates: &[AggregateExpr],
    schema: SchemaRef,
    partitions: &mut [Vec<SpillFile>],
    context: &QueryContext,
) -> Result<()> {
    if states.is_empty() {
        return Ok(());
    }
    let mut partitioned: Vec<Vec<GroupState>> = (0..partitions.len()).map(|_| Vec::new()).collect();
    for state in states.drain(..) {
        let partition = partition_for_key(&state.key, partitions.len(), 0);
        partitioned[partition].push(state);
    }
    group_index.clear();
    for (partition, states) in partitioned.into_iter().enumerate() {
        if states.is_empty() {
            continue;
        }
        let batch = build_partial_batch(&states, groups, aggregates, Arc::clone(&schema))?;
        let file = context.spill.write_record_batches(
            &format!("aggregate-p{partition}"),
            Arc::clone(&schema),
            [batch],
        )?;
        partitions[partition].push(file);
    }
    Ok(())
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
        for batch in context.spill.read_batches(file)? {
            for row in 0..batch.num_rows() {
                let key = group_key(&batch, row, groups.len())?;
                let index = if let Some(index) = group_index.get(&key) {
                    *index
                } else {
                    let state = GroupState::new(key.clone(), aggregates);
                    let bytes = estimate_group_bytes(&state);
                    if reservation.try_grow(bytes).is_err() {
                        if states.is_empty() {
                            return Err(single_group_error(bytes, context));
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

pub(super) fn repartition_partition(
    files: &[SpillFile],
    groups: &[BoundExpr],
    depth: usize,
    context: &QueryContext,
) -> Result<Vec<Vec<SpillFile>>> {
    let mut child_files: Vec<Vec<SpillFile>> = (0..SPILL_PARTITIONS).map(|_| Vec::new()).collect();
    let seed = seed_for_depth(depth);

    for source in files {
        for batch in context.spill.read_batches(source)? {
            context.check_cancelled()?;
            let mut row_indices: Vec<Vec<u32>> =
                (0..SPILL_PARTITIONS).map(|_| Vec::new()).collect();
            for row in 0..batch.num_rows() {
                let key = group_key(&batch, row, groups.len())?;
                let partition = partition_for_key(&key, SPILL_PARTITIONS, seed);
                row_indices[partition].push(u32::try_from(row).map_err(|_| {
                    Error::ResourceExhausted("aggregate spill batch exceeds UINT32_MAX rows".into())
                })?);
            }

            for (partition, indices) in row_indices.into_iter().enumerate() {
                if indices.is_empty() {
                    continue;
                }
                let batch = take_rows(&batch, indices)?;
                let file = context.spill.write_record_batches(
                    &format!("aggregate-r{depth}-p{partition}"),
                    batch.schema(),
                    [batch],
                )?;
                child_files[partition].push(file);
            }
        }
    }
    Ok(child_files)
}

pub(super) fn remove_files(context: &QueryContext, files: &[SpillFile]) {
    for file in files {
        context.spill.remove_file(file);
    }
}

pub(super) fn single_group_error(bytes: usize, context: &QueryContext) -> Error {
    Error::ResourceExhausted(format!(
        "cannot reserve {bytes} bytes for one aggregate group (query limit {} bytes, \
         currently available {} bytes)",
        context.memory.limit(),
        context.memory.available()
    ))
}

fn group_key(batch: &RecordBatch, row: usize, group_count: usize) -> Result<Vec<super::CellValue>> {
    (0..group_count)
        .map(|index| super::cell(batch.column(index), row))
        .collect()
}

fn take_rows(batch: &RecordBatch, indices: Vec<u32>) -> Result<RecordBatch> {
    let indices = UInt32Array::from(indices);
    let columns = batch
        .columns()
        .iter()
        .map(|column| take(column.as_ref(), &indices, None))
        .collect::<std::result::Result<Vec<_>, _>>()?;
    Ok(RecordBatch::try_new(batch.schema(), columns)?)
}

fn seed_for_depth(depth: usize) -> u64 {
    SEED_STEP.wrapping_mul(depth as u64)
}

pub(super) fn partition_for_key(key: &[super::CellValue], partitions: usize, seed: u64) -> usize {
    let mut hasher = DefaultHasher::new();
    seed.hash(&mut hasher);
    key.hash(&mut hasher);
    (hasher.finish() as usize) % partitions
}
