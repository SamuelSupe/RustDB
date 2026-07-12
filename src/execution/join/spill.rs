use crate::{
    Result,
    runtime::{BatchEnvelope, QueryContext, SpillFile},
    sql::{BoundExpr, JoinType},
};

mod build;
mod partition;

pub(super) use build::{BuildPartition, load_build_partition};
pub(super) use partition::{PartitionSpiller, Side, spill_batch_with_null_keys, spill_stream};
#[cfg(test)]
pub(super) use partition::{partition_for_key, spill_batch};

pub(super) const PARTITIONS: usize = 64;
pub(super) const MAX_REPARTITION_DEPTH: usize = 4;
const SEED_STEP: u64 = 0x9e37_79b9_7f4a_7c15;

pub(super) struct PartitionTask {
    pub(super) left: Vec<SpillFile>,
    pub(super) right: Vec<SpillFile>,
    pub(super) depth: usize,
    pub(super) stagnant_repartitions: usize,
}

impl PartitionTask {
    fn new(left: Vec<SpillFile>, right: Vec<SpillFile>, depth: usize) -> Self {
        Self {
            left,
            right,
            depth,
            stagnant_repartitions: 0,
        }
    }
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

pub(super) fn repartition(
    task: &PartitionTask,
    left_expressions: &[BoundExpr],
    right_expressions: &[BoundExpr],
    join_type: JoinType,
    null_equal_keys: bool,
    next_depth: usize,
    context: &QueryContext,
) -> Result<Repartitioned> {
    let seed = seed_for_depth(next_depth);
    let mut left_spiller = PartitionSpiller::new(context, format!("join-left-r{next_depth}"));
    let mut right_rows = vec![0usize; PARTITIONS];

    for file in &task.left {
        for batch in context.spill.read_file(file)? {
            let batch =
                BatchEnvelope::try_new(batch?, &context.memory, "join repartition left batch")?;
            let (batch, _batch_memory) = batch.into_parts();
            spill_batch_with_null_keys(
                batch,
                left_expressions,
                Side::Left,
                join_type,
                null_equal_keys,
                &mut left_spiller,
                seed,
            )?;
        }
    }
    let left = left_spiller.finish()?;

    let mut right_spiller = PartitionSpiller::new(context, format!("join-right-r{next_depth}"));
    for file in &task.right {
        for batch in context.spill.read_file(file)? {
            let batch =
                BatchEnvelope::try_new(batch?, &context.memory, "join repartition right batch")?;
            let (batch, _batch_memory) = batch.into_parts();
            let counts = spill_batch_with_null_keys(
                batch,
                right_expressions,
                Side::Right,
                join_type,
                null_equal_keys,
                &mut right_spiller,
                seed,
            )?;
            for (total, count) in right_rows.iter_mut().zip(counts) {
                *total = total.saturating_add(count);
            }
        }
    }
    let right = right_spiller.finish()?;

    let largest_build_rows = right_rows.into_iter().max().unwrap_or(0);
    let tasks = left
        .into_iter()
        .zip(right)
        .filter(|(left, right)| !left.is_empty() || !right.is_empty())
        .map(|(left, right)| PartitionTask::new(left, right, next_depth))
        .collect::<Vec<_>>();
    context
        .metrics
        .record_spill(0, u64::try_from(tasks.len()).unwrap_or(u64::MAX));
    Ok(Repartitioned {
        tasks,
        largest_build_rows,
    })
}

pub(super) fn remove_task(context: &QueryContext, task: &PartitionTask) -> Result<()> {
    remove_files(context, &task.left)?;
    remove_files(context, &task.right)
}

pub(super) fn remove_tasks(context: &QueryContext, tasks: &[PartitionTask]) -> Result<()> {
    for task in tasks {
        remove_task(context, task)?;
    }
    Ok(())
}

pub(super) fn remove_files(context: &QueryContext, files: &[SpillFile]) -> Result<()> {
    for file in files {
        context.spill.remove_file(file)?;
    }
    Ok(())
}

fn seed_for_depth(depth: usize) -> u64 {
    SEED_STEP.wrapping_mul(depth as u64)
}
