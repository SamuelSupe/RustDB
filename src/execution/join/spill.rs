use crate::{
    Result,
    runtime::{QueryContext, SpillFile},
    sql::{BoundExpr, JoinType},
};

mod build;
mod partition;

pub(super) use build::{BuildPartition, load_build_partition};
#[cfg(test)]
pub(super) use partition::partition_for_key;
pub(super) use partition::{PartitionSpiller, Side, spill_batch, spill_stream};

pub(super) const PARTITIONS: usize = 64;
pub(super) const MAX_REPARTITION_DEPTH: usize = 4;
const SEED_STEP: u64 = 0x9e37_79b9_7f4a_7c15;

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
    next_depth: usize,
    context: &QueryContext,
) -> Result<Repartitioned> {
    let seed = seed_for_depth(next_depth);
    let mut left_spiller = PartitionSpiller::new(context, format!("join-left-r{next_depth}"));
    let mut right_rows = vec![0usize; PARTITIONS];

    for file in &task.left {
        for batch in context.spill.read_file(file)? {
            spill_batch(
                batch?,
                left_expressions,
                Side::Left,
                join_type,
                &mut left_spiller,
                seed,
            )?;
        }
    }
    let left = left_spiller.finish()?;

    let mut right_spiller = PartitionSpiller::new(context, format!("join-right-r{next_depth}"));
    for file in &task.right {
        for batch in context.spill.read_file(file)? {
            let counts = spill_batch(
                batch?,
                right_expressions,
                Side::Right,
                join_type,
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

fn seed_for_depth(depth: usize) -> u64 {
    SEED_STEP.wrapping_mul(depth as u64)
}
