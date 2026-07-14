use crate::{
    Result,
    runtime::{BatchEnvelope, MAX_ACTIVE_SPILL_FILES, QueryContext, SpillFile},
    sql::{BoundExpr, JoinType},
};

mod build;
mod partition;

pub(super) use build::{BuildPartition, load_build_partition};
pub(super) use partition::{
    PartitionManifest, PartitionSpiller, Side, batch_logical_buffer_bytes,
    estimated_build_footprint, spill_batch_with_null_keys, spill_stream,
};
#[cfg(test)]
pub(super) use partition::{partition_for_key, spill_batch};

pub(super) const PARTITIONS: usize = 256;
const MIN_PARTITIONS: usize = 2;
// A sort-merge fallback retains at most one final fan-in per side. Two group
// files plus one publish-before-delete replacement slot make the remaining
// three slots explicit.
pub(super) const FALLBACK_FILE_HEADROOM: usize = crate::execution::sort::MERGE_FAN_IN * 2 + 3;
const MAX_PARTITIONS_WITH_REPARTITION: usize = 128;
#[cfg(test)]
pub(super) const MAX_REPARTITION_DEPTH: usize = 4;
const SEED_STEP: u64 = 0x9e37_79b9_7f4a_7c15;

pub(super) struct PartitionTask {
    pub(super) left: Vec<SpillFile>,
    pub(super) right: Vec<SpillFile>,
    pub(super) build: BuildPartitionStats,
    pub(super) depth: usize,
    pub(super) stagnant_repartitions: usize,
}

#[derive(Clone, Copy)]
pub(super) struct BuildPartitionStats {
    pub(super) estimated_bytes: usize,
    pub(super) data_bytes: usize,
    pub(super) batches: usize,
    pub(super) max_batch_bytes: usize,
    pub(super) rows: usize,
}

impl BuildPartitionStats {
    #[cfg(test)]
    pub(super) fn rows_only(rows: usize) -> Self {
        Self {
            estimated_bytes: 0,
            data_bytes: 0,
            batches: 0,
            max_batch_bytes: 0,
            rows,
        }
    }
}

impl PartitionTask {
    fn new(
        left: Vec<SpillFile>,
        right: Vec<SpillFile>,
        build: BuildPartitionStats,
        depth: usize,
    ) -> Self {
        Self {
            left,
            right,
            build,
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
    right: PartitionManifest,
) -> Vec<PartitionTask> {
    let PartitionManifest { files, build } = right;
    left.into_iter()
        .zip(files)
        .enumerate()
        .filter(|(_, (left, right))| !left.is_empty() || !right.is_empty())
        .map(|(index, (left, right))| PartitionTask::new(left, right, build[index], 0))
        .collect()
}

pub(super) fn pop_largest_task(tasks: &mut Vec<PartitionTask>) -> Option<PartitionTask> {
    let largest = tasks
        .iter()
        .enumerate()
        .max_by_key(|(_, task)| task.build.estimated_bytes)
        .map(|(index, _)| index)?;
    Some(tasks.swap_remove(largest))
}

pub(super) fn repartition(
    task: &PartitionTask,
    left_expressions: &[BoundExpr],
    right_expressions: &[BoundExpr],
    join_type: JoinType,
    null_equal_keys: bool,
    next_depth: usize,
    context: &QueryContext,
) -> Result<Option<Repartitioned>> {
    let seed = seed_for_depth(next_depth);
    // Refuse to start a generation that cannot retain at least two files per
    // side plus rotation headroom. The caller falls back to sort-merge while
    // the original partition is still intact.
    let Some(partitions) = repartition_partition_count(context, task.build.estimated_bytes) else {
        return Ok(None);
    };
    let mut left_spiller = PartitionSpiller::for_repartition(
        context,
        format!("join-left-r{next_depth}"),
        partitions,
        next_depth,
    );
    let mut repartition_bytes = 0u64;

    for file in &task.left {
        for batch in context.spill.read_file(file)? {
            let batch =
                BatchEnvelope::try_new(batch?, &context.memory, "join repartition left batch")?;
            repartition_bytes = repartition_bytes.saturating_add(
                u64::try_from(batch.batch().get_array_memory_size()).unwrap_or(u64::MAX),
            );
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

    let mut right_spiller = PartitionSpiller::for_repartition(
        context,
        format!("join-right-r{next_depth}"),
        partitions,
        next_depth,
    );
    for file in &task.right {
        for batch in context.spill.read_file(file)? {
            let batch =
                BatchEnvelope::try_new(batch?, &context.memory, "join repartition right batch")?;
            let batch_bytes = batch.batch().get_array_memory_size();
            repartition_bytes =
                repartition_bytes.saturating_add(u64::try_from(batch_bytes).unwrap_or(u64::MAX));
            let (batch, _batch_memory) = batch.into_parts();
            spill_batch_with_null_keys(
                batch,
                right_expressions,
                Side::Right,
                join_type,
                null_equal_keys,
                &mut right_spiller,
                seed,
            )?;
        }
    }
    let right = right_spiller.finish_manifest()?;

    let largest_build_rows = right
        .build
        .iter()
        .map(|stats| stats.rows)
        .max()
        .unwrap_or(usize::MAX);
    let largest_build_bytes = right
        .build
        .iter()
        .map(|stats| u64::try_from(stats.estimated_bytes).unwrap_or(u64::MAX))
        .max()
        .unwrap_or(0);
    context
        .metrics
        .record_repartition(repartition_bytes, next_depth, largest_build_bytes);
    let PartitionManifest { files, build } = right;
    let tasks = left
        .into_iter()
        .zip(files)
        .enumerate()
        .filter(|(_, (left, right))| !left.is_empty() || !right.is_empty())
        .map(|(index, (left, right))| PartitionTask::new(left, right, build[index], next_depth))
        .collect::<Vec<_>>();
    context
        .metrics
        .record_spill(0, u64::try_from(tasks.len()).unwrap_or(u64::MAX));
    Ok(Some(Repartitioned {
        tasks,
        largest_build_rows,
    }))
}

pub(super) fn adaptive_partition_count(context: &QueryContext, estimated_bytes: usize) -> usize {
    const MIN_TARGET_BYTES: usize = 8 << 20;

    let lanes = context.scheduler.partitioning_lanes();
    let lane_target = context
        .memory
        .limit()
        .checked_div(lanes.saturating_mul(2))
        .unwrap_or(0);
    let default_target = lane_target.clamp(MIN_TARGET_BYTES, 64 << 20);
    let target = context
        .execution
        .spill_partition_target_bytes
        .unwrap_or(default_target)
        .max(1);
    desired_partition_count(estimated_bytes, target)
        .min(file_budget_partition_cap(context).unwrap_or(MIN_PARTITIONS))
}

fn repartition_partition_count(context: &QueryContext, estimated_bytes: usize) -> Option<usize> {
    const MIN_TARGET_BYTES: usize = 8 << 20;

    let lanes = context.scheduler.partitioning_lanes();
    let lane_target = context
        .memory
        .limit()
        .checked_div(lanes.saturating_mul(2))
        .unwrap_or(0);
    let default_target = lane_target.clamp(MIN_TARGET_BYTES, 64 << 20);
    let target = context
        .execution
        .spill_partition_target_bytes
        .unwrap_or(default_target)
        .max(1);
    let cap = file_budget_partition_cap(context)?;
    Some(desired_partition_count(estimated_bytes, target).min(cap))
}

fn desired_partition_count(estimated_bytes: usize, target: usize) -> usize {
    estimated_bytes
        .max(1)
        .div_ceil(target)
        .checked_next_power_of_two()
        .unwrap_or(PARTITIONS)
        .clamp(MIN_PARTITIONS, PARTITIONS)
}

fn file_budget_partition_cap(context: &QueryContext) -> Option<usize> {
    partition_cap_for_active_files(
        context.spill.active_file_count(),
        context.execution.max_repartition_depth,
    )
}

fn partition_cap_for_active_files(active: usize, max_repartition_depth: usize) -> Option<usize> {
    const SIDES: usize = 2;

    let headroom = if max_repartition_depth == 0 {
        0
    } else {
        FALLBACK_FILE_HEADROOM
    };
    let available = MAX_ACTIVE_SPILL_FILES
        .saturating_sub(active)
        .saturating_sub(headroom);
    if available < SIDES * MIN_PARTITIONS {
        return None;
    }
    let generation_cap = available / SIDES;
    let generation_cap = previous_power_of_two(generation_cap.min(PARTITIONS));
    let depth_cap = if max_repartition_depth == 0 {
        PARTITIONS
    } else {
        MAX_PARTITIONS_WITH_REPARTITION
    };
    Some(generation_cap.min(depth_cap).max(MIN_PARTITIONS))
}

fn previous_power_of_two(value: usize) -> usize {
    let next = value.checked_next_power_of_two().unwrap_or(PARTITIONS);
    if next == value { value } else { next / 2 }
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

#[cfg(test)]
mod adaptive_tests {
    use super::{
        MAX_PARTITIONS_WITH_REPARTITION, PARTITIONS, PartitionTask, adaptive_partition_count,
        partition_cap_for_active_files, pop_largest_task,
    };
    use crate::runtime::{MemoryPool, QueryContext};

    #[test]
    fn configured_target_selects_bounded_power_of_two_fanout() {
        let directory = tempfile::tempdir().unwrap();
        let mut context = QueryContext::new(MemoryPool::new(128 << 20), directory.path()).unwrap();
        context.execution.spill_partition_target_bytes = Some(8 << 20);
        assert_eq!(adaptive_partition_count(&context, 20 << 20), 4);
        assert_eq!(adaptive_partition_count(&context, 80 << 20), 16);
        assert_eq!(
            adaptive_partition_count(&context, usize::MAX),
            MAX_PARTITIONS_WITH_REPARTITION
        );

        context.execution.max_repartition_depth = 0;
        assert_eq!(adaptive_partition_count(&context, usize::MAX), PARTITIONS);
    }

    #[test]
    fn repartition_file_budget_never_invents_missing_slots() {
        assert_eq!(partition_cap_for_active_files(0, 2), Some(128));
        assert_eq!(partition_cap_for_active_files(489, 2), Some(2));
        assert_eq!(partition_cap_for_active_files(490, 2), None);
        assert_eq!(partition_cap_for_active_files(508, 0), Some(2));
        assert_eq!(partition_cap_for_active_files(509, 0), None);
    }

    #[test]
    fn default_target_uses_stable_configured_lanes() {
        let directory = tempfile::tempdir().unwrap();
        let context = QueryContext::new(MemoryPool::new(128 << 20), directory.path()).unwrap();
        context.scheduler.configure_unbounded(2);
        let expected = adaptive_partition_count(&context, 160 << 20);
        assert_eq!(expected, 8);

        let _first = context.scheduler.enter_lane();
        assert_eq!(adaptive_partition_count(&context, 160 << 20), expected);
        let _second = context.scheduler.enter_lane();
        assert_eq!(adaptive_partition_count(&context, 160 << 20), expected);
    }

    #[test]
    fn default_target_clamps_to_eight_and_sixty_four_mibibytes() {
        let directory = tempfile::tempdir().unwrap();
        let large = QueryContext::new(MemoryPool::new(4 << 30), directory.path()).unwrap();
        assert_eq!(adaptive_partition_count(&large, 129 << 20), 4);

        let small = QueryContext::new(MemoryPool::new(16 << 20), directory.path()).unwrap();
        small.scheduler.configure_unbounded(4);
        let _lanes = (0..4)
            .map(|_| small.scheduler.enter_lane())
            .collect::<Vec<_>>();
        assert_eq!(adaptive_partition_count(&small, 17 << 20), 4);
    }

    #[test]
    fn task_queue_selects_largest_estimated_build_first() {
        let mut tasks = vec![
            PartitionTask::new(
                Vec::new(),
                Vec::new(),
                super::BuildPartitionStats {
                    estimated_bytes: 4 << 20,
                    data_bytes: 0,
                    batches: 0,
                    max_batch_bytes: 0,
                    rows: 0,
                },
                0,
            ),
            PartitionTask::new(
                Vec::new(),
                Vec::new(),
                super::BuildPartitionStats {
                    estimated_bytes: 32 << 20,
                    data_bytes: 0,
                    batches: 0,
                    max_batch_bytes: 0,
                    rows: 0,
                },
                0,
            ),
            PartitionTask::new(
                Vec::new(),
                Vec::new(),
                super::BuildPartitionStats {
                    estimated_bytes: 8 << 20,
                    data_bytes: 0,
                    batches: 0,
                    max_batch_bytes: 0,
                    rows: 0,
                },
                0,
            ),
        ];

        assert_eq!(
            pop_largest_task(&mut tasks)
                .expect("largest task")
                .build
                .estimated_bytes,
            32 << 20
        );
        assert_eq!(
            pop_largest_task(&mut tasks)
                .expect("next task")
                .build
                .estimated_bytes,
            8 << 20
        );
    }
}
