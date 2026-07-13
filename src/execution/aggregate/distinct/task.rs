use crate::{Error, Result, runtime::SpillFile};

use super::super::spill::{PartitionTask, SpillPartition};

pub(super) struct DistinctPartitionTask {
    pub(super) files: Vec<SpillFile>,
    depth: usize,
    estimated_bytes: u64,
}

impl DistinctPartitionTask {
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
                "DISTINCT spill partition exceeds memory after {max_depth} full-key repartition levels"
            )));
        }
        Ok(self.depth + 1)
    }

    pub(super) fn estimated_bytes(&self) -> usize {
        usize::try_from(self.estimated_bytes).unwrap_or(usize::MAX)
    }
}

pub(super) fn pop_largest_distinct(
    tasks: &mut Vec<DistinctPartitionTask>,
) -> Option<DistinctPartitionTask> {
    let largest = tasks
        .iter()
        .enumerate()
        .max_by_key(|(_, task)| task.estimated_bytes)
        .map(|(index, _)| index)?;
    Some(tasks.swap_remove(largest))
}

pub(super) struct StatePartitionTask {
    pub(super) task: PartitionTask,
    estimated_bytes: u64,
}

impl StatePartitionTask {
    pub(super) fn initial(partition: SpillPartition) -> Self {
        let estimated_bytes = partition.estimated_bytes;
        Self {
            task: PartitionTask::initial(partition),
            estimated_bytes,
        }
    }

    pub(super) fn child(partition: SpillPartition, depth: usize) -> Self {
        let estimated_bytes = partition.estimated_bytes;
        Self {
            task: PartitionTask::child(partition, depth),
            estimated_bytes,
        }
    }
}

pub(super) fn pop_largest(tasks: &mut Vec<StatePartitionTask>) -> Option<StatePartitionTask> {
    let largest = tasks
        .iter()
        .enumerate()
        .max_by_key(|(_, task)| task.estimated_bytes)
        .map(|(index, _)| index)?;
    Some(tasks.swap_remove(largest))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn task(estimated_bytes: u64) -> StatePartitionTask {
        StatePartitionTask::initial(SpillPartition {
            files: Vec::new(),
            estimated_bytes,
        })
    }

    #[test]
    fn state_merge_pops_largest_partition_first() {
        let mut tasks = vec![task(4), task(32), task(16)];
        assert_eq!(pop_largest(&mut tasks).unwrap().estimated_bytes, 32);
        assert_eq!(pop_largest(&mut tasks).unwrap().estimated_bytes, 16);
        assert_eq!(pop_largest(&mut tasks).unwrap().estimated_bytes, 4);
    }

    #[test]
    fn distinct_merge_pops_largest_partition_first() {
        let task = |estimated_bytes| {
            DistinctPartitionTask::initial(SpillPartition {
                files: Vec::new(),
                estimated_bytes,
            })
        };
        let mut tasks = vec![task(8), task(64), task(24)];
        assert_eq!(
            pop_largest_distinct(&mut tasks).unwrap().estimated_bytes,
            64
        );
        assert_eq!(
            pop_largest_distinct(&mut tasks).unwrap().estimated_bytes,
            24
        );
        assert_eq!(pop_largest_distinct(&mut tasks).unwrap().estimated_bytes, 8);
    }
}
