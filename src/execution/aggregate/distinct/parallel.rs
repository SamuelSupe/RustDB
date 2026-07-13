use std::sync::Arc;

use arrow::datatypes::SchemaRef;

use crate::{
    Error, Result,
    runtime::{MemoryPool, QueryContext},
    sql::{AggregateExpr, BoundExpr},
};

use super::super::StateSpiller;
use super::super::spill::{SpillPartition, remove_files};
use super::{
    spill::{self, MergeDistinct},
    state,
    task::{DistinctPartitionTask, pop_largest_distinct},
};

enum WorkerOutcome {
    Merged(Vec<SpillPartition>),
    Repartition,
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn merge_partitions(
    partitions: Vec<SpillPartition>,
    groups: &[BoundExpr],
    aggregates: &[AggregateExpr],
    state_schema: SchemaRef,
    state_spiller: &mut StateSpiller,
    context: Arc<QueryContext>,
    distinct_pool: MemoryPool,
    state_pool: MemoryPool,
) -> Result<()> {
    let repartition_count = partitions.len();
    let state_partition_count = state_spiller.partition_count();
    let mut pending = partitions
        .into_iter()
        .filter(|partition| !partition.files.is_empty())
        .map(DistinctPartitionTask::initial)
        .collect::<Vec<_>>();
    let groups = Arc::new(groups.to_vec());
    let aggregates = Arc::new(aggregates.to_vec());

    while !pending.is_empty() {
        context.check_cancelled()?;
        let configured_lanes = context.scheduler.configured_lanes().max(1);
        let memory_lanes = context
            .memory
            .limit()
            .checked_div(32 << 20)
            .unwrap_or(0)
            .max(usize::from(configured_lanes > 1).saturating_add(1));
        let max_lanes = configured_lanes.min(memory_lanes).min(pending.len());
        let mut wave = Vec::with_capacity(max_lanes);
        let mut wave_bytes = 0usize;
        while wave.len() < max_lanes {
            let task =
                pop_largest_distinct(&mut pending).expect("wave size is bounded by pending tasks");
            let next_bytes = wave_bytes.saturating_add(task.estimated_bytes());
            if !wave.is_empty() && next_bytes > distinct_pool.limit() {
                pending.push(task);
                break;
            }
            wave_bytes = next_bytes;
            wave.push(task);
        }
        let lanes = wave.len();
        let (sender, mut receiver) = tokio::sync::mpsc::channel(lanes);
        for task in wave {
            let sender = sender.clone();
            let groups = Arc::clone(&groups);
            let aggregates = Arc::clone(&aggregates);
            let state_schema = Arc::clone(&state_schema);
            let worker_context = Arc::clone(&context);
            let distinct_pool = distinct_pool.clone();
            let state_pool = state_pool.clone();
            context
                .tasks
                .spawn("distinct-aggregate-partition", async move {
                    let _active = worker_context.scheduler.enter_lane();
                    let outcome = match spill::load_partition(
                        &task.files,
                        aggregates.len(),
                        &worker_context,
                        distinct_pool,
                    ) {
                        Ok(MergeDistinct::Merged(loaded)) => {
                            let (keys, key_memory) = loaded.into_parts();
                            let mut worker_spiller =
                                StateSpiller::new(&worker_context, state_partition_count);
                            let mut state_memory = state_pool.reservation();
                            let result = state::contribute(
                                keys,
                                &groups,
                                &aggregates,
                                state_schema,
                                &mut worker_spiller,
                                &worker_context,
                                &mut state_memory,
                            )
                            .and_then(|()| worker_spiller.finish(&worker_context))
                            .map(WorkerOutcome::Merged);
                            drop(key_memory);
                            result
                        }
                        Ok(MergeDistinct::Repartition) => Ok(WorkerOutcome::Repartition),
                        Err(error) => Err(error),
                    };
                    sender
                        .send((task, outcome))
                        .await
                        .map_err(|_| Error::Cancelled)
                })?;
        }
        drop(sender);

        for _ in 0..lanes {
            let Some((task, outcome)) = receiver.recv().await else {
                return Err(context.tasks.first_failure().unwrap_or_else(|| {
                    Error::Execution("DISTINCT aggregate partition worker stopped".into())
                }));
            };
            match outcome {
                Ok(WorkerOutcome::Merged(files)) => {
                    state_spiller.extend_files(files)?;
                    remove_files(&context, &task.files)?;
                }
                Ok(WorkerOutcome::Repartition) => {
                    let depth = task.next_depth(context.execution.max_repartition_depth)?;
                    let children = spill::repartition(
                        &task.files,
                        task.estimated_bytes(),
                        depth,
                        repartition_count,
                        &context,
                    )?;
                    remove_files(&context, &task.files)?;
                    for partition in children {
                        if !partition.files.is_empty() {
                            pending.push(DistinctPartitionTask::child(partition, depth));
                        }
                    }
                }
                Err(error) => {
                    context.record_task_failure(&error);
                    return Err(error);
                }
            }
        }
    }
    Ok(())
}
