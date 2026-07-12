use std::sync::Arc;

use arrow::datatypes::SchemaRef;

use crate::{
    Error, Result,
    runtime::{MemoryPool, QueryContext, SpillFile},
    sql::{AggregateExpr, BoundExpr},
};

use super::super::spill::remove_files;
use super::super::{SPILL_PARTITIONS, StateSpiller};
use super::{
    spill::{self, DistinctPartitionTask, MergeDistinct},
    state,
};

enum WorkerOutcome {
    Merged(Vec<Vec<SpillFile>>),
    Repartition,
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn merge_partitions(
    partitions: Vec<Vec<SpillFile>>,
    groups: &[BoundExpr],
    aggregates: &[AggregateExpr],
    state_schema: SchemaRef,
    state_spiller: &mut StateSpiller,
    context: Arc<QueryContext>,
    distinct_pool: MemoryPool,
    state_pool: MemoryPool,
) -> Result<()> {
    let repartition_count = partitions.len();
    let mut pending = partitions
        .into_iter()
        .rev()
        .filter(|files| !files.is_empty())
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
        let lanes = configured_lanes.min(memory_lanes).min(pending.len());
        let (sender, mut receiver) = tokio::sync::mpsc::channel(lanes);
        for _ in 0..lanes {
            let task = pending
                .pop()
                .expect("wave size is bounded by pending tasks");
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
                                StateSpiller::new(&worker_context, SPILL_PARTITIONS);
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
                            .and_then(|()| worker_spiller.finish())
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
                    let depth = task.next_depth()?;
                    let children =
                        spill::repartition(&task.files, depth, repartition_count, &context)?;
                    remove_files(&context, &task.files)?;
                    for files in children.into_iter().rev() {
                        if !files.is_empty() {
                            pending.push(DistinctPartitionTask::child(files, depth));
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
