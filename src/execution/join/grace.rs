use std::{collections::VecDeque, sync::Arc, time::Instant};

use arrow::datatypes::SchemaRef;
use futures::StreamExt;
use tokio::sync::{Mutex, mpsc};
use tokio_util::sync::CancellationToken;

use crate::{
    Error, Result,
    runtime::{BatchEnvelope, MemoryBatchStream, QueryContext, boxed_memory_batch_stream},
    sql::{BoundExpr, JoinType},
};

use super::{
    EvaluatedKeys, ProbeCursor,
    condition::JoinPredicates,
    evaluate_keys_accounted, evaluate_optional_values,
    matched::BuildMatchTracker,
    optional_array, optional_memory,
    output::build_unmatched_right_envelope,
    probe::try_build_hash_table_with_nulls,
    sort_merge,
    spill::{self, BuildPartition, MAX_REPARTITION_DEPTH, PartitionTask},
};

const MIN_MEMORY_PER_LANE: usize = 512 << 10;

pub(super) fn is_supported(context: &QueryContext, tasks: usize) -> bool {
    lane_count(context, tasks) > 1
}

#[allow(clippy::too_many_arguments)]
pub(super) fn join(
    tasks: Vec<PartitionTask>,
    left_key_expressions: Vec<BoundExpr>,
    right_key_expressions: Vec<BoundExpr>,
    left_schema: SchemaRef,
    right_schema: SchemaRef,
    predicates: JoinPredicates,
    null_equal_keys: bool,
    join_type: JoinType,
    schema: SchemaRef,
    context: Arc<QueryContext>,
    batch_size: usize,
) -> MemoryBatchStream {
    boxed_memory_batch_stream(async_stream::try_stream! {
        let lanes = lane_count(&context, tasks.len());
        let pending = Arc::new(Mutex::new(VecDeque::from(tasks)));
        let cancellation = CancellationToken::new();
        let _cancel_on_drop = CancelOnDrop(cancellation.clone());
        let (sender, mut receiver) = mpsc::channel(lanes.saturating_mul(2).max(2));

        for _ in 0..lanes {
            context.tasks.spawn("grace-join-worker", run_worker(
                Arc::clone(&pending),
                sender.clone(),
                cancellation.clone(),
                left_key_expressions.clone(),
                right_key_expressions.clone(),
                Arc::clone(&left_schema),
                Arc::clone(&right_schema),
                predicates.clone(),
                null_equal_keys,
                join_type,
                Arc::clone(&schema),
                Arc::clone(&context),
                batch_size.max(1),
                lanes,
            ))?;
        }
        drop(pending);
        // Keep the coordinator sender alive until all workers report Done.
        // TaskGroup records a worker panic only after that worker has unwound
        // and dropped its sender clone.

        let mut completed = 0usize;
        while completed < lanes {
            let message: Result<Option<WorkerMessage>> = tokio::select! {
                biased;
                _ = context.control.cancelled() => Err(context
                    .check_cancelled()
                    .expect_err("cancelled query has a terminal error")),
                message = receiver.recv() => Ok(message),
            };
            let message = message?;
            match message {
                Some(WorkerMessage::Batch(batch)) => yield batch,
                Some(WorkerMessage::Done) => completed += 1,
                None => {
                    Err(Error::Execution(
                        "parallel Grace join workers stopped before completing all partitions".into(),
                    ))?;
                }
            }
        }
        drop(sender);
    })
}

enum WorkerMessage {
    Batch(BatchEnvelope),
    Done,
}

#[allow(clippy::too_many_arguments)]
async fn run_worker(
    pending: Arc<Mutex<VecDeque<PartitionTask>>>,
    sender: mpsc::Sender<WorkerMessage>,
    cancellation: CancellationToken,
    left_key_expressions: Vec<BoundExpr>,
    right_key_expressions: Vec<BoundExpr>,
    left_schema: SchemaRef,
    right_schema: SchemaRef,
    predicates: JoinPredicates,
    null_equal_keys: bool,
    join_type: JoinType,
    schema: SchemaRef,
    context: Arc<QueryContext>,
    batch_size: usize,
    worker_lanes: usize,
) -> Result<()> {
    run_worker_inner(
        pending,
        &sender,
        &cancellation,
        &left_key_expressions,
        &right_key_expressions,
        &left_schema,
        &right_schema,
        &predicates,
        null_equal_keys,
        join_type,
        &schema,
        &context,
        batch_size,
        worker_lanes,
    )
    .await?;
    sender
        .send(WorkerMessage::Done)
        .await
        .map_err(|_| Error::Cancelled)
}

#[allow(clippy::too_many_arguments)]
async fn run_worker_inner(
    pending: Arc<Mutex<VecDeque<PartitionTask>>>,
    sender: &mpsc::Sender<WorkerMessage>,
    cancellation: &CancellationToken,
    left_key_expressions: &[BoundExpr],
    right_key_expressions: &[BoundExpr],
    left_schema: &SchemaRef,
    right_schema: &SchemaRef,
    predicates: &JoinPredicates,
    null_equal_keys: bool,
    join_type: JoinType,
    schema: &SchemaRef,
    context: &Arc<QueryContext>,
    batch_size: usize,
    worker_lanes: usize,
) -> Result<()> {
    loop {
        let task = { pending.lock().await.pop_front() };
        let Some(task) = task else { break };
        let mut local = vec![task];
        let worker_pool = context.memory.child(
            format!("Grace-join-worker-{}", context.query_id),
            grace_worker_limit(context.memory.limit(), worker_lanes),
        );
        let mut reservation = worker_pool.reservation();
        while let Some(task) = local.pop() {
            check_running(cancellation, context)?;
            let build = {
                let _active = context.scheduler.enter_lane();
                match spill::load_build_partition(
                    &task.right,
                    right_schema,
                    context,
                    &mut reservation,
                )? {
                    BuildPartition::Loaded(right_batch) => {
                        let right_keys = evaluate_keys_accounted(
                            right_key_expressions,
                            &right_batch,
                            context,
                            "Grace join build keys",
                        )?;
                        let rows = right_batch.num_rows();
                        let hash_table = try_build_hash_table_with_nulls(
                            &right_keys,
                            rows,
                            super::can_deduplicate_build(join_type, predicates),
                            null_equal_keys,
                            &mut reservation,
                        )?;
                        drop(right_keys);
                        let right_values = evaluate_optional_values(
                            predicates.right_value(),
                            &right_batch,
                            context,
                            "Grace join build membership value",
                        )?;
                        match hash_table {
                            Some(hash_table)
                                if let Some(matched_build) = super::try_build_match_tracker(
                                    join_type,
                                    rows,
                                    &mut reservation,
                                ) =>
                            {
                                TaskHashBuild::Ready(
                                    right_batch,
                                    hash_table,
                                    right_values,
                                    matched_build,
                                )
                            }
                            Some(hash_table) => {
                                drop(hash_table);
                                drop(right_values);
                                TaskHashBuild::TooLarge(rows)
                            }
                            None => {
                                drop(right_values);
                                TaskHashBuild::TooLarge(rows)
                            }
                        }
                    }
                    BuildPartition::TooLarge { rows } => TaskHashBuild::TooLarge(rows),
                }
            };
            match build {
                TaskHashBuild::Ready(right_batch, hash_table, right_values, matched_build) => {
                    for file in &task.left {
                        for left_batch in context.spill.read_file(file)? {
                            check_running(cancellation, context)?;
                            let (left_batch, left_keys) = {
                                let _active = context.scheduler.enter_lane();
                                let left_batch = BatchEnvelope::try_new(
                                    left_batch?,
                                    &context.memory,
                                    "join spill probe",
                                )?;
                                let left_keys = evaluate_keys_accounted(
                                    left_key_expressions,
                                    left_batch.batch(),
                                    context,
                                    "Grace join probe keys",
                                )?;
                                (left_batch, left_keys)
                            };
                            let left_values = evaluate_optional_values(
                                predicates.left_value(),
                                left_batch.batch(),
                                context,
                                "Grace join probe membership value",
                            )?;
                            let mut probe = ProbeCursor::new(
                                left_batch.batch(),
                                &right_batch,
                                &left_keys,
                                &hash_table,
                                predicates,
                                optional_array(&left_values),
                                optional_array(&right_values),
                                None,
                                null_equal_keys,
                                matched_build.clone(),
                                join_type,
                                Arc::clone(schema),
                                batch_size,
                                reservation
                                    .size()
                                    .saturating_add(left_batch.memory_size())
                                    .saturating_add(left_keys.memory_size())
                                    .saturating_add(optional_memory(&left_values))
                                    .saturating_add(optional_memory(&right_values)),
                            );
                            loop {
                                let output = probe.next_batch(context).await?;
                                let Some(output) = output else { break };
                                send(sender, output, cancellation, context).await?;
                            }
                        }
                    }
                    if let Some(matched) = &matched_build {
                        let mut start = 0;
                        loop {
                            let indices = matched
                                .unmatched_from(start, batch_size, context, reservation.size())
                                .await?;
                            let Some(last) = indices.last().copied() else {
                                break;
                            };
                            start = last as usize + 1;
                            let output = build_unmatched_right_envelope(
                                left_schema,
                                &right_batch,
                                &indices,
                                Arc::clone(schema),
                                context,
                                reservation.size().saturating_add(indices.memory_size()),
                            )
                            .await?;
                            send(sender, output, cancellation, context).await?;
                        }
                    }
                    spill::remove_task(context, &task)?;
                    reservation.try_resize(0)?;
                }
                TaskHashBuild::TooLarge(rows) => {
                    reservation.try_resize(0)?;
                    if task.depth < MAX_REPARTITION_DEPTH {
                        let next_depth = task.depth + 1;
                        let repartitioned = {
                            let _active = context.scheduler.enter_lane();
                            spill::repartition(
                                &task,
                                left_key_expressions,
                                right_key_expressions,
                                join_type,
                                null_equal_keys,
                                next_depth,
                                context,
                            )?
                        };
                        let shrank = repartitioned.largest_build_rows < rows;
                        if shrank || task.stagnant_repartitions == 0 {
                            spill::remove_task(context, &task)?;
                            let stagnant = if shrank {
                                0
                            } else {
                                task.stagnant_repartitions + 1
                            };
                            local.extend(repartitioned.tasks.into_iter().rev().map(|mut child| {
                                child.stagnant_repartitions = stagnant;
                                child
                            }));
                            continue;
                        }
                        spill::remove_tasks(context, &repartitioned.tasks)?;
                    }

                    let mut fallback = sort_merge::fallback_with_null_keys(
                        task,
                        left_key_expressions.to_vec(),
                        right_key_expressions.to_vec(),
                        Arc::clone(left_schema),
                        Arc::clone(right_schema),
                        predicates.clone(),
                        null_equal_keys,
                        join_type,
                        Arc::clone(schema),
                        Arc::clone(context),
                        batch_size,
                    );
                    while let Some(output) = fallback.next().await {
                        send(sender, output?, cancellation, context).await?;
                    }
                    reservation.try_resize(0)?;
                }
            }
        }
    }
    Ok(())
}

fn grace_worker_limit(query_limit: usize, lanes: usize) -> usize {
    // Bound all concurrent partition hash tables to three quarters of the
    // query budget, leaving room for decoded probe batches, output queues,
    // and downstream operators such as COUNT(*).
    query_limit
        .checked_div(4)
        .unwrap_or(0)
        .saturating_mul(3)
        .checked_div(lanes.max(1))
        .unwrap_or(0)
        .max(1)
}

fn lane_count(context: &QueryContext, tasks: usize) -> usize {
    memory_bounded_lane_count(
        context.memory.limit(),
        context.scheduler.configured_lanes(),
        tasks,
    )
}

fn memory_bounded_lane_count(query_limit: usize, configured_lanes: usize, tasks: usize) -> usize {
    let memory_lanes = query_limit
        .checked_div(MIN_MEMORY_PER_LANE)
        .unwrap_or(0)
        .max(1);
    configured_lanes.min(tasks).min(memory_lanes).max(1)
}

enum TaskHashBuild {
    Ready(
        arrow::record_batch::RecordBatch,
        std::collections::HashMap<Vec<super::CellValue>, Vec<u32>>,
        Option<EvaluatedKeys>,
        Option<BuildMatchTracker>,
    ),
    TooLarge(usize),
}

async fn send(
    sender: &mpsc::Sender<WorkerMessage>,
    batch: BatchEnvelope,
    cancellation: &CancellationToken,
    context: &QueryContext,
) -> Result<()> {
    let started = Instant::now();
    let result = tokio::select! {
        _ = cancellation.cancelled() => return Err(Error::Cancelled),
        _ = context.control.cancelled() => return Err(Error::Cancelled),
        result = sender.send(WorkerMessage::Batch(batch)) => result,
    };
    context.scheduler.record_wait(started.elapsed());
    result.map_err(|_| Error::Cancelled)
}

fn check_running(cancellation: &CancellationToken, context: &QueryContext) -> Result<()> {
    if cancellation.is_cancelled() {
        Err(Error::Cancelled)
    } else {
        context.check_cancelled()
    }
}

struct CancelOnDrop(CancellationToken);

impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        self.0.cancel();
    }
}

#[cfg(test)]
mod tests {
    use super::{grace_worker_limit, memory_bounded_lane_count};

    #[test]
    fn low_memory_limits_lanes_and_splits_the_build_budget_across_actual_workers() {
        let memory_limit = 2 << 20;
        let lanes = memory_bounded_lane_count(memory_limit, 18, 64);
        assert_eq!(lanes, 4);
        assert_eq!(
            grace_worker_limit(memory_limit, lanes) * lanes,
            3 * (memory_limit / 4)
        );
    }

    #[test]
    fn lane_count_still_obeys_tasks_and_configured_parallelism() {
        assert_eq!(memory_bounded_lane_count(128 << 20, 18, 64), 18);
        assert_eq!(memory_bounded_lane_count(128 << 20, 18, 2), 2);
        assert_eq!(memory_bounded_lane_count(128 << 20, 1, 64), 1);
    }
}
