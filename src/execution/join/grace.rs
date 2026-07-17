use std::{sync::Arc, time::Instant};

use arrow::datatypes::SchemaRef;
use futures::StreamExt;
use tokio::sync::{Barrier, Mutex, mpsc};
use tokio_util::sync::CancellationToken;

use crate::{
    Error, Result,
    runtime::{BatchEnvelope, MemoryBatchStream, QueryContext, boxed_memory_batch_stream},
    sql::{BoundExpr, JoinType},
};

mod admission;
mod build;

use admission::BuildAdmission;
use build::{TaskHashBuild, load_with_admission};

use super::{
    ProbeCursor,
    condition::JoinPredicates,
    evaluate_keys_accounted, evaluate_optional_values, optional_array, optional_memory,
    output::{BatchOutputTarget, JoinEmission, build_unmatched_right_envelope},
    sort_merge,
    spill::{self, PartitionTask},
};

const MIN_MEMORY_PER_LANE: usize = 512 << 10;

pub(super) fn is_supported(context: &QueryContext, tasks: &[PartitionTask]) -> bool {
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
        let lanes = lane_count(&context, &tasks);
        let admission = BuildAdmission::new(context.memory.limit());
        let pending = Arc::new(Mutex::new(tasks));
        let spill_generation = Arc::new(Mutex::new(()));
        let workers_ready = Arc::new(Barrier::new(lanes));
        let cancellation = CancellationToken::new();
        let _cancel_on_drop = CancelOnDrop(cancellation.clone());
        let (sender, mut receiver) = mpsc::channel(lanes.saturating_mul(2).max(2));

        for _ in 0..lanes {
            let worker = run_worker(
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
                admission.clone(),
                Arc::clone(&workers_ready),
                Arc::clone(&spill_generation),
            );
            if let Err(error) = context.tasks.spawn("grace-join-worker", worker) {
                cancellation.cancel();
                Err(error)?;
            }
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
    pending: Arc<Mutex<Vec<PartitionTask>>>,
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
    admission: BuildAdmission,
    workers_ready: Arc<Barrier>,
    spill_generation: Arc<Mutex<()>>,
) -> Result<()> {
    tokio::select! {
        _ = workers_ready.wait() => {}
        _ = cancellation.cancelled() => return Err(Error::Cancelled),
        _ = context.control.cancelled() => return Err(Error::Cancelled),
    }
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
        &admission,
        &spill_generation,
    )
    .await?;
    sender
        .send(WorkerMessage::Done)
        .await
        .map_err(|_| Error::Cancelled)
}

#[allow(clippy::too_many_arguments)]
async fn run_worker_inner(
    pending: Arc<Mutex<Vec<PartitionTask>>>,
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
    admission: &BuildAdmission,
    spill_generation: &Mutex<()>,
) -> Result<()> {
    loop {
        let task = {
            let mut pending = pending.lock().await;
            spill::pop_largest_task(&mut pending)
        };
        let Some(task) = task else { break };
        let mut local = vec![task];
        while let Some(task) = spill::pop_largest_task(&mut local) {
            check_running(cancellation, context)?;
            let (build, mut reservation, build_permit) = load_with_admission(
                &task,
                admission,
                cancellation,
                right_key_expressions,
                right_schema,
                predicates,
                null_equal_keys,
                join_type,
                left_schema.fields().len(),
                context,
            )
            .await?;
            match build {
                TaskHashBuild::Ready(right_batch, hash_table, right_values, matched_build) => {
                    for file in &task.left {
                        for left_batch in context.spill.read_file(file)? {
                            check_running(cancellation, context)?;
                            let left_batch = BatchEnvelope::try_new(
                                left_batch?,
                                &context.memory,
                                "join spill probe",
                            )?;
                            let (left_keys, left_values) = {
                                let _permit = context
                                    .acquire_compute_until_cancelled(cancellation)
                                    .await?;
                                let _active = context.scheduler.enter_lane();
                                let left_keys = evaluate_keys_accounted(
                                    left_key_expressions,
                                    left_batch.batch(),
                                    context,
                                    "Grace join probe keys",
                                )?;
                                let left_values = evaluate_optional_values(
                                    predicates.left_value(),
                                    left_batch.batch(),
                                    context,
                                    "Grace join probe membership value",
                                )?;
                                (left_keys, left_values)
                            };
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
                            let mut target = BatchOutputTarget;
                            loop {
                                match probe.next_output(&mut target, context).await? {
                                    JoinEmission::Batch(output) => {
                                        send(sender, output, cancellation, context).await?
                                    }
                                    JoinEmission::Consumed { .. } => {}
                                    JoinEmission::Exhausted => break,
                                }
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
                    drop(reservation);
                    drop(build_permit);
                    // Repartition and sort-merge both create Spill files while
                    // their parent partition remains live. Serialize those
                    // generations so every worker observes a stable query-wide
                    // file budget instead of racing to the 512-file ceiling.
                    let _spill_generation = tokio::select! {
                        _ = cancellation.cancelled() => return Err(Error::Cancelled),
                        _ = context.control.cancelled() => return Err(Error::Cancelled),
                        guard = spill_generation.lock() => guard,
                    };
                    if task.depth < context.execution.max_repartition_depth {
                        let next_depth = task.depth + 1;
                        let repartitioned = spill::repartition_scheduled(
                            &task,
                            left_key_expressions,
                            right_key_expressions,
                            join_type,
                            null_equal_keys,
                            next_depth,
                            context,
                            cancellation,
                        )
                        .await?;
                        if let Some(repartitioned) = repartitioned {
                            let shrank = repartitioned.largest_build_rows < rows;
                            if shrank || task.stagnant_repartitions == 0 {
                                spill::remove_task(context, &task)?;
                                let stagnant = if shrank {
                                    0
                                } else {
                                    task.stagnant_repartitions + 1
                                };
                                local.extend(repartitioned.tasks.into_iter().map(|mut child| {
                                    child.stagnant_repartitions = stagnant;
                                    child
                                }));
                                continue;
                            }
                            spill::remove_tasks(context, &repartitioned.tasks)?;
                        }
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
                }
            }
        }
    }
    Ok(())
}

fn lane_count(context: &QueryContext, tasks: &[PartitionTask]) -> usize {
    memory_bounded_lane_count(
        context.memory.limit(),
        context.scheduler.configured_lanes(),
        tasks.len(),
    )
}

fn memory_bounded_lane_count(query_limit: usize, configured_lanes: usize, tasks: usize) -> usize {
    let memory_lanes = query_limit
        .checked_div(MIN_MEMORY_PER_LANE)
        .unwrap_or(0)
        .max(1);
    configured_lanes.min(tasks).min(memory_lanes).max(1)
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
    use super::memory_bounded_lane_count;

    #[test]
    fn low_memory_limits_worker_count() {
        let memory_limit = 2 << 20;
        assert_eq!(memory_bounded_lane_count(memory_limit, 18, 64), 4);
    }

    #[test]
    fn lane_count_still_obeys_tasks_and_configured_parallelism() {
        assert_eq!(memory_bounded_lane_count(128 << 20, 18, 64), 18);
        assert_eq!(memory_bounded_lane_count(128 << 20, 18, 2), 2);
        assert_eq!(memory_bounded_lane_count(128 << 20, 1, 64), 1);
    }

    #[test]
    fn footprint_is_admitted_dynamically_instead_of_pre_slicing_workers() {
        assert_eq!(memory_bounded_lane_count(64 << 20, 8, 8), 8);
    }
}
