use std::{collections::VecDeque, sync::Arc, time::Instant};

use arrow::datatypes::SchemaRef;
use futures::StreamExt;
use tokio::sync::{Mutex, mpsc};
use tokio_util::sync::CancellationToken;

use crate::{
    Error, Result,
    runtime::{
        BatchEnvelope, MemoryBatchStream, QueryContext, SpillManager, boxed_memory_batch_stream,
    },
    sql::{BoundExpr, JoinType},
};

use super::{
    ProbeCursor, evaluate_keys_accounted, sort_merge,
    spill::{self, BuildPartition, MAX_REPARTITION_DEPTH, PartitionTask},
    try_build_hash_table,
};

pub(super) fn is_supported(context: &QueryContext, tasks: usize) -> bool {
    context.scheduler.configured_lanes() > 1 && tasks > 1
}

#[allow(clippy::too_many_arguments)]
pub(super) fn join(
    tasks: Vec<PartitionTask>,
    left_key_expressions: Vec<BoundExpr>,
    right_key_expressions: Vec<BoundExpr>,
    left_schema: SchemaRef,
    right_schema: SchemaRef,
    join_type: JoinType,
    schema: SchemaRef,
    context: Arc<QueryContext>,
    batch_size: usize,
) -> MemoryBatchStream {
    boxed_memory_batch_stream(async_stream::try_stream! {
        let lanes = context.scheduler.lanes_for(tasks.len());
        let pending = Arc::new(Mutex::new(VecDeque::from(tasks)));
        let cancellation = CancellationToken::new();
        let mut spill_cleanup = SpillCleanup::new(context.spill.clone());
        let _cancel_on_drop = CancelOnDrop(cancellation.clone());
        let (sender, mut receiver) = mpsc::channel(lanes.saturating_mul(2).max(2));

        for _ in 0..lanes {
            tokio::spawn(run_worker(
                Arc::clone(&pending),
                sender.clone(),
                cancellation.clone(),
                left_key_expressions.clone(),
                right_key_expressions.clone(),
                Arc::clone(&left_schema),
                Arc::clone(&right_schema),
                join_type,
                Arc::clone(&schema),
                Arc::clone(&context),
                batch_size.max(1),
            ));
        }
        drop(pending);
        drop(sender);

        let mut completed = 0usize;
        while completed < lanes {
            let message: Result<Option<WorkerMessage>> = tokio::select! {
                _ = context.control.cancelled() => Err(Error::Cancelled),
                message = receiver.recv() => Ok(message),
            };
            let message = match message {
                Ok(message) => message,
                Err(error) => Err(spill_cleanup.fail(error))?,
            };
            match message {
                Some(WorkerMessage::Batch(batch)) => yield batch,
                Some(WorkerMessage::Error(error)) => {
                    cancellation.cancel();
                    Err(spill_cleanup.fail(error))?;
                }
                Some(WorkerMessage::Done) => completed += 1,
                None => {
                    Err(spill_cleanup.fail(Error::Execution(
                        "parallel Grace join workers stopped before completing all partitions".into(),
                    )))?;
                }
            }
        }
        spill_cleanup.disarm();
    })
}

enum WorkerMessage {
    Batch(BatchEnvelope),
    Error(Error),
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
    join_type: JoinType,
    schema: SchemaRef,
    context: Arc<QueryContext>,
    batch_size: usize,
) {
    let result = run_worker_inner(
        pending,
        &sender,
        &cancellation,
        &left_key_expressions,
        &right_key_expressions,
        &left_schema,
        &right_schema,
        join_type,
        &schema,
        &context,
        batch_size,
    )
    .await;
    if let Err(error) = result
        && !cancellation.is_cancelled()
        && !context.control.is_cancelled()
    {
        let _ = sender.send(WorkerMessage::Error(error)).await;
        cancellation.cancel();
    }
    let _ = sender.send(WorkerMessage::Done).await;
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
    join_type: JoinType,
    schema: &SchemaRef,
    context: &Arc<QueryContext>,
    batch_size: usize,
) -> Result<()> {
    loop {
        let task = { pending.lock().await.pop_front() };
        let Some(task) = task else { break };
        let mut local = vec![task];
        let worker_pool = context.memory.child(
            format!("Grace-join-worker-{}", context.query_id),
            grace_worker_limit(context.memory.limit(), context.scheduler.configured_lanes()),
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
                        let hash_table = try_build_hash_table(
                            &right_keys,
                            rows,
                            matches!(join_type, JoinType::Semi | JoinType::Anti),
                            &mut reservation,
                        )?;
                        drop(right_keys);
                        match hash_table {
                            Some(hash_table) => TaskHashBuild::Ready(right_batch, hash_table),
                            None => TaskHashBuild::TooLarge(rows),
                        }
                    }
                    BuildPartition::TooLarge { rows } => TaskHashBuild::TooLarge(rows),
                }
            };
            match build {
                TaskHashBuild::Ready(right_batch, hash_table) => {
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
                            let mut probe = ProbeCursor::new(
                                left_batch.batch(),
                                &right_batch,
                                &left_keys,
                                &hash_table,
                                join_type,
                                Arc::clone(schema),
                                batch_size,
                                reservation
                                    .size()
                                    .saturating_add(left_batch.memory_size())
                                    .saturating_add(left_keys.memory_size()),
                            );
                            loop {
                                let output = probe.next_batch(context).await?;
                                let Some(output) = output else { break };
                                send(sender, output, cancellation, context).await?;
                            }
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

                    let mut fallback = sort_merge::fallback(
                        task,
                        left_key_expressions.to_vec(),
                        right_key_expressions.to_vec(),
                        Arc::clone(left_schema),
                        Arc::clone(right_schema),
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

enum TaskHashBuild {
    Ready(
        arrow::record_batch::RecordBatch,
        std::collections::HashMap<Vec<super::CellValue>, Vec<u32>>,
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

struct SpillCleanup {
    spill: SpillManager,
    armed: bool,
}

impl SpillCleanup {
    fn new(spill: SpillManager) -> Self {
        Self { spill, armed: true }
    }

    fn fail(&mut self, error: Error) -> Error {
        self.armed = false;
        match self.spill.cleanup() {
            Ok(()) => error,
            Err(cleanup) => Error::Execution(format!(
                "{error}; additionally failed to clean Grace join spill: {cleanup}"
            )),
        }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for SpillCleanup {
    fn drop(&mut self) {
        if self.armed
            && let Err(error) = self.spill.cleanup()
        {
            tracing::error!(%error, "failed to clean abandoned Grace join spill");
        }
    }
}
