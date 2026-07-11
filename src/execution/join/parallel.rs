use std::{collections::HashMap, sync::Arc, time::Instant};

use arrow::record_batch::RecordBatch;
use futures::StreamExt;
use tokio::sync::{Mutex, mpsc};
use tokio_util::sync::CancellationToken;

use crate::{
    Error, Result,
    runtime::{
        BatchEnvelope, MemoryBatchStream, MemoryReservation, QueryContext,
        boxed_memory_batch_stream,
    },
    sql::{BoundExpr, JoinType},
};

use super::{CellValue, ProbeCursor, evaluate_keys_accounted};

const MIN_PARALLEL_MEMORY: usize = 64 << 20;

pub(super) struct FrozenBuild {
    batch: RecordBatch,
    hash_table: HashMap<Vec<CellValue>, Vec<u32>>,
    _memory: MemoryReservation,
}

impl FrozenBuild {
    pub(super) fn new(
        batch: RecordBatch,
        hash_table: HashMap<Vec<CellValue>, Vec<u32>>,
        memory: MemoryReservation,
    ) -> Self {
        Self {
            batch,
            hash_table,
            _memory: memory,
        }
    }

    fn memory_size(&self) -> usize {
        self._memory.size()
    }
}

pub(super) fn is_supported(context: &QueryContext, build_bytes: usize) -> bool {
    context.scheduler.configured_lanes() > 1
        && context.memory.limit() >= MIN_PARALLEL_MEMORY
        && build_bytes <= context.memory.limit() / 4
}

#[allow(clippy::too_many_arguments)]
pub(super) fn probe(
    left: MemoryBatchStream,
    left_keys: Vec<BoundExpr>,
    build: FrozenBuild,
    join_type: JoinType,
    schema: arrow::datatypes::SchemaRef,
    context: Arc<QueryContext>,
    batch_size: usize,
) -> MemoryBatchStream {
    boxed_memory_batch_stream(async_stream::try_stream! {
        let lanes = context.scheduler.configured_lanes();
        let input = Arc::new(Mutex::new(left));
        let build = Arc::new(build);
        let cancellation = CancellationToken::new();
        let _cancel_on_drop = CancelOnDrop(cancellation.clone());
        let (sender, mut receiver) = mpsc::channel(lanes.saturating_mul(2).max(2));

        for _ in 0..lanes {
            tokio::spawn(run_lane(
                Arc::clone(&input),
                sender.clone(),
                cancellation.clone(),
                left_keys.clone(),
                Arc::clone(&build),
                join_type,
                Arc::clone(&schema),
                Arc::clone(&context),
                batch_size.max(1),
            ));
        }
        drop(input);
        drop(build);
        drop(sender);

        let mut completed = 0;
        while completed < lanes {
            let message: Result<Option<LaneMessage>> = tokio::select! {
                _ = context.control.cancelled() => Err(Error::Cancelled),
                message = receiver.recv() => Ok(message),
            };
            let message = message?;
            match message {
                Some(LaneMessage::Batch(batch)) => yield batch,
                Some(LaneMessage::Error(error)) => {
                    cancellation.cancel();
                    Err(error)?;
                }
                Some(LaneMessage::Done) => completed += 1,
                None => {
                    cancellation.cancel();
                    Err(Error::Execution(format!(
                        "parallel hash join stopped after {completed} of {lanes} lanes completed"
                    )))?;
                }
            }
        }
    })
}

enum LaneMessage {
    Batch(BatchEnvelope),
    Error(Error),
    Done,
}

#[allow(clippy::too_many_arguments)]
async fn run_lane(
    input: Arc<Mutex<MemoryBatchStream>>,
    sender: mpsc::Sender<LaneMessage>,
    cancellation: CancellationToken,
    left_key_expressions: Vec<BoundExpr>,
    build: Arc<FrozenBuild>,
    join_type: JoinType,
    schema: arrow::datatypes::SchemaRef,
    context: Arc<QueryContext>,
    batch_size: usize,
) {
    let result = run_lane_inner(
        input,
        &sender,
        &cancellation,
        &left_key_expressions,
        &build,
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
        let _ = sender.send(LaneMessage::Error(error)).await;
        cancellation.cancel();
    }
    drop(build);
    let _ = sender.send(LaneMessage::Done).await;
}

#[allow(clippy::too_many_arguments)]
async fn run_lane_inner(
    input: Arc<Mutex<MemoryBatchStream>>,
    sender: &mpsc::Sender<LaneMessage>,
    cancellation: &CancellationToken,
    left_key_expressions: &[BoundExpr],
    build: &FrozenBuild,
    join_type: JoinType,
    schema: &arrow::datatypes::SchemaRef,
    context: &QueryContext,
    batch_size: usize,
) -> Result<()> {
    loop {
        check_running(cancellation, context)?;
        let next = {
            let mut input = input.lock().await;
            tokio::select! {
                _ = cancellation.cancelled() => return Ok(()),
                _ = context.control.cancelled() => return Err(Error::Cancelled),
                next = input.next() => next,
            }
        };
        let Some(left_batch) = next else {
            return Ok(());
        };
        let left_batch = left_batch?;
        let left_keys = {
            let _active = context.scheduler.enter_lane();
            evaluate_keys_accounted(
                left_key_expressions,
                left_batch.batch(),
                context,
                "parallel join probe keys",
            )?
        };
        let mut cursor = ProbeCursor::new(
            left_batch.batch(),
            &build.batch,
            &left_keys,
            &build.hash_table,
            join_type,
            Arc::clone(schema),
            batch_size,
            build
                .memory_size()
                .saturating_add(left_batch.memory_size())
                .saturating_add(left_keys.memory_size()),
        );
        loop {
            let output = cursor.next_batch(context).await?;
            let Some(output) = output else { break };
            send(sender, output, cancellation, context).await?;
        }
    }
}

async fn send(
    sender: &mpsc::Sender<LaneMessage>,
    batch: BatchEnvelope,
    cancellation: &CancellationToken,
    context: &QueryContext,
) -> Result<()> {
    let started = Instant::now();
    let result = tokio::select! {
        _ = cancellation.cancelled() => return Err(Error::Cancelled),
        _ = context.control.cancelled() => return Err(Error::Cancelled),
        result = sender.send(LaneMessage::Batch(batch)) => result,
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
