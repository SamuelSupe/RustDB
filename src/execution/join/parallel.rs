use std::{collections::HashMap, sync::Arc, time::Instant};

#[cfg(test)]
use std::{
    collections::HashSet,
    sync::{Mutex as StdMutex, OnceLock},
};

use arrow::record_batch::RecordBatch;
use futures::StreamExt;
#[cfg(test)]
use tokio::sync::Barrier;
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

use super::{
    CellValue, EvaluatedKeys, ProbeCursor, condition::JoinPredicates, evaluate_keys_accounted,
    evaluate_optional_values, matched::BuildMatchTracker, optional_array, optional_memory,
    output::build_unmatched_right_envelope, probe::GlobalMembershipState,
};

const MIN_PARALLEL_MEMORY: usize = 64 << 20;

pub(super) struct FrozenBuild {
    batch: RecordBatch,
    hash_table: HashMap<Vec<CellValue>, Vec<u32>>,
    right_values: Option<EvaluatedKeys>,
    global_membership: Option<GlobalMembershipState>,
    null_equal_keys: bool,
    matched_build: Option<BuildMatchTracker>,
    _memory: MemoryReservation,
}

impl FrozenBuild {
    pub(super) fn new(
        batch: RecordBatch,
        hash_table: HashMap<Vec<CellValue>, Vec<u32>>,
        right_values: Option<EvaluatedKeys>,
        global_membership: Option<GlobalMembershipState>,
        null_equal_keys: bool,
        matched_build: Option<BuildMatchTracker>,
        memory: MemoryReservation,
    ) -> Self {
        Self {
            batch,
            hash_table,
            right_values,
            global_membership,
            null_equal_keys,
            matched_build,
            _memory: memory,
        }
    }

    fn memory_size(&self) -> usize {
        self._memory
            .size()
            .saturating_add(optional_memory(&self.right_values))
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
    predicates: JoinPredicates,
    join_type: JoinType,
    left_schema: arrow::datatypes::SchemaRef,
    schema: arrow::datatypes::SchemaRef,
    context: Arc<QueryContext>,
    batch_size: usize,
) -> MemoryBatchStream {
    boxed_memory_batch_stream(async_stream::try_stream! {
        let lanes = context.scheduler.configured_lanes();
        #[cfg(test)]
        let probe_start = take_probe_start(context.query_id, lanes);
        let input = Arc::new(Mutex::new(left));
        let build = Arc::new(build);
        let cancellation = CancellationToken::new();
        let _cancel_on_drop = CancelOnDrop(cancellation.clone());
        let (sender, mut receiver) = mpsc::channel(lanes.saturating_mul(2).max(2));

        for _ in 0..lanes {
            context.tasks.spawn("hash-join-probe-lane", run_lane(
                Arc::clone(&input),
                sender.clone(),
                cancellation.clone(),
                left_keys.clone(),
                Arc::clone(&build),
                predicates.clone(),
                join_type,
                Arc::clone(&schema),
                Arc::clone(&context),
                batch_size.max(1),
                #[cfg(test)]
                probe_start.clone(),
            ))?;
        }
        drop(input);
        // Keep the coordinator sender alive across worker unwinding so query
        // cancellation exposes TaskGroup's real panic/error before the result
        // channel can appear generically closed.

        let mut completed = 0;
        while completed < lanes {
            let message: Result<Option<LaneMessage>> = tokio::select! {
                biased;
                _ = context.control.cancelled() => Err(context
                    .check_cancelled()
                    .expect_err("cancelled query has a terminal error")),
                message = receiver.recv() => Ok(message),
            };
            let message = message?;
            match message {
                Some(LaneMessage::Batch(batch)) => yield batch,
                Some(LaneMessage::Done) => completed += 1,
                None => {
                    cancellation.cancel();
                    Err(Error::Execution(format!(
                        "parallel hash join stopped after {completed} of {lanes} lanes completed"
                    )))?;
                }
            }
        }
        if let Some(matched) = &build.matched_build {
            let mut start = 0;
            loop {
                let indices = matched
                    .unmatched_from(
                        start,
                        batch_size.max(1),
                        &context,
                        build.memory_size(),
                    )
                    .await?;
                let Some(last) = indices.last().copied() else { break };
                start = last as usize + 1;
                yield build_unmatched_right_envelope(
                    &left_schema,
                    &build.batch,
                    &indices,
                    Arc::clone(&schema),
                    &context,
                    build.memory_size().saturating_add(indices.memory_size()),
                ).await?;
            }
        }
        drop(sender);
    })
}

enum LaneMessage {
    Batch(BatchEnvelope),
    Done,
}

#[allow(clippy::too_many_arguments)]
async fn run_lane(
    input: Arc<Mutex<MemoryBatchStream>>,
    sender: mpsc::Sender<LaneMessage>,
    cancellation: CancellationToken,
    left_key_expressions: Vec<BoundExpr>,
    build: Arc<FrozenBuild>,
    predicates: JoinPredicates,
    join_type: JoinType,
    schema: arrow::datatypes::SchemaRef,
    context: Arc<QueryContext>,
    batch_size: usize,
    #[cfg(test)] probe_start: Option<Arc<Barrier>>,
) -> Result<()> {
    run_lane_inner(
        input,
        &sender,
        &cancellation,
        &left_key_expressions,
        &build,
        &predicates,
        join_type,
        &schema,
        &context,
        batch_size,
        #[cfg(test)]
        probe_start,
    )
    .await?;
    drop(build);
    sender
        .send(LaneMessage::Done)
        .await
        .map_err(|_| Error::Cancelled)
}

#[allow(clippy::too_many_arguments)]
async fn run_lane_inner(
    input: Arc<Mutex<MemoryBatchStream>>,
    sender: &mpsc::Sender<LaneMessage>,
    cancellation: &CancellationToken,
    left_key_expressions: &[BoundExpr],
    build: &FrozenBuild,
    predicates: &JoinPredicates,
    join_type: JoinType,
    schema: &arrow::datatypes::SchemaRef,
    context: &QueryContext,
    batch_size: usize,
    #[cfg(test)] probe_start: Option<Arc<Barrier>>,
) -> Result<()> {
    #[cfg(test)]
    if let Some(probe_start) = probe_start {
        let _active = context.scheduler.enter_lane();
        probe_start.wait().await;
    }
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
        let left_values = {
            let _active = context.scheduler.enter_lane();
            evaluate_optional_values(
                predicates.left_value(),
                left_batch.batch(),
                context,
                "parallel join probe membership value",
            )?
        };
        let left_keys = if build.global_membership.is_some() {
            None
        } else {
            let _active = context.scheduler.enter_lane();
            Some(evaluate_keys_accounted(
                left_key_expressions,
                left_batch.batch(),
                context,
                "parallel join probe keys",
            )?)
        };
        let probe_keys = if build.global_membership.is_some() {
            std::slice::from_ref(optional_array(&left_values).ok_or_else(|| {
                Error::Internal("global membership hash is missing its left value array".into())
            })?)
        } else {
            left_keys
                .as_deref()
                .expect("regular join evaluated its keys")
        };
        let mut cursor = ProbeCursor::new(
            left_batch.batch(),
            &build.batch,
            probe_keys,
            &build.hash_table,
            predicates,
            optional_array(&left_values),
            optional_array(&build.right_values),
            build.global_membership,
            build.null_equal_keys,
            build.matched_build.clone(),
            join_type,
            Arc::clone(schema),
            batch_size,
            build
                .memory_size()
                .saturating_add(left_batch.memory_size())
                .saturating_add(optional_memory(&left_keys))
                .saturating_add(optional_memory(&left_values)),
        );
        loop {
            let output = cursor.next_batch(context).await?;
            let Some(output) = output else { break };
            send(sender, output, cancellation, context).await?;
        }
    }
}

#[cfg(test)]
pub(super) fn synchronize_probe_start(query_id: uuid::Uuid) {
    probe_start_requests()
        .lock()
        .expect("probe-start request lock poisoned")
        .insert(query_id);
}

#[cfg(test)]
fn take_probe_start(query_id: uuid::Uuid, lanes: usize) -> Option<Arc<Barrier>> {
    probe_start_requests()
        .lock()
        .expect("probe-start request lock poisoned")
        .remove(&query_id)
        .then(|| Arc::new(Barrier::new(lanes)))
}

#[cfg(test)]
fn probe_start_requests() -> &'static StdMutex<HashSet<uuid::Uuid>> {
    static REQUESTS: OnceLock<StdMutex<HashSet<uuid::Uuid>>> = OnceLock::new();
    REQUESTS.get_or_init(|| StdMutex::new(HashSet::new()))
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
