use std::{sync::Arc, time::Instant};

use arrow::datatypes::SchemaRef;
use futures::StreamExt;
use tokio::sync::mpsc;

use crate::{
    Error, Result,
    runtime::{BatchEnvelope, MemoryBatchStream, QueryContext, boxed_memory_batch_stream},
    sql::{AggregateExpr, BoundExpr},
};

use super::{admission::PartialMergeAdmission, merge_partial_aggregate, serial_partial_aggregate};

const MIN_PARALLEL_MEMORY: usize = 64 << 20;

enum PartialEvent {
    Lane(Option<LaneEvent>),
    Batch(Option<BatchEnvelope>),
    Error(Error),
}

enum LaneEvent {
    Done,
}

pub(super) fn is_supported(aggregates: &[AggregateExpr], context: &QueryContext) -> bool {
    context.scheduler.configured_lanes() > 1
        && context.memory.limit() >= MIN_PARALLEL_MEMORY
        && !aggregates.is_empty()
}

pub(super) fn aggregate(
    mut input: MemoryBatchStream,
    groups: Vec<BoundExpr>,
    aggregates: Vec<AggregateExpr>,
    schema: SchemaRef,
    context: Arc<QueryContext>,
    batch_size: usize,
) -> MemoryBatchStream {
    boxed_memory_batch_stream(async_stream::try_stream! {
        let lanes = context.scheduler.configured_lanes();
        let (partial_sender, partial_receiver) = mpsc::channel(lanes.saturating_mul(2).max(2));
        let (lane_event_sender, mut lane_event_receiver) = mpsc::unbounded_channel();
        let mut lane_senders = Vec::with_capacity(lanes);
        let partial_merge_admission = PartialMergeAdmission::new(lanes);

        for _ in 0..lanes {
            let (sender, receiver) = mpsc::channel(1);
            lane_senders.push(sender);
            let partial_sender = partial_sender.clone();
            let lane_event_sender = lane_event_sender.clone();
            let groups = groups.clone();
            let aggregates = aggregates.clone();
            let context = Arc::clone(&context);
            let partial_merge_admission = partial_merge_admission.clone();
            let tasks = context.tasks.clone();
            tasks.spawn("aggregate-partial-lane", async move {
                let result = run_lane(
                    receiver,
                    partial_sender,
                    groups,
                    aggregates,
                    Arc::clone(&context),
                    batch_size,
                    partial_merge_admission,
                )
                .await;
                if result.is_err() {
                    hold_input_failure_publication_for_test(context.query_id).await;
                }
                result?;
                lane_event_sender
                    .send(LaneEvent::Done)
                    .map_err(|_| Error::Cancelled)
            })?;
        }
        drop(partial_sender);
        // Keep one coordinator sender alive until every lane has reported
        // completion. A panicking task drops its own sender while unwinding,
        // before TaskGroup has captured the panic and cancelled siblings. If
        // the coordinator sender were dropped here too, the receiver could
        // observe a closed channel first and replace the real task failure
        // with a generic "lane stopped" error.

        let mut next_lane = 0;
        while let Some(batch) = input.next().await {
            context.check_cancelled()?;
            let batch = batch?;
            let sender = &lane_senders[next_lane];
            let started = Instant::now();
            let sent: Result<()> = match sender.try_send(batch) {
                Ok(()) => Ok(()),
                Err(mpsc::error::TrySendError::Full(batch)) => {
                    let backpressure_started = Instant::now();
                    release_input_failure_for_test(context.query_id);
                    let outcome: Option<Result<()>> = tokio::select! {
                        biased;
                        _ = context.control.cancelled() => Some(Err(context
                            .check_cancelled()
                            .expect_err("cancelled query has a terminal error"))),
                        event = lane_event_receiver.recv() => Some(Err(input_lane_error(event))),
                        result = sender.send(batch) => match result {
                            Ok(()) => Some(Ok(())),
                            Err(_) => None,
                        },
                    };
                    context
                        .metrics
                        .record_aggregate_lane_dispatch_queue_wait(
                            backpressure_started.elapsed(),
                        );
                    match outcome {
                        Some(result) => result,
                        None => {
                            release_failure_publication_for_test(context.query_id);
                            Err(wait_for_lane_failure(&mut lane_event_receiver, &context).await)
                        }
                    }
                }
                Err(mpsc::error::TrySendError::Closed(_)) => {
                    release_failure_publication_for_test(context.query_id);
                    Err(wait_for_lane_failure(&mut lane_event_receiver, &context).await)
                }
            };
            context.scheduler.record_wait(started.elapsed());
            sent?;
            next_lane = (next_lane + 1) % lanes;
        }
        drop(lane_senders);

        let partial_context = Arc::clone(&context);
        let partial_stream = boxed_memory_batch_stream(async_stream::try_stream! {
            let mut receiver = partial_receiver;
            let mut completed = 0usize;
            let mut batches_open = true;
            while completed < lanes {
                let event = if batches_open {
                    tokio::select! {
                        biased;
                        _ = partial_context.control.cancelled() => PartialEvent::Error(
                            partial_context
                                .check_cancelled()
                                .expect_err("cancelled query has a terminal error")
                        ),
                        lane = lane_event_receiver.recv() => PartialEvent::Lane(lane),
                        batch = receiver.recv() => PartialEvent::Batch(batch),
                    }
                } else {
                    tokio::select! {
                        biased;
                        _ = partial_context.control.cancelled() => PartialEvent::Error(
                            partial_context
                                .check_cancelled()
                                .expect_err("cancelled query has a terminal error")
                        ),
                        lane = lane_event_receiver.recv() => PartialEvent::Lane(lane),
                    }
                };
                match event {
                    PartialEvent::Error(error) => Err(error)?,
                    PartialEvent::Lane(Some(LaneEvent::Done)) => completed += 1,
                    PartialEvent::Lane(None) => {
                        Err(Error::Execution(format!(
                            "parallel aggregate stopped after {completed} of {lanes} lanes completed"
                        )))?;
                    }
                    PartialEvent::Batch(Some(batch)) => yield batch,
                    PartialEvent::Batch(None) => batches_open = false,
                }
            }
            while let Some(batch) = receiver.recv().await {
                yield batch;
            }
        });
        let mut output = merge_partial_aggregate(
            partial_stream,
            groups,
            aggregates,
            schema,
            context,
            batch_size,
        );
        while let Some(batch) = output.next().await {
            yield batch?;
        }
        drop(lane_event_sender);
    })
}

async fn run_lane(
    receiver: mpsc::Receiver<BatchEnvelope>,
    partial_sender: mpsc::Sender<BatchEnvelope>,
    groups: Vec<BoundExpr>,
    aggregates: Vec<AggregateExpr>,
    context: Arc<QueryContext>,
    batch_size: usize,
    partial_merge_admission: PartialMergeAdmission,
) -> Result<()> {
    let lane_input = lane_input_stream(receiver, Arc::clone(&context));
    let mut partial = serial_partial_aggregate(
        lane_input,
        groups,
        aggregates,
        Arc::clone(&context),
        batch_size,
        partial_merge_admission,
    );
    while let Some(batch) = partial.next().await {
        let batch = batch?;
        let sent = match partial_sender.try_send(batch) {
            Ok(()) => Ok(()),
            Err(mpsc::error::TrySendError::Full(batch)) => {
                let started = Instant::now();
                let result = tokio::select! {
                    _ = context.control.cancelled() => Err(context
                        .check_cancelled()
                        .expect_err("cancelled query has a terminal error")),
                    result = partial_sender.send(batch) => {
                        result.map_err(|_| Error::Cancelled)
                    },
                };
                context
                    .metrics
                    .record_aggregate_partial_output_queue_wait(started.elapsed());
                result
            }
            Err(mpsc::error::TrySendError::Closed(_)) => Err(Error::Cancelled),
        };
        sent?;
    }
    #[cfg(test)]
    fault_injection::panic_if_armed(context.query_id);
    Ok(())
}

#[cfg(not(test))]
fn lane_input_stream(
    mut receiver: mpsc::Receiver<BatchEnvelope>,
    context: Arc<QueryContext>,
) -> MemoryBatchStream {
    boxed_memory_batch_stream(async_stream::try_stream! {
        while let Some(batch) = receiver.recv().await {
            // Both guards remain alive across `yield` until the partial
            // aggregator asks for its next batch. Channel waits therefore do
            // not occupy global compute capacity, while batch processing does.
            let _compute = context.acquire_compute().await?;
            let _active = context.scheduler.enter_lane();
            yield batch;
        }
    })
}

#[cfg(test)]
fn lane_input_stream(
    mut receiver: mpsc::Receiver<BatchEnvelope>,
    context: Arc<QueryContext>,
) -> MemoryBatchStream {
    boxed_memory_batch_stream(async_stream::try_stream! {
        while let Some(batch) = receiver.recv().await {
            fault_injection::fail_input_on_dispatch_pressure(context.query_id).await?;
            let _compute = context.acquire_compute().await?;
            let _active = context.scheduler.enter_lane();
            yield batch;
        }
    })
}

#[inline]
fn release_input_failure_for_test(query_id: uuid::Uuid) {
    #[cfg(test)]
    fault_injection::release_input_failure(query_id);
    #[cfg(not(test))]
    let _ = query_id;
}

async fn hold_input_failure_publication_for_test(query_id: uuid::Uuid) {
    #[cfg(test)]
    fault_injection::hold_input_failure_publication(query_id).await;
    #[cfg(not(test))]
    let _ = query_id;
}

#[inline]
fn release_failure_publication_for_test(query_id: uuid::Uuid) {
    #[cfg(test)]
    fault_injection::release_failure_publication(query_id);
    #[cfg(not(test))]
    let _ = query_id;
}

fn input_lane_error(event: Option<LaneEvent>) -> Error {
    match event {
        Some(LaneEvent::Done) => {
            Error::Execution("parallel aggregate lane stopped before input completed".into())
        }
        None => {
            Error::Execution("all parallel aggregate lanes stopped before input completed".into())
        }
    }
}

async fn wait_for_lane_failure(
    lane_events: &mut mpsc::UnboundedReceiver<LaneEvent>,
    context: &QueryContext,
) -> Error {
    tokio::select! {
        biased;
        _ = context.control.cancelled() => context
            .check_cancelled()
            .expect_err("cancelled query has a terminal error"),
        event = lane_events.recv() => context
            .tasks
            .first_failure()
            .unwrap_or_else(|| input_lane_error(event)),
    }
}

#[cfg(test)]
mod fault_injection {
    use std::{
        collections::{HashMap, HashSet},
        sync::{Arc, Mutex},
    };

    use tokio::sync::Notify;
    use uuid::Uuid;

    use crate::{Error, Result};

    static PANIC_QUERIES: Mutex<Option<HashSet<Uuid>>> = Mutex::new(None);
    static INPUT_FAILURE_QUERIES: Mutex<Option<HashMap<Uuid, InputFailure>>> = Mutex::new(None);

    struct InputFailure {
        claimed: bool,
        input_release: Arc<Notify>,
        publication_release: Arc<Notify>,
    }

    pub(super) fn arm(query_id: Uuid) {
        PANIC_QUERIES
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .get_or_insert_with(HashSet::new)
            .insert(query_id);
    }

    pub(super) fn panic_if_armed(query_id: Uuid) {
        let armed = PANIC_QUERIES
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .as_mut()
            .is_some_and(|queries| queries.remove(&query_id));
        if armed {
            panic!("injected parallel aggregate lane panic");
        }
    }

    pub(super) fn arm_input_failure(query_id: Uuid) {
        INPUT_FAILURE_QUERIES
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .get_or_insert_with(HashMap::new)
            .insert(
                query_id,
                InputFailure {
                    claimed: false,
                    input_release: Arc::new(Notify::new()),
                    publication_release: Arc::new(Notify::new()),
                },
            );
    }

    pub(super) async fn fail_input_on_dispatch_pressure(query_id: Uuid) -> Result<()> {
        let release = INPUT_FAILURE_QUERIES
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .as_mut()
            .and_then(|queries| queries.get_mut(&query_id))
            .and_then(|failure| {
                (!failure.claimed).then(|| {
                    failure.claimed = true;
                    Arc::clone(&failure.input_release)
                })
            });
        let Some(release) = release else {
            return Ok(());
        };
        release.notified().await;
        Err(Error::Execution(
            "injected parallel aggregate lane input failure".into(),
        ))
    }

    pub(super) fn release_input_failure(query_id: Uuid) {
        let release = INPUT_FAILURE_QUERIES
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .as_ref()
            .and_then(|queries| queries.get(&query_id))
            .map(|failure| Arc::clone(&failure.input_release));
        if let Some(release) = release {
            release.notify_one();
        }
    }

    pub(super) async fn hold_input_failure_publication(query_id: Uuid) {
        let release = INPUT_FAILURE_QUERIES
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .as_ref()
            .and_then(|queries| queries.get(&query_id))
            .filter(|failure| failure.claimed)
            .map(|failure| Arc::clone(&failure.publication_release));
        if let Some(release) = release {
            release.notified().await;
        }
    }

    pub(super) fn release_failure_publication(query_id: Uuid) {
        let release = INPUT_FAILURE_QUERIES
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .as_mut()
            .and_then(|queries| queries.remove(&query_id))
            .map(|failure| failure.publication_release);
        if let Some(release) = release {
            release.notify_one();
        }
    }
}

#[cfg(test)]
mod tests;
