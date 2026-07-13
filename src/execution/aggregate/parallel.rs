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
                run_lane(
                    receiver,
                    partial_sender,
                    groups,
                    aggregates,
                    Arc::clone(&context),
                    batch_size,
                    partial_merge_admission,
                )
                .await?;
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
            let sent: Result<()> = tokio::select! {
                biased;
                _ = context.control.cancelled() => Err(context
                    .check_cancelled()
                    .expect_err("cancelled query has a terminal error")),
                event = lane_event_receiver.recv() => Err(input_lane_error(event)),
                result = sender.send(batch) => result.map_err(|_| {
                    match lane_event_receiver.try_recv() {
                        Ok(event) => input_lane_error(Some(event)),
                        Err(_) => Error::Execution(
                            "parallel aggregate lane stopped before input completed".into(),
                        ),
                    }
                }),
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
    mut receiver: mpsc::Receiver<BatchEnvelope>,
    partial_sender: mpsc::Sender<BatchEnvelope>,
    groups: Vec<BoundExpr>,
    aggregates: Vec<AggregateExpr>,
    context: Arc<QueryContext>,
    batch_size: usize,
    partial_merge_admission: PartialMergeAdmission,
) -> Result<()> {
    let lane_context = Arc::clone(&context);
    let lane_input = boxed_memory_batch_stream(async_stream::try_stream! {
        while let Some(batch) = receiver.recv().await {
            // The guard remains alive across `yield` until the partial
            // aggregator asks for its next batch, covering batch processing
            // without counting an idle receiver.
            let _active = lane_context.scheduler.enter_lane();
            yield batch;
        }
    });
    let mut partial = serial_partial_aggregate(
        lane_input,
        groups,
        aggregates,
        Arc::clone(&context),
        batch_size,
        partial_merge_admission,
    );
    while let Some(batch) = partial.next().await {
        partial_sender
            .send(batch?)
            .await
            .map_err(|_| Error::Cancelled)?;
    }
    #[cfg(test)]
    fault_injection::panic_if_armed(context.query_id);
    Ok(())
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

#[cfg(test)]
mod fault_injection {
    use std::{collections::HashSet, sync::Mutex};

    use uuid::Uuid;

    static PANIC_QUERIES: Mutex<Option<HashSet<Uuid>>> = Mutex::new(None);

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
}

#[cfg(test)]
mod tests;
