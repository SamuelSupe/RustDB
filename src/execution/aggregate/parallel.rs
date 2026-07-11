use std::{panic::AssertUnwindSafe, sync::Arc, time::Instant};

use arrow::datatypes::SchemaRef;
use futures::{FutureExt, StreamExt};
use tokio::sync::mpsc;

use crate::{
    Error, Result,
    runtime::{BatchEnvelope, MemoryBatchStream, QueryContext, boxed_memory_batch_stream},
    sql::{AggregateExpr, BoundExpr},
};

use super::{merge_partial_aggregate, serial_partial_aggregate};

const MIN_PARALLEL_MEMORY: usize = 64 << 20;

enum PartialEvent {
    Lane(Option<LaneEvent>),
    Batch(Option<BatchEnvelope>),
}

enum LaneEvent {
    Error(Error),
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

        for _ in 0..lanes {
            let (sender, receiver) = mpsc::channel(1);
            lane_senders.push(sender);
            let partial_sender = partial_sender.clone();
            let lane_event_sender = lane_event_sender.clone();
            let groups = groups.clone();
            let aggregates = aggregates.clone();
            let context = Arc::clone(&context);
            tokio::spawn(async move {
                let result = AssertUnwindSafe(run_lane(
                    receiver,
                    partial_sender,
                    groups,
                    aggregates,
                    Arc::clone(&context),
                    batch_size,
                ))
                .catch_unwind()
                .await
                .unwrap_or_else(|_| {
                    Err(Error::Internal(
                        "parallel aggregate lane panicked".to_owned(),
                    ))
                });
                let event = match result {
                    Ok(()) => LaneEvent::Done,
                    Err(error) => LaneEvent::Error(error),
                };
                let _ = lane_event_sender.send(event);
            });
        }
        drop(partial_sender);
        drop(lane_event_sender);

        let mut next_lane = 0;
        while let Some(batch) = input.next().await {
            context.check_cancelled()?;
            let batch = batch?;
            let sender = &lane_senders[next_lane];
            let started = Instant::now();
            let sent: Result<()> = tokio::select! {
                biased;
                _ = context.control.cancelled() => Err(Error::Cancelled),
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

        let partial_stream = boxed_memory_batch_stream(async_stream::try_stream! {
            let mut receiver = partial_receiver;
            let mut completed = 0usize;
            let mut batches_open = true;
            while completed < lanes {
                let event = if batches_open {
                    tokio::select! {
                        biased;
                        lane = lane_event_receiver.recv() => PartialEvent::Lane(lane),
                        batch = receiver.recv() => PartialEvent::Batch(batch),
                    }
                } else {
                    PartialEvent::Lane(lane_event_receiver.recv().await)
                };
                match event {
                    PartialEvent::Lane(Some(LaneEvent::Error(error))) => Err(error)?,
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
    })
}

async fn run_lane(
    mut receiver: mpsc::Receiver<BatchEnvelope>,
    partial_sender: mpsc::Sender<BatchEnvelope>,
    groups: Vec<BoundExpr>,
    aggregates: Vec<AggregateExpr>,
    context: Arc<QueryContext>,
    batch_size: usize,
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
        Some(LaneEvent::Error(error)) => error,
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
mod tests {
    use std::{collections::HashSet, sync::Arc, time::Duration};

    use arrow::{
        array::{Float64Array, Int64Array, StringArray},
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    };
    use futures::{TryStreamExt, stream};

    use super::fault_injection;
    use crate::{
        execution::aggregate::aggregate,
        runtime::{MemoryPool, QueryContext, boxed_record_batch_stream},
        sql::{AggregateExpr, AggregateFunction, BoundExpr},
    };

    #[tokio::test]
    async fn float_sum_and_average_use_partial_final_aggregation() {
        let input_schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Float64,
            false,
        )]));
        let batches = (0..8).map(move |_| {
            Ok(RecordBatch::try_new(
                Arc::clone(&input_schema),
                vec![Arc::new(Float64Array::from(vec![1.0, 2.0]))],
            )
            .unwrap())
        });
        let output_schema = Arc::new(Schema::new(vec![
            Field::new("total", DataType::Float64, true),
            Field::new("average", DataType::Float64, true),
        ]));
        let root = tempfile::tempdir().unwrap();
        let context = Arc::new(QueryContext::new(MemoryPool::new(64 << 20), root.path()).unwrap());
        context.configure_compute_lanes(4);
        let expressions = vec![
            AggregateExpr {
                function: AggregateFunction::Sum,
                expr: Some(BoundExpr::column(0, DataType::Float64, "value")),
                data_type: DataType::Float64,
                display_name: "sum(value)".into(),
            },
            AggregateExpr {
                function: AggregateFunction::Avg,
                expr: Some(BoundExpr::column(0, DataType::Float64, "value")),
                data_type: DataType::Float64,
                display_name: "avg(value)".into(),
            },
        ];

        let batches = aggregate(
            boxed_record_batch_stream(stream::iter(batches)),
            Vec::new(),
            expressions,
            output_schema,
            context,
            64,
        )
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
        let total = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        let average = batches[0]
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(total.value(0), 24.0);
        assert_eq!(average.value(0), 1.5);
    }

    #[tokio::test]
    async fn high_cardinality_count_finishes_without_partial_channel_deadlock() {
        const LANES: usize = 4;
        const ROWS: i64 = 12_000;
        let input_schema = Arc::new(Schema::new(vec![Field::new("key", DataType::Int64, false)]));
        let batches = (0..12)
            .map(|partition| {
                let start = partition * 1_000;
                RecordBatch::try_new(
                    Arc::clone(&input_schema),
                    vec![Arc::new(Int64Array::from_iter_values(start..start + 1_000))],
                )
                .unwrap()
            })
            .collect::<Vec<_>>();
        let input = boxed_record_batch_stream(stream::iter(batches.into_iter().map(Ok)));
        let output_schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::Int64, false),
            Field::new("rows", DataType::Int64, false),
            Field::new("total", DataType::Int64, false),
        ]));
        let root = tempfile::tempdir().unwrap();
        let context = Arc::new(QueryContext::new(MemoryPool::new(128 << 20), root.path()).unwrap());
        context.configure_compute_lanes(LANES);
        let output = aggregate(
            input,
            vec![BoundExpr::column(0, DataType::Int64, "key")],
            vec![
                AggregateExpr {
                    function: AggregateFunction::Count,
                    expr: None,
                    data_type: DataType::Int64,
                    display_name: "count(*)".into(),
                },
                AggregateExpr {
                    function: AggregateFunction::Sum,
                    expr: Some(BoundExpr::column(0, DataType::Int64, "key")),
                    data_type: DataType::Int64,
                    display_name: "sum(key)".into(),
                },
            ],
            output_schema,
            Arc::clone(&context),
            64,
        );
        let batches = tokio::time::timeout(Duration::from_secs(5), output.try_collect::<Vec<_>>())
            .await
            .expect("parallel partial/final aggregation must not deadlock")
            .unwrap();

        let mut keys = HashSet::new();
        for batch in &batches {
            let batch_keys = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            let counts = batch
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            let sums = batch
                .column(2)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            for row in 0..batch.num_rows() {
                let key = batch_keys.value(row);
                keys.insert(key);
                assert_eq!(counts.value(row), 1);
                assert_eq!(sums.value(row), key);
            }
        }
        assert_eq!(keys.len(), ROWS as usize);
        let peak = context.metrics.snapshot().peak_active_lanes;
        assert!((1..=LANES as u64).contains(&peak), "unexpected peak {peak}");
    }

    #[tokio::test]
    async fn idle_aggregate_receivers_are_not_counted_as_active_lanes() {
        let input_schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            false,
        )]));
        let batch = RecordBatch::try_new(
            input_schema,
            vec![Arc::new(Int64Array::from(vec![1_i64, 2, 3]))],
        )
        .unwrap();
        let input = boxed_record_batch_stream(stream::iter([Ok(batch)]));
        let output_schema = Arc::new(Schema::new(vec![Field::new(
            "rows",
            DataType::Int64,
            false,
        )]));
        let root = tempfile::tempdir().unwrap();
        let context = Arc::new(QueryContext::new(MemoryPool::new(64 << 20), root.path()).unwrap());
        context.configure_compute_lanes(4);
        aggregate(
            input,
            Vec::new(),
            vec![AggregateExpr {
                function: AggregateFunction::Count,
                expr: None,
                data_type: DataType::Int64,
                display_name: "count(*)".into(),
            }],
            output_schema,
            Arc::clone(&context),
            64,
        )
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
        assert_eq!(context.metrics.snapshot().peak_active_lanes, 1);
    }

    #[tokio::test]
    async fn lane_error_is_propagated_without_generic_channel_error() {
        let input_schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Utf8,
            false,
        )]));
        let batches = (0..8).map(move |_| {
            Ok(RecordBatch::try_new(
                Arc::clone(&input_schema),
                vec![Arc::new(StringArray::from(vec!["not-an-integer"]))],
            )
            .unwrap())
        });
        let input = boxed_record_batch_stream(stream::iter(batches));
        let output_schema = Arc::new(Schema::new(vec![Field::new(
            "total",
            DataType::Int64,
            true,
        )]));
        let root = tempfile::tempdir().unwrap();
        let context = Arc::new(QueryContext::new(MemoryPool::new(64 << 20), root.path()).unwrap());
        context.configure_compute_lanes(4);
        let error = aggregate(
            input,
            Vec::new(),
            vec![AggregateExpr {
                function: AggregateFunction::Sum,
                expr: Some(BoundExpr::column(0, DataType::Int64, "value")),
                data_type: DataType::Int64,
                display_name: "sum(value)".into(),
            }],
            output_schema,
            context,
            64,
        )
        .try_collect::<Vec<_>>()
        .await
        .unwrap_err();
        let message = error.to_string();
        assert!(message.contains("incompatible value"), "{message}");
        assert!(!message.contains("lane stopped"), "{message}");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn lane_panic_is_a_terminal_error_not_partial_success() {
        let input_schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            false,
        )]));
        let batches = (0..2_i64).map(move |value| {
            Ok(RecordBatch::try_new(
                Arc::clone(&input_schema),
                vec![Arc::new(Int64Array::from(vec![value]))],
            )
            .unwrap())
        });
        let input = boxed_record_batch_stream(stream::iter(batches));
        let output_schema = Arc::new(Schema::new(vec![Field::new(
            "rows",
            DataType::Int64,
            false,
        )]));
        let root = tempfile::tempdir().unwrap();
        let context = Arc::new(QueryContext::new(MemoryPool::new(64 << 20), root.path()).unwrap());
        context.configure_compute_lanes(2);
        fault_injection::arm(context.query_id);

        let output = aggregate(
            input,
            Vec::new(),
            vec![AggregateExpr {
                function: AggregateFunction::Count,
                expr: None,
                data_type: DataType::Int64,
                display_name: "count(*)".into(),
            }],
            output_schema,
            Arc::clone(&context),
            64,
        );
        let error = tokio::time::timeout(Duration::from_secs(5), output.try_collect::<Vec<_>>())
            .await
            .expect("panicked aggregate lane must terminate the query")
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("parallel aggregate lane panicked"),
            "unexpected error: {error}"
        );

        tokio::time::timeout(Duration::from_secs(2), async {
            while context.memory.used() != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("all aggregate lane reservations must be released");
    }
}
