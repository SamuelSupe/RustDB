use std::{collections::HashSet, sync::Arc, time::Duration};

use arrow::{
    array::{Decimal128Array, Float64Array, Int64Array, StringArray},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use futures::{TryStreamExt, stream};

use super::fault_injection;
use crate::{
    Engine, EngineConfig,
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
            distinct: false,
            data_type: DataType::Float64,
            display_name: "sum(value)".into(),
        },
        AggregateExpr {
            function: AggregateFunction::Avg,
            expr: Some(BoundExpr::column(0, DataType::Float64, "value")),
            distinct: false,
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
async fn high_cardinality_count_reports_partial_queue_edge_wait_without_deadlock() {
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
        Field::new("total", DataType::Decimal128(38, 0), false),
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
                distinct: false,
                data_type: DataType::Int64,
                display_name: "count(*)".into(),
            },
            AggregateExpr {
                function: AggregateFunction::Sum,
                expr: Some(BoundExpr::column(0, DataType::Int64, "key")),
                distinct: false,
                data_type: DataType::Decimal128(38, 0),
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
            .downcast_ref::<Decimal128Array>()
            .unwrap();
        for row in 0..batch.num_rows() {
            let key = batch_keys.value(row);
            keys.insert(key);
            assert_eq!(counts.value(row), 1);
            assert_eq!(sums.value(row), i128::from(key));
        }
    }
    assert_eq!(keys.len(), ROWS as usize);
    let metrics = context.metrics.snapshot();
    let peak = metrics.peak_active_lanes;
    assert!((1..=LANES as u64).contains(&peak), "unexpected peak {peak}");
    assert!(
        !metrics.aggregate_partial_output_queue_wait.is_zero(),
        "bounded partial output must report its blocked send"
    );
    assert!(
        metrics.queue_backpressure_wait >= metrics.aggregate_partial_output_queue_wait,
        "the typed edge must remain included in the legacy total"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn spilled_partial_lanes_share_one_larger_merge_budget() {
    const LANES: usize = 4;
    const BATCHES_PER_LANE: usize = 64;
    const ROWS_PER_BATCH: usize = 1_000;
    let input_schema = Arc::new(Schema::new(vec![Field::new("key", DataType::Utf8, false)]));
    let suffix = "x".repeat(240);
    let batches = (0..LANES * BATCHES_PER_LANE).map(move |batch_index| {
        let sequence = batch_index / LANES;
        let keys = (0..ROWS_PER_BATCH)
            .map(|row| format!("{:08}-{suffix}", sequence * ROWS_PER_BATCH + row))
            .collect::<Vec<_>>();
        Ok(RecordBatch::try_new(
            Arc::clone(&input_schema),
            vec![Arc::new(StringArray::from(keys))],
        )
        .unwrap())
    });
    let output_schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::Utf8, false),
        Field::new("rows", DataType::Int64, false),
    ]));
    let root = tempfile::tempdir().unwrap();
    let mut context = QueryContext::new(MemoryPool::new(128 << 20), root.path()).unwrap();
    context.execution.max_repartition_depth = 0;
    context.configure_compute_lanes(LANES);
    let context = Arc::new(context);
    let mut output = aggregate(
        boxed_record_batch_stream(stream::iter(batches)),
        vec![BoundExpr::column(0, DataType::Utf8, "key")],
        vec![AggregateExpr {
            function: AggregateFunction::Count,
            expr: None,
            distinct: false,
            data_type: DataType::Int64,
            display_name: "count(*)".into(),
        }],
        output_schema,
        Arc::clone(&context),
        1_024,
    );

    let mut rows = 0usize;
    tokio::time::timeout(Duration::from_secs(60), async {
        while let Some(batch) = output.try_next().await.unwrap() {
            let counts = batch
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            assert!(counts.values().iter().all(|count| *count == LANES as i64));
            rows += batch.num_rows();
        }
    })
    .await
    .expect("serialized partial spill merge must finish without repartition");
    drop(output);

    assert_eq!(rows, BATCHES_PER_LANE * ROWS_PER_BATCH);
    let metrics = context.metrics.snapshot();
    assert!(metrics.spill_write_bytes > 0);
    assert_eq!(metrics.max_repartition_depth, 0);
    assert!(metrics.peak_memory_bytes <= context.memory.limit() as u64);
    assert_eq!(metrics.active_spill_bytes, 0);
    assert_eq!(metrics.active_spill_files, 0);
    assert_eq!(context.tasks.active_tasks(), 0);
    assert_eq!(context.memory.used(), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn one_spilled_lane_forces_resident_siblings_to_release_state() {
    const LANES: usize = 4;
    const BATCHES_PER_LANE: usize = 48;
    const ROWS_PER_BATCH: usize = 1_000;
    let input_schema = Arc::new(Schema::new(vec![Field::new("key", DataType::Utf8, false)]));
    let suffix = "x".repeat(240);
    let batches = (0..LANES * BATCHES_PER_LANE).map(move |batch_index| {
        let lane = batch_index % LANES;
        let sequence = batch_index / LANES;
        let keys = (0..ROWS_PER_BATCH)
            .map(|row| {
                if lane == 0 {
                    format!("{:08}-{suffix}", sequence * ROWS_PER_BATCH + row)
                } else {
                    format!("lane-{lane}-{suffix}")
                }
            })
            .collect::<Vec<_>>();
        Ok(RecordBatch::try_new(
            Arc::clone(&input_schema),
            vec![Arc::new(StringArray::from(keys))],
        )
        .unwrap())
    });
    let output_schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::Utf8, false),
        Field::new("rows", DataType::Int64, false),
    ]));
    let root = tempfile::tempdir().unwrap();
    let mut context = QueryContext::new(MemoryPool::new(128 << 20), root.path()).unwrap();
    context.execution.max_repartition_depth = 0;
    context.configure_compute_lanes(LANES);
    let context = Arc::new(context);
    let mut output = aggregate(
        boxed_record_batch_stream(stream::iter(batches)),
        vec![BoundExpr::column(0, DataType::Utf8, "key")],
        vec![AggregateExpr {
            function: AggregateFunction::Count,
            expr: None,
            distinct: false,
            data_type: DataType::Int64,
            display_name: "count(*)".into(),
        }],
        output_schema,
        Arc::clone(&context),
        1_024,
    );

    let mut groups = 0usize;
    let mut input_rows = 0i64;
    tokio::time::timeout(Duration::from_secs(60), async {
        while let Some(batch) = output.try_next().await.unwrap() {
            let counts = batch
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            groups += batch.num_rows();
            input_rows += counts.values().iter().sum::<i64>();
        }
    })
    .await
    .expect("mixed spilled and resident partial lanes must make progress");
    drop(output);

    assert_eq!(groups, BATCHES_PER_LANE * ROWS_PER_BATCH + LANES - 1);
    assert_eq!(
        input_rows,
        (LANES * BATCHES_PER_LANE * ROWS_PER_BATCH) as i64
    );
    let metrics = context.metrics.snapshot();
    assert!(metrics.spill_files > LANES as u64);
    assert_eq!(metrics.max_repartition_depth, 0);
    assert_eq!(metrics.active_spill_bytes, 0);
    assert_eq!(metrics.active_spill_files, 0);
    assert_eq!(context.tasks.active_tasks(), 0);
    assert_eq!(context.memory.used(), 0);
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
            distinct: false,
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn partial_batches_wait_for_the_engine_compute_slot() {
    let root = tempfile::tempdir().unwrap();
    let engine = Engine::new(
        EngineConfig::builder()
            .memory_limit(128 << 20)
            .compute_threads(1)
            .max_concurrent_queries(2)
            .spill_directory(root.path().join("spill"))
            .build(),
    )
    .unwrap();
    let blocker_context = engine.query_context_for_test().unwrap();
    let context = engine.query_context_for_test().unwrap();
    // Exercise the parallel partial path while keeping the engine-wide budget
    // at one slot.
    context.configure_compute_lanes_unbounded_for_test(2);
    let blocker = blocker_context.acquire_compute().await.unwrap();

    let input_schema = Arc::new(Schema::new(vec![Field::new("key", DataType::Int64, false)]));
    let batches = (0..4).map(move |_| {
        Ok(RecordBatch::try_new(
            Arc::clone(&input_schema),
            vec![Arc::new(Int64Array::from(vec![1_i64, 1]))],
        )
        .unwrap())
    });
    let output_schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::Int64, false),
        Field::new("rows", DataType::Int64, false),
    ]));
    let worker_context = Arc::clone(&context);
    let worker = tokio::spawn(async move {
        aggregate(
            boxed_record_batch_stream(stream::iter(batches)),
            vec![BoundExpr::column(0, DataType::Int64, "key")],
            vec![AggregateExpr {
                function: AggregateFunction::Count,
                expr: None,
                distinct: false,
                data_type: DataType::Int64,
                display_name: "count(*)".into(),
            }],
            output_schema,
            worker_context,
            64,
        )
        .try_collect::<Vec<_>>()
        .await
    });

    tokio::time::timeout(Duration::from_secs(2), async {
        while engine.compute_scheduler_counts_for_test().2 == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("partial aggregate lane should wait for the shared compute slot");
    let (active, peak, queued, waiting_queries) = engine.compute_scheduler_counts_for_test();
    assert_eq!(active, 1);
    assert_eq!(peak, 1);
    assert!(queued >= 1);
    assert_eq!(waiting_queries, 1);

    drop(blocker);
    let output = tokio::time::timeout(Duration::from_secs(5), worker)
        .await
        .expect("aggregate should resume after the compute slot is released")
        .expect("aggregate task should not panic")
        .unwrap();
    let counts = output[0]
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(counts.value(0), 8);
    let metrics = context.metrics.snapshot();
    assert!(metrics.scheduler_wait > Duration::ZERO);
    assert!(metrics.compute_permit_wait > Duration::ZERO);
    drop(output);
    assert_eq!(context.tasks.active_tasks(), 0);
    assert_eq!(context.memory.used(), 0);
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
        DataType::Decimal128(38, 0),
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
            distinct: false,
            data_type: DataType::Decimal128(38, 0),
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
    assert!(message.contains("does not support Utf8"), "{message}");
    assert!(!message.contains("lane stopped"), "{message}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lane_failure_during_pressured_dispatch_preserves_first_error() {
    const LANES: usize = 4;
    let input_schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int64,
        false,
    )]));
    let batches = (0_i64..64).map(move |batch| {
        Ok(RecordBatch::try_new(
            Arc::clone(&input_schema),
            vec![Arc::new(Int64Array::from(vec![batch; 1_024]))],
        )
        .unwrap())
    });
    let output_schema = Arc::new(Schema::new(vec![Field::new(
        "rows",
        DataType::Int64,
        false,
    )]));
    let root = tempfile::tempdir().unwrap();
    let context = Arc::new(QueryContext::new(MemoryPool::new(128 << 20), root.path()).unwrap());
    context.configure_compute_lanes(LANES);
    fault_injection::arm_input_failure(context.query_id);

    let output = aggregate(
        boxed_record_batch_stream(stream::iter(batches)),
        Vec::new(),
        vec![AggregateExpr {
            function: AggregateFunction::Count,
            expr: None,
            distinct: false,
            data_type: DataType::Int64,
            display_name: "count(*)".into(),
        }],
        output_schema,
        Arc::clone(&context),
        64,
    );
    let error = tokio::time::timeout(Duration::from_secs(5), output.try_collect::<Vec<_>>())
        .await
        .expect("pressured aggregate dispatch must observe the lane failure")
        .unwrap_err();
    let message = error.to_string();
    assert!(
        message.contains("injected parallel aggregate lane input failure"),
        "unexpected error: {message}"
    );
    assert!(!message.contains("lane stopped"), "{message}");

    tokio::time::timeout(Duration::from_secs(2), async {
        while context.tasks.active_tasks() != 0 || context.memory.used() != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("failed aggregate lanes must release tasks and reservations");
    let metrics = context.metrics.snapshot();
    assert!(
        !metrics.aggregate_lane_dispatch_queue_wait.is_zero(),
        "the injected failure must occur after bounded dispatch pressure"
    );
    assert_eq!(context.tasks.active_tasks(), 0);
    assert_eq!(context.memory.used(), 0);
    assert!(
        context.tasks.first_failure().is_some_and(|error| error
            .to_string()
            .contains("injected parallel aggregate lane input failure")),
        "TaskGroup must preserve the original lane failure"
    );
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
            distinct: false,
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
        error.to_string().contains(
            "query task 'aggregate-partial-lane' panicked: injected parallel aggregate lane panic"
        ),
        "unexpected error: {error}"
    );

    tokio::time::timeout(Duration::from_secs(2), async {
        while context.memory.used() != 0 || context.tasks.active_tasks() != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("all aggregate lanes and their reservations must be released");
    assert_eq!(context.tasks.active_tasks(), 0);
    assert!(
        context
            .tasks
            .first_failure()
            .is_some_and(|error| error.to_string().contains(
                "query task 'aggregate-partial-lane' panicked: injected parallel aggregate lane panic"
            )),
        "TaskGroup must preserve the first lane panic"
    );
}
