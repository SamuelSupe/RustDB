use std::{sync::Arc, time::Duration};

use arrow::{
    array::{Array, Decimal128Array, Float64Array, Int64Array, StringArray},
    datatypes::{DataType, Field, Schema, SchemaRef},
    record_batch::RecordBatch,
};
use async_trait::async_trait;
use futures::{TryStreamExt, stream};

use crate::datasource::{ScanRequest, TableProvider, TableStatistics};
use crate::runtime::{MemoryPool, QueryContext, RecordBatchStream, boxed_record_batch_stream};
use crate::sql::{
    BoundExpr, SortExpr, WindowExpr, WindowFrame, WindowFrameBound, WindowFrameUnits,
    WindowFunction, plan_sql,
};
use crate::{Catalog, Engine, EngineConfig, Result, TableEntry};

#[tokio::test]
async fn ranks_rows_and_peers_per_partition() {
    let batches = run("SELECT g, v, \
         row_number() OVER (PARTITION BY g ORDER BY v) AS rn, \
         rank() OVER (PARTITION BY g ORDER BY v) AS r, \
         dense_rank() OVER (PARTITION BY g ORDER BY v) AS dr \
         FROM events ORDER BY g, v, rn")
    .await;
    assert_eq!(strings(&batches, 0), vec!["a", "a", "a", "b"]);
    assert_eq!(ints(&batches, 1), vec![1, 1, 2, 5]);
    assert_eq!(ints(&batches, 2), vec![1, 2, 3, 1]);
    assert_eq!(ints(&batches, 3), vec![1, 1, 3, 1]);
    assert_eq!(ints(&batches, 4), vec![1, 1, 2, 1]);
}

#[tokio::test]
async fn distributes_rows_and_reports_relative_rank() {
    let batches = run("SELECT g, v, \
         ntile(2) OVER (PARTITION BY g ORDER BY v) AS tile, \
         percent_rank() OVER (PARTITION BY g ORDER BY v) AS percent, \
         cume_dist() OVER (PARTITION BY g ORDER BY v) AS cumulative \
         FROM events ORDER BY g, v, row_number() OVER (PARTITION BY g ORDER BY v)")
    .await;
    assert_eq!(ints(&batches, 2), vec![1, 1, 2, 1]);
    assert_eq!(floats(&batches, 3), vec![0.0, 0.0, 1.0, 0.0]);
    assert_eq!(floats(&batches, 4), vec![2.0 / 3.0, 2.0 / 3.0, 1.0, 1.0]);
}

#[tokio::test]
async fn executes_default_range_rows_and_whole_partition_frames() {
    let batches = run(
        "SELECT g, v, \
         sum(v) OVER (PARTITION BY g ORDER BY v) AS range_sum, \
         sum(v) OVER (PARTITION BY g ORDER BY v ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS rows_sum, \
         avg(v) OVER (PARTITION BY g ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS whole_avg \
         FROM events ORDER BY g, v, rows_sum",
    )
    .await;
    assert_eq!(decimals(&batches, 2), vec![2, 2, 4, 5]);
    assert_eq!(decimals(&batches, 3), vec![1, 2, 4, 5]);
    assert_eq!(
        floats(&batches, 4),
        vec![4.0 / 3.0, 4.0 / 3.0, 4.0 / 3.0, 5.0]
    );
}

#[tokio::test]
async fn resolves_named_windows_qualify_and_aggregate_results() {
    let batches = run("SELECT g, sum(v) AS total, rank() OVER w AS r \
         FROM events GROUP BY g \
         WINDOW w AS (ORDER BY sum(v) DESC) \
         QUALIFY r = 1 ORDER BY g")
    .await;
    assert_eq!(strings(&batches, 0), vec!["b"]);
    assert_eq!(decimals(&batches, 1), vec![5]);
    assert_eq!(ints(&batches, 2), vec![1]);
}

#[tokio::test]
async fn discovers_aggregates_only_referenced_by_window_phases() {
    for sql in [
        "SELECT rank() OVER (ORDER BY sum(v)) AS r FROM events",
        "SELECT row_number() OVER () AS rn FROM events QUALIFY count(*) > 0",
        "SELECT rank() OVER w AS r FROM events WINDOW w AS (ORDER BY sum(v))",
    ] {
        let batches = run(sql).await;
        assert_eq!(ints(&batches, 0), vec![1], "{sql}");
    }
}

#[tokio::test]
async fn ignores_windows_inside_subqueries_when_validating_outer_clauses() {
    let scalar = run("SELECT v FROM events \
         WHERE (SELECT row_number() OVER ()) = 1 ORDER BY v")
    .await;
    assert_eq!(ints(&scalar, 0), vec![1, 1, 2, 5]);

    let exists = run("SELECT g, count(*) AS n FROM events GROUP BY g \
         HAVING EXISTS (SELECT row_number() OVER ()) ORDER BY g")
    .await;
    assert_eq!(strings(&exists, 0), vec!["a", "b"]);
    assert_eq!(ints(&exists, 1), vec![3, 1]);

    assert!(
        plan_sql(
            &catalog(),
            "SELECT v FROM events WHERE row_number() OVER () = 1",
        )
        .is_err()
    );
}

#[tokio::test]
async fn executes_zero_column_count_star_and_multiple_specs() {
    let one = run("SELECT count(*) OVER () AS n, row_number() OVER () AS rn").await;
    assert_eq!(ints(&one, 0), vec![1]);
    assert_eq!(ints(&one, 1), vec![1]);

    let batches = run("SELECT g, v, \
         row_number() OVER (PARTITION BY g ORDER BY v) AS within_group, \
         row_number() OVER (ORDER BY v DESC) AS global_rank \
         FROM events ORDER BY g, v, within_group")
    .await;
    assert_eq!(ints(&batches, 2), vec![1, 2, 3, 1]);
    assert_eq!(ints(&batches, 3), vec![3, 4, 2, 1]);

    let hidden = run("SELECT v FROM events \
         ORDER BY row_number() OVER (ORDER BY v DESC)")
    .await;
    assert_eq!(ints(&hidden, 0), vec![5, 2, 1, 1]);

    let collision = run("SELECT row_number() OVER (ORDER BY __rustdb_window_0) \
         FROM (SELECT v AS __rustdb_window_0 FROM events) derived ORDER BY 1")
    .await;
    assert_eq!(ints(&collision, 0), vec![1, 2, 3, 4]);
}

#[tokio::test]
async fn large_partition_spools_with_bounded_memory_and_cleans_files() {
    let rows = 20_000usize;
    let catalog = Catalog::default();
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("g", DataType::Utf8, false),
            Field::new("v", DataType::Int64, false),
        ])),
        vec![
            Arc::new(StringArray::from(vec!["one"; rows])),
            Arc::new(Int64Array::from_iter_values(0..rows as i64)),
        ],
    )
    .unwrap();
    catalog
        .register(TableEntry::new("big", Arc::new(MemoryTable(batch))))
        .unwrap();
    let plan = plan_sql(
        &catalog,
        "SELECT row_number() OVER (PARTITION BY g ORDER BY v) AS rn, \
         sum(v) OVER (PARTITION BY g ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS total \
         FROM big",
    )
    .unwrap();
    let temp = tempfile::tempdir().unwrap();
    let context = QueryContext::shared(MemoryPool::new(8 << 20), temp.path()).unwrap();
    let batches = super::super::execute(plan, Arc::clone(&context))
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_eq!(
        batches.iter().map(RecordBatch::num_rows).sum::<usize>(),
        rows
    );
    assert!(context.metrics.snapshot().spill_write_bytes > 0);
    assert!(
        context.metrics.snapshot().peak_memory_bytes
            <= u64::try_from(context.memory.limit()).unwrap()
    );
    assert_eq!(context.tasks.active_tasks(), 0);
    let spill_files = std::fs::read_dir(context.spill.directory())
        .unwrap()
        .filter_map(std::result::Result::ok)
        .filter(|entry| {
            entry
                .path()
                .extension()
                .is_some_and(|extension| extension == "arrow")
        })
        .count();
    assert_eq!(spill_files, 0);
}

#[tokio::test]
async fn null_ordering_forms_one_peer_group() {
    let catalog = Catalog::default();
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, true)])),
        vec![Arc::new(Int64Array::from(vec![
            None,
            Some(1),
            None,
            Some(2),
        ]))],
    )
    .unwrap();
    catalog
        .register(TableEntry::new("nullable", Arc::new(MemoryTable(batch))))
        .unwrap();
    let plan = plan_sql(
        &catalog,
        "SELECT rank() OVER (ORDER BY v NULLS FIRST) AS r, \
         dense_rank() OVER (ORDER BY v NULLS FIRST) AS dr \
         FROM nullable ORDER BY r, dr",
    )
    .unwrap();
    let temp = tempfile::tempdir().unwrap();
    let context = QueryContext::shared(MemoryPool::new(8 << 20), temp.path()).unwrap();
    let batches = super::super::execute(plan, context)
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_eq!(ints(&batches, 0), vec![1, 1, 3, 4]);
    assert_eq!(ints(&batches, 1), vec![1, 1, 2, 3]);
}

#[tokio::test]
async fn evaluates_independent_partitions_on_multiple_lanes() {
    let rows = 40_000usize;
    let groups = (0..rows)
        .map(|row| format!("g{}", row % 8))
        .collect::<Vec<_>>();
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("g", DataType::Utf8, false),
            Field::new("v", DataType::Int64, false),
        ])),
        vec![
            Arc::new(StringArray::from_iter_values(&groups)),
            Arc::new(Int64Array::from_iter_values(0..rows as i64)),
        ],
    )
    .unwrap();
    let catalog = Catalog::default();
    catalog
        .register(TableEntry::new("lanes", Arc::new(MemoryTable(batch))))
        .unwrap();
    for memory_limit in [64 << 20, 128 << 20] {
        let plan = plan_sql(
            &catalog,
            "SELECT row_number() OVER (PARTITION BY g ORDER BY v) FROM lanes",
        )
        .unwrap();
        let temp = tempfile::tempdir().unwrap();
        let context = QueryContext::shared(MemoryPool::new(memory_limit), temp.path()).unwrap();
        context.scheduler.configure_unbounded(4);
        let batches = super::super::execute(plan, Arc::clone(&context))
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(
            batches.iter().map(RecordBatch::num_rows).sum::<usize>(),
            rows
        );
        let peak_active_lanes = context.metrics.snapshot().peak_active_lanes;
        assert!((1..=4).contains(&peak_active_lanes));
        assert_eq!(context.tasks.active_tasks(), 0);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn window_cpu_waits_for_the_engine_compute_slot_and_releases_it() {
    let root = tempfile::tempdir().unwrap();
    let engine = Engine::new(
        EngineConfig::builder()
            .memory_limit(64 << 20)
            .compute_threads(1)
            .max_concurrent_queries(2)
            .spill_directory(root.path().join("spill"))
            .build(),
    )
    .unwrap();
    let blocker_context = engine.query_context_for_test().unwrap();
    let context = engine.query_context_for_test().unwrap();
    let blocker = blocker_context.acquire_compute().await.unwrap();
    let (input, expression, input_schema, output_schema) = single_slot_window_input();
    let worker_context = Arc::clone(&context);
    let worker = tokio::spawn(async move {
        super::window(
            input,
            vec![expression],
            input_schema,
            output_schema,
            worker_context,
            2,
        )
        .try_collect::<Vec<_>>()
        .await
    });

    wait_for_compute_waiter(&engine).await;
    assert_eq!(engine.compute_scheduler_counts_for_test(), (1, 1, 1, 1));
    assert_eq!(context.scheduler.active_lanes(), 0);

    drop(blocker);
    let output = tokio::time::timeout(Duration::from_secs(5), worker)
        .await
        .expect("window should resume after the compute slot is released")
        .expect("window worker should not panic")
        .unwrap();
    assert_eq!(
        output.iter().map(|batch| batch.num_rows()).sum::<usize>(),
        4
    );
    drop(output);
    wait_for_window_tasks(&context).await;
    assert_eq!(engine.compute_scheduler_counts_for_test(), (0, 1, 0, 0));
    assert_eq!(context.scheduler.active_lanes(), 0);
    assert_eq!(context.tasks.active_tasks(), 0);
    assert_eq!(context.memory.used(), 0);
    assert!(context.metrics.snapshot().compute_permit_wait > Duration::ZERO);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelling_a_window_compute_waiter_does_not_leak_the_slot() {
    let root = tempfile::tempdir().unwrap();
    let engine = Engine::new(
        EngineConfig::builder()
            .memory_limit(64 << 20)
            .compute_threads(1)
            .max_concurrent_queries(2)
            .spill_directory(root.path().join("spill"))
            .build(),
    )
    .unwrap();
    let blocker_context = engine.query_context_for_test().unwrap();
    let context = engine.query_context_for_test().unwrap();
    let blocker = blocker_context.acquire_compute().await.unwrap();
    let (input, expression, input_schema, output_schema) = single_slot_window_input();
    let worker_context = Arc::clone(&context);
    let worker = tokio::spawn(async move {
        super::window(
            input,
            vec![expression],
            input_schema,
            output_schema,
            worker_context,
            2,
        )
        .try_collect::<Vec<_>>()
        .await
    });

    wait_for_compute_waiter(&engine).await;
    context.control.cancel();
    let error = tokio::time::timeout(Duration::from_secs(5), worker)
        .await
        .expect("cancelled window waiter should stop")
        .expect("window worker should not panic")
        .unwrap_err();
    assert!(error.to_string().contains("cancelled"));
    assert_eq!(engine.compute_scheduler_counts_for_test(), (1, 1, 0, 0));
    assert_eq!(context.scheduler.active_lanes(), 0);
    assert_eq!(context.tasks.active_tasks(), 0);

    drop(blocker);
    assert_eq!(engine.compute_scheduler_counts_for_test(), (0, 1, 0, 0));
    assert_eq!(context.memory.used(), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn abandoning_parallel_window_releases_tasks_memory_and_spill() {
    let rows = 40_000usize;
    let input_schema = Arc::new(Schema::new(vec![
        Field::new("g", DataType::Int64, false),
        Field::new("v", DataType::Int64, false),
    ]));
    let output_schema = Arc::new(Schema::new(vec![
        Field::new("g", DataType::Int64, false),
        Field::new("v", DataType::Int64, false),
        Field::new("rn", DataType::Int64, false),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&input_schema),
        vec![
            Arc::new(Int64Array::from_iter_values(
                (0..rows).map(|row| (row % 8) as i64),
            )),
            Arc::new(Int64Array::from_iter_values(0..rows as i64)),
        ],
    )
    .unwrap();
    let expression = WindowExpr {
        function: WindowFunction::RowNumber,
        partition_by: vec![BoundExpr::column(0, DataType::Int64, "g")],
        order_by: vec![SortExpr {
            expr: BoundExpr::column(1, DataType::Int64, "v"),
            descending: false,
            nulls_first: false,
        }],
        frame: WindowFrame {
            units: WindowFrameUnits::Range,
            start: WindowFrameBound::UnboundedPreceding,
            end: WindowFrameBound::CurrentRow,
        },
        data_type: DataType::Int64,
        display_name: "row_number() OVER (PARTITION BY g ORDER BY v)".into(),
    };
    let input = boxed_record_batch_stream(stream::once(async move { Ok(batch) }));
    let temp = tempfile::tempdir().unwrap();
    let context = QueryContext::shared(MemoryPool::new(64 << 20), temp.path()).unwrap();
    context.scheduler.configure_unbounded(4);
    let spill_directory = context.spill.directory().to_path_buf();
    let mut output = super::window(
        input,
        vec![expression],
        input_schema,
        output_schema,
        Arc::clone(&context),
        256,
    );
    let first = output.try_next().await.unwrap().unwrap();
    drop(first);
    drop(output);
    context.cleanup_spill_after_tasks().await.unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while context.memory.used() != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("window lanes did not release their reservations");
    assert_eq!(context.tasks.active_tasks(), 0);
    drop(context);
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
    while spill_directory.exists() && std::time::Instant::now() < deadline {
        std::thread::sleep(std::time::Duration::from_millis(5));
    }
    assert!(!spill_directory.exists());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn partition_worker_panic_cancels_query_and_cleans_spill() {
    let plan = plan_sql(
        &catalog(),
        "SELECT row_number() OVER (PARTITION BY g ORDER BY v) FROM events",
    )
    .unwrap();
    let temp = tempfile::tempdir().unwrap();
    let context = QueryContext::shared(MemoryPool::new(16 << 20), temp.path()).unwrap();
    super::fault_injection::arm(context.query_id);
    let error = super::super::execute(plan, Arc::clone(&context))
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("window-partition-lane"), "{error}");
    assert!(error.contains("panicked"), "{error}");
    context.cleanup_spill_after_tasks().await.unwrap();
    assert_eq!(context.tasks.active_tasks(), 0);
    assert_eq!(context.memory.used(), 0);
}

fn single_slot_window_input() -> (RecordBatchStream, WindowExpr, SchemaRef, SchemaRef) {
    let input_schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
    let output_schema = Arc::new(Schema::new(vec![
        Field::new("v", DataType::Int64, false),
        Field::new("rn", DataType::Int64, false),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&input_schema),
        vec![Arc::new(Int64Array::from(vec![1_i64, 2, 3, 4]))],
    )
    .unwrap();
    let expression = WindowExpr {
        function: WindowFunction::RowNumber,
        partition_by: vec![],
        order_by: vec![],
        frame: WindowFrame {
            units: WindowFrameUnits::Range,
            start: WindowFrameBound::UnboundedPreceding,
            end: WindowFrameBound::CurrentRow,
        },
        data_type: DataType::Int64,
        display_name: "row_number() OVER ()".into(),
    };
    (
        boxed_record_batch_stream(stream::once(async move { Ok(batch) })),
        expression,
        input_schema,
        output_schema,
    )
}

async fn wait_for_compute_waiter(engine: &Engine) {
    tokio::time::timeout(Duration::from_secs(2), async {
        while engine.compute_scheduler_counts_for_test().2 == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("window should wait for the shared compute slot");
}

async fn wait_for_window_tasks(context: &QueryContext) {
    tokio::time::timeout(Duration::from_secs(2), async {
        while context.tasks.active_tasks() != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("window partition tasks should quiesce");
}

#[test]
fn rejects_window_contexts_and_unsupported_frames() {
    let catalog = catalog();
    for sql in [
        "SELECT v FROM events WHERE row_number() OVER () = 1",
        "SELECT v FROM events GROUP BY row_number() OVER ()",
        "SELECT v FROM events HAVING row_number() OVER () = 1",
        "SELECT sum(v) OVER (ORDER BY v ROWS 1 PRECEDING) FROM events",
        "SELECT sum(v) OVER (ORDER BY v GROUPS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM events",
        "SELECT count(DISTINCT v) OVER () FROM events",
        "SELECT row_number() OVER (ORDER BY rank() OVER ()) FROM events",
        "SELECT ntile(0) OVER () FROM events",
        "SELECT ntile(v) OVER () FROM events",
    ] {
        assert!(plan_sql(&catalog, sql).is_err(), "{sql}");
    }
}

async fn run(sql: &str) -> Vec<RecordBatch> {
    let plan = plan_sql(&catalog(), sql).unwrap_or_else(|error| panic!("{sql}: {error}"));
    let temp = tempfile::tempdir().unwrap();
    let context = QueryContext::shared(MemoryPool::new(16 << 20), temp.path()).unwrap();
    super::super::execute(plan, context)
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap_or_else(|error| panic!("{sql}: {error}"))
}

fn catalog() -> Catalog {
    let catalog = Catalog::default();
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("g", DataType::Utf8, false),
            Field::new("v", DataType::Int64, false),
        ])),
        vec![
            Arc::new(StringArray::from(vec!["a", "a", "a", "b"])),
            Arc::new(Int64Array::from(vec![2, 1, 1, 5])),
        ],
    )
    .unwrap();
    catalog
        .register(TableEntry::new("events", Arc::new(MemoryTable(batch))))
        .unwrap();
    catalog
}

fn ints(batches: &[RecordBatch], column: usize) -> Vec<i64> {
    batches
        .iter()
        .flat_map(|batch| {
            batch
                .column(column)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values()
                .iter()
                .copied()
                .collect::<Vec<_>>()
        })
        .collect()
}

fn decimals(batches: &[RecordBatch], column: usize) -> Vec<i128> {
    batches
        .iter()
        .flat_map(|batch| {
            batch
                .column(column)
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .unwrap()
                .values()
                .iter()
                .copied()
                .collect::<Vec<_>>()
        })
        .collect()
}

fn floats(batches: &[RecordBatch], column: usize) -> Vec<f64> {
    batches
        .iter()
        .flat_map(|batch| {
            batch
                .column(column)
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .values()
                .iter()
                .copied()
                .collect::<Vec<_>>()
        })
        .collect()
}

fn strings(batches: &[RecordBatch], column: usize) -> Vec<String> {
    batches
        .iter()
        .flat_map(|batch| {
            let values = batch
                .column(column)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            (0..values.len())
                .map(|row| values.value(row).to_owned())
                .collect::<Vec<_>>()
        })
        .collect()
}

#[derive(Clone)]
struct MemoryTable(RecordBatch);

#[async_trait]
impl TableProvider for MemoryTable {
    fn schema(&self) -> SchemaRef {
        self.0.schema()
    }

    fn statistics(&self) -> TableStatistics {
        TableStatistics {
            row_count: Some(self.0.num_rows() as u64),
            total_byte_size: Some(self.0.get_array_memory_size() as u64),
            file_count: 1,
        }
    }

    async fn scan(
        &self,
        request: ScanRequest,
        _context: Arc<QueryContext>,
    ) -> Result<RecordBatchStream> {
        let batch = match request.projection {
            Some(projection) => self.0.project(&projection)?,
            None => self.0.clone(),
        };
        Ok(boxed_record_batch_stream(stream::once(
            async move { Ok(batch) },
        )))
    }
}
