use std::{
    collections::HashSet,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Poll},
    time::Duration,
};

use arrow::{
    array::Int64Array,
    datatypes::{DataType, Field, Schema, SchemaRef},
    record_batch::{RecordBatch, RecordBatchOptions},
};
use async_trait::async_trait;
use futures::{Stream, StreamExt, TryStreamExt, stream};
use parking_lot::Mutex;
use tokio::sync::Barrier;

use crate::{
    Error, Result,
    datasource::{ScanRequest, ScanTask, TableProvider, TableStatistics},
    runtime::{
        ComputeRuntime, MemoryPool, QueryContext, RecordBatchStream, boxed_record_batch_stream,
        estimate_schema_batch_bytes,
    },
    sql::{BinaryOp, BoundExpr, ExprKind, LogicalPlan, PlanSchema, ScalarValue, StatementPlan},
};

#[test]
fn join_decode_batch_shortens_for_concurrent_queries() {
    assert_eq!(super::join_decode_batch_size(1), 65_536);
    assert_eq!(super::join_decode_batch_size(2), 32_768);
    assert_eq!(super::join_decode_batch_size(4), 32_768);
}

#[derive(Clone)]
struct PartitionedTable {
    schema: SchemaRef,
    tasks: usize,
    barrier: Arc<Barrier>,
    threads: Arc<Mutex<Vec<String>>>,
    requested_tasks: Arc<AtomicUsize>,
}

#[async_trait]
impl TableProvider for PartitionedTable {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn statistics(&self) -> TableStatistics {
        TableStatistics {
            row_count: Some(self.tasks as u64),
            total_byte_size: None,
            file_count: self.tasks,
        }
    }

    async fn scan(
        &self,
        _request: ScanRequest,
        _context: Arc<QueryContext>,
    ) -> Result<RecordBatchStream> {
        Err(Error::Internal(
            "partitioned provider unexpectedly used compatibility scan".into(),
        ))
    }

    async fn scan_tasks(
        &self,
        request: ScanRequest,
        context: Arc<QueryContext>,
        target_tasks: usize,
    ) -> Result<Vec<ScanTask>> {
        self.requested_tasks.store(target_tasks, Ordering::Release);
        let schema = request.projected_schema(&self.schema)?;
        let preclaim = estimate_schema_batch_bytes(schema.as_ref(), request.batch_size);
        Ok((0..self.tasks)
            .map(|task| {
                let schema = Arc::clone(&schema);
                let barrier = Arc::clone(&self.barrier);
                let threads = Arc::clone(&self.threads);
                let task_context = Arc::clone(&context);
                let stream = boxed_record_batch_stream(stream::once(async move {
                    barrier.wait().await;
                    threads.lock().push(
                        std::thread::current()
                            .name()
                            .unwrap_or("unnamed")
                            .to_owned(),
                    );
                    if schema.fields().is_empty() {
                        let options = RecordBatchOptions::new().with_row_count(Some(1));
                        RecordBatch::try_new_with_options(schema, Vec::new(), &options)
                            .map_err(Into::into)
                    } else {
                        RecordBatch::try_new(
                            schema,
                            vec![Arc::new(Int64Array::from(vec![task as i64]))],
                        )
                        .map_err(Into::into)
                    }
                }));
                ScanTask::from_public(
                    task,
                    stream,
                    task_context,
                    preclaim,
                    "partitioned test scan",
                )
            })
            .collect())
    }
}

#[derive(Clone)]
struct BatchedTable {
    schema: SchemaRef,
    tasks: usize,
    batches_per_task: usize,
}

#[async_trait]
impl TableProvider for BatchedTable {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn statistics(&self) -> TableStatistics {
        TableStatistics::default()
    }

    async fn scan(
        &self,
        _request: ScanRequest,
        _context: Arc<QueryContext>,
    ) -> Result<RecordBatchStream> {
        unreachable!("scan_tasks is implemented")
    }

    async fn scan_tasks(
        &self,
        request: ScanRequest,
        context: Arc<QueryContext>,
        _target_tasks: usize,
    ) -> Result<Vec<ScanTask>> {
        let schema = request.projected_schema(&self.schema)?;
        let preclaim = estimate_schema_batch_bytes(schema.as_ref(), request.batch_size);
        Ok((0..self.tasks)
            .map(|task| {
                let values = (0..request.batch_size)
                    .map(|row| 1 + row as i64 + task as i64)
                    .collect::<Vec<_>>();
                let batch = RecordBatch::try_new(
                    Arc::clone(&schema),
                    vec![Arc::new(Int64Array::from(values))],
                )
                .unwrap();
                let batches = self.batches_per_task;
                let stream = boxed_record_batch_stream(stream::iter(
                    (0..batches).map(move |_| Ok(batch.clone())),
                ));
                ScanTask::from_public(
                    task,
                    stream,
                    Arc::clone(&context),
                    preclaim,
                    "batched test scan",
                )
            })
            .collect())
    }
}

#[tokio::test]
async fn wide_parallel_projection_backpressures_under_low_memory() {
    const LANES: usize = 4;
    const BATCH_SIZE: usize = 8_192;
    const TASKS: usize = 4;
    const BATCHES_PER_TASK: usize = 4;
    const OUTPUT_COLUMNS: usize = 72;
    const MEMORY_LIMIT: usize = 64 << 20;

    let input_schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]));
    let provider = Arc::new(BatchedTable {
        schema: Arc::clone(&input_schema),
        tasks: TASKS,
        batches_per_task: BATCHES_PER_TASK,
    });
    let scan = LogicalPlan::Scan {
        table_name: "wide_backpressure".into(),
        provider,
        statistics: TableStatistics::default(),
        projection: None,
        pushed_filter: None,
        exact_filter: None,
        limit: None,
        schema: PlanSchema::unqualified(Arc::clone(&input_schema)),
    };
    let predicate = BoundExpr::literal(ScalarValue::Boolean(true));
    let filter = LogicalPlan::Filter {
        input: Box::new(scan),
        predicate,
        schema: PlanSchema::unqualified(Arc::clone(&input_schema)),
    };
    let expressions = (0..OUTPUT_COLUMNS)
        .map(|column| BoundExpr {
            kind: ExprKind::Binary {
                left: Box::new(BoundExpr::column(0, DataType::Int64, "x")),
                op: BinaryOp::Add,
                right: Box::new(BoundExpr::literal(ScalarValue::Int64(column as i64))),
            },
            data_type: DataType::Int64,
            display_name: format!("x + {column}"),
        })
        .collect::<Vec<_>>();
    let output_schema = Arc::new(Schema::new(
        (0..OUTPUT_COLUMNS)
            .map(|column| Field::new(format!("c{column}"), DataType::Int64, false))
            .collect::<Vec<_>>(),
    ));
    let plan = LogicalPlan::Projection {
        input: Box::new(filter),
        expressions,
        schema: PlanSchema::unqualified(output_schema),
    };

    let temp = tempfile::tempdir().unwrap();
    let context = Arc::new(
        QueryContext::with_query_id_and_batch_size(
            uuid::Uuid::new_v4(),
            MemoryPool::new(MEMORY_LIMIT),
            temp.path(),
            BATCH_SIZE,
        )
        .unwrap(),
    );
    let internal = super::super::runner::execute(StatementPlan::Query(plan), Arc::clone(&context))
        .await
        .unwrap();
    let compute = ComputeRuntime::new(LANES).unwrap();
    let mut output = compute.pipe(internal, Arc::clone(&context));
    let mut rows = 0usize;
    tokio::time::timeout(Duration::from_secs(20), async {
        while let Some(batch) = output.next().await {
            let batch = batch.expect("wide projection must not fail under queue pressure");
            assert_eq!(batch.num_columns(), OUTPUT_COLUMNS);
            rows += batch.num_rows();
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .expect("memory wait must make progress with a slow consumer");

    assert_eq!(rows, TASKS * BATCHES_PER_TASK * BATCH_SIZE);
    assert!(context.memory.peak() <= MEMORY_LIMIT);
    assert_eq!(context.memory.used(), 0);
}

#[tokio::test]
async fn zero_column_scan_survives_fused_filter_and_literal_projection() {
    let scan_schema = Arc::new(Schema::new(vec![Field::new(
        "unused",
        DataType::Int64,
        false,
    )]));
    let output_schema = Arc::new(Schema::new(vec![Field::new(
        "answer",
        DataType::Int64,
        false,
    )]));
    let provider = Arc::new(PartitionedTable {
        schema: Arc::clone(&scan_schema),
        tasks: 1,
        barrier: Arc::new(Barrier::new(1)),
        threads: Arc::new(Mutex::new(Vec::new())),
        requested_tasks: Arc::new(AtomicUsize::new(0)),
    });
    let scan = LogicalPlan::Scan {
        table_name: "metadata_only".into(),
        provider,
        statistics: TableStatistics::default(),
        projection: Some(Vec::new()),
        pushed_filter: None,
        exact_filter: None,
        limit: None,
        schema: PlanSchema::unqualified(Arc::clone(&scan_schema)),
    };
    let filter = LogicalPlan::Filter {
        input: Box::new(scan),
        predicate: BoundExpr::literal(ScalarValue::Boolean(true)),
        schema: PlanSchema::unqualified(scan_schema),
    };
    let plan = LogicalPlan::Projection {
        input: Box::new(filter),
        expressions: vec![BoundExpr::literal(ScalarValue::Int64(42))],
        schema: PlanSchema::unqualified(output_schema),
    };

    let temp = tempfile::tempdir().unwrap();
    let context = Arc::new(QueryContext::new(MemoryPool::new(16 << 20), temp.path()).unwrap());
    let internal = super::super::runner::execute(StatementPlan::Query(plan), Arc::clone(&context))
        .await
        .unwrap();
    let batches = ComputeRuntime::new(1)
        .unwrap()
        .pipe(internal, context)
        .try_collect::<Vec<_>>()
        .await
        .unwrap();

    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].num_rows(), 1);
    assert_eq!(batches[0].num_columns(), 1);
    assert_eq!(
        batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        42
    );
}

#[tokio::test]
async fn scan_tasks_run_on_multiple_named_runtime_workers() {
    const LANES: usize = 4;
    let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]));
    let threads = Arc::new(Mutex::new(Vec::new()));
    let requested_tasks = Arc::new(AtomicUsize::new(0));
    let provider = Arc::new(PartitionedTable {
        schema: Arc::clone(&schema),
        tasks: LANES,
        barrier: Arc::new(Barrier::new(LANES)),
        threads: Arc::clone(&threads),
        requested_tasks: Arc::clone(&requested_tasks),
    });
    let plan = LogicalPlan::Scan {
        table_name: "parallel".into(),
        provider,
        statistics: TableStatistics::default(),
        projection: None,
        pushed_filter: None,
        exact_filter: None,
        limit: None,
        schema: PlanSchema::unqualified(schema),
    };
    let temp = tempfile::tempdir().unwrap();
    let context = Arc::new(QueryContext::new(MemoryPool::new(128 << 20), temp.path()).unwrap());
    let internal = super::super::runner::execute(StatementPlan::Query(plan), Arc::clone(&context))
        .await
        .unwrap();
    let compute = ComputeRuntime::new(LANES).unwrap();
    let batches = compute
        .pipe(internal, Arc::clone(&context))
        .try_collect::<Vec<_>>()
        .await
        .unwrap();

    let mut values = batches
        .iter()
        .map(|batch| {
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0)
        })
        .collect::<Vec<_>>();
    values.sort_unstable();
    assert_eq!(values, vec![0, 1, 2, 3]);
    assert_eq!(requested_tasks.load(Ordering::Acquire), LANES);
    let observed = threads.lock().clone();
    assert_eq!(observed.len(), LANES);
    let names = observed.into_iter().collect::<HashSet<_>>();
    assert!(names.iter().all(|name| name.starts_with("rustdb-compute-")));
}

struct DropCountingStream {
    batch: RecordBatch,
    dropped: Arc<AtomicUsize>,
}

#[derive(Clone)]
struct PanickingTable {
    schema: SchemaRef,
}

#[async_trait]
impl TableProvider for PanickingTable {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn statistics(&self) -> TableStatistics {
        TableStatistics::default()
    }

    async fn scan(
        &self,
        _request: ScanRequest,
        _context: Arc<QueryContext>,
    ) -> Result<RecordBatchStream> {
        unreachable!("scan_tasks is implemented")
    }

    async fn scan_tasks(
        &self,
        request: ScanRequest,
        context: Arc<QueryContext>,
        _target_tasks: usize,
    ) -> Result<Vec<ScanTask>> {
        let schema = request.projected_schema(&self.schema)?;
        let preclaim = estimate_schema_batch_bytes(schema.as_ref(), request.batch_size);
        Ok(vec![ScanTask::from_public(
            0,
            boxed_record_batch_stream(stream::once(async move {
                let _ = schema;
                panic!("injected scan lane panic")
            })),
            context,
            preclaim,
            "panicking test scan",
        )])
    }
}

#[tokio::test]
async fn scan_lane_panic_is_a_terminal_error_not_partial_success() {
    let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]));
    let plan = LogicalPlan::Scan {
        table_name: "panicking".into(),
        provider: Arc::new(PanickingTable {
            schema: Arc::clone(&schema),
        }),
        statistics: TableStatistics::default(),
        projection: None,
        pushed_filter: None,
        exact_filter: None,
        limit: None,
        schema: PlanSchema::unqualified(schema),
    };
    let temp = tempfile::tempdir().unwrap();
    let context = Arc::new(QueryContext::new(MemoryPool::new(16 << 20), temp.path()).unwrap());
    let internal = super::super::runner::execute(StatementPlan::Query(plan), Arc::clone(&context))
        .await
        .unwrap();
    let error = ComputeRuntime::new(1)
        .unwrap()
        .pipe(internal, context)
        .try_collect::<Vec<_>>()
        .await
        .unwrap_err();

    assert!(
        error
            .to_string()
            .contains("query task 'scan-pipeline-lane' panicked: injected scan lane panic"),
        "unexpected error: {error}"
    );
}

impl Stream for DropCountingStream {
    type Item = Result<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Poll::Ready(Some(Ok(self.batch.clone())))
    }
}

impl Drop for DropCountingStream {
    fn drop(&mut self) {
        self.dropped.fetch_add(1, Ordering::AcqRel);
    }
}

#[derive(Clone)]
struct CancellableTable {
    schema: SchemaRef,
    dropped: Arc<AtomicUsize>,
}

#[async_trait]
impl TableProvider for CancellableTable {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn statistics(&self) -> TableStatistics {
        TableStatistics::default()
    }

    async fn scan(
        &self,
        _request: ScanRequest,
        _context: Arc<QueryContext>,
    ) -> Result<RecordBatchStream> {
        unreachable!("scan_tasks is implemented")
    }

    async fn scan_tasks(
        &self,
        request: ScanRequest,
        context: Arc<QueryContext>,
        target_tasks: usize,
    ) -> Result<Vec<ScanTask>> {
        let preclaim = estimate_schema_batch_bytes(self.schema.as_ref(), request.batch_size);
        Ok((0..target_tasks)
            .map(|task| {
                let batch = RecordBatch::try_new(
                    Arc::clone(&self.schema),
                    vec![Arc::new(Int64Array::from(vec![task as i64]))],
                )
                .unwrap();
                ScanTask::from_public(
                    task,
                    Box::pin(DropCountingStream {
                        batch,
                        dropped: Arc::clone(&self.dropped),
                    }),
                    Arc::clone(&context),
                    preclaim,
                    "cancellable test scan",
                )
            })
            .collect())
    }
}

#[tokio::test]
async fn limit_drops_all_remaining_scan_tasks() {
    const LANES: usize = 4;
    let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]));
    let dropped = Arc::new(AtomicUsize::new(0));
    let scan = LogicalPlan::Scan {
        table_name: "cancellable".into(),
        provider: Arc::new(CancellableTable {
            schema: Arc::clone(&schema),
            dropped: Arc::clone(&dropped),
        }),
        statistics: TableStatistics::default(),
        projection: None,
        pushed_filter: None,
        exact_filter: None,
        limit: None,
        schema: PlanSchema::unqualified(Arc::clone(&schema)),
    };
    let plan = LogicalPlan::Limit {
        input: Box::new(scan),
        offset: 0,
        limit: Some(1),
        schema: PlanSchema::unqualified(schema),
    };
    let temp = tempfile::tempdir().unwrap();
    let context = Arc::new(QueryContext::new(MemoryPool::new(128 << 20), temp.path()).unwrap());
    let internal = super::super::runner::execute(StatementPlan::Query(plan), Arc::clone(&context))
        .await
        .unwrap();
    let compute = ComputeRuntime::new(LANES).unwrap();
    let batches = compute
        .pipe(internal, Arc::clone(&context))
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);

    tokio::time::timeout(Duration::from_secs(2), async {
        while dropped.load(Ordering::Acquire) != LANES {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("all task streams must be dropped after LIMIT is satisfied");
    assert!(!context.control.is_cancelled());
}
