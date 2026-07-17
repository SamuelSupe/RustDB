use std::sync::Arc;

use arrow::{
    array::{Array, ArrayRef, DictionaryArray, Int64Array, StringArray, UInt32Array},
    datatypes::{DataType, Field, Schema, SchemaRef, UInt32Type},
    record_batch::RecordBatch,
};
use async_trait::async_trait;
use futures::{StreamExt, stream};
use parking_lot::Mutex;

use super::plan;
use crate::{
    Result,
    datasource::{ScanRequest, ScanTask, TableProvider, TableStatistics},
    execution::pipeline::{FusedPipeline, PipelineOperator, plan::ScanStage},
    runtime::{
        MemoryPool, QueryContext, RecordBatchStream, boxed_record_batch_stream,
        estimate_schema_batch_bytes,
    },
    sql::{
        AggregateExpr, AggregateFunction, BoundExpr, ExprKind, LogicalPlan, PlanSchema,
        ScalarValue, StatementPlan,
    },
};

#[derive(Debug)]
struct TestTable(SchemaRef);

#[async_trait]
impl TableProvider for TestTable {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.0)
    }

    fn statistics(&self) -> TableStatistics {
        TableStatistics::default()
    }

    async fn scan(
        &self,
        _request: ScanRequest,
        _context: Arc<QueryContext>,
    ) -> Result<RecordBatchStream> {
        unreachable!("dictionary planner tests do not execute the provider")
    }
}

#[test]
fn retains_a_direct_group_column_unused_by_filter_and_aggregate() {
    let pipeline = pipeline(filter_on_flag());
    let dictionaries = plan(&pipeline, &groups(), &aggregates(1));

    assert_eq!(dictionaries.scan_columns, vec![0]);
    assert_eq!(dictionaries.output_columns, vec![0]);
}

#[test]
fn rejects_group_columns_used_by_filters_or_aggregate_values() {
    let filtered = pipeline(BoundExpr {
        kind: ExprKind::IsNull {
            expr: Box::new(BoundExpr::column(0, DataType::Utf8, "key")),
            negated: false,
        },
        data_type: DataType::Boolean,
        display_name: "key IS NULL".into(),
    });
    assert!(
        plan(&filtered, &groups(), &aggregates(1))
            .scan_columns
            .is_empty()
    );

    let pipeline = pipeline(filter_on_flag());
    assert!(
        plan(&pipeline, &groups(), &aggregates(0))
            .scan_columns
            .is_empty()
    );
}

#[tokio::test]
async fn grouped_pipeline_keeps_dictionary_internal_and_returns_logical_utf8() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("payload", DataType::Utf8, true),
        Field::new("key", DataType::Utf8, true),
        Field::new("flag", DataType::Boolean, false),
    ]));
    let requested = Arc::new(Mutex::new((Vec::new(), None)));
    let scan = LogicalPlan::Scan {
        table_name: "dictionary_hint".into(),
        provider: Arc::new(DictionaryTable {
            schema: Arc::clone(&schema),
            requested: Arc::clone(&requested),
        }),
        statistics: TableStatistics::default(),
        projection: Some(vec![0, 1, 2]),
        pushed_filter: None,
        exact_filter: None,
        limit: None,
        schema: PlanSchema::unqualified(Arc::clone(&schema)),
    };
    let projection = LogicalPlan::Projection {
        input: Box::new(LogicalPlan::Filter {
            input: Box::new(scan),
            predicate: filter_on_flag(),
            schema: PlanSchema::unqualified(Arc::clone(&schema)),
        }),
        expressions: vec![
            BoundExpr::column(1, DataType::Utf8, "key"),
            BoundExpr::column(0, DataType::Utf8, "payload"),
        ],
        schema: PlanSchema::unqualified(Arc::new(Schema::new(vec![
            Field::new("key", DataType::Utf8, true),
            Field::new("payload", DataType::Utf8, true),
        ]))),
    };
    let output_schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::Utf8, true),
        Field::new("rows", DataType::Int64, false),
    ]));
    let aggregate = LogicalPlan::Aggregate {
        input: Box::new(projection),
        group_exprs: groups(),
        aggregate_exprs: vec![AggregateExpr {
            function: AggregateFunction::Count,
            expr: Some(BoundExpr::column(1, DataType::Utf8, "payload")),
            distinct: false,
            data_type: DataType::Int64,
            display_name: "count(payload)".into(),
        }],
        schema: PlanSchema::unqualified(Arc::clone(&output_schema)),
    };
    let temp = tempfile::tempdir().unwrap();
    let context = Arc::new(QueryContext::new(MemoryPool::new(16 << 20), temp.path()).unwrap());
    context.configure_compute_lanes(2);
    let mut output =
        crate::execution::runner::execute(StatementPlan::Query(aggregate), Arc::clone(&context))
            .await
            .unwrap();

    let mut rows = Vec::new();
    while let Some(batch) = output.next().await {
        let batch = batch.unwrap();
        assert_eq!(batch.batch().schema(), output_schema);
        let keys = batch
            .batch()
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let counts = batch
            .batch()
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        rows.extend((0..batch.num_rows()).map(|row| {
            (
                (!keys.is_null(row)).then(|| keys.value(row).to_owned()),
                counts.value(row),
            )
        }));
    }
    rows.sort();

    let requested = requested.lock();
    assert_eq!(requested.0, vec![1]);
    assert_eq!(
        requested.1, None,
        "filtered pipelines keep 8192 decode batches"
    );
    assert_eq!(
        rows,
        vec![(None, 1), (Some("a".into()), 1), (Some("b".into()), 0)]
    );
    assert_eq!(context.memory.used(), 0);
}

#[tokio::test]
async fn unfiltered_group_dictionary_pipeline_requests_larger_decode_batches() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("payload", DataType::Utf8, true),
        Field::new("key", DataType::Utf8, true),
        Field::new("flag", DataType::Boolean, false),
    ]));
    let requested = Arc::new(Mutex::new((Vec::new(), None)));
    let projected_schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::Utf8, true),
        Field::new("payload", DataType::Utf8, true),
    ]));
    let pipeline = FusedPipeline {
        scan: ScanStage {
            provider: Arc::new(DictionaryTable {
                schema: Arc::clone(&schema),
                requested: Arc::clone(&requested),
            }),
            projection: Some(vec![0, 1, 2]),
            pushed_filter: None,
            exact_filter: None,
            limit: None,
            schema,
        },
        operators: vec![PipelineOperator::Projection {
            expressions: vec![
                BoundExpr::column(1, DataType::Utf8, "key"),
                BoundExpr::column(0, DataType::Utf8, "payload"),
            ],
            schema: projected_schema,
        }],
    };
    let aggregates = vec![AggregateExpr {
        function: AggregateFunction::Count,
        expr: Some(BoundExpr::column(1, DataType::Utf8, "payload")),
        distinct: false,
        data_type: DataType::Int64,
        display_name: "count(payload)".into(),
    }];
    let dictionaries = plan(&pipeline, &groups(), &aggregates);
    assert!(dictionaries.enabled());

    let temp = tempfile::tempdir().unwrap();
    let context = Arc::new(QueryContext::new(MemoryPool::new(512 << 20), temp.path()).unwrap());
    context.configure_compute_lanes(2);
    let mut output = super::super::run::execute(
        pipeline,
        dictionaries,
        Some(super::super::PRIVATE_BLOCKING_DECODE_BATCH_SIZE),
        context,
        None,
    );
    while let Some(batch) = output.next().await {
        drop(batch.unwrap());
    }

    let requested = requested.lock();
    assert_eq!(requested.0, vec![1]);
    assert_eq!(requested.1, Some(65_536));
}

struct DictionaryTable {
    schema: SchemaRef,
    requested: Arc<Mutex<(Vec<usize>, Option<usize>)>>,
}

#[async_trait]
impl TableProvider for DictionaryTable {
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
        unreachable!("dictionary test provider exposes scan tasks")
    }

    async fn scan_tasks(
        &self,
        request: ScanRequest,
        context: Arc<QueryContext>,
        _target_tasks: usize,
    ) -> Result<Vec<ScanTask>> {
        *self.requested.lock() = (
            request.dictionary_columns.clone(),
            request.decode_batch_size,
        );
        let payload = dictionary(vec![Some(0), None, Some(0), Some(1)], vec!["x", "y"])?;
        let key = dictionary(vec![Some(0), Some(1), Some(0), None], vec!["a", "b"])?;
        // Return payload as an unrequested dictionary as a hardening case.
        // The grouped projection must decode it before COUNT(payload).
        let flag = Arc::new(arrow::array::BooleanArray::from(vec![
            true, true, false, true,
        ])) as ArrayRef;
        let schema = Arc::new(Schema::new(vec![
            Field::new("payload", payload.data_type().clone(), true),
            Field::new("key", key.data_type().clone(), true),
            Field::new("flag", DataType::Boolean, false),
        ]));
        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![payload, key, flag])?;
        let preclaim = estimate_schema_batch_bytes(schema.as_ref(), request.batch_size);
        Ok(vec![ScanTask::from_public(
            0,
            boxed_record_batch_stream(stream::once(async move { Ok(batch) })),
            context,
            preclaim,
            "dictionary pipeline test scan",
        )])
    }
}

fn dictionary(keys: Vec<Option<u32>>, values: Vec<&str>) -> Result<ArrayRef> {
    let keys = UInt32Array::from(keys);
    let values = Arc::new(StringArray::from(values)) as ArrayRef;
    Ok(Arc::new(DictionaryArray::<UInt32Type>::try_new(
        keys, values,
    )?))
}

fn pipeline(filter: BoundExpr) -> FusedPipeline {
    let scan_schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::Utf8, true),
        Field::new("value", DataType::Int64, false),
        Field::new("flag", DataType::Boolean, false),
    ]));
    let output_schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::Utf8, true),
        Field::new("value", DataType::Int64, false),
    ]));
    FusedPipeline {
        scan: ScanStage {
            provider: Arc::new(TestTable(Arc::clone(&scan_schema))),
            projection: Some(vec![0, 1, 2]),
            pushed_filter: None,
            exact_filter: None,
            limit: None,
            schema: scan_schema,
        },
        operators: vec![
            PipelineOperator::Filter(filter),
            PipelineOperator::Projection {
                expressions: vec![
                    BoundExpr::column(0, DataType::Utf8, "key"),
                    BoundExpr::column(1, DataType::Int64, "value"),
                ],
                schema: output_schema,
            },
        ],
    }
}

fn filter_on_flag() -> BoundExpr {
    filter_on_column(2)
}

fn filter_on_column(column: usize) -> BoundExpr {
    BoundExpr {
        kind: ExprKind::Binary {
            left: Box::new(BoundExpr::column(column, DataType::Boolean, "flag")),
            op: crate::sql::BinaryOp::Eq,
            right: Box::new(BoundExpr::literal(ScalarValue::Boolean(true))),
        },
        data_type: DataType::Boolean,
        display_name: "flag = true".into(),
    }
}

fn groups() -> Vec<BoundExpr> {
    vec![BoundExpr::column(0, DataType::Utf8, "key")]
}

fn aggregates(column: usize) -> Vec<AggregateExpr> {
    vec![AggregateExpr {
        function: AggregateFunction::Count,
        expr: Some(BoundExpr::column(
            column,
            if column == 0 {
                DataType::Utf8
            } else {
                DataType::Int64
            },
            "value",
        )),
        distinct: false,
        data_type: DataType::Int64,
        display_name: "count(value)".into(),
    }]
}
