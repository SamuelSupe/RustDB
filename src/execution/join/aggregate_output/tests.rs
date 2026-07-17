use std::sync::Arc;

use arrow::{
    array::{Array, Decimal128Array, Int64Array, StringArray},
    datatypes::{DataType, Field, Schema, SchemaRef},
    record_batch::RecordBatch,
};
use futures::{StreamExt, stream};
use uuid::Uuid;

use crate::{
    execution::join::build::multiplicity::{force_fallback_after, take_observed_build},
    runtime::{BatchEnvelope, MemoryPool, QueryContext, boxed_memory_batch_stream},
    sql::{AggregateExpr, AggregateFunction, BoundExpr},
};

use super::join_global_aggregate;

#[tokio::test]
async fn multiplicity_aggregate_output_uses_counted_build_only_for_probe_payloads() {
    let fixture = Fixture::new();

    let (context, output) = fixture.run(probe_aggregates(), None, false).await;
    assert_eq!(output, (5, Some(90)));
    assert!(take_observed_build(context.query_id));
    finish(context).await;

    let (context, output) = fixture.run(build_aggregates(), None, false).await;
    assert_eq!(output, (5, Some(39)));
    assert!(!take_observed_build(context.query_id));
    finish(context).await;
}

#[tokio::test]
async fn multiplicity_aggregate_output_spills_after_a_partial_counted_build() {
    let fixture = Fixture::new();
    let query_id = Uuid::new_v4();
    force_fallback_after(query_id, 1);

    let (context, output) = fixture.run(probe_aggregates(), Some(query_id), false).await;
    assert_eq!(output, (5, Some(90)));
    assert!(!take_observed_build(query_id));
    let snapshot = context.metrics.snapshot();
    assert!(snapshot.spill_write_bytes > 0);
    let join = snapshot
        .operators
        .iter()
        .find(|operator| operator.name == "Join")
        .expect("forced Spill keeps the Join metric");
    let spill = snapshot
        .operators
        .iter()
        .find(|operator| {
            operator.name == "JoinSpillExecution" && operator.parent_id == Some(join.id)
        })
        .expect("forced Spill records its execution phase");
    assert!(spill.elapsed.as_nanos() > 0);
    assert!(
        !snapshot
            .operators
            .iter()
            .any(|operator| operator.name == "JoinProbe" && operator.parent_id == Some(join.id))
    );
    finish(context).await;
}

struct Fixture {
    root: tempfile::TempDir,
    left_schema: SchemaRef,
    right_schema: SchemaRef,
    join_schema: SchemaRef,
    aggregate_schema: SchemaRef,
}

impl Fixture {
    fn new() -> Self {
        let left_schema = side_schema("left");
        let right_schema = side_schema("right");
        let join_schema = Arc::new(Schema::new(
            left_schema
                .fields()
                .iter()
                .chain(right_schema.fields())
                .cloned()
                .collect::<Vec<_>>(),
        ));
        let aggregate_schema = Arc::new(Schema::new(vec![
            Field::new("count(*)", DataType::Int64, false),
            Field::new("sum(value)", DataType::Decimal128(38, 0), true),
        ]));
        Self {
            root: tempfile::tempdir().unwrap(),
            left_schema,
            right_schema,
            join_schema,
            aggregate_schema,
        }
    }

    async fn run(
        &self,
        aggregates: Vec<AggregateExpr>,
        query_id: Option<Uuid>,
        empty_right: bool,
    ) -> (Arc<QueryContext>, (i64, Option<i128>)) {
        let query_id = query_id.unwrap_or_else(Uuid::new_v4);
        let context = Arc::new(
            QueryContext::with_query_id(query_id, MemoryPool::new(128 << 20), self.root.path())
                .unwrap(),
        );
        context.configure_compute_lanes_unbounded_for_test(2);

        let left = input_stream(
            vec![left_batch(&self.left_schema)],
            &context,
            "multiplicity left test input",
        );
        let right = input_stream(
            if empty_right {
                Vec::new()
            } else {
                right_batches(&self.right_schema)
            },
            &context,
            "multiplicity right test input",
        );
        let operator = context.metrics.register_operator("Join", None);
        let mut output = join_global_aggregate(
            left,
            right,
            vec![
                (
                    BoundExpr::column(0, DataType::Utf8, "left.k1"),
                    BoundExpr::column(0, DataType::Utf8, "right.k1"),
                ),
                (
                    BoundExpr::column(1, DataType::Utf8, "left.k2"),
                    BoundExpr::column(1, DataType::Utf8, "right.k2"),
                ),
            ],
            Arc::clone(&self.left_schema),
            Arc::clone(&self.right_schema),
            Arc::clone(&self.join_schema),
            aggregates,
            Arc::clone(&self.aggregate_schema),
            Arc::clone(&context),
            2,
            None,
            operator,
        );

        let batch = output.next().await.unwrap().unwrap();
        assert!(output.next().await.is_none());
        let sum = batch
            .batch()
            .column(1)
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .unwrap();
        let values = (
            batch
                .batch()
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            (!sum.is_null(0)).then(|| sum.value(0)),
        );
        drop(batch);
        drop(output);
        (context, values)
    }
}

fn side_schema(prefix: &str) -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new(format!("{prefix}_k1"), DataType::Utf8, true),
        Field::new(format!("{prefix}_k2"), DataType::Utf8, false),
        Field::new(format!("{prefix}_value"), DataType::Int64, false),
    ]))
}

fn left_batch(schema: &SchemaRef) -> RecordBatch {
    RecordBatch::try_new(
        Arc::clone(schema),
        vec![
            Arc::new(StringArray::from(vec![
                Some("a"),
                Some("a"),
                Some("b"),
                None,
            ])),
            Arc::new(StringArray::from(vec!["x", "x", "y", "z"])),
            Arc::new(Int64Array::from(vec![10, 20, 30, 40])),
        ],
    )
    .unwrap()
}

fn right_batches(schema: &SchemaRef) -> Vec<RecordBatch> {
    vec![
        RecordBatch::try_new(
            Arc::clone(schema),
            vec![
                Arc::new(StringArray::from(vec!["a", "b"])),
                Arc::new(StringArray::from(vec!["x", "y"])),
                Arc::new(Int64Array::from(vec![5, 7])),
            ],
        )
        .unwrap(),
        RecordBatch::try_new(
            Arc::clone(schema),
            vec![
                Arc::new(StringArray::from(vec!["a", "c"])),
                Arc::new(StringArray::from(vec!["x", "z"])),
                Arc::new(Int64Array::from(vec![11, 13])),
            ],
        )
        .unwrap(),
    ]
}

fn input_stream(
    batches: Vec<RecordBatch>,
    context: &QueryContext,
    owner: &'static str,
) -> crate::runtime::MemoryBatchStream {
    let batches = batches
        .into_iter()
        .map(|batch| BatchEnvelope::try_new(batch, &context.memory, owner))
        .collect::<Vec<_>>();
    boxed_memory_batch_stream(stream::iter(batches))
}

fn probe_aggregates() -> Vec<AggregateExpr> {
    aggregates(2)
}

fn build_aggregates() -> Vec<AggregateExpr> {
    aggregates(5)
}

fn aggregates(sum_column: usize) -> Vec<AggregateExpr> {
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
            expr: Some(BoundExpr::column(sum_column, DataType::Int64, "value")),
            distinct: false,
            data_type: DataType::Decimal128(38, 0),
            display_name: "sum(value)".into(),
        },
    ]
}

async fn finish(context: Arc<QueryContext>) {
    let directory = context.spill.directory().to_owned();
    context.cleanup_spill_after_tasks().await.unwrap();
    assert_eq!(context.memory.used(), 0);
    assert!(!directory.exists());
}
