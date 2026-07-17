use std::sync::Arc;

use arrow::{
    array::{Decimal128Array, Int64Array},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use futures::{StreamExt, stream};

use crate::{
    execution::join::{condition::JoinPredicates, probe::try_build_hash_table},
    runtime::{BatchEnvelope, MemoryPool, QueryContext, boxed_memory_batch_stream},
    sql::{AggregateExpr, AggregateFunction, BoundExpr},
};

use super::{FrozenBuild, probe_global_aggregate};

#[tokio::test]
async fn four_lanes_count_and_sum_join_selections_without_materializing() {
    let directory = tempfile::tempdir().unwrap();
    let context = QueryContext::shared(MemoryPool::new(128 << 20), directory.path()).unwrap();
    context.configure_compute_lanes_unbounded_for_test(4);

    let left_schema = Arc::new(Schema::new(vec![
        Field::new("left_key", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
    ]));
    let right_schema = Arc::new(Schema::new(vec![Field::new(
        "right_key",
        DataType::Int64,
        false,
    )]));
    let join_schema = Arc::new(Schema::new(vec![
        Field::new("left_key", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
        Field::new("right_key", DataType::Int64, false),
    ]));
    let aggregate_schema = Arc::new(Schema::new(vec![
        Field::new("count(*)", DataType::Int64, false),
        Field::new("sum(value)", DataType::Decimal128(38, 0), true),
    ]));

    let right = RecordBatch::try_new(
        Arc::clone(&right_schema),
        vec![Arc::new(Int64Array::from(vec![1, 1, 2]))],
    )
    .unwrap();
    let mut build_memory = context.memory.reservation();
    build_memory
        .try_grow(right.get_array_memory_size())
        .unwrap();
    let right_keys = vec![Arc::clone(right.column(0))];
    let hash_table = try_build_hash_table(&right_keys, right.num_rows(), false, &mut build_memory)
        .unwrap()
        .unwrap();
    let build = FrozenBuild::new(right, hash_table, None, None, false, None, build_memory);

    let left_batches = vec![
        left_batch(&left_schema, vec![1, 2], vec![10, 20]),
        left_batch(&left_schema, vec![1, 3], vec![30, 40]),
    ];
    let input_batches = left_batches
        .into_iter()
        .map(|batch| BatchEnvelope::try_new(batch, &context.memory, "parallel join aggregate test"))
        .collect::<Vec<_>>();
    let input = boxed_memory_batch_stream(stream::iter(input_batches));
    let aggregates = vec![count_star(), sum_value()];
    let predicates = JoinPredicates::new(None, None, &left_schema, &right_schema);
    let operator = context.metrics.register_operator("Join", None);
    let mut output = probe_global_aggregate(
        input,
        vec![BoundExpr::column(0, DataType::Int64, "left_key")],
        build,
        predicates,
        join_schema,
        aggregates,
        aggregate_schema,
        operator,
        Arc::clone(&context),
        2,
    );

    let batch = output.next().await.unwrap().unwrap();
    assert!(output.next().await.is_none());
    assert_eq!(
        batch
            .batch()
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        5
    );
    assert_eq!(
        batch
            .batch()
            .column(1)
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .unwrap()
            .value(0),
        100
    );

    let metrics = context.metrics.snapshot();
    let join = metrics
        .operators
        .iter()
        .find(|operator| operator.name == "Join")
        .unwrap();
    assert_eq!(join.output_rows, 5);
    assert_eq!(join.output_bytes, 0);
}

fn left_batch(schema: &Arc<Schema>, keys: Vec<i64>, values: Vec<i64>) -> RecordBatch {
    RecordBatch::try_new(
        Arc::clone(schema),
        vec![
            Arc::new(Int64Array::from(keys)),
            Arc::new(Int64Array::from(values)),
        ],
    )
    .unwrap()
}

fn count_star() -> AggregateExpr {
    AggregateExpr {
        function: AggregateFunction::Count,
        expr: None,
        distinct: false,
        data_type: DataType::Int64,
        display_name: "count(*)".into(),
    }
}

fn sum_value() -> AggregateExpr {
    AggregateExpr {
        function: AggregateFunction::Sum,
        expr: Some(BoundExpr::column(1, DataType::Int64, "value")),
        distinct: false,
        data_type: DataType::Decimal128(38, 0),
        display_name: "sum(value)".into(),
    }
}
