use std::sync::Arc;

use arrow::{
    array::{Decimal128Array, Int64Array},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use futures::TryStreamExt;

use crate::{
    execution::aggregate::aggregate,
    runtime::{MemoryPool, QueryContext, boxed_record_batch_stream},
    sql::{AggregateExpr, AggregateFunction, BoundExpr},
};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn amplification_limit_allows_fifty_thousand_values_repeated_twice() {
    const UNIQUE: i64 = 50_000;
    let input_schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int64,
        false,
    )]));
    let batches = (0..UNIQUE)
        .chain(0..UNIQUE)
        .collect::<Vec<_>>()
        .chunks(256)
        .map(|values| {
            RecordBatch::try_new(
                Arc::clone(&input_schema),
                vec![Arc::new(Int64Array::from(values.to_vec()))],
            )
            .unwrap()
        })
        .collect::<Vec<_>>();
    let aggregates = vec![
        distinct(AggregateFunction::Count, DataType::Int64),
        distinct(AggregateFunction::Sum, DataType::Decimal128(38, 0)),
    ];
    let output_schema = Arc::new(Schema::new(vec![
        Field::new("count", DataType::Int64, false),
        Field::new("sum", DataType::Decimal128(38, 0), true),
    ]));
    let temp = tempfile::tempdir().unwrap();
    let mut query_context = QueryContext::new(MemoryPool::new(4 << 20), temp.path()).unwrap();
    query_context.execution.max_spill_write_amplification = Some(1.2);
    let context = Arc::new(query_context);
    context.configure_compute_lanes_unbounded_for_test(4);

    let output = aggregate(
        boxed_record_batch_stream(futures::stream::iter(batches.into_iter().map(Ok))),
        Vec::new(),
        aggregates,
        output_schema,
        Arc::clone(&context),
        256,
    )
    .try_collect::<Vec<_>>()
    .await
    .unwrap();

    let count = output[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let sum = output[0]
        .column(1)
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .unwrap();
    assert_eq!(count.value(0), UNIQUE);
    assert_eq!(
        sum.value(0),
        i128::from(UNIQUE) * i128::from(UNIQUE - 1) / 2
    );
    let metrics = context.metrics.snapshot();
    assert!(metrics.spill_write_bytes > 0, "metrics: {metrics:?}");
    assert_eq!(metrics.max_repartition_depth, 0, "metrics: {metrics:?}");
    assert!(
        metrics.spill_write_amplification().unwrap() <= 1.2,
        "metrics: {metrics:?}"
    );
    assert_eq!(metrics.spill_quota_rejections, 0, "metrics: {metrics:?}");
    drop(output);
    assert_eq!(context.memory.used(), 0);
}

fn distinct(function: AggregateFunction, output_type: DataType) -> AggregateExpr {
    AggregateExpr {
        function,
        expr: Some(BoundExpr::column(0, DataType::Int64, "value")),
        distinct: true,
        data_type: output_type,
        display_name: format!("{function}(distinct value)"),
    }
}
