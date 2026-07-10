use std::{collections::HashMap, sync::Arc};

use arrow::{
    array::{Int64Array, StringArray},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use futures::TryStreamExt;

use super::{aggregate, spill, state::AggregateState};
use crate::{
    Error,
    runtime::{MemoryPool, QueryContext, boxed_record_batch_stream},
    sql::{AggregateExpr, AggregateFunction, BoundExpr},
};

use super::super::value::CellValue;

#[test]
fn decimal_aggregates_preserve_scale_nulls_and_overflow() {
    let data_type = DataType::Decimal128(5, 2);
    let sum_expr = aggregate_expr(AggregateFunction::Sum, data_type.clone());
    let mut sum = AggregateState::new(&sum_expr);
    sum.update(&sum_expr, Some(CellValue::Null)).unwrap();
    assert_eq!(sum.finish().unwrap(), CellValue::Null);
    sum.update(&sum_expr, Some(CellValue::Decimal128(125)))
        .unwrap();
    sum.update(&sum_expr, Some(CellValue::Decimal128(275)))
        .unwrap();
    assert_eq!(sum.finish().unwrap(), CellValue::Decimal128(400));

    let avg_expr = aggregate_expr(AggregateFunction::Avg, data_type);
    let mut avg = AggregateState::new(&avg_expr);
    avg.update(&avg_expr, Some(CellValue::Decimal128(100)))
        .unwrap();
    avg.update(&avg_expr, Some(CellValue::Null)).unwrap();
    avg.update(&avg_expr, Some(CellValue::Decimal128(200)))
        .unwrap();
    assert_eq!(avg.finish().unwrap(), CellValue::Decimal128(150));

    let overflow_expr = aggregate_expr(AggregateFunction::Sum, DataType::Decimal128(3, 0));
    let mut overflow = AggregateState::new(&overflow_expr);
    overflow
        .update(&overflow_expr, Some(CellValue::Decimal128(999)))
        .unwrap();
    overflow
        .update(&overflow_expr, Some(CellValue::Decimal128(1)))
        .unwrap();
    assert!(overflow.finish().is_err());
}

fn aggregate_expr(function: AggregateFunction, data_type: DataType) -> AggregateExpr {
    AggregateExpr {
        function,
        expr: Some(BoundExpr::column(0, data_type.clone(), "value")),
        data_type,
        display_name: function.to_string(),
    }
}

#[tokio::test]
async fn recursively_repartitions_a_seed_skewed_spill_partition() {
    const MEMORY_LIMIT: usize = 2_048;
    const GROUPS: usize = 96;

    let keys = (0_i64..100_000)
        .filter(|key| {
            spill::partition_for_key(&[CellValue::Int64(*key)], spill::SPILL_PARTITIONS, 0) == 0
        })
        .take(GROUPS)
        .collect::<Vec<_>>();
    assert_eq!(keys.len(), GROUPS);
    let values = keys.iter().chain(keys.iter()).copied().collect::<Vec<_>>();
    let input_schema = Arc::new(Schema::new(vec![Field::new("key", DataType::Int64, false)]));
    let input_batch = RecordBatch::try_new(
        Arc::clone(&input_schema),
        vec![Arc::new(Int64Array::from(values))],
    )
    .unwrap();
    let input = boxed_record_batch_stream(futures::stream::once(async move { Ok(input_batch) }));
    let output_schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::Int64, false),
        Field::new("rows", DataType::Int64, false),
    ]));
    let count = AggregateExpr {
        function: AggregateFunction::Count,
        expr: None,
        data_type: DataType::Int64,
        display_name: "count(*)".into(),
    };
    let temp = tempfile::tempdir().unwrap();
    let context = QueryContext::shared(MemoryPool::new(MEMORY_LIMIT), temp.path()).unwrap();

    let batches = aggregate(
        input,
        vec![BoundExpr::column(0, DataType::Int64, "key")],
        vec![count],
        output_schema,
        Arc::clone(&context),
        11,
    )
    .try_collect::<Vec<_>>()
    .await
    .unwrap();

    let mut actual = HashMap::new();
    for batch in batches {
        let output_keys = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let counts = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for row in 0..batch.num_rows() {
            actual.insert(output_keys.value(row), counts.value(row));
        }
    }
    assert_eq!(actual.len(), GROUPS);
    assert!(actual.values().all(|count| *count == 2));
    assert!(context.metrics.snapshot().spill_partitions > spill::SPILL_PARTITIONS as u64);
    assert_eq!(context.memory.used(), 0);
}

#[tokio::test]
async fn reports_when_one_group_cannot_fit() {
    let value = "x".repeat(1_024);
    let input_schema = Arc::new(Schema::new(vec![Field::new("key", DataType::Utf8, false)]));
    let input_batch =
        RecordBatch::try_new(input_schema, vec![Arc::new(StringArray::from(vec![value]))]).unwrap();
    let input = boxed_record_batch_stream(futures::stream::once(async move { Ok(input_batch) }));
    let output_schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::Utf8, false),
        Field::new("rows", DataType::Int64, false),
    ]));
    let count = AggregateExpr {
        function: AggregateFunction::Count,
        expr: None,
        data_type: DataType::Int64,
        display_name: "count(*)".into(),
    };
    let temp = tempfile::tempdir().unwrap();
    let context = QueryContext::shared(MemoryPool::new(256), temp.path()).unwrap();

    let error = aggregate(
        input,
        vec![BoundExpr::column(0, DataType::Utf8, "key")],
        vec![count],
        output_schema,
        context,
        8,
    )
    .try_collect::<Vec<_>>()
    .await
    .unwrap_err();
    assert!(matches!(
        error,
        Error::ResourceExhausted(message)
            if message.contains("one aggregate group") && message.contains("query limit")
    ));
}
