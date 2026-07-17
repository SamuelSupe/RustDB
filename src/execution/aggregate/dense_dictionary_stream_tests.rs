use std::{collections::HashMap, sync::Arc};

use arrow::{
    array::{
        Array, ArrayRef, Decimal128Array, DictionaryArray, Int64Array, StringArray, UInt32Array,
    },
    datatypes::{DataType, Field, Schema, UInt32Type},
    record_batch::RecordBatch,
};
use futures::TryStreamExt;

use super::super::aggregate;
use crate::{
    runtime::{MemoryPool, QueryContext, boxed_record_batch_stream},
    sql::{AggregateExpr, AggregateFunction, BoundExpr},
};

#[tokio::test]
async fn stream_preserves_groups_across_reordered_duplicate_dictionaries() {
    let decimal_type = DataType::Decimal128(10, 2);
    let dictionary_type =
        DataType::Dictionary(Box::new(DataType::UInt32), Box::new(DataType::Utf8));
    let input_schema = Arc::new(Schema::new(vec![
        Field::new("key", dictionary_type, true),
        Field::new("quantity", decimal_type.clone(), true),
    ]));
    let first = batch(
        Arc::clone(&input_schema),
        vec![Some(0), Some(1), Some(2), None],
        vec!["A", "B", "A"],
        vec![Some(100), Some(200), None, Some(400)],
    );
    let second = batch(
        Arc::clone(&input_schema),
        vec![Some(1), Some(0), Some(2), None, Some(1)],
        vec!["B", "A", "B"],
        vec![Some(300), Some(50), Some(75), None, Some(25)],
    );
    let aggregates = vec![
        AggregateExpr {
            function: AggregateFunction::Count,
            expr: None,
            distinct: false,
            data_type: DataType::Int64,
            display_name: "count(*)".into(),
        },
        AggregateExpr {
            function: AggregateFunction::Sum,
            expr: Some(BoundExpr::column(1, decimal_type, "quantity")),
            distinct: false,
            data_type: DataType::Decimal128(38, 2),
            display_name: "sum(quantity)".into(),
        },
    ];
    let output_schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::Utf8, true),
        Field::new("rows", DataType::Int64, false),
        Field::new("quantity", DataType::Decimal128(38, 2), true),
    ]));
    let temp = tempfile::tempdir().unwrap();
    let context = QueryContext::shared(MemoryPool::new(16 << 20), temp.path()).unwrap();
    context.configure_compute_lanes(1);

    let output = aggregate(
        boxed_record_batch_stream(futures::stream::iter([Ok(first), Ok(second)])),
        vec![BoundExpr::column(0, DataType::Utf8, "key")],
        aggregates,
        output_schema,
        Arc::clone(&context),
        64,
    )
    .try_collect::<Vec<_>>()
    .await
    .unwrap();

    let mut actual = HashMap::new();
    for batch in &output {
        let keys = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
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
            let key = (!keys.is_null(row)).then(|| keys.value(row).to_owned());
            actual.insert(key, (counts.value(row), sums.value(row)));
        }
    }
    assert_eq!(actual[&Some("A".into())], (4, 425));
    assert_eq!(actual[&Some("B".into())], (3, 325));
    assert_eq!(actual[&None], (2, 400));

    drop(output);
    assert_eq!(context.memory.used(), 0);
}

fn batch(
    schema: Arc<Schema>,
    keys: Vec<Option<u32>>,
    dictionary: Vec<&str>,
    values: Vec<Option<i128>>,
) -> RecordBatch {
    let groups = DictionaryArray::<UInt32Type>::try_new(
        UInt32Array::from(keys),
        Arc::new(StringArray::from(dictionary)),
    )
    .unwrap();
    let values = Decimal128Array::from(values)
        .with_precision_and_scale(10, 2)
        .unwrap();
    RecordBatch::try_new(schema, vec![Arc::new(groups) as ArrayRef, Arc::new(values)]).unwrap()
}
