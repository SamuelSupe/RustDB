use std::sync::Arc;

use arrow::{
    array::{
        Array, ArrayRef, Decimal128Array, Int8Array, Int16Array, StringDictionaryBuilder,
        TimestampSecondArray,
    },
    datatypes::{DataType, Field, Int8Type, Schema, TimeUnit},
    record_batch::RecordBatch,
};

use super::align_batch_to_schema;

fn schema(fields: Vec<(&str, DataType, bool)>) -> Schema {
    Schema::new(
        fields
            .into_iter()
            .map(|(name, data_type, nullable)| Field::new(name, data_type, nullable))
            .collect::<Vec<_>>(),
    )
}

#[test]
fn casts_values_and_fills_nullable_missing_columns() {
    let source_schema = Arc::new(schema(vec![
        ("id", DataType::Int8, false),
        ("ignored", DataType::Int16, false),
    ]));
    let batch = RecordBatch::try_new(
        source_schema,
        vec![
            Arc::new(Int8Array::from(vec![1_i8, 2])) as ArrayRef,
            Arc::new(Int16Array::from(vec![10_i16, 20])) as ArrayRef,
        ],
    )
    .unwrap();
    let target = Arc::new(schema(vec![
        ("id", DataType::Int16, false),
        ("new", DataType::Utf8, true),
    ]));

    let aligned =
        align_batch_to_schema(batch, Arc::clone(&target), "file:///data.parquet").unwrap();
    assert_eq!(aligned.schema(), target);
    assert_eq!(aligned.column(0).data_type(), &DataType::Int16);
    assert_eq!(aligned.column(1).null_count(), 2);
}

#[test]
fn decodes_dictionary_values() {
    let mut builder = StringDictionaryBuilder::<Int8Type>::new();
    builder.append("alpha").unwrap();
    builder.append("beta").unwrap();
    let dictionary = Arc::new(builder.finish()) as ArrayRef;
    let batch = RecordBatch::try_from_iter([("name", dictionary)]).unwrap();
    let target = Arc::new(schema(vec![("name", DataType::Utf8, false)]));
    let aligned = align_batch_to_schema(batch, target, "s3://bucket/data.parquet").unwrap();
    assert_eq!(aligned.column(0).data_type(), &DataType::Utf8);
}

#[test]
fn rejects_overflow_nullability_and_lossy_casts() {
    let timestamp = Arc::new(TimestampSecondArray::from(vec![i64::MAX])) as ArrayRef;
    let batch = RecordBatch::try_from_iter([("created_at", timestamp)]).unwrap();
    let target = Arc::new(schema(vec![(
        "created_at",
        DataType::Timestamp(TimeUnit::Nanosecond, None),
        false,
    )]));
    let error = align_batch_to_schema(batch, target, "file:///overflow.parquet")
        .unwrap_err()
        .to_string();
    assert!(error.contains("overflow.parquet"), "{error}");
    assert!(error.contains("created_at"), "{error}");
    assert!(error.contains("failed"), "{error}");

    let nullable_batch = RecordBatch::try_new(
        Arc::new(schema(vec![("id", DataType::Int8, true)])),
        vec![Arc::new(Int8Array::from(vec![Some(1), None])) as ArrayRef],
    )
    .unwrap();
    let non_nullable = Arc::new(schema(vec![("id", DataType::Int8, false)]));
    let error = align_batch_to_schema(nullable_batch, non_nullable, "file:///null.parquet")
        .unwrap_err()
        .to_string();
    assert!(error.contains("NULL values"), "{error}");

    let missing_batch =
        RecordBatch::try_from_iter([("other", Arc::new(Int8Array::from(vec![1_i8])) as ArrayRef)])
            .unwrap();
    let required = Arc::new(schema(vec![("id", DataType::Int8, false)]));
    let error = align_batch_to_schema(missing_batch, required, "file:///missing.parquet")
        .unwrap_err()
        .to_string();
    assert!(error.contains("missing.parquet"), "{error}");
    assert!(error.contains("column 'id'"), "{error}");
    assert!(error.contains("missing"), "{error}");

    let float_batch = RecordBatch::try_from_iter([(
        "value",
        Arc::new(arrow::array::Float64Array::from(vec![1.5])) as ArrayRef,
    )])
    .unwrap();
    let integer_target = Arc::new(schema(vec![("value", DataType::Int64, false)]));
    let error = align_batch_to_schema(float_batch, integer_target, "s3://bucket/lossy.parquet")
        .unwrap_err()
        .to_string();
    assert!(error.contains("lossy.parquet"), "{error}");
    assert!(error.contains("column 'value'"), "{error}");
}

#[test]
fn checked_decimal_cast_preserves_scale() {
    let decimal = Decimal128Array::from(vec![Some(123_i128)])
        .with_precision_and_scale(3, 0)
        .unwrap();
    let batch = RecordBatch::try_from_iter([("amount", Arc::new(decimal) as ArrayRef)]).unwrap();
    let target = Arc::new(schema(vec![("amount", DataType::Decimal128(5, 2), false)]));
    let aligned = align_batch_to_schema(batch, target, "file:///decimal.parquet").unwrap();
    let values = aligned
        .column(0)
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .unwrap();
    assert_eq!(values.value(0), 12_300);
}
