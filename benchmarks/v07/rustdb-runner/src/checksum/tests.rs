use std::sync::Arc;

use arrow::{
    array::{
        BinaryArray, BooleanArray, Date32Array, Decimal128Array, Float64Array, Int64Array,
        NullArray, RecordBatch, StringArray, TimestampMicrosecondArray, UInt64Array,
    },
    datatypes::{DataType, Field, Schema, TimeUnit},
};

use super::MultisetChecksum;

fn checksum(batch: &RecordBatch) -> String {
    let mut checksum = MultisetChecksum::new();
    checksum.update_batch(batch).unwrap();
    checksum.finish()
}

#[test]
fn checksum_is_order_independent_and_duplicate_sensitive() {
    let batch = RecordBatch::try_from_iter(vec![
        ("name", Arc::new(StringArray::from(vec!["a", "b"])) as _),
        ("count", Arc::new(Int64Array::from(vec![1, 2])) as _),
    ])
    .unwrap();
    let reversed = RecordBatch::try_from_iter(vec![
        ("name", Arc::new(StringArray::from(vec!["b", "a"])) as _),
        ("count", Arc::new(Int64Array::from(vec![2, 1])) as _),
    ])
    .unwrap();
    let duplicate = RecordBatch::try_from_iter(vec![
        (
            "name",
            Arc::new(StringArray::from(vec!["a", "b", "a"])) as _,
        ),
        ("count", Arc::new(Int64Array::from(vec![1, 2, 1])) as _),
    ])
    .unwrap();

    assert_eq!(checksum(&batch), checksum(&reversed));
    assert_ne!(checksum(&batch), checksum(&duplicate));
}

#[test]
fn typed_nulls_do_not_hide_type_mismatches() {
    let integer =
        RecordBatch::try_from_iter(vec![("value", Arc::new(Int64Array::from(vec![None])) as _)])
            .unwrap();
    let float = RecordBatch::try_from_iter(vec![(
        "value",
        Arc::new(Float64Array::from(vec![None])) as _,
    )])
    .unwrap();

    assert_ne!(checksum(&integer), checksum(&float));
}

#[test]
fn untyped_null_matches_duckdb_default_integer_null() {
    let untyped =
        RecordBatch::try_from_iter(vec![("value", Arc::new(NullArray::new(1)) as _)]).unwrap();
    let integer =
        RecordBatch::try_from_iter(vec![("value", Arc::new(Int64Array::from(vec![None])) as _)])
            .unwrap();

    assert_eq!(checksum(&untyped), checksum(&integer));
}

#[test]
fn finite_float_and_timestamp_have_stable_v2_encoding() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("value", DataType::Float64, true),
        Field::new(
            "time",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        ),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Float64Array::from(vec![
                Some(1e20),
                Some(-0.0),
                Some(f64::NAN),
                Some(f64::INFINITY),
            ])),
            Arc::new(TimestampMicrosecondArray::from(vec![
                Some(123_456),
                Some(-1),
                None,
                Some(0),
            ])),
        ],
    )
    .unwrap();

    assert_eq!(
        checksum(&batch),
        "ca02c4c79bed388dfc877ef715693cd03b629f3b0eee195845c1b0486d1f4a8c"
    );
}

#[test]
fn v2_golden_covers_supported_scalar_families() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("flag", DataType::Boolean, true),
        Field::new("unsigned", DataType::UInt64, false),
        Field::new("decimal", DataType::Decimal128(38, 4), true),
        Field::new("float", DataType::Float64, false),
        Field::new("text", DataType::Utf8, true),
        Field::new("bytes", DataType::Binary, false),
        Field::new("date", DataType::Date32, false),
        Field::new(
            "time",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        ),
    ]));
    let decimals = Decimal128Array::from(vec![Some(1_234_500), None])
        .with_precision_and_scale(38, 4)
        .unwrap();
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(BooleanArray::from(vec![Some(true), None])),
            Arc::new(UInt64Array::from(vec![u64::MAX, 0])),
            Arc::new(decimals),
            Arc::new(Float64Array::from(vec![1e20, f64::NAN])),
            Arc::new(StringArray::from(vec![Some("héllo"), None])),
            Arc::new(BinaryArray::from(vec![
                Some(&[0_u8, 255][..]),
                Some(&[][..]),
            ])),
            Arc::new(Date32Array::from(vec![1, -1])),
            Arc::new(TimestampMicrosecondArray::from(vec![123_456, -1])),
        ],
    )
    .unwrap();

    assert_eq!(
        checksum(&batch),
        "0b00e14b84691ab79ceeec727f7f8dcbfde96c03896117872f6787035f8115ad"
    );
}
