use std::{
    collections::hash_map::DefaultHasher,
    hash::{Hash, Hasher},
    sync::Arc,
};

use arrow::{
    array::{
        ArrayRef, Date32Array, Date64Array, DurationMicrosecondArray, DurationMillisecondArray,
        DurationNanosecondArray, DurationSecondArray, Time32MillisecondArray, Time32SecondArray,
        Time64MicrosecondArray, Time64NanosecondArray, TimestampMicrosecondArray,
        TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray,
    },
    datatypes::DataType,
};

use super::{CellValue, cell};

#[test]
fn reads_temporal_physical_values_without_changing_units() {
    let arrays: Vec<(ArrayRef, i64)> = vec![
        (Arc::new(Date32Array::from(vec![Some(-12)])), -12),
        (
            Arc::new(Date64Array::from(vec![Some(86_400_001)])),
            86_400_001,
        ),
        (
            Arc::new(Time32SecondArray::from(vec![Some(43_210)])),
            43_210,
        ),
        (
            Arc::new(Time32MillisecondArray::from(vec![Some(43_210_123)])),
            43_210_123,
        ),
        (
            Arc::new(Time64MicrosecondArray::from(vec![Some(43_210_123_456)])),
            43_210_123_456,
        ),
        (
            Arc::new(Time64NanosecondArray::from(vec![Some(43_210_123_456_789)])),
            43_210_123_456_789,
        ),
        (Arc::new(TimestampSecondArray::from(vec![Some(-17)])), -17),
        (
            Arc::new(TimestampMillisecondArray::from(vec![Some(1_234)])),
            1_234,
        ),
        (
            Arc::new(TimestampMicrosecondArray::from(vec![Some(1_234_567)])),
            1_234_567,
        ),
        (
            Arc::new(TimestampNanosecondArray::from(vec![Some(1_234_567_890)])),
            1_234_567_890,
        ),
        (Arc::new(DurationSecondArray::from(vec![Some(-2)])), -2),
        (
            Arc::new(DurationMillisecondArray::from(vec![Some(-2_003)])),
            -2_003,
        ),
        (
            Arc::new(DurationMicrosecondArray::from(vec![Some(-2_003_004)])),
            -2_003_004,
        ),
        (
            Arc::new(DurationNanosecondArray::from(vec![Some(-2_003_004_005)])),
            -2_003_004_005,
        ),
    ];

    for (array, expected) in arrays {
        assert_eq!(cell(&array, 0).unwrap(), CellValue::Int64(expected));
    }
}

#[test]
fn reads_zoned_timestamps_as_their_physical_values() {
    let arrays: Vec<ArrayRef> = vec![
        Arc::new(TimestampSecondArray::from(vec![Some(17)]).with_timezone("UTC")),
        Arc::new(TimestampMillisecondArray::from(vec![Some(17)]).with_timezone("Asia/Singapore")),
        Arc::new(TimestampMicrosecondArray::from(vec![Some(17)]).with_timezone("UTC")),
        Arc::new(TimestampNanosecondArray::from(vec![Some(17)]).with_timezone("Asia/Singapore")),
    ];

    for array in arrays {
        assert!(matches!(array.data_type(), DataType::Timestamp(_, Some(_))));
        assert_eq!(cell(&array, 0).unwrap(), CellValue::Int64(17));
    }
}

#[test]
fn returns_null_for_every_temporal_family() {
    let arrays: Vec<ArrayRef> = vec![
        Arc::new(Date32Array::from(vec![None])),
        Arc::new(Date64Array::from(vec![None])),
        Arc::new(Time32SecondArray::from(vec![None])),
        Arc::new(Time32MillisecondArray::from(vec![None])),
        Arc::new(Time64MicrosecondArray::from(vec![None])),
        Arc::new(Time64NanosecondArray::from(vec![None])),
        Arc::new(TimestampSecondArray::from(vec![None])),
        Arc::new(TimestampMillisecondArray::from(vec![None])),
        Arc::new(TimestampMicrosecondArray::from(vec![None])),
        Arc::new(TimestampNanosecondArray::from(vec![None])),
        Arc::new(DurationSecondArray::from(vec![None])),
        Arc::new(DurationMillisecondArray::from(vec![None])),
        Arc::new(DurationMicrosecondArray::from(vec![None])),
        Arc::new(DurationNanosecondArray::from(vec![None])),
    ];

    for array in arrays {
        assert_eq!(cell(&array, 0).unwrap(), CellValue::Null);
    }
}

#[test]
fn equivalent_interval_families_share_equality_order_and_hash() {
    let day = CellValue::IntervalDayTime(1, 0);
    let hours = CellValue::IntervalMonthDayNano(0, 0, 86_400_000_000_000);
    let month = CellValue::IntervalYearMonth(1);
    let days = CellValue::IntervalMonthDayNano(0, 30, 0);

    for (left, right) in [(day, hours), (month, days)] {
        assert_eq!(left, right);
        assert!(left.compare(&right).unwrap().is_eq());
        let mut left_hash = DefaultHasher::new();
        left.hash(&mut left_hash);
        let mut right_hash = DefaultHasher::new();
        right.hash(&mut right_hash);
        assert_eq!(left_hash.finish(), right_hash.finish());
    }
}
