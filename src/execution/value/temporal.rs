use arrow::{
    array::{ArrayRef, PrimitiveArray},
    datatypes::{
        ArrowPrimitiveType, DataType, Date32Type, Date64Type, DurationMicrosecondType,
        DurationMillisecondType, DurationNanosecondType, DurationSecondType, Time32MillisecondType,
        Time32SecondType, Time64MicrosecondType, Time64NanosecondType, TimeUnit,
        TimestampMicrosecondType, TimestampMillisecondType, TimestampNanosecondType,
        TimestampSecondType,
    },
};

use super::{CellValue, downcast};
use crate::{Error, Result};

pub(super) fn cell(array: &ArrayRef, row: usize) -> Result<CellValue> {
    match array.data_type() {
        DataType::Date32 => primitive::<Date32Type>(array, row),
        DataType::Date64 => primitive::<Date64Type>(array, row),
        DataType::Time32(TimeUnit::Second) => primitive::<Time32SecondType>(array, row),
        DataType::Time32(TimeUnit::Millisecond) => primitive::<Time32MillisecondType>(array, row),
        DataType::Time64(TimeUnit::Microsecond) => primitive::<Time64MicrosecondType>(array, row),
        DataType::Time64(TimeUnit::Nanosecond) => primitive::<Time64NanosecondType>(array, row),
        DataType::Timestamp(TimeUnit::Second, _) => primitive::<TimestampSecondType>(array, row),
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            primitive::<TimestampMillisecondType>(array, row)
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            primitive::<TimestampMicrosecondType>(array, row)
        }
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            primitive::<TimestampNanosecondType>(array, row)
        }
        DataType::Duration(TimeUnit::Second) => primitive::<DurationSecondType>(array, row),
        DataType::Duration(TimeUnit::Millisecond) => {
            primitive::<DurationMillisecondType>(array, row)
        }
        DataType::Duration(TimeUnit::Microsecond) => {
            primitive::<DurationMicrosecondType>(array, row)
        }
        DataType::Duration(TimeUnit::Nanosecond) => primitive::<DurationNanosecondType>(array, row),
        other => Err(Error::Unsupported(format!(
            "execution does not support values of type {other}"
        ))),
    }
}

fn primitive<T>(array: &ArrayRef, row: usize) -> Result<CellValue>
where
    T: ArrowPrimitiveType,
    T::Native: Into<i64>,
{
    Ok(CellValue::Int64(
        downcast::<PrimitiveArray<T>>(array)?.value(row).into(),
    ))
}
