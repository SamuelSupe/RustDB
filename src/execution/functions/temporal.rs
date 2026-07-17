use std::sync::Arc;

use arrow::{
    array::{
        Array, ArrayRef, Date32Array, Int32Array, Int64Array, StringArray,
        TimestampMicrosecondArray,
    },
    compute::{
        cast,
        kernels::temporal::{DatePart, date_part as arrow_date_part},
    },
    datatypes::{DataType, TimeUnit},
};

use crate::sql::DateTimePart;
use crate::sql::temporal::{
    date32_to_timestamp, format_date32, format_timestamp_microsecond, parse_date32,
    parse_timestamp_microsecond, timestamp_to_date32, truncate_date, truncate_timestamp,
};
use crate::{Error, Result};

pub(super) fn date_part(part: DateTimePart, array: &ArrayRef) -> Result<ArrayRef> {
    let part = match part {
        DateTimePart::Year => DatePart::Year,
        DateTimePart::Month => DatePart::Month,
        DateTimePart::Day => DatePart::Day,
        DateTimePart::Hour => DatePart::Hour,
        DateTimePart::Minute => DatePart::Minute,
        DateTimePart::Second => DatePart::Second,
    };
    let output = arrow_date_part(array.as_ref(), part)?;
    Ok(cast(output.as_ref(), &DataType::Int64)?)
}

pub(super) fn to_timestamp_seconds(array: &ArrayRef) -> Result<ArrayRef> {
    let values = downcast::<Int64Array>(array, "Int64")?;
    let output = (0..values.len())
        .map(|row| {
            values
                .is_valid(row)
                .then(|| {
                    values.value(row).checked_mul(1_000_000).ok_or_else(|| {
                        Error::Execution(format!(
                            "to_timestamp_seconds overflowed TIMESTAMP at row {row}"
                        ))
                    })
                })
                .transpose()
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(Arc::new(TimestampMicrosecondArray::from(output)))
}

pub(super) fn integer_to_date(array: &ArrayRef) -> Result<ArrayRef> {
    let days = cast(array.as_ref(), &DataType::Int32)?;
    let days = downcast::<Int32Array>(&days, "Int32")?;
    Ok(Arc::new(Date32Array::from_iter(days.iter())))
}

pub(super) fn is_integer(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
    )
}

pub(super) fn date_trunc(part: DateTimePart, array: &ArrayRef) -> Result<ArrayRef> {
    match array.data_type() {
        DataType::Date32 => {
            let values = downcast::<Date32Array>(array, "Date32")?;
            if is_calendar_part(part) {
                Ok(Arc::new(Date32Array::from_iter((0..values.len()).map(
                    |row| {
                        values
                            .is_valid(row)
                            .then(|| truncate_date(values.value(row), part))
                    },
                ))))
            } else {
                let output = (0..values.len())
                    .map(|row| {
                        values
                            .is_valid(row)
                            .then(|| date32_to_timestamp(values.value(row)))
                            .transpose()
                    })
                    .collect::<Result<Vec<_>>>()?;
                Ok(Arc::new(TimestampMicrosecondArray::from(output)))
            }
        }
        DataType::Timestamp(_, None) => {
            let values = timestamp_microseconds(array)?;
            if is_calendar_part(part) {
                let output = (0..values.len())
                    .map(|row| {
                        values
                            .is_valid(row)
                            .then(|| {
                                timestamp_to_date32(values.value(row))
                                    .map(|days| truncate_date(days, part))
                            })
                            .transpose()
                    })
                    .collect::<Result<Vec<_>>>()?;
                return Ok(Arc::new(Date32Array::from(output)));
            }
            let output = TimestampMicrosecondArray::from_iter(
                (0..values.len())
                    .map(|row| {
                        values
                            .is_valid(row)
                            .then(|| truncate_timestamp(values.value(row), part))
                            .transpose()
                    })
                    .collect::<Result<Vec<_>>>()?,
            );
            Ok(Arc::new(output))
        }
        other => Err(Error::Execution(format!(
            "date_trunc does not support {other}"
        ))),
    }
}

fn is_calendar_part(part: DateTimePart) -> bool {
    matches!(
        part,
        DateTimePart::Year | DateTimePart::Month | DateTimePart::Day
    )
}

pub(super) fn string_to_date(array: &ArrayRef) -> Result<ArrayRef> {
    let values = downcast::<StringArray>(array, "Utf8")?;
    let output = (0..values.len())
        .map(|row| {
            values
                .is_valid(row)
                .then(|| parse_date32(values.value(row)))
                .transpose()
                .map_err(|error| cast_error(error, row, "DATE"))
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(Arc::new(Date32Array::from(output)))
}

pub(super) fn string_to_timestamp(array: &ArrayRef) -> Result<ArrayRef> {
    let values = downcast::<StringArray>(array, "Utf8")?;
    let output = (0..values.len())
        .map(|row| {
            values
                .is_valid(row)
                .then(|| parse_timestamp_microsecond(values.value(row)))
                .transpose()
                .map_err(|error| cast_error(error, row, "TIMESTAMP"))
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(Arc::new(TimestampMicrosecondArray::from(output)))
}

pub(super) fn date_to_string(array: &ArrayRef) -> Result<ArrayRef> {
    let values = downcast::<Date32Array>(array, "Date32")?;
    Ok(string_array((0..values.len()).map(|row| {
        values
            .is_valid(row)
            .then(|| format_date32(values.value(row)))
    })))
}

pub(super) fn timestamp_to_string(array: &ArrayRef) -> Result<ArrayRef> {
    let values = timestamp_microseconds(array)?;
    Ok(string_array((0..values.len()).map(|row| {
        values
            .is_valid(row)
            .then(|| format_timestamp_microsecond(values.value(row)))
    })))
}

pub(super) fn date_to_timestamp(array: &ArrayRef) -> Result<ArrayRef> {
    let values = downcast::<Date32Array>(array, "Date32")?;
    let output = (0..values.len())
        .map(|row| {
            values
                .is_valid(row)
                .then(|| date32_to_timestamp(values.value(row)))
                .transpose()
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(Arc::new(TimestampMicrosecondArray::from(output)))
}

pub(super) fn timestamp_to_date(array: &ArrayRef) -> Result<ArrayRef> {
    let values = timestamp_microseconds(array)?;
    let output = (0..values.len())
        .map(|row| {
            values
                .is_valid(row)
                .then(|| timestamp_to_date32(values.value(row)))
                .transpose()
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(Arc::new(Date32Array::from(output)))
}

fn timestamp_microseconds(array: &ArrayRef) -> Result<ArrayRefGuard<'_>> {
    if array.data_type() == &DataType::Timestamp(TimeUnit::Microsecond, None) {
        let values = downcast::<TimestampMicrosecondArray>(array, "Timestamp(Microsecond)")?;
        Ok(ArrayRefGuard::Borrowed(values))
    } else {
        let converted = cast(
            array.as_ref(),
            &DataType::Timestamp(TimeUnit::Microsecond, None),
        )?;
        Ok(ArrayRefGuard::Owned(converted))
    }
}

enum ArrayRefGuard<'a> {
    Borrowed(&'a TimestampMicrosecondArray),
    Owned(ArrayRef),
}

impl std::ops::Deref for ArrayRefGuard<'_> {
    type Target = TimestampMicrosecondArray;

    fn deref(&self) -> &Self::Target {
        match self {
            Self::Borrowed(values) => values,
            Self::Owned(values) => values
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .expect("cast requested Timestamp(Microsecond)"),
        }
    }
}

fn string_array(values: impl IntoIterator<Item = Option<String>>) -> ArrayRef {
    let values = values.into_iter().collect::<Vec<_>>();
    Arc::new(StringArray::from_iter(
        values.iter().map(|value| value.as_deref()),
    ))
}

fn cast_error(error: Error, row: usize, target: &str) -> Error {
    Error::Execution(format!(
        "strict CAST to {target} failed at row {row}: {error}"
    ))
}

fn downcast<'a, T: 'static>(array: &'a ArrayRef, name: &str) -> Result<&'a T> {
    array
        .as_any()
        .downcast_ref::<T>()
        .ok_or_else(|| Error::Internal(format!("expected {name} array, got {}", array.data_type())))
}

#[cfg(test)]
mod tests;
