use std::{
    cmp::Ordering,
    hash::{Hash, Hasher},
    sync::Arc,
};

use arrow::{
    array::{
        Array, ArrayRef, BinaryArray, BooleanArray, Decimal128Array, DictionaryArray,
        FixedSizeBinaryArray, FixedSizeBinaryBuilder, Float16Array, Float32Array, Float64Array,
        Int8Array, Int16Array, Int32Array, Int64Array, IntervalDayTimeArray,
        IntervalMonthDayNanoArray, IntervalYearMonthArray, LargeBinaryArray, LargeStringArray,
        StringArray, UInt8Array, UInt16Array, UInt32Array, UInt64Array, new_null_array,
    },
    compute::cast,
    datatypes::{
        ArrowDictionaryKeyType, DataType, Int8Type, Int16Type, Int32Type, Int64Type,
        IntervalDayTimeType, IntervalMonthDayNanoType, IntervalUnit, UInt8Type, UInt16Type,
        UInt32Type, UInt64Type,
    },
};

use crate::{Error, Result};

mod temporal;

#[derive(Clone, Debug)]
pub(crate) enum CellValue {
    Null,
    Boolean(bool),
    Int64(i64),
    UInt64(u64),
    Float64(f64),
    Utf8(String),
    Binary(Vec<u8>),
    Decimal128(i128),
    IntervalYearMonth(i32),
    IntervalDayTime(i32, i32),
    IntervalMonthDayNano(i32, i32, i64),
}

impl CellValue {
    pub fn is_null(&self) -> bool {
        matches!(self, Self::Null)
    }

    pub fn as_f64(&self) -> Result<f64> {
        match self {
            Self::Int64(value) => Ok(*value as f64),
            Self::UInt64(value) => Ok(*value as f64),
            Self::Float64(value) => Ok(*value),
            other => Err(Error::Execution(format!(
                "expected numeric value, got {other:?}"
            ))),
        }
    }

    pub fn compare(&self, other: &Self) -> Result<Ordering> {
        if let (Some(left), Some(right)) = (
            interval_comparison_nanos(self),
            interval_comparison_nanos(other),
        ) {
            return Ok(left.cmp(&right));
        }
        match (self, other) {
            (Self::Boolean(left), Self::Boolean(right)) => Ok(left.cmp(right)),
            (Self::Int64(left), Self::Int64(right)) => Ok(left.cmp(right)),
            (Self::UInt64(left), Self::UInt64(right)) => Ok(left.cmp(right)),
            (Self::Float64(left), Self::Float64(right)) => {
                Ok(f64::from_bits(normalized_float(*left))
                    .total_cmp(&f64::from_bits(normalized_float(*right))))
            }
            (Self::Utf8(left), Self::Utf8(right)) => Ok(left.cmp(right)),
            (Self::Binary(left), Self::Binary(right)) => Ok(left.cmp(right)),
            (Self::Decimal128(left), Self::Decimal128(right)) => Ok(left.cmp(right)),
            _ => Err(Error::Execution(format!(
                "cannot compare {self:?} and {other:?}"
            ))),
        }
    }
}

impl PartialEq for CellValue {
    fn eq(&self, other: &Self) -> bool {
        if let (Some(left), Some(right)) = (
            interval_comparison_nanos(self),
            interval_comparison_nanos(other),
        ) {
            return left == right;
        }
        match (self, other) {
            (Self::Null, Self::Null) => true,
            (Self::Boolean(left), Self::Boolean(right)) => left == right,
            (Self::Int64(left), Self::Int64(right)) => left == right,
            (Self::UInt64(left), Self::UInt64(right)) => left == right,
            (Self::Float64(left), Self::Float64(right)) => {
                normalized_float(*left) == normalized_float(*right)
            }
            (Self::Utf8(left), Self::Utf8(right)) => left == right,
            (Self::Binary(left), Self::Binary(right)) => left == right,
            (Self::Decimal128(left), Self::Decimal128(right)) => left == right,
            _ => false,
        }
    }
}

impl Eq for CellValue {}

impl Hash for CellValue {
    fn hash<H: Hasher>(&self, state: &mut H) {
        if let Some(value) = interval_comparison_nanos(self) {
            // All physical interval families share one SQL equality domain.
            b"rustdb-interval".hash(state);
            value.hash(state);
            return;
        }
        std::mem::discriminant(self).hash(state);
        match self {
            Self::Null => {}
            Self::Boolean(value) => value.hash(state),
            Self::Int64(value) => value.hash(state),
            Self::UInt64(value) => value.hash(state),
            Self::Float64(value) => normalized_float(*value).hash(state),
            Self::Utf8(value) => value.hash(state),
            Self::Binary(value) => value.hash(state),
            Self::Decimal128(value) => value.hash(state),
            Self::IntervalYearMonth(_)
            | Self::IntervalDayTime(_, _)
            | Self::IntervalMonthDayNano(_, _, _) => unreachable!("interval hash returned early"),
        }
    }
}

const NANOS_PER_DAY: i128 = 86_400_000_000_000;
const NANOS_PER_MONTH: i128 = 30 * NANOS_PER_DAY;

pub(crate) fn interval_comparison_nanos(value: &CellValue) -> Option<i128> {
    match value {
        CellValue::IntervalYearMonth(months) => Some(i128::from(*months) * NANOS_PER_MONTH),
        CellValue::IntervalDayTime(days, millis) => {
            Some(i128::from(*days) * NANOS_PER_DAY + i128::from(*millis) * 1_000_000)
        }
        CellValue::IntervalMonthDayNano(months, days, nanos) => Some(
            i128::from(*months) * NANOS_PER_MONTH
                + i128::from(*days) * NANOS_PER_DAY
                + i128::from(*nanos),
        ),
        _ => None,
    }
}

pub(crate) fn cell(array: &ArrayRef, row: usize) -> Result<CellValue> {
    if array.is_null(row) {
        return Ok(CellValue::Null);
    }
    let value = match array.data_type() {
        DataType::Boolean => CellValue::Boolean(downcast::<BooleanArray>(array)?.value(row)),
        DataType::Int8 => CellValue::Int64(i64::from(downcast::<Int8Array>(array)?.value(row))),
        DataType::Int16 => CellValue::Int64(i64::from(downcast::<Int16Array>(array)?.value(row))),
        DataType::Int32 => CellValue::Int64(i64::from(downcast::<Int32Array>(array)?.value(row))),
        DataType::Int64 => CellValue::Int64(downcast::<Int64Array>(array)?.value(row)),
        DataType::Date32
        | DataType::Date64
        | DataType::Time32(_)
        | DataType::Time64(_)
        | DataType::Timestamp(_, _)
        | DataType::Duration(_) => temporal::cell(array, row)?,
        DataType::UInt8 => CellValue::UInt64(u64::from(downcast::<UInt8Array>(array)?.value(row))),
        DataType::UInt16 => {
            CellValue::UInt64(u64::from(downcast::<UInt16Array>(array)?.value(row)))
        }
        DataType::UInt32 => {
            CellValue::UInt64(u64::from(downcast::<UInt32Array>(array)?.value(row)))
        }
        DataType::UInt64 => CellValue::UInt64(downcast::<UInt64Array>(array)?.value(row)),
        DataType::Float16 => {
            CellValue::Float64(f64::from(downcast::<Float16Array>(array)?.value(row)))
        }
        DataType::Float32 => {
            CellValue::Float64(f64::from(downcast::<Float32Array>(array)?.value(row)))
        }
        DataType::Float64 => CellValue::Float64(downcast::<Float64Array>(array)?.value(row)),
        DataType::Utf8 => CellValue::Utf8(downcast::<StringArray>(array)?.value(row).to_owned()),
        DataType::LargeUtf8 => {
            CellValue::Utf8(downcast::<LargeStringArray>(array)?.value(row).to_owned())
        }
        DataType::Binary => CellValue::Binary(downcast::<BinaryArray>(array)?.value(row).to_vec()),
        DataType::LargeBinary => {
            CellValue::Binary(downcast::<LargeBinaryArray>(array)?.value(row).to_vec())
        }
        DataType::FixedSizeBinary(_) => {
            CellValue::Binary(downcast::<FixedSizeBinaryArray>(array)?.value(row).to_vec())
        }
        DataType::Decimal128(_, _) => {
            CellValue::Decimal128(downcast::<Decimal128Array>(array)?.value(row))
        }
        DataType::Interval(IntervalUnit::YearMonth) => {
            CellValue::IntervalYearMonth(downcast::<IntervalYearMonthArray>(array)?.value(row))
        }
        DataType::Interval(IntervalUnit::DayTime) => {
            let (days, millis) =
                IntervalDayTimeType::to_parts(downcast::<IntervalDayTimeArray>(array)?.value(row));
            CellValue::IntervalDayTime(days, millis)
        }
        DataType::Interval(IntervalUnit::MonthDayNano) => {
            let (months, days, nanos) = IntervalMonthDayNanoType::to_parts(
                downcast::<IntervalMonthDayNanoArray>(array)?.value(row),
            );
            CellValue::IntervalMonthDayNano(months, days, nanos)
        }
        DataType::Dictionary(key, _) => match key.as_ref() {
            DataType::Int8 => dictionary_cell::<Int8Type>(array, row)?,
            DataType::Int16 => dictionary_cell::<Int16Type>(array, row)?,
            DataType::Int32 => dictionary_cell::<Int32Type>(array, row)?,
            DataType::Int64 => dictionary_cell::<Int64Type>(array, row)?,
            DataType::UInt8 => dictionary_cell::<UInt8Type>(array, row)?,
            DataType::UInt16 => dictionary_cell::<UInt16Type>(array, row)?,
            DataType::UInt32 => dictionary_cell::<UInt32Type>(array, row)?,
            DataType::UInt64 => dictionary_cell::<UInt64Type>(array, row)?,
            other => {
                return Err(Error::Unsupported(format!(
                    "execution does not support dictionary keys of type {other}"
                )));
            }
        },
        other => {
            return Err(Error::Unsupported(format!(
                "execution does not support values of type {other}"
            )));
        }
    };
    Ok(value)
}

fn dictionary_cell<K: ArrowDictionaryKeyType>(array: &ArrayRef, row: usize) -> Result<CellValue> {
    let dictionary = downcast::<DictionaryArray<K>>(array)?;
    let Some(index) = dictionary.key(row) else {
        return Ok(CellValue::Null);
    };
    cell(dictionary.values(), index)
}

pub(crate) fn canonicalize_sort_key(array: ArrayRef) -> Result<ArrayRef> {
    if matches!(array.data_type(), DataType::Interval(_)) {
        let values = (0..array.len())
            .map(|row| {
                if array.is_null(row) {
                    return Ok(None);
                }
                interval_comparison_nanos(&cell(&array, row)?)
                    .map(Some)
                    .ok_or_else(|| {
                        Error::Internal("interval key did not decode as an interval".into())
                    })
            })
            .collect::<Result<Vec<_>>>()?;
        return Ok(Arc::new(
            Decimal128Array::from(values).with_precision_and_scale(38, 0)?,
        ));
    }
    if !matches!(
        array.data_type(),
        DataType::Float16 | DataType::Float32 | DataType::Float64
    ) {
        return Ok(array);
    }
    let values = (0..array.len())
        .map(|row| match cell(&array, row)? {
            CellValue::Float64(value) => {
                Ok(CellValue::Float64(f64::from_bits(normalized_float(value))))
            }
            value => Ok(value),
        })
        .collect::<Result<Vec<_>>>()?;
    values_to_array(&values, array.data_type())
}

pub(crate) fn canonical_sort_key_type(data_type: &DataType) -> DataType {
    if matches!(data_type, DataType::Interval(_)) {
        DataType::Decimal128(38, 0)
    } else {
        data_type.clone()
    }
}

pub(crate) fn values_to_array(values: &[CellValue], data_type: &DataType) -> Result<ArrayRef> {
    if values.iter().all(CellValue::is_null) {
        return Ok(new_null_array(data_type, values.len()));
    }
    let canonical: ArrayRef = match data_type {
        DataType::Boolean => Arc::new(BooleanArray::from(
            values
                .iter()
                .map(|value| match value {
                    CellValue::Null => Ok(None),
                    CellValue::Boolean(value) => Ok(Some(*value)),
                    other => Err(type_error(data_type, other)),
                })
                .collect::<Result<Vec<_>>>()?,
        )),
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::Date32
        | DataType::Date64
        | DataType::Time32(_)
        | DataType::Time64(_)
        | DataType::Timestamp(_, _)
        | DataType::Duration(_) => Arc::new(Int64Array::from(
            values
                .iter()
                .map(|value| match value {
                    CellValue::Null => Ok(None),
                    CellValue::Int64(value) => Ok(Some(*value)),
                    other => Err(type_error(data_type, other)),
                })
                .collect::<Result<Vec<_>>>()?,
        )),
        DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => {
            Arc::new(UInt64Array::from(
                values
                    .iter()
                    .map(|value| match value {
                        CellValue::Null => Ok(None),
                        CellValue::UInt64(value) => Ok(Some(*value)),
                        other => Err(type_error(data_type, other)),
                    })
                    .collect::<Result<Vec<_>>>()?,
            ))
        }
        DataType::Float16 | DataType::Float32 | DataType::Float64 => Arc::new(Float64Array::from(
            values
                .iter()
                .map(|value| match value {
                    CellValue::Null => Ok(None),
                    CellValue::Float64(value) => Ok(Some(*value)),
                    CellValue::Int64(value) => Ok(Some(*value as f64)),
                    CellValue::UInt64(value) => Ok(Some(*value as f64)),
                    other => Err(type_error(data_type, other)),
                })
                .collect::<Result<Vec<_>>>()?,
        )),
        DataType::Utf8 | DataType::LargeUtf8 => Arc::new(StringArray::from(
            values
                .iter()
                .map(|value| match value {
                    CellValue::Null => Ok(None),
                    CellValue::Utf8(value) => Ok(Some(value.as_str())),
                    other => Err(type_error(data_type, other)),
                })
                .collect::<Result<Vec<_>>>()?,
        )),
        DataType::Binary | DataType::LargeBinary => Arc::new(BinaryArray::from_opt_vec(
            values
                .iter()
                .map(|value| match value {
                    CellValue::Null => Ok(None),
                    CellValue::Binary(value) => Ok(Some(value.as_slice())),
                    other => Err(type_error(data_type, other)),
                })
                .collect::<Result<Vec<_>>>()?,
        )),
        DataType::FixedSizeBinary(width) => {
            let mut builder = FixedSizeBinaryBuilder::with_capacity(values.len(), *width);
            for value in values {
                match value {
                    CellValue::Null => builder.append_null(),
                    CellValue::Binary(value) => builder.append_value(value)?,
                    other => return Err(type_error(data_type, other)),
                }
            }
            Arc::new(builder.finish())
        }
        DataType::Decimal128(precision, scale) => {
            let array = Decimal128Array::from(
                values
                    .iter()
                    .map(|value| match value {
                        CellValue::Null => Ok(None),
                        CellValue::Decimal128(value) => Ok(Some(*value)),
                        other => Err(type_error(data_type, other)),
                    })
                    .collect::<Result<Vec<_>>>()?,
            )
            .with_precision_and_scale(*precision, *scale)?;
            Arc::new(array)
        }
        DataType::Interval(IntervalUnit::YearMonth) => Arc::new(IntervalYearMonthArray::from(
            values
                .iter()
                .map(|value| match value {
                    CellValue::Null => Ok(None),
                    CellValue::IntervalYearMonth(value) => Ok(Some(*value)),
                    other => Err(type_error(data_type, other)),
                })
                .collect::<Result<Vec<_>>>()?,
        )),
        DataType::Interval(IntervalUnit::DayTime) => Arc::new(IntervalDayTimeArray::from(
            values
                .iter()
                .map(|value| match value {
                    CellValue::Null => Ok(None),
                    CellValue::IntervalDayTime(days, millis) => {
                        Ok(Some(IntervalDayTimeType::make_value(*days, *millis)))
                    }
                    other => Err(type_error(data_type, other)),
                })
                .collect::<Result<Vec<_>>>()?,
        )),
        DataType::Interval(IntervalUnit::MonthDayNano) => {
            Arc::new(IntervalMonthDayNanoArray::from(
                values
                    .iter()
                    .map(|value| match value {
                        CellValue::Null => Ok(None),
                        CellValue::IntervalMonthDayNano(months, days, nanos) => Ok(Some(
                            IntervalMonthDayNanoType::make_value(*months, *days, *nanos),
                        )),
                        other => Err(type_error(data_type, other)),
                    })
                    .collect::<Result<Vec<_>>>()?,
            ))
        }
        other => {
            return Err(Error::Unsupported(format!(
                "cannot build output arrays of type {other}"
            )));
        }
    };
    if canonical.data_type() == data_type {
        Ok(canonical)
    } else {
        Ok(cast(canonical.as_ref(), data_type)?)
    }
}

fn downcast<T: Array + 'static>(array: &ArrayRef) -> Result<&T> {
    array.as_any().downcast_ref::<T>().ok_or_else(|| {
        Error::Internal(format!(
            "array reported type {} but had an incompatible implementation",
            array.data_type()
        ))
    })
}

fn type_error(expected: &DataType, value: &CellValue) -> Error {
    Error::Internal(format!(
        "cannot write value {value:?} into output type {expected}"
    ))
}

fn normalized_float(value: f64) -> u64 {
    if value == 0.0 {
        0
    } else if value.is_nan() {
        f64::NAN.to_bits()
    } else {
        value.to_bits()
    }
}

#[cfg(test)]
mod tests;
