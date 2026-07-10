use std::{
    cmp::Ordering,
    hash::{Hash, Hasher},
    sync::Arc,
};

use arrow::{
    array::{
        Array, ArrayRef, BinaryArray, BooleanArray, Decimal128Array, Float16Array, Float32Array,
        Float64Array, Int8Array, Int16Array, Int32Array, Int64Array, LargeBinaryArray,
        LargeStringArray, StringArray, UInt8Array, UInt16Array, UInt32Array, UInt64Array,
        new_null_array,
    },
    compute::cast,
    datatypes::DataType,
};

use crate::{Error, Result};

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
        match (self, other) {
            (Self::Boolean(left), Self::Boolean(right)) => Ok(left.cmp(right)),
            (Self::Int64(left), Self::Int64(right)) => Ok(left.cmp(right)),
            (Self::UInt64(left), Self::UInt64(right)) => Ok(left.cmp(right)),
            (Self::Float64(left), Self::Float64(right)) => Ok(left.total_cmp(right)),
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
        }
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
        | DataType::Duration(_) => {
            let numeric = cast(array.as_ref(), &DataType::Int64)?;
            CellValue::Int64(downcast::<Int64Array>(&numeric)?.value(row))
        }
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
        DataType::Decimal128(_, _) => {
            CellValue::Decimal128(downcast::<Decimal128Array>(array)?.value(row))
        }
        other => {
            return Err(Error::Unsupported(format!(
                "execution does not support values of type {other}"
            )));
        }
    };
    Ok(value)
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
