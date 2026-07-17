use arrow::{
    array::{
        Array, ArrayRef, Decimal128Array, Int8Array, Int16Array, Int32Array, Int64Array,
        UInt8Array, UInt16Array, UInt32Array, UInt64Array,
    },
    datatypes::DataType,
};

use crate::{Error, Result};

pub(in crate::execution::aggregate::join_sink) enum NumericValues<'a> {
    Signed(SignedValues<'a>),
    Unsigned(UnsignedValues<'a>),
    Decimal(&'a Decimal128Array),
}

impl<'a> NumericValues<'a> {
    pub(in crate::execution::aggregate::join_sink) fn bind(array: &'a ArrayRef) -> Result<Self> {
        Ok(match array.data_type() {
            DataType::Int8 => Self::Signed(SignedValues::I8(downcast(array)?)),
            DataType::Int16 => Self::Signed(SignedValues::I16(downcast(array)?)),
            DataType::Int32 => Self::Signed(SignedValues::I32(downcast(array)?)),
            DataType::Int64 => Self::Signed(SignedValues::I64(downcast(array)?)),
            DataType::UInt8 => Self::Unsigned(UnsignedValues::U8(downcast(array)?)),
            DataType::UInt16 => Self::Unsigned(UnsignedValues::U16(downcast(array)?)),
            DataType::UInt32 => Self::Unsigned(UnsignedValues::U32(downcast(array)?)),
            DataType::UInt64 => Self::Unsigned(UnsignedValues::U64(downcast(array)?)),
            DataType::Decimal128(_, _) => Self::Decimal(downcast(array)?),
            data_type => {
                return Err(Error::Internal(format!(
                    "fixed join aggregate does not support {data_type}"
                )));
            }
        })
    }
}

pub(in crate::execution::aggregate::join_sink) enum SignedValues<'a> {
    I8(&'a Int8Array),
    I16(&'a Int16Array),
    I32(&'a Int32Array),
    I64(&'a Int64Array),
}

impl SignedValues<'_> {
    pub(in crate::execution::aggregate::join_sink) fn len(&self) -> usize {
        match self {
            Self::I8(values) => values.len(),
            Self::I16(values) => values.len(),
            Self::I32(values) => values.len(),
            Self::I64(values) => values.len(),
        }
    }

    pub(in crate::execution::aggregate::join_sink) fn get(&self, row: usize) -> Option<i128> {
        match self {
            Self::I8(values) => (!values.is_null(row)).then(|| i128::from(values.value(row))),
            Self::I16(values) => (!values.is_null(row)).then(|| i128::from(values.value(row))),
            Self::I32(values) => (!values.is_null(row)).then(|| i128::from(values.value(row))),
            Self::I64(values) => (!values.is_null(row)).then(|| i128::from(values.value(row))),
        }
    }
}

pub(in crate::execution::aggregate::join_sink) enum UnsignedValues<'a> {
    U8(&'a UInt8Array),
    U16(&'a UInt16Array),
    U32(&'a UInt32Array),
    U64(&'a UInt64Array),
}

impl UnsignedValues<'_> {
    pub(in crate::execution::aggregate::join_sink) fn len(&self) -> usize {
        match self {
            Self::U8(values) => values.len(),
            Self::U16(values) => values.len(),
            Self::U32(values) => values.len(),
            Self::U64(values) => values.len(),
        }
    }

    pub(in crate::execution::aggregate::join_sink) fn get(&self, row: usize) -> Option<u128> {
        match self {
            Self::U8(values) => (!values.is_null(row)).then(|| u128::from(values.value(row))),
            Self::U16(values) => (!values.is_null(row)).then(|| u128::from(values.value(row))),
            Self::U32(values) => (!values.is_null(row)).then(|| u128::from(values.value(row))),
            Self::U64(values) => (!values.is_null(row)).then(|| u128::from(values.value(row))),
        }
    }
}

pub(in crate::execution::aggregate::join_sink) fn decimal_len(values: &Decimal128Array) -> usize {
    values.len()
}

pub(in crate::execution::aggregate::join_sink) fn decimal_get(
    values: &Decimal128Array,
    row: usize,
) -> Option<i128> {
    (!values.is_null(row)).then(|| values.value(row))
}

fn downcast<A: 'static>(array: &ArrayRef) -> Result<&A> {
    array.as_any().downcast_ref::<A>().ok_or_else(|| {
        Error::Internal(format!(
            "fixed join aggregate expected {}, but Arrow downcast failed",
            array.data_type()
        ))
    })
}
