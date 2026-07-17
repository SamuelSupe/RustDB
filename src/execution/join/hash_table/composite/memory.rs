use std::mem::size_of;

use arrow::{
    array::{ArrayRef, BinaryArray, LargeBinaryArray, LargeStringArray, StringArray},
    datatypes::DataType,
    row::Rows,
};

use crate::{Error, Result};

const BASE_SCRATCH_BYTES: usize = 1_024;
const PER_COLUMN_SCRATCH_BYTES: usize = 512;
const VARIABLE_ROW_OVERHEAD: usize = 37;

pub(super) fn converter_upper_bound(types: &[DataType]) -> usize {
    types
        .iter()
        .map(DataType::size)
        .fold(BASE_SCRATCH_BYTES, usize::saturating_add)
        .saturating_mul(2)
        .saturating_add(
            types
                .len()
                .saturating_mul(size_of::<DataType>().saturating_add(PER_COLUMN_SCRATCH_BYTES)),
        )
        .max(1)
}

/// Upper bound for Arrow row conversion of the supported flat Join types.
///
/// Variable-width row encoding pads values in small blocks. `payload +
/// payload / 32 + 37 * rows` bounds that padding. The remaining terms cover
/// the retained offsets, the transient LengthTracker and encoder/slice
/// metadata that coexist while `convert_columns` runs.
pub(super) fn encode_peak_bytes(arrays: &[ArrayRef]) -> Result<usize> {
    let rows = arrays.first().map_or(0, |array| array.len());
    if arrays.iter().any(|array| array.len() != rows) {
        return Err(Error::Internal(
            "composite Join key arrays have different row counts".into(),
        ));
    }

    let mut variable = false;
    let encoded = arrays.iter().try_fold(0usize, |bytes, array| {
        let column = match array.data_type() {
            DataType::Boolean | DataType::Int8 | DataType::UInt8 => rows.saturating_mul(2),
            DataType::Int16 | DataType::UInt16 => rows.saturating_mul(3),
            DataType::Int32 | DataType::UInt32 | DataType::Date32 | DataType::Time32(_) => {
                rows.saturating_mul(5)
            }
            DataType::Int64
            | DataType::UInt64
            | DataType::Date64
            | DataType::Time64(_)
            | DataType::Timestamp(_, _)
            | DataType::Duration(_) => rows.saturating_mul(9),
            DataType::Decimal128(_, _) => rows.saturating_mul(17),
            DataType::Utf8 => {
                variable = true;
                variable_bytes(string_payload::<StringArray>(array)?, rows)
            }
            DataType::LargeUtf8 => {
                variable = true;
                variable_bytes(string_payload::<LargeStringArray>(array)?, rows)
            }
            DataType::Binary => {
                variable = true;
                variable_bytes(binary_payload::<BinaryArray>(array)?, rows)
            }
            DataType::LargeBinary => {
                variable = true;
                variable_bytes(binary_payload::<LargeBinaryArray>(array)?, rows)
            }
            other => {
                return Err(Error::Internal(format!(
                    "unsupported composite Join row-encoding type {other}"
                )));
            }
        };
        Ok::<_, Error>(bytes.saturating_add(column))
    })?;

    let offsets = rows.saturating_add(1).saturating_mul(size_of::<usize>());
    let length_tracker = if variable {
        rows.saturating_mul(size_of::<usize>())
    } else {
        0
    };
    Ok(encoded
        .saturating_add(offsets)
        .saturating_add(length_tracker)
        .saturating_add(size_of::<Rows>())
        .saturating_add(arrays.len().saturating_mul(PER_COLUMN_SCRATCH_BYTES))
        .saturating_add(BASE_SCRATCH_BYTES)
        .max(1))
}

fn variable_bytes(payload: usize, rows: usize) -> usize {
    payload
        .saturating_add(payload.div_ceil(32))
        .saturating_add(rows.saturating_mul(VARIABLE_ROW_OVERHEAD))
}

trait StringPayload {
    fn selected_payload_bytes(&self) -> Result<usize>;
}

impl StringPayload for StringArray {
    fn selected_payload_bytes(&self) -> Result<usize> {
        offset_span(self.value_offsets())
    }
}

impl StringPayload for LargeStringArray {
    fn selected_payload_bytes(&self) -> Result<usize> {
        offset_span(self.value_offsets())
    }
}

trait BinaryPayload {
    fn selected_payload_bytes(&self) -> Result<usize>;
}

impl BinaryPayload for BinaryArray {
    fn selected_payload_bytes(&self) -> Result<usize> {
        offset_span(self.value_offsets())
    }
}

impl BinaryPayload for LargeBinaryArray {
    fn selected_payload_bytes(&self) -> Result<usize> {
        offset_span(self.value_offsets())
    }
}

fn string_payload<A: StringPayload + 'static>(array: &ArrayRef) -> Result<usize> {
    array
        .as_any()
        .downcast_ref::<A>()
        .ok_or_else(|| Error::Internal("composite Join string array type mismatch".into()))?
        .selected_payload_bytes()
}

fn binary_payload<A: BinaryPayload + 'static>(array: &ArrayRef) -> Result<usize> {
    array
        .as_any()
        .downcast_ref::<A>()
        .ok_or_else(|| Error::Internal("composite Join binary array type mismatch".into()))?
        .selected_payload_bytes()
}

fn offset_span<O>(offsets: &[O]) -> Result<usize>
where
    O: Copy + TryInto<i128>,
{
    let Some((first, rest)) = offsets.split_first() else {
        return Ok(0);
    };
    let last = rest.last().unwrap_or(first);
    let first = (*first)
        .try_into()
        .map_err(|_| Error::Internal("composite Join offset conversion failed".into()))?;
    let last = (*last)
        .try_into()
        .map_err(|_| Error::Internal("composite Join offset conversion failed".into()))?;
    usize::try_from(last.saturating_sub(first)).map_err(|_| {
        Error::ResourceExhausted("composite Join variable-width payload exceeds usize".into())
    })
}
