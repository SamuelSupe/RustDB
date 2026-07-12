use std::mem::size_of;

use arrow::array::{Array, ArrayRef, BinaryArray, LargeBinaryArray, LargeStringArray, StringArray};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;

use crate::runtime::estimate_array_bytes;
use crate::sql::WindowExpr;
use crate::{Error, Result};

use super::super::value::CellValue;

pub(super) fn cell_payload_bytes(value: &CellValue) -> usize {
    match value {
        CellValue::Utf8(value) => value.len(),
        CellValue::Binary(value) => value.len(),
        CellValue::Decimal128(_) => 16,
        CellValue::Null => 0,
        _ => 8,
    }
}

pub(super) fn array_value_payload_bytes(array: &ArrayRef, row: usize) -> Result<usize> {
    if array.is_null(row) {
        return Ok(0);
    }
    Ok(match array.data_type() {
        DataType::Utf8 => downcast::<StringArray>(array)?.value(row).len(),
        DataType::LargeUtf8 => downcast::<LargeStringArray>(array)?.value(row).len(),
        DataType::Binary => downcast::<BinaryArray>(array)?.value(row).len(),
        DataType::LargeBinary => downcast::<LargeBinaryArray>(array)?.value(row).len(),
        DataType::Decimal128(_, _) => 16,
        DataType::Boolean => 1,
        _ => 8,
    })
}

pub(super) fn row_payload_bytes(arrays: &[ArrayRef], row: usize) -> Result<usize> {
    arrays.iter().try_fold(0usize, |bytes, array| {
        Ok(bytes.saturating_add(array_value_payload_bytes(array, row)?))
    })
}

pub(super) fn variable_output_bound(
    expression: &WindowExpr,
    whole_value: Option<&CellValue>,
    peer_value: Option<&CellValue>,
    input: Option<&ArrayRef>,
    retained_payload: usize,
    offset: usize,
    rows: usize,
) -> Result<usize> {
    let owned_values = rows
        .saturating_mul(size_of::<CellValue>())
        // values_to_array builds a temporary typed Option vector.
        .saturating_add(rows.saturating_mul(size_of::<CellValue>()))
        .saturating_add(512);
    if !is_variable(&expression.data_type) {
        return Ok(estimate_array_bytes(&expression.data_type, rows).saturating_add(owned_values));
    }
    let payload_per_row = if let Some(value) = whole_value {
        cell_payload_bytes(value)
    } else if let Some(value) = peer_value {
        cell_payload_bytes(value)
    } else if let Some(input) = input {
        let mut maximum = retained_payload;
        for row in offset..offset + rows {
            maximum = maximum.max(array_value_payload_bytes(input, row)?);
        }
        maximum
    } else {
        retained_payload
    };
    let offsets = match expression.data_type {
        DataType::LargeUtf8 | DataType::LargeBinary => 8,
        _ => 4,
    };
    let arrow = rows
        .saturating_mul(payload_per_row.saturating_add(offsets))
        .saturating_add(rows.div_ceil(8))
        .saturating_add(512);
    // CellValue owns one payload copy until values_to_array has finished.
    // LargeUtf8/LargeBinary are first materialized canonically and then cast,
    // so one additional payload buffer can coexist transiently.
    let transient_copies = if matches!(
        expression.data_type,
        DataType::LargeUtf8 | DataType::LargeBinary
    ) {
        2
    } else {
        1
    };
    Ok(arrow
        .saturating_add(
            rows.saturating_mul(payload_per_row)
                .saturating_mul(transient_copies),
        )
        .saturating_add(owned_values))
}

pub(super) fn original_slice_bytes(batch: &RecordBatch, offset: usize, rows: usize) -> usize {
    batch.slice(offset, rows).get_array_memory_size()
}

pub(super) fn is_variable(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Binary | DataType::LargeBinary
    )
}

fn downcast<T: Array + 'static>(array: &ArrayRef) -> Result<&T> {
    array.as_any().downcast_ref::<T>().ok_or_else(|| {
        Error::Internal(format!(
            "window memory estimator expected {}, got {}",
            std::any::type_name::<T>(),
            array.data_type()
        ))
    })
}
