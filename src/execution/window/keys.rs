use arrow::{array::ArrayRef, record_batch::RecordBatch};

use crate::{Result, sql::BoundExpr};

use super::super::{
    expr::evaluate,
    value::{CellValue, cell},
};

pub(super) fn evaluate_keys(
    expressions: &[BoundExpr],
    batch: &RecordBatch,
) -> Result<Vec<ArrayRef>> {
    expressions
        .iter()
        .map(|expression| evaluate(expression, batch))
        .collect()
}

pub(super) fn row_key(arrays: &[ArrayRef], row: usize) -> Result<Vec<CellValue>> {
    arrays.iter().map(|array| cell(array, row)).collect()
}

pub(super) fn payload_bytes(values: &[CellValue]) -> usize {
    values.iter().fold(0usize, |bytes, value| {
        bytes.saturating_add(match value {
            CellValue::Utf8(value) => value.len(),
            CellValue::Binary(value) => value.len(),
            CellValue::Decimal128(_) => 16,
            CellValue::Null => 1,
            _ => 8,
        })
    })
}
