use arrow::array::Decimal128Array;

use crate::{Error, Result};

use super::values::{SignedValues, UnsignedValues, decimal_get, decimal_len};

#[derive(Clone, Copy)]
pub(super) enum Side {
    Probe,
    Build,
}

pub(super) fn count(count: &mut i64, rows: usize) -> Result<()> {
    let rows =
        i64::try_from(rows).map_err(|_| Error::Execution("count overflowed INT64".into()))?;
    *count = count
        .checked_add(rows)
        .ok_or_else(|| Error::Execution("count overflowed INT64".into()))?;
    Ok(())
}

pub(super) fn signed(
    side: Side,
    values: &SignedValues<'_>,
    sum: &mut i128,
    seen: &mut bool,
    probe_row: usize,
    matches: &[u32],
) -> Result<()> {
    match side {
        Side::Probe => {
            if let Some(value) = values.get(probe_row) {
                let rows =
                    i128::try_from(matches.len()).map_err(|_| overflow("signed sum overflow"))?;
                add_i128(sum, seen, value.checked_mul(rows), "signed sum overflow")?;
            }
        }
        Side::Build => {
            for &row in matches {
                if let Some(value) = values.get(build_row(row, values.len())?) {
                    add_i128(sum, seen, Some(value), "signed sum overflow")?;
                }
            }
        }
    }
    Ok(())
}

pub(super) fn unsigned(
    side: Side,
    values: &UnsignedValues<'_>,
    sum: &mut u128,
    seen: &mut bool,
    probe_row: usize,
    matches: &[u32],
) -> Result<()> {
    match side {
        Side::Probe => {
            if let Some(value) = values.get(probe_row) {
                let rows =
                    u128::try_from(matches.len()).map_err(|_| overflow("unsigned sum overflow"))?;
                let value = value
                    .checked_mul(rows)
                    .ok_or_else(|| overflow("unsigned sum overflow"))?;
                *sum = sum
                    .checked_add(value)
                    .ok_or_else(|| overflow("unsigned sum overflow"))?;
                *seen = true;
            }
        }
        Side::Build => {
            for &row in matches {
                if let Some(value) = values.get(build_row(row, values.len())?) {
                    *sum = sum
                        .checked_add(value)
                        .ok_or_else(|| overflow("unsigned sum overflow"))?;
                    *seen = true;
                }
            }
        }
    }
    Ok(())
}

pub(super) fn decimal(
    side: Side,
    values: &Decimal128Array,
    sum: &mut i128,
    seen: &mut bool,
    probe_row: usize,
    matches: &[u32],
) -> Result<()> {
    const MESSAGE: &str = "decimal sum overflowed i128";
    match side {
        Side::Probe => {
            if let Some(value) = decimal_get(values, probe_row) {
                let rows = i128::try_from(matches.len()).map_err(|_| overflow(MESSAGE))?;
                add_i128(sum, seen, value.checked_mul(rows), MESSAGE)?;
            }
        }
        Side::Build => {
            for &row in matches {
                if let Some(value) = decimal_get(values, build_row(row, decimal_len(values))?) {
                    add_i128(sum, seen, Some(value), MESSAGE)?;
                }
            }
        }
    }
    Ok(())
}

fn add_i128(
    sum: &mut i128,
    seen: &mut bool,
    value: Option<i128>,
    message: &'static str,
) -> Result<()> {
    let value = value.ok_or_else(|| overflow(message))?;
    *sum = sum.checked_add(value).ok_or_else(|| overflow(message))?;
    *seen = true;
    Ok(())
}

fn build_row(row: u32, len: usize) -> Result<usize> {
    let row = row as usize;
    (row < len)
        .then_some(row)
        .ok_or_else(|| Error::Internal("fixed join build row is out of bounds".into()))
}

fn overflow(message: &'static str) -> Error {
    Error::Execution(message.into())
}
