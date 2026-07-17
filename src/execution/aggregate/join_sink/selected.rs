use arrow::{
    array::{
        Array, ArrayRef, Decimal128Array, Float32Array, Float64Array, Int8Array, Int16Array,
        Int32Array, Int64Array, UInt8Array, UInt16Array, UInt32Array, UInt64Array,
    },
    datatypes::DataType,
    record_batch::RecordBatch,
};

use crate::{
    Error, Result,
    sql::{AggregateExpr, AggregateFunction, ExprKind},
};

use super::AggregateState;

pub(super) fn update(
    states: &mut [AggregateState],
    aggregates: &[AggregateExpr],
    left: &RecordBatch,
    right: &RecordBatch,
    left_indices: &[u32],
    right_indices: &[Option<u32>],
) -> Result<()> {
    if left_indices.len() != right_indices.len() || states.len() != aggregates.len() {
        return Err(Error::Internal(
            "join selection aggregate width does not match its inputs".into(),
        ));
    }
    for (state, aggregate) in states.iter_mut().zip(aggregates) {
        match aggregate.function {
            AggregateFunction::Count => add_count(state, left_indices.len())?,
            AggregateFunction::Sum => {
                let expression = aggregate.expr.as_ref().ok_or_else(|| {
                    Error::Internal("join selection SUM is missing its argument".into())
                })?;
                let ExprKind::Column(column) = &expression.kind else {
                    return Err(Error::Internal(
                        "join selection SUM argument is not a column".into(),
                    ));
                };
                if *column < left.num_columns() {
                    add_values(
                        state,
                        left.column(*column),
                        left_indices.iter().map(|row| Some(*row)),
                    )?;
                } else {
                    let column = column - left.num_columns();
                    let array = right.columns().get(column).ok_or_else(|| {
                        Error::Internal("join selection SUM column is out of bounds".into())
                    })?;
                    add_values(state, array, right_indices.iter().copied())?;
                }
            }
            AggregateFunction::Min | AggregateFunction::Max | AggregateFunction::Avg => {
                return Err(Error::Internal(
                    "unsupported function reached join selection aggregate".into(),
                ));
            }
        }
    }
    Ok(())
}

fn add_count(state: &mut AggregateState, rows: usize) -> Result<()> {
    let AggregateState::Count(count) = state else {
        return Err(Error::Internal(
            "COUNT reached an incompatible join aggregate state".into(),
        ));
    };
    let rows =
        i64::try_from(rows).map_err(|_| Error::Execution("count overflowed INT64".into()))?;
    *count = count
        .checked_add(rows)
        .ok_or_else(|| Error::Execution("count overflowed INT64".into()))?;
    Ok(())
}

fn add_values<I>(state: &mut AggregateState, array: &ArrayRef, rows: I) -> Result<()>
where
    I: IntoIterator<Item = Option<u32>>,
{
    match state {
        AggregateState::SumSigned { value, seen, .. } => match array.data_type() {
            DataType::Int8 => add_signed::<Int8Array, _>(value, seen, array, rows),
            DataType::Int16 => add_signed::<Int16Array, _>(value, seen, array, rows),
            DataType::Int32 => add_signed::<Int32Array, _>(value, seen, array, rows),
            DataType::Int64 => add_signed::<Int64Array, _>(value, seen, array, rows),
            other => type_error("signed SUM", other),
        },
        AggregateState::SumUnsigned { value, seen, .. } => match array.data_type() {
            DataType::UInt8 => add_unsigned::<UInt8Array, _>(value, seen, array, rows),
            DataType::UInt16 => add_unsigned::<UInt16Array, _>(value, seen, array, rows),
            DataType::UInt32 => add_unsigned::<UInt32Array, _>(value, seen, array, rows),
            DataType::UInt64 => add_unsigned::<UInt64Array, _>(value, seen, array, rows),
            other => type_error("unsigned SUM", other),
        },
        AggregateState::SumFloat { value, seen } => match array.data_type() {
            DataType::Float32 => add_float32(value, seen, array, rows),
            DataType::Float64 => add_float64(value, seen, array, rows),
            other => type_error("floating SUM", other),
        },
        AggregateState::SumDecimal { value, seen, .. } => add_decimal(value, seen, array, rows),
        _ => Err(Error::Internal(
            "SUM reached an incompatible join aggregate state".into(),
        )),
    }
}

trait SignedArray: Array {
    fn signed(&self, row: usize) -> i128;
}

macro_rules! signed_array {
    ($array:ty) => {
        impl SignedArray for $array {
            fn signed(&self, row: usize) -> i128 {
                i128::from(self.value(row))
            }
        }
    };
}
signed_array!(Int8Array);
signed_array!(Int16Array);
signed_array!(Int32Array);
signed_array!(Int64Array);

trait UnsignedArray: Array {
    fn unsigned(&self, row: usize) -> u128;
}

macro_rules! unsigned_array {
    ($array:ty) => {
        impl UnsignedArray for $array {
            fn unsigned(&self, row: usize) -> u128 {
                u128::from(self.value(row))
            }
        }
    };
}
unsigned_array!(UInt8Array);
unsigned_array!(UInt16Array);
unsigned_array!(UInt32Array);
unsigned_array!(UInt64Array);

fn add_signed<A, I>(sum: &mut i128, seen: &mut bool, array: &ArrayRef, rows: I) -> Result<()>
where
    A: SignedArray + 'static,
    I: IntoIterator<Item = Option<u32>>,
{
    let values = downcast::<A>(array)?;
    for row in rows {
        let row = selected_row(row, values.len())?;
        if !values.is_null(row) {
            *sum = sum
                .checked_add(values.signed(row))
                .ok_or_else(|| Error::Execution("signed sum overflow".into()))?;
            *seen = true;
        }
    }
    Ok(())
}

fn add_unsigned<A, I>(sum: &mut u128, seen: &mut bool, array: &ArrayRef, rows: I) -> Result<()>
where
    A: UnsignedArray + 'static,
    I: IntoIterator<Item = Option<u32>>,
{
    let values = downcast::<A>(array)?;
    for row in rows {
        let row = selected_row(row, values.len())?;
        if !values.is_null(row) {
            *sum = sum
                .checked_add(values.unsigned(row))
                .ok_or_else(|| Error::Execution("unsigned sum overflow".into()))?;
            *seen = true;
        }
    }
    Ok(())
}

fn add_float32<I>(sum: &mut f64, seen: &mut bool, array: &ArrayRef, rows: I) -> Result<()>
where
    I: IntoIterator<Item = Option<u32>>,
{
    let values = downcast::<Float32Array>(array)?;
    for row in rows {
        let row = selected_row(row, values.len())?;
        if !values.is_null(row) {
            *sum += f64::from(values.value(row));
            *seen = true;
        }
    }
    Ok(())
}

fn add_float64<I>(sum: &mut f64, seen: &mut bool, array: &ArrayRef, rows: I) -> Result<()>
where
    I: IntoIterator<Item = Option<u32>>,
{
    let values = downcast::<Float64Array>(array)?;
    for row in rows {
        let row = selected_row(row, values.len())?;
        if !values.is_null(row) {
            *sum += values.value(row);
            *seen = true;
        }
    }
    Ok(())
}

fn add_decimal<I>(sum: &mut i128, seen: &mut bool, array: &ArrayRef, rows: I) -> Result<()>
where
    I: IntoIterator<Item = Option<u32>>,
{
    let values = downcast::<Decimal128Array>(array)?;
    for row in rows {
        let row = selected_row(row, values.len())?;
        if !values.is_null(row) {
            *sum = sum
                .checked_add(values.value(row))
                .ok_or_else(|| Error::Execution("decimal sum overflowed i128".into()))?;
            *seen = true;
        }
    }
    Ok(())
}

fn selected_row(row: Option<u32>, len: usize) -> Result<usize> {
    let row = row.ok_or_else(|| {
        Error::Internal("inner join selection contains an unmatched build row".into())
    })? as usize;
    if row >= len {
        return Err(Error::Internal(
            "join selection row is out of bounds".into(),
        ));
    }
    Ok(row)
}

fn downcast<A: 'static>(array: &ArrayRef) -> Result<&A> {
    array.as_any().downcast_ref::<A>().ok_or_else(|| {
        Error::Internal(format!(
            "join selection aggregate expected {}, found another Arrow array",
            array.data_type()
        ))
    })
}

fn type_error<T>(operation: &str, data_type: &DataType) -> Result<T> {
    Err(Error::Internal(format!(
        "join selection {operation} does not support {data_type}"
    )))
}
