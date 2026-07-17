use arrow::{
    array::{
        Array, ArrayRef, Decimal128Array, Float32Array, Float64Array, Int8Array, Int16Array,
        Int32Array, Int64Array, UInt8Array, UInt16Array, UInt32Array, UInt64Array,
    },
    datatypes::DataType,
};

use crate::{
    Error, Result,
    sql::{AggregateExpr, AggregateFunction},
};

use super::state::AggregateState;

pub(super) fn supports(aggregates: &[AggregateExpr]) -> bool {
    !aggregates.is_empty()
        && aggregates.iter().all(|aggregate| match aggregate.function {
            AggregateFunction::Count => true,
            AggregateFunction::Sum | AggregateFunction::Avg => aggregate
                .expr
                .as_ref()
                .is_some_and(|expr| supported_numeric(&expr.data_type)),
            AggregateFunction::Min | AggregateFunction::Max => false,
        })
}

pub(super) fn update(
    states: &mut [AggregateState],
    aggregates: &[AggregateExpr],
    arrays: &[Option<ArrayRef>],
    rows: usize,
) -> Result<()> {
    if states.len() != aggregates.len() || states.len() != arrays.len() {
        return Err(Error::Internal(
            "global aggregate batch state width does not match expressions".into(),
        ));
    }
    for ((state, aggregate), array) in states.iter_mut().zip(aggregates).zip(arrays) {
        update_one(state, aggregate, array.as_ref(), rows)?;
    }
    Ok(())
}

fn update_one(
    state: &mut AggregateState,
    aggregate: &AggregateExpr,
    array: Option<&ArrayRef>,
    rows: usize,
) -> Result<()> {
    match state {
        AggregateState::Count(count) => {
            let values = rows.saturating_sub(array.map_or(0, |array| array.null_count()));
            add_i64(count, values, "count overflowed INT64")
        }
        AggregateState::SumSigned { value, seen, .. } => {
            update_signed(value, seen, required(array, aggregate)?)
        }
        AggregateState::SumUnsigned { value, seen, .. } => {
            update_unsigned(value, seen, required(array, aggregate)?)
        }
        AggregateState::SumFloat { value, seen } => {
            update_float(value, seen, required(array, aggregate)?)
        }
        AggregateState::SumDecimal { value, seen, .. } => update_decimal(
            value,
            seen,
            required(array, aggregate)?,
            "decimal sum overflowed i128",
        ),
        AggregateState::Avg { sum, count } => {
            update_average(sum, count, required(array, aggregate)?)
        }
        AggregateState::AvgDecimal { sum, count, .. } => {
            update_decimal_average(sum, count, required(array, aggregate)?)
        }
        AggregateState::Min(_) | AggregateState::Max(_) => Err(Error::Internal(
            "MIN/MAX reached the numeric global aggregate batch path".into(),
        )),
    }
}

fn update_signed(sum: &mut i128, seen: &mut bool, array: &ArrayRef) -> Result<()> {
    macro_rules! add {
        ($ty:ty) => {{
            let values = downcast::<$ty>(array)?;
            for value in values.iter().flatten() {
                *sum = sum
                    .checked_add(i128::from(value))
                    .ok_or_else(|| Error::Execution("signed sum overflow".into()))?;
                *seen = true;
            }
            Ok(())
        }};
    }
    match array.data_type() {
        DataType::Int8 => add!(Int8Array),
        DataType::Int16 => add!(Int16Array),
        DataType::Int32 => add!(Int32Array),
        DataType::Int64 => add!(Int64Array),
        other => type_error("signed SUM", other),
    }
}

fn update_unsigned(sum: &mut u128, seen: &mut bool, array: &ArrayRef) -> Result<()> {
    macro_rules! add {
        ($ty:ty) => {{
            let values = downcast::<$ty>(array)?;
            for value in values.iter().flatten() {
                *sum = sum
                    .checked_add(u128::from(value))
                    .ok_or_else(|| Error::Execution("unsigned sum overflow".into()))?;
                *seen = true;
            }
            Ok(())
        }};
    }
    match array.data_type() {
        DataType::UInt8 => add!(UInt8Array),
        DataType::UInt16 => add!(UInt16Array),
        DataType::UInt32 => add!(UInt32Array),
        DataType::UInt64 => add!(UInt64Array),
        other => type_error("unsigned SUM", other),
    }
}

fn update_float(sum: &mut f64, seen: &mut bool, array: &ArrayRef) -> Result<()> {
    match array.data_type() {
        DataType::Float32 => {
            for value in downcast::<Float32Array>(array)?.iter().flatten() {
                *sum += f64::from(value);
                *seen = true;
            }
            Ok(())
        }
        DataType::Float64 => {
            for value in downcast::<Float64Array>(array)?.iter().flatten() {
                *sum += value;
                *seen = true;
            }
            Ok(())
        }
        other => type_error("floating SUM", other),
    }
}

fn update_decimal(
    sum: &mut i128,
    seen: &mut bool,
    array: &ArrayRef,
    overflow: &'static str,
) -> Result<()> {
    let values = downcast::<Decimal128Array>(array)?;
    for value in values.iter().flatten() {
        *sum = sum
            .checked_add(value)
            .ok_or_else(|| Error::Execution(overflow.into()))?;
        *seen = true;
    }
    Ok(())
}

fn update_average(sum: &mut f64, count: &mut u64, array: &ArrayRef) -> Result<()> {
    macro_rules! add {
        ($ty:ty) => {{
            for value in downcast::<$ty>(array)?.iter().flatten() {
                *sum += value as f64;
                *count = count
                    .checked_add(1)
                    .ok_or_else(|| Error::Execution("average count overflow".into()))?;
            }
            Ok(())
        }};
    }
    match array.data_type() {
        DataType::Int8 => add!(Int8Array),
        DataType::Int16 => add!(Int16Array),
        DataType::Int32 => add!(Int32Array),
        DataType::Int64 => add!(Int64Array),
        DataType::UInt8 => add!(UInt8Array),
        DataType::UInt16 => add!(UInt16Array),
        DataType::UInt32 => add!(UInt32Array),
        DataType::UInt64 => add!(UInt64Array),
        DataType::Float32 => add!(Float32Array),
        DataType::Float64 => add!(Float64Array),
        other => type_error("AVG", other),
    }
}

fn update_decimal_average(sum: &mut i128, count: &mut u64, array: &ArrayRef) -> Result<()> {
    for value in downcast::<Decimal128Array>(array)?.iter().flatten() {
        *sum = sum
            .checked_add(value)
            .ok_or_else(|| Error::Execution("decimal average sum overflowed i128".into()))?;
        *count = count
            .checked_add(1)
            .ok_or_else(|| Error::Execution("decimal average count overflow".into()))?;
    }
    Ok(())
}

fn add_i64(value: &mut i64, additional: usize, overflow: &'static str) -> Result<()> {
    let additional = i64::try_from(additional).map_err(|_| Error::Execution(overflow.into()))?;
    *value = value
        .checked_add(additional)
        .ok_or_else(|| Error::Execution(overflow.into()))?;
    Ok(())
}

fn required<'a>(array: Option<&'a ArrayRef>, aggregate: &AggregateExpr) -> Result<&'a ArrayRef> {
    array.ok_or_else(|| {
        Error::Internal(format!(
            "aggregate {} is missing its batch input",
            aggregate.display_name
        ))
    })
}

fn supported_numeric(data_type: &DataType) -> bool {
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
            | DataType::Float32
            | DataType::Float64
            | DataType::Decimal128(_, _)
    )
}

fn downcast<A: 'static>(array: &ArrayRef) -> Result<&A> {
    array.as_any().downcast_ref::<A>().ok_or_else(|| {
        Error::Internal(format!(
            "global aggregate batch expected {}, found another Arrow array",
            array.data_type()
        ))
    })
}

fn type_error<T>(operation: &str, data_type: &DataType) -> Result<T> {
    Err(Error::Internal(format!(
        "global aggregate batch {operation} does not support {data_type}"
    )))
}

#[cfg(test)]
#[path = "batch/tests.rs"]
mod tests;
