use arrow::record_batch::RecordBatch;
use std::ops::Range;

use crate::{
    Error, Result,
    runtime::QueryContext,
    sql::{AggregateExpr, AggregateFunction, ExprKind},
};

use super::{
    AggregateState,
    fixed::values::{NumericValues, SignedValues, UnsignedValues, decimal_get},
};

enum Updater<'a> {
    Count(usize),
    Signed(usize, SignedValues<'a>),
    Unsigned(usize, UnsignedValues<'a>),
    Decimal(usize, &'a arrow::array::Decimal128Array),
}

pub(super) fn update_range<F>(
    states: &mut [AggregateState],
    aggregates: &[AggregateExpr],
    probe: &RecordBatch,
    rows: Range<usize>,
    mut lookup: F,
    context: &QueryContext,
) -> Result<u64>
where
    F: FnMut(usize) -> Result<u64>,
{
    if states.len() != aggregates.len() {
        return Err(Error::Internal(
            "multiplicity join aggregate width does not match its state".into(),
        ));
    }
    let updaters = aggregates
        .iter()
        .enumerate()
        .map(|(index, aggregate)| bind(index, aggregate, states, probe))
        .collect::<Result<Vec<_>>>()?;
    let mut matched = 0u64;

    if rows.end > probe.num_rows() || rows.start > rows.end {
        return Err(Error::Internal(
            "multiplicity aggregate row range is out of bounds".into(),
        ));
    }
    for row in rows {
        if row & 1_023 == 0 {
            context.check_cancelled()?;
        }
        let count = lookup(row)?;
        if count == 0 {
            continue;
        }
        matched = matched.checked_add(count).ok_or_else(|| {
            Error::ResourceExhausted("join candidate count overflowed UINT64".into())
        })?;
        for updater in &updaters {
            apply(updater, states, row, count)?;
        }
    }
    Ok(matched)
}

fn bind<'a>(
    state_index: usize,
    aggregate: &AggregateExpr,
    states: &[AggregateState],
    probe: &'a RecordBatch,
) -> Result<Updater<'a>> {
    if aggregate.distinct {
        return unsupported(aggregate);
    }
    let state = states.get(state_index).ok_or_else(|| {
        Error::Internal("multiplicity aggregate state index is out of bounds".into())
    })?;
    match aggregate.function {
        AggregateFunction::Count
            if aggregate.expr.is_none() && matches!(state, AggregateState::Count(_)) =>
        {
            Ok(Updater::Count(state_index))
        }
        AggregateFunction::Sum => {
            let expression = aggregate.expr.as_ref().ok_or_else(|| {
                Error::Internal("multiplicity SUM is missing its argument".into())
            })?;
            let ExprKind::Column(column) = &expression.kind else {
                return unsupported(aggregate);
            };
            let values = probe.columns().get(*column).ok_or_else(|| {
                Error::Internal("multiplicity SUM is not a probe-side column".into())
            })?;
            if values.data_type() != &expression.data_type {
                return Err(Error::Internal(
                    "multiplicity SUM input type does not match its column".into(),
                ));
            }
            match NumericValues::bind(values)? {
                NumericValues::Signed(values)
                    if matches!(state, AggregateState::SumSigned { .. }) =>
                {
                    Ok(Updater::Signed(state_index, values))
                }
                NumericValues::Unsigned(values)
                    if matches!(state, AggregateState::SumUnsigned { .. }) =>
                {
                    Ok(Updater::Unsigned(state_index, values))
                }
                NumericValues::Decimal(values)
                    if matches!(state, AggregateState::SumDecimal { .. }) =>
                {
                    Ok(Updater::Decimal(state_index, values))
                }
                _ => Err(Error::Internal(
                    "multiplicity SUM reached an incompatible aggregate state".into(),
                )),
            }
        }
        _ => unsupported(aggregate),
    }
}

fn apply(
    updater: &Updater<'_>,
    states: &mut [AggregateState],
    row: usize,
    count: u64,
) -> Result<()> {
    match updater {
        Updater::Count(index) => {
            let AggregateState::Count(value) = state(states, *index)? else {
                return changed();
            };
            let count = i64::try_from(count)
                .map_err(|_| Error::Execution("count overflowed INT64".into()))?;
            *value = value
                .checked_add(count)
                .ok_or_else(|| Error::Execution("count overflowed INT64".into()))?;
        }
        Updater::Signed(index, values) => {
            let AggregateState::SumSigned { value, seen, .. } = state(states, *index)? else {
                return changed();
            };
            if let Some(input) = values.get(row) {
                let product = input
                    .checked_mul(i128::from(count))
                    .ok_or_else(|| Error::Execution("signed sum overflow".into()))?;
                *value = value
                    .checked_add(product)
                    .ok_or_else(|| Error::Execution("signed sum overflow".into()))?;
                *seen = true;
            }
        }
        Updater::Unsigned(index, values) => {
            let AggregateState::SumUnsigned { value, seen, .. } = state(states, *index)? else {
                return changed();
            };
            if let Some(input) = values.get(row) {
                let product = input
                    .checked_mul(u128::from(count))
                    .ok_or_else(|| Error::Execution("unsigned sum overflow".into()))?;
                *value = value
                    .checked_add(product)
                    .ok_or_else(|| Error::Execution("unsigned sum overflow".into()))?;
                *seen = true;
            }
        }
        Updater::Decimal(index, values) => {
            let AggregateState::SumDecimal { value, seen, .. } = state(states, *index)? else {
                return changed();
            };
            if let Some(input) = decimal_get(values, row) {
                let product = input
                    .checked_mul(i128::from(count))
                    .ok_or_else(|| Error::Execution("decimal sum overflowed i128".into()))?;
                *value = value
                    .checked_add(product)
                    .ok_or_else(|| Error::Execution("decimal sum overflowed i128".into()))?;
                *seen = true;
            }
        }
    }
    Ok(())
}

fn state(states: &mut [AggregateState], index: usize) -> Result<&mut AggregateState> {
    states.get_mut(index).ok_or_else(|| {
        Error::Internal("multiplicity aggregate state index is out of bounds".into())
    })
}

fn changed<T>() -> Result<T> {
    Err(Error::Internal(
        "multiplicity aggregate state changed after binding".into(),
    ))
}

fn unsupported<T>(aggregate: &AggregateExpr) -> Result<T> {
    Err(Error::Internal(format!(
        "unsupported aggregate reached multiplicity join sink: {}",
        aggregate.display_name
    )))
}

#[cfg(test)]
#[path = "multiplicity/tests.rs"]
mod tests;
