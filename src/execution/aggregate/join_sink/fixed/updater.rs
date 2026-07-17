use arrow::{array::ArrayRef, record_batch::RecordBatch};

use crate::{
    Error, Result,
    sql::{AggregateExpr, AggregateFunction, ExprKind},
};

use super::{
    AggregateState,
    apply::{self, Side},
    values::{NumericValues, SignedValues, UnsignedValues},
};

enum Kind<'a> {
    Count,
    Signed(Side, SignedValues<'a>),
    Unsigned(Side, UnsignedValues<'a>),
    Decimal(Side, &'a arrow::array::Decimal128Array),
}

pub(super) struct Updater<'a> {
    state_index: usize,
    kind: Kind<'a>,
}

impl<'a> Updater<'a> {
    pub(super) fn bind(
        state_index: usize,
        aggregate: &AggregateExpr,
        states: &[AggregateState],
        probe: &'a RecordBatch,
        build: &'a RecordBatch,
    ) -> Result<Self> {
        if aggregate.distinct {
            return unsupported(aggregate);
        }
        let state = states.get(state_index).ok_or_else(|| {
            Error::Internal("fixed join aggregate state index is out of bounds".into())
        })?;
        let kind = match aggregate.function {
            AggregateFunction::Count
                if aggregate.expr.is_none() && matches!(state, AggregateState::Count(_)) =>
            {
                Kind::Count
            }
            AggregateFunction::Sum => {
                let expression = aggregate.expr.as_ref().ok_or_else(|| {
                    Error::Internal("fixed join SUM is missing its argument".into())
                })?;
                let ExprKind::Column(column) = &expression.kind else {
                    return unsupported(aggregate);
                };
                let (side, values) = column_values(*column, probe, build)?;
                if values.data_type() != &expression.data_type {
                    return Err(Error::Internal(
                        "fixed join SUM input type does not match its column".into(),
                    ));
                }
                match NumericValues::bind(values)? {
                    NumericValues::Signed(values)
                        if matches!(state, AggregateState::SumSigned { .. }) =>
                    {
                        Kind::Signed(side, values)
                    }
                    NumericValues::Unsigned(values)
                        if matches!(state, AggregateState::SumUnsigned { .. }) =>
                    {
                        Kind::Unsigned(side, values)
                    }
                    NumericValues::Decimal(values)
                        if matches!(state, AggregateState::SumDecimal { .. }) =>
                    {
                        Kind::Decimal(side, values)
                    }
                    _ => {
                        return Err(Error::Internal(
                            "fixed join SUM reached an incompatible aggregate state".into(),
                        ));
                    }
                }
            }
            _ => return unsupported(aggregate),
        };
        Ok(Self { state_index, kind })
    }

    pub(super) fn apply(
        &self,
        states: &mut [AggregateState],
        probe_row: usize,
        matches: &[u32],
    ) -> Result<()> {
        let state = states.get_mut(self.state_index).ok_or_else(|| {
            Error::Internal("fixed join aggregate state index is out of bounds".into())
        })?;
        match (&self.kind, state) {
            (Kind::Count, AggregateState::Count(count)) => apply::count(count, matches.len()),
            (Kind::Signed(side, values), AggregateState::SumSigned { value, seen, .. }) => {
                apply::signed(*side, values, value, seen, probe_row, matches)
            }
            (Kind::Unsigned(side, values), AggregateState::SumUnsigned { value, seen, .. }) => {
                apply::unsigned(*side, values, value, seen, probe_row, matches)
            }
            (Kind::Decimal(side, values), AggregateState::SumDecimal { value, seen, .. }) => {
                apply::decimal(*side, values, value, seen, probe_row, matches)
            }
            _ => Err(Error::Internal(
                "fixed join updater state changed after binding".into(),
            )),
        }
    }
}

fn column_values<'a>(
    column: usize,
    probe: &'a RecordBatch,
    build: &'a RecordBatch,
) -> Result<(Side, &'a ArrayRef)> {
    if column < probe.num_columns() {
        return Ok((Side::Probe, probe.column(column)));
    }
    let build_column = column - probe.num_columns();
    build
        .columns()
        .get(build_column)
        .map(|values| (Side::Build, values))
        .ok_or_else(|| Error::Internal("fixed join SUM column is out of bounds".into()))
}

fn unsupported<T>(aggregate: &AggregateExpr) -> Result<T> {
    Err(Error::Internal(format!(
        "unsupported aggregate reached fixed join sink: {}",
        aggregate.display_name
    )))
}
