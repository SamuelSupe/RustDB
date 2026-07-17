use arrow::record_batch::RecordBatch;

use crate::{Error, Result, runtime::QueryContext, sql::AggregateExpr};

use super::AggregateState;

mod apply;
mod updater;
pub(super) mod values;

use updater::Updater;

/// Updates every aggregate while traversing the probe batch exactly once.
/// The lookup result is shared by all bound updaters for that probe row.
pub(super) fn update<'a, F>(
    states: &mut [AggregateState],
    aggregates: &[AggregateExpr],
    probe: &RecordBatch,
    build: &RecordBatch,
    mut lookup: F,
    context: &QueryContext,
) -> Result<usize>
where
    F: FnMut(usize) -> Option<&'a [u32]>,
{
    if states.len() != aggregates.len() {
        return Err(Error::Internal(
            "fixed join aggregate width does not match its state".into(),
        ));
    }

    let updaters = aggregates
        .iter()
        .enumerate()
        .map(|(index, aggregate)| Updater::bind(index, aggregate, states, probe, build))
        .collect::<Result<Vec<_>>>()?;
    let mut matched_rows = 0usize;

    for probe_row in 0..probe.num_rows() {
        if probe_row & 1_023 == 0 {
            context.check_cancelled()?;
        }
        let Some(matches) = lookup(probe_row) else {
            continue;
        };
        if matches.is_empty() {
            continue;
        }
        matched_rows = matched_rows.checked_add(matches.len()).ok_or_else(|| {
            Error::ResourceExhausted("fixed join output row count overflowed usize".into())
        })?;
        for updater in &updaters {
            updater.apply(states, probe_row, matches)?;
        }
    }

    Ok(matched_rows)
}

#[cfg(test)]
#[path = "fixed/tests.rs"]
mod tests;
