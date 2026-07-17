use crate::{Error, Result, runtime::QueryContext, sql::JoinType};

use super::{ProbeCursor, RowState, finish_row};

impl ProbeCursor<'_> {
    pub(super) fn fill_simple_indices(
        &mut self,
        left_indices: &mut Vec<u32>,
        right_indices: &mut Vec<Option<u32>>,
        markers: &mut Vec<Option<bool>>,
        context: &QueryContext,
    ) -> Result<()> {
        while self.row < self.left.num_rows() && left_indices.len() < self.batch_size {
            context.check_cancelled()?;
            let left_row = self.row;
            let matches = self.matches(left_row)?;
            let Some(matches) = matches.filter(|matches| !matches.is_empty()) else {
                finish_row(
                    left_row,
                    self.join_type,
                    &RowState::default(),
                    left_indices,
                    right_indices,
                    markers,
                )?;
                self.advance_row();
                continue;
            };

            match self.join_type {
                JoinType::Inner | JoinType::Left | JoinType::Right | JoinType::Full => {
                    let available = self.batch_size.saturating_sub(left_indices.len());
                    let end = self
                        .match_index
                        .saturating_add(available)
                        .min(matches.len());
                    let left_row = row_index(left_row)?;
                    for right_row in &matches[self.match_index..end] {
                        left_indices.push(left_row);
                        right_indices.push(Some(*right_row));
                        if let Some(matched) = &self.matched_build {
                            matched.mark(*right_row);
                        }
                    }
                    context.metrics.add_join_candidates(
                        u64::try_from(end.saturating_sub(self.match_index)).unwrap_or(u64::MAX),
                    );
                    self.match_index = end;
                    if end == matches.len() {
                        self.advance_row();
                    }
                }
                JoinType::Semi | JoinType::Anti | JoinType::Mark => {
                    context.metrics.add_join_candidates(1);
                    if matches.len() > 1 {
                        context.metrics.add_join_short_circuits(1);
                    }
                    let state = RowState {
                        matches: 1,
                        first_right: matches.first().copied(),
                        unknown: false,
                    };
                    finish_row(
                        left_row,
                        self.join_type,
                        &state,
                        left_indices,
                        right_indices,
                        markers,
                    )?;
                    self.advance_row();
                }
                JoinType::LeftSingle => {
                    context
                        .metrics
                        .add_join_candidates(u64::try_from(matches.len().min(2)).unwrap_or(2));
                    if matches.len() > 1 {
                        return Err(Error::Execution(
                            "scalar subquery returned more than one row".into(),
                        ));
                    }
                    let right_row = matches[0];
                    if let Some(matched) = &self.matched_build {
                        matched.mark(right_row);
                    }
                    finish_row(
                        left_row,
                        self.join_type,
                        &RowState {
                            matches: 1,
                            first_right: Some(right_row),
                            unknown: false,
                        },
                        left_indices,
                        right_indices,
                        markers,
                    )?;
                    self.advance_row();
                }
                JoinType::NullAwareAnti => {
                    return Err(Error::Internal(
                        "NullAwareAnti reached the simple equality join path".into(),
                    ));
                }
            }
        }
        Ok(())
    }

    fn advance_row(&mut self) {
        self.row += 1;
        self.match_index = 0;
        self.current_state = RowState::default();
    }
}

fn row_index(row: usize) -> Result<u32> {
    u32::try_from(row)
        .map_err(|_| Error::ResourceExhausted("join probe batch exceeds UINT32_MAX rows".into()))
}

#[cfg(test)]
#[path = "simple/tests.rs"]
mod tests;
