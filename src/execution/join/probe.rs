use std::{collections::HashMap, mem::size_of, sync::Arc};

use arrow::{array::ArrayRef, datatypes::SchemaRef, record_batch::RecordBatch};

use crate::{
    Error, Result,
    runtime::{BatchEnvelope, MemoryReservation, QueryContext},
    sql::JoinType,
};

use super::{
    CellValue, cell,
    condition::{JoinPredicates, SqlTruth},
    matched::BuildMatchTracker,
    output::{build_output, candidate_workspace_bytes, grow_workspace, output_workspace_bytes},
    row_key,
};

#[derive(Clone, Copy, Debug)]
pub(super) struct GlobalMembershipState {
    rhs_nonempty: bool,
    rhs_has_null: bool,
}

impl GlobalMembershipState {
    pub(super) fn new(rhs_rows: usize, right_values: &ArrayRef) -> Self {
        Self {
            rhs_nonempty: rhs_rows != 0,
            rhs_has_null: right_values.null_count() != 0,
        }
    }
}

pub(super) fn try_build_hash_table(
    key_arrays: &[ArrayRef],
    rows: usize,
    deduplicate: bool,
    reservation: &mut MemoryReservation,
) -> Result<Option<HashMap<Vec<CellValue>, Vec<u32>>>> {
    try_build_hash_table_with_nulls(key_arrays, rows, deduplicate, false, reservation)
}

pub(super) fn try_build_hash_table_with_nulls(
    key_arrays: &[ArrayRef],
    rows: usize,
    deduplicate: bool,
    null_equal_keys: bool,
    reservation: &mut MemoryReservation,
) -> Result<Option<HashMap<Vec<CellValue>, Vec<u32>>>> {
    try_build_summarized_hash_table_with_nulls(
        key_arrays,
        rows,
        deduplicate,
        null_equal_keys,
        None,
        reservation,
    )
}

pub(super) fn try_build_existence_hash_table_with_nulls(
    key_arrays: &[ArrayRef],
    rows: usize,
    null_equal_keys: bool,
    summary_values: &ArrayRef,
    reservation: &mut MemoryReservation,
) -> Result<Option<HashMap<Vec<CellValue>, Vec<u32>>>> {
    try_build_summarized_hash_table_with_nulls(
        key_arrays,
        rows,
        false,
        null_equal_keys,
        Some(summary_values),
        reservation,
    )
}

fn try_build_summarized_hash_table_with_nulls(
    key_arrays: &[ArrayRef],
    rows: usize,
    deduplicate: bool,
    null_equal_keys: bool,
    summary_values: Option<&ArrayRef>,
    reservation: &mut MemoryReservation,
) -> Result<Option<HashMap<Vec<CellValue>, Vec<u32>>>> {
    let initial_reservation = reservation.size();
    let mut hash_table: HashMap<Vec<CellValue>, Vec<u32>> = HashMap::new();
    for row in 0..rows {
        let key = match row_key(key_arrays, row) {
            Ok(key) => key,
            Err(error) => {
                reset_hash_build(&mut hash_table, reservation, initial_reservation)?;
                return Err(error);
            }
        };
        if !null_equal_keys && key.iter().any(CellValue::is_null) {
            continue;
        }
        let row_index = match u32::try_from(row) {
            Ok(row) => row,
            Err(_) => {
                reset_hash_build(&mut hash_table, reservation, initial_reservation)?;
                return Err(Error::ResourceExhausted(
                    "hash join build side exceeds UINT32_MAX rows".into(),
                ));
            }
        };
        if let Some(summary_values) = summary_values {
            let value = match cell(summary_values, row) {
                Ok(value) => value,
                Err(error) => {
                    reset_hash_build(&mut hash_table, reservation, initial_reservation)?;
                    return Err(error);
                }
            };
            if value.is_null() {
                continue;
            }
            if let Some(matches) = hash_table.get(&key)
                && (matches.len() >= 2
                    || matches.iter().try_fold(false, |duplicate, index| {
                        Ok::<_, Error>(duplicate || cell(summary_values, *index as usize)? == value)
                    })?)
            {
                continue;
            }
        }
        if hash_table.contains_key(&key) {
            let (length, capacity) = {
                let matches = hash_table.get(&key).expect("occupied hash key");
                (matches.len(), matches.capacity())
            };
            if deduplicate && length != 0 {
                continue;
            }
            let estimated = predicted_vec_growth(length, capacity);
            if reservation.try_grow(estimated).is_err() {
                reset_hash_build(&mut hash_table, reservation, initial_reservation)?;
                return Ok(None);
            }
            hash_table
                .get_mut(&key)
                .expect("occupied hash key")
                .push(row_index);
            let actual = hash_table
                .get(&key)
                .expect("occupied hash key")
                .capacity()
                .saturating_sub(capacity)
                .saturating_mul(size_of::<u32>());
            if !adjust_reserved_growth(
                &mut hash_table,
                reservation,
                initial_reservation,
                estimated,
                actual,
            )? {
                return Ok(None);
            }
        } else {
            let map_capacity = hash_table.capacity();
            let key_bytes = key_heap_bytes(&key, key.capacity());
            let map_estimate = if hash_table.len() == map_capacity {
                map_capacity
                    .max(4)
                    .saturating_mul(2)
                    .saturating_mul(hash_bucket_bytes())
            } else {
                0
            };
            let estimated = key_bytes
                .saturating_add(4 * size_of::<u32>())
                .saturating_add(map_estimate);
            if reservation.try_grow(estimated).is_err() {
                reset_hash_build(&mut hash_table, reservation, initial_reservation)?;
                return Ok(None);
            }
            let matches = vec![row_index];
            let row_bytes = matches.capacity().saturating_mul(size_of::<u32>());
            hash_table.insert(key, matches);
            let actual = key_bytes.saturating_add(row_bytes).saturating_add(
                hash_table
                    .capacity()
                    .saturating_sub(map_capacity)
                    .saturating_mul(hash_bucket_bytes()),
            );
            if !adjust_reserved_growth(
                &mut hash_table,
                reservation,
                initial_reservation,
                estimated,
                actual,
            )? {
                return Ok(None);
            }
        }
    }
    Ok(Some(hash_table))
}

fn predicted_vec_growth(length: usize, capacity: usize) -> usize {
    if length < capacity {
        0
    } else {
        capacity
            .max(4)
            .saturating_sub(capacity)
            .max(capacity)
            .saturating_mul(size_of::<u32>())
    }
}

fn adjust_reserved_growth(
    hash_table: &mut HashMap<Vec<CellValue>, Vec<u32>>,
    reservation: &mut MemoryReservation,
    initial_reservation: usize,
    estimated: usize,
    actual: usize,
) -> Result<bool> {
    if actual > estimated && reservation.try_grow(actual - estimated).is_err() {
        reset_hash_build(hash_table, reservation, initial_reservation)?;
        return Ok(false);
    }
    reservation.shrink(estimated.saturating_sub(actual));
    Ok(true)
}

fn reset_hash_build(
    hash_table: &mut HashMap<Vec<CellValue>, Vec<u32>>,
    reservation: &mut MemoryReservation,
    initial_reservation: usize,
) -> Result<()> {
    *hash_table = HashMap::new();
    reservation.try_resize(initial_reservation)
}

fn key_heap_bytes(key: &[CellValue], capacity: usize) -> usize {
    capacity
        .saturating_mul(size_of::<CellValue>())
        .saturating_add(key.iter().fold(0usize, |bytes, value| {
            bytes.saturating_add(match value {
                CellValue::Utf8(value) => value.capacity(),
                CellValue::Binary(value) => value.capacity(),
                _ => 0,
            })
        }))
}

fn hash_bucket_bytes() -> usize {
    size_of::<Vec<CellValue>>()
        .saturating_add(size_of::<Vec<u32>>())
        .saturating_add(16)
}

pub(super) struct ProbeCursor<'a> {
    left: &'a RecordBatch,
    right: &'a RecordBatch,
    left_keys: &'a [ArrayRef],
    hash_table: &'a HashMap<Vec<CellValue>, Vec<u32>>,
    predicates: &'a JoinPredicates,
    left_values: Option<&'a ArrayRef>,
    right_values: Option<&'a ArrayRef>,
    global_membership: Option<GlobalMembershipState>,
    null_equal_keys: bool,
    matched_build: Option<BuildMatchTracker>,
    join_type: JoinType,
    schema: SchemaRef,
    batch_size: usize,
    held_bytes: usize,
    row: usize,
    match_index: usize,
    current_state: RowState,
}

#[derive(Clone, Copy, Default)]
struct RowState {
    matches: usize,
    first_right: Option<u32>,
    unknown: bool,
}

struct CandidateGroup {
    left_row: usize,
    start: usize,
    end: usize,
    complete: bool,
    state: RowState,
}

impl<'a> ProbeCursor<'a> {
    #[allow(clippy::too_many_arguments)]
    pub(super) fn new(
        left: &'a RecordBatch,
        right: &'a RecordBatch,
        left_keys: &'a [ArrayRef],
        hash_table: &'a HashMap<Vec<CellValue>, Vec<u32>>,
        predicates: &'a JoinPredicates,
        left_values: Option<&'a ArrayRef>,
        right_values: Option<&'a ArrayRef>,
        global_membership: Option<GlobalMembershipState>,
        null_equal_keys: bool,
        matched_build: Option<BuildMatchTracker>,
        join_type: JoinType,
        schema: SchemaRef,
        batch_size: usize,
        held_bytes: usize,
    ) -> Self {
        Self {
            left,
            right,
            left_keys,
            hash_table,
            predicates,
            left_values,
            right_values,
            global_membership,
            null_equal_keys,
            matched_build,
            join_type,
            schema,
            batch_size: batch_size.max(1),
            held_bytes,
            row: 0,
            match_index: 0,
            current_state: RowState::default(),
        }
    }

    pub(super) async fn next_batch(
        &mut self,
        context: &QueryContext,
    ) -> Result<Option<BatchEnvelope>> {
        context.check_cancelled()?;
        let index_bytes = self
            .batch_size
            .saturating_mul(
                size_of::<u32>()
                    .saturating_mul(3)
                    .saturating_add(size_of::<Option<u32>>())
                    .saturating_add(size_of::<CandidateGroup>()),
            )
            .saturating_add(size_of::<RowState>())
            .saturating_add(1_024)
            .max(1);
        let mut workspace = context
            .reserve_memory_while_holding(
                index_bytes,
                self.held_bytes,
                "join output index workspace",
            )
            .await?;
        let mut left_indices = Vec::with_capacity(self.batch_size);
        let mut right_indices = Vec::with_capacity(self.batch_size);
        let mut markers = Vec::with_capacity(self.batch_size);

        while self.row < self.left.num_rows() && left_indices.len() < self.batch_size {
            let mut candidate_left = Vec::with_capacity(self.batch_size);
            let mut candidate_right = Vec::with_capacity(self.batch_size);
            let mut candidate_groups = Vec::new();
            {
                let _active = context.scheduler.enter_lane();
                while self.row < self.left.num_rows()
                    && left_indices.len().saturating_add(candidate_left.len()) < self.batch_size
                {
                    let left_row = self.row;
                    let key = row_key(self.left_keys, left_row)?;
                    let matches = if !self.null_equal_keys && key.iter().any(CellValue::is_null) {
                        None
                    } else {
                        self.hash_table.get(&key)
                    };
                    let Some(matches) = matches.filter(|matches| !matches.is_empty()) else {
                        let state = self.initial_state(left_row)?;
                        finish_row(
                            left_row,
                            self.join_type,
                            &state,
                            &mut left_indices,
                            &mut right_indices,
                            &mut markers,
                        )?;
                        self.row += 1;
                        self.match_index = 0;
                        self.current_state = RowState::default();
                        continue;
                    };

                    let available = self
                        .batch_size
                        .saturating_sub(left_indices.len())
                        .saturating_sub(candidate_left.len());
                    let take = available.min(matches.len().saturating_sub(self.match_index));
                    let start = candidate_left.len();
                    let state = if self.match_index == 0 {
                        self.initial_state(left_row)?
                    } else {
                        self.current_state
                    };
                    let end = self.match_index + take;
                    for right_row in &matches[self.match_index..end] {
                        candidate_left.push(u32::try_from(left_row).map_err(|_| {
                            Error::ResourceExhausted(
                                "join probe batch exceeds UINT32_MAX rows".into(),
                            )
                        })?);
                        candidate_right.push(*right_row);
                    }
                    let complete = end == matches.len();
                    candidate_groups.push(CandidateGroup {
                        left_row,
                        start,
                        end: candidate_left.len(),
                        complete,
                        state,
                    });
                    self.match_index = end;
                    if complete {
                        self.row += 1;
                        self.match_index = 0;
                        self.current_state = RowState::default();
                    }
                }
            }

            let had_candidates = !candidate_left.is_empty();
            if had_candidates {
                grow_workspace(
                    &mut workspace,
                    index_bytes.saturating_add(candidate_workspace_bytes(
                        self.left,
                        self.right,
                        &candidate_left,
                        &candidate_right,
                    )?),
                    context,
                    self.held_bytes,
                )
                .await?;
                let outcomes = {
                    let _active = context.scheduler.enter_lane();
                    self.predicates.evaluate_candidates(
                        self.left,
                        self.right,
                        &candidate_left,
                        &candidate_right,
                        self.left_values,
                        self.right_values,
                    )?
                };
                context
                    .metrics
                    .add_join_candidates(u64::try_from(outcomes.len()).unwrap_or(u64::MAX));
                for group in &candidate_groups {
                    let mut state = group.state;
                    for candidate in group.start..group.end {
                        let right_row = candidate_right[candidate];
                        let outcome = outcomes[candidate];
                        if outcome.qualifies {
                            if self.predicates.is_null_aware() {
                                match outcome.membership.expect("null-aware outcome") {
                                    SqlTruth::True => {
                                        state.matches = 1;
                                        state.first_right.get_or_insert(right_row);
                                    }
                                    SqlTruth::False => {}
                                    SqlTruth::Unknown => state.unknown = true,
                                }
                            } else {
                                state.matches = state.matches.saturating_add(1);
                                state.first_right.get_or_insert(right_row);
                                if let Some(matched) = &self.matched_build {
                                    matched.mark(right_row);
                                }
                                if self.join_type == JoinType::LeftSingle && state.matches > 1 {
                                    return Err(Error::Execution(
                                        "scalar subquery returned more than one row".into(),
                                    ));
                                }
                                if matches!(
                                    self.join_type,
                                    JoinType::Inner
                                        | JoinType::Left
                                        | JoinType::Right
                                        | JoinType::Full
                                ) {
                                    left_indices.push(group.left_row as u32);
                                    right_indices.push(Some(right_row));
                                }
                            }
                        }
                    }
                    if group.complete || result_is_decided(self.join_type, &state) {
                        // Semi/Anti/Mark joins need only an existence answer. Once a
                        // qualifying candidate is observed, do not rescan a large
                        // duplicate-key group in subsequent batches. Q21's supplier
                        // inequality predicates benefit directly while general residual
                        // semantics stay intact.
                        if !group.complete {
                            context.metrics.add_join_short_circuits(1);
                            self.row = self.row.saturating_add(1);
                            self.match_index = 0;
                            self.current_state = RowState::default();
                        }
                        finish_row(
                            group.left_row,
                            self.join_type,
                            &state,
                            &mut left_indices,
                            &mut right_indices,
                            &mut markers,
                        )?;
                    } else {
                        self.current_state = state;
                    }
                }
                drop(outcomes);
                drop(candidate_groups);
                drop(candidate_left);
                drop(candidate_right);
                workspace.try_resize(index_bytes)?;
            }

            if left_indices.len() >= self.batch_size {
                break;
            }
            if !had_candidates && self.row >= self.left.num_rows() {
                break;
            }
            context.check_cancelled()?;
        }

        if left_indices.is_empty() {
            Ok(None)
        } else {
            grow_workspace(
                &mut workspace,
                index_bytes.saturating_add(output_workspace_bytes(
                    self.left,
                    self.right,
                    &left_indices,
                    &right_indices,
                    self.join_type,
                )?),
                context,
                self.held_bytes,
            )
            .await?;
            let output = {
                let _active = context.scheduler.enter_lane();
                build_output(
                    self.left,
                    self.right,
                    &left_indices,
                    &right_indices,
                    marker_slice(self.join_type, &markers),
                    self.join_type,
                    Arc::clone(&self.schema),
                )?
            };
            Ok(Some(BatchEnvelope::from_reservation(
                output,
                workspace,
                "join output",
            )?))
        }
    }

    fn initial_state(&self, left_row: usize) -> Result<RowState> {
        let Some(global) = self.global_membership else {
            return Ok(RowState::default());
        };
        let left_values = self.left_values.ok_or_else(|| {
            Error::Internal("global membership hash is missing its left value array".into())
        })?;
        let left_is_null = cell(left_values, left_row)?.is_null();
        Ok(RowState {
            unknown: if left_is_null {
                global.rhs_nonempty
            } else {
                global.rhs_has_null
            },
            ..RowState::default()
        })
    }
}

fn result_is_decided(join_type: JoinType, state: &RowState) -> bool {
    match join_type {
        JoinType::Semi | JoinType::Anti | JoinType::Mark => state.matches != 0,
        // UNKNOWN can still be replaced by a later exact membership match.
        JoinType::NullAwareAnti => state.matches != 0,
        JoinType::Inner
        | JoinType::Left
        | JoinType::Right
        | JoinType::Full
        | JoinType::LeftSingle => false,
    }
}

fn finish_row(
    row: usize,
    join_type: JoinType,
    state: &RowState,
    left_indices: &mut Vec<u32>,
    right_indices: &mut Vec<Option<u32>>,
    markers: &mut Vec<Option<bool>>,
) -> Result<()> {
    let emit = match join_type {
        JoinType::Inner => return Ok(()),
        JoinType::Left => state.matches == 0,
        JoinType::Right => false,
        // Interim FULL behavior emits the left side; unmatched build rows are
        // appended by the v0.4 FULL-join finalization path.
        JoinType::Full => state.matches == 0,
        JoinType::Semi => state.matches != 0,
        JoinType::Anti => state.matches == 0,
        JoinType::LeftSingle | JoinType::Mark => true,
        JoinType::NullAwareAnti => state.matches == 0 && !state.unknown,
    };
    if !emit {
        return Ok(());
    }
    left_indices.push(u32::try_from(row).map_err(|_| {
        Error::ResourceExhausted("join probe batch exceeds UINT32_MAX rows".into())
    })?);
    match join_type {
        JoinType::LeftSingle => right_indices.push(state.first_right),
        JoinType::Mark => {
            right_indices.push(None);
            markers.push(if state.matches != 0 {
                Some(true)
            } else if state.unknown {
                None
            } else {
                Some(false)
            });
        }
        _ => right_indices.push(None),
    }
    Ok(())
}

fn marker_slice(join_type: JoinType, markers: &[Option<bool>]) -> Option<&[Option<bool>]> {
    (join_type == JoinType::Mark).then_some(markers)
}

#[cfg(test)]
mod summary_tests {
    use std::sync::Arc;

    use arrow::array::{ArrayRef, Int64Array};

    use super::{RowState, result_is_decided, try_build_existence_hash_table_with_nulls};
    use crate::runtime::MemoryPool;
    use crate::sql::JoinType;

    #[test]
    fn existence_hash_keeps_two_distinct_non_null_values_per_key() {
        let keys: Vec<ArrayRef> = vec![Arc::new(Int64Array::from(vec![1, 1, 1, 1, 1]))];
        let values: ArrayRef = Arc::new(Int64Array::from(vec![
            Some(10),
            Some(10),
            Some(20),
            Some(30),
            None,
        ]));
        let pool = MemoryPool::new(1 << 20);
        let mut reservation = pool.reservation();
        let hash =
            try_build_existence_hash_table_with_nulls(&keys, 5, false, &values, &mut reservation)
                .unwrap()
                .unwrap();
        let representatives = hash.values().next().unwrap();
        assert_eq!(representatives.as_slice(), &[0, 2]);
    }

    #[test]
    fn null_aware_unknown_does_not_short_circuit_before_exact_match() {
        let unknown = RowState {
            matches: 0,
            first_right: None,
            unknown: true,
        };
        assert!(!result_is_decided(JoinType::NullAwareAnti, &unknown));
        let matched = RowState {
            matches: 1,
            ..unknown
        };
        assert!(result_is_decided(JoinType::NullAwareAnti, &matched));
    }
}
