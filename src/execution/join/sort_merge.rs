mod bounded;
mod cursor;

use std::{cmp::Ordering, sync::Arc};

use crate::{
    Error, Result,
    runtime::{
        BatchEnvelope, MemoryBatchStream, QueryContext, SpillFile, boxed_memory_batch_stream,
    },
    sql::{BoundExpr, JoinType, SortExpr},
};
use arrow::{datatypes::SchemaRef, record_batch::RecordBatch};
use futures::StreamExt;

use super::{
    condition::{JoinPredicates, SqlTruth},
    evaluate_optional_values,
    matched::BuildMatchTracker,
    optional_array, optional_memory,
    output::{build_output_envelope, build_unmatched_right_envelope, candidate_workspace_bytes},
    spill::{PartitionTask, remove_files},
};
use crate::execution::{sort, value::CellValue};
use cursor::SortedCursor;

#[allow(clippy::too_many_arguments)]
pub(super) fn fallback_with_null_keys(
    task: PartitionTask,
    left_expressions: Vec<BoundExpr>,
    right_expressions: Vec<BoundExpr>,
    left_schema: SchemaRef,
    right_schema: SchemaRef,
    predicates: JoinPredicates,
    null_equal_keys: bool,
    join_type: JoinType,
    schema: SchemaRef,
    context: Arc<QueryContext>,
    batch_size: usize,
) -> MemoryBatchStream {
    fallback_impl(
        task,
        left_expressions,
        right_expressions,
        left_schema,
        right_schema,
        predicates,
        null_equal_keys,
        join_type,
        schema,
        context,
        batch_size,
        usize::MAX,
    )
}

#[allow(clippy::too_many_arguments)]
fn fallback_impl(
    task: PartitionTask,
    left_expressions: Vec<BoundExpr>,
    right_expressions: Vec<BoundExpr>,
    left_schema: SchemaRef,
    right_schema: SchemaRef,
    predicates: JoinPredicates,
    null_equal_keys: bool,
    join_type: JoinType,
    schema: SchemaRef,
    context: Arc<QueryContext>,
    batch_size: usize,
    match_tracker_budget: usize,
) -> MemoryBatchStream {
    boxed_memory_batch_stream(async_stream::try_stream! {
        let cleanup = TaskCleanup::new(
            context.clone(),
            task.left.clone(),
            task.right.clone(),
        );
        let left_input = spill_input(task.left, Arc::clone(&context), "join sort left") ;
        let right_input = spill_input(task.right, Arc::clone(&context), "join sort right");
        let left_sort = if left_expressions.is_empty() {
            left_input
        } else {
            sort::sort(
                left_input,
                sort_keys(&left_expressions),
                None,
                Arc::clone(&left_schema),
                Arc::clone(&context),
                batch_size,
            )
        };
        let right_sort = if right_expressions.is_empty() {
            right_input
        } else {
            sort::sort(
                right_input,
                sort_keys(&right_expressions),
                None,
                Arc::clone(&right_schema),
                Arc::clone(&context),
                batch_size,
            )
        };
        let mut left = SortedCursor::new(
            left_sort,
            left_expressions,
            Arc::clone(&context),
        );
        let mut right = SortedCursor::new(
            right_sort,
            right_expressions,
            Arc::clone(&context),
        );
        let empty_right = RecordBatch::new_empty(Arc::clone(&right_schema));

        while left.ensure_row().await? {
            context.check_cancelled()?;
            let left_key = left.key()?;
            if !null_equal_keys && has_null(&left_key) {
                if emits_empty_group(join_type) {
                    let held_bytes = left.held_bytes().saturating_add(right.held_bytes());
                    yield left_only(
                        left.take_row(),
                        &empty_right,
                        join_type,
                        Arc::clone(&schema),
                        &context,
                        held_bytes,
                    ).await?;
                } else {
                    left.take_row();
                }
                continue;
            }

            while right.ensure_row().await? {
                let right_key = right.key()?;
                if compare_keys(&right_key, &left_key)? != Ordering::Less {
                    break;
                }
                let row = right.take_row();
                if tracks_build_matches(join_type) {
                    let held_bytes = left.held_bytes().saturating_add(right.held_bytes());
                    yield right_only(
                        &left_schema,
                        row,
                        Arc::clone(&schema),
                        &context,
                        held_bytes,
                    ).await?;
                }
            }

            if !right.ensure_row().await? {
                if emits_empty_group(join_type) {
                    let held_bytes = left.held_bytes().saturating_add(right.held_bytes());
                    yield left_only(
                        left.take_row(),
                        &empty_right,
                        join_type,
                        Arc::clone(&schema),
                        &context,
                        held_bytes,
                    ).await?;
                } else {
                    left.take_row();
                }
                continue;
            }

            let right_key = right.key()?;
            if compare_keys(&left_key, &right_key)? == Ordering::Less {
                if emits_empty_group(join_type) {
                    let held_bytes = left.held_bytes().saturating_add(right.held_bytes());
                    yield left_only(
                        left.take_row(),
                        &empty_right,
                        join_type,
                        Arc::clone(&schema),
                        &context,
                        held_bytes,
                    ).await?;
                } else {
                    left.take_row();
                }
                continue;
            }

            let group_key = left_key;
            let (group_file, group_rows) = spill_right_group(
                &mut right,
                &group_key,
                Arc::clone(&right_schema),
                &context,
            ).await?;
            let mut group_memory = context.memory.reservation();
            let matched_build = if tracks_build_matches(join_type)
                && group_rows <= u32::MAX as usize
                && BuildMatchTracker::required_bytes(group_rows) <= match_tracker_budget
            {
                BuildMatchTracker::try_new(group_rows, &mut group_memory)
            } else {
                None
            };

            if tracks_build_matches(join_type)
                && matched_build.is_none()
                && predicates.residual().is_some()
            {
                let left_group = spill_left_group(
                    &mut left,
                    &group_key,
                    Arc::clone(&left_schema),
                    &context,
                ).await?;
                let mut output = bounded::process(
                    left_group,
                    group_file,
                    Arc::clone(&left_schema),
                    Arc::clone(&right_schema),
                    predicates.clone(),
                    join_type,
                    Arc::clone(&schema),
                    Arc::clone(&context),
                    batch_size,
                    left.held_bytes().saturating_add(right.held_bytes()),
                );
                while let Some(batch) = output.next().await {
                    yield batch?;
                }
                continue;
            }
            // With no residual every row in the equal-key build group matches
            // at least one left row, so an unavailable whole-group tracker can
            // be elided without producing unmatched-right rows.

            while left.ensure_row().await? && left.key()? == group_key {
                let left_run = left.take_equal_run(&group_key)?;
                let mut output = process_equal_run(
                    left_run,
                    &group_file,
                    &empty_right,
                    &predicates,
                    matched_build.clone(),
                    join_type,
                    true,
                    Arc::clone(&schema),
                    Arc::clone(&context),
                    batch_size,
                    left.held_bytes()
                        .saturating_add(right.held_bytes())
                        .saturating_add(group_memory.size()),
                );
                while let Some(batch) = output.next().await {
                    yield batch?;
                }
            }
            if let Some(matched) = &matched_build {
                let mut group_offset = 0usize;
                for right_batch in context.spill.read_file(&group_file)? {
                    let right_batch = BatchEnvelope::try_new(
                        right_batch?,
                        &context.memory,
                        "join sort-merge unmatched group",
                    )?;
                    let mut local_start = 0usize;
                    while local_start < right_batch.num_rows() {
                        let global_start = group_offset.checked_add(local_start).ok_or_else(|| {
                            Error::ResourceExhausted(
                                "sort-merge unmatched row offset overflowed usize".into(),
                            )
                        })?;
                        let global_end = group_offset
                            .checked_add(right_batch.num_rows())
                            .ok_or_else(|| {
                                Error::ResourceExhausted(
                                    "sort-merge unmatched row count overflowed usize".into(),
                                )
                            })?;
                        let held_bytes = left
                            .held_bytes()
                            .saturating_add(right.held_bytes())
                            .saturating_add(group_memory.size())
                            .saturating_add(right_batch.memory_size());
                        let indices = matched
                            .unmatched_range(
                                global_start,
                                global_end,
                                group_offset,
                                batch_size.max(1),
                                &context,
                                held_bytes,
                            )
                            .await?;
                        let Some(last) = indices.last().copied() else {
                            break;
                        };
                        local_start = last as usize + 1;
                        yield build_unmatched_right_envelope(
                            &left_schema,
                            right_batch.batch(),
                            &indices,
                            Arc::clone(&schema),
                            &context,
                            held_bytes.saturating_add(indices.memory_size()),
                        ).await?;
                    }
                    group_offset = group_offset.saturating_add(right_batch.num_rows());
                }
            }
            context.spill.remove_file(&group_file)?;
        }
        if tracks_build_matches(join_type) {
            while right.ensure_row().await? {
                let held_bytes = left.held_bytes().saturating_add(right.held_bytes());
                yield right_only(
                    &left_schema,
                    right.take_row(),
                    Arc::clone(&schema),
                    &context,
                    held_bytes,
                ).await?;
            }
        }
        drop(cleanup);
    })
}

#[allow(clippy::too_many_arguments)]
#[cfg(test)]
pub(super) fn fallback(
    task: PartitionTask,
    left_expressions: Vec<BoundExpr>,
    right_expressions: Vec<BoundExpr>,
    left_schema: SchemaRef,
    right_schema: SchemaRef,
    predicates: JoinPredicates,
    join_type: JoinType,
    schema: SchemaRef,
    context: Arc<QueryContext>,
    batch_size: usize,
) -> MemoryBatchStream {
    fallback_with_null_keys(
        task,
        left_expressions,
        right_expressions,
        left_schema,
        right_schema,
        predicates,
        false,
        join_type,
        schema,
        context,
        batch_size,
    )
}

#[allow(clippy::too_many_arguments)]
#[cfg(test)]
pub(super) fn fallback_with_tracker_budget(
    task: PartitionTask,
    left_expressions: Vec<BoundExpr>,
    right_expressions: Vec<BoundExpr>,
    left_schema: SchemaRef,
    right_schema: SchemaRef,
    predicates: JoinPredicates,
    join_type: JoinType,
    schema: SchemaRef,
    context: Arc<QueryContext>,
    batch_size: usize,
    match_tracker_budget: usize,
) -> MemoryBatchStream {
    fallback_impl(
        task,
        left_expressions,
        right_expressions,
        left_schema,
        right_schema,
        predicates,
        false,
        join_type,
        schema,
        context,
        batch_size,
        match_tracker_budget,
    )
}

#[derive(Clone, Copy, Default)]
struct RunState {
    matches: usize,
    unknown: bool,
    first_right: Option<usize>,
}

#[allow(clippy::too_many_arguments)]
fn process_equal_run(
    left: RecordBatch,
    group_file: &SpillFile,
    empty_right: &RecordBatch,
    predicates: &JoinPredicates,
    matched_build: Option<BuildMatchTracker>,
    join_type: JoinType,
    emit_matches: bool,
    schema: SchemaRef,
    context: Arc<QueryContext>,
    batch_size: usize,
    held_bytes: usize,
) -> MemoryBatchStream {
    let group_file = group_file.clone();
    let predicates = predicates.clone();
    let empty_right = empty_right.clone();
    boxed_memory_batch_stream(async_stream::try_stream! {
        let state_bytes = left
            .num_rows()
            .saturating_mul(
                std::mem::size_of::<RunState>()
                    .saturating_add(std::mem::size_of::<u32>())
                    .saturating_add(std::mem::size_of::<Option<u32>>())
                    .saturating_add(std::mem::size_of::<Option<bool>>()),
            )
            .saturating_add(
                batch_size.max(1).saturating_mul(
                    std::mem::size_of::<u32>()
                        .saturating_mul(2)
                        .saturating_add(std::mem::size_of::<super::condition::CandidateOutcome>()),
                ),
            )
            .saturating_add(
                left.num_rows()
                    .saturating_mul(std::mem::size_of::<(u32, usize)>()),
            )
            .saturating_add(1_024)
            .max(1);
        let state_memory = context
            .reserve_memory_while_holding(state_bytes, held_bytes, "join sort-merge state")
            .await?;
        let left_values = evaluate_optional_values(
            predicates.left_value(),
            &left,
            &context,
            "join sort-merge left membership value",
        )?;
        let retained = held_bytes
            .saturating_add(state_memory.size())
            .saturating_add(optional_memory(&left_values));
        let mut states = vec![RunState::default(); left.num_rows()];

        let existence_shortcut = predicates.residual().is_none()
            && !predicates.is_null_aware()
            && matches!(join_type, JoinType::Semi | JoinType::Anti | JoinType::Mark);
        if existence_shortcut {
            for state in &mut states {
                state.matches = 1;
            }
        }

        let mut right_offset = 0usize;
        for right_batch in if existence_shortcut {
            None
        } else {
            Some(context.spill.read_file(&group_file)?)
        }
        .into_iter()
        .flatten()
        {
            context.check_cancelled()?;
            let right_batch = BatchEnvelope::try_new(
                right_batch?,
                &context.memory,
                "join sort-merge group",
            )?;
            if right_batch.num_rows() == 0 {
                continue;
            }
            let right_values = evaluate_optional_values(
                predicates.right_value(),
                right_batch.batch(),
                &context,
                "join sort-merge right membership value",
            )?;
            let short_circuit = matches!(
                join_type,
                JoinType::Semi | JoinType::Anti | JoinType::Mark | JoinType::NullAwareAnti
            );
            let mut left_cursor = 0usize;
            let mut right_cursor = 0usize;
            while left_cursor < left.num_rows() {
                context.check_cancelled()?;
                let rows = batch_size.max(1);
                let mut left_indices = Vec::with_capacity(rows);
                let mut right_indices = Vec::with_capacity(rows);
                while left_indices.len() < rows && left_cursor < left.num_rows() {
                    if short_circuit && states[left_cursor].matches != 0 {
                        left_cursor += 1;
                        right_cursor = 0;
                        continue;
                    }
                    left_indices.push(u32::try_from(left_cursor).map_err(|_| {
                        Error::ResourceExhausted(
                            "sort-merge join left index exceeds UINT32_MAX".into(),
                        )
                    })?);
                    right_indices.push(u32::try_from(right_cursor).map_err(|_| {
                        Error::ResourceExhausted(
                            "sort-merge join right index exceeds UINT32_MAX".into(),
                        )
                    })?);
                    right_cursor += 1;
                    if right_cursor == right_batch.num_rows() {
                        left_cursor += 1;
                        right_cursor = 0;
                    }
                }
                if left_indices.is_empty() {
                    break;
                }
                let candidate_bytes = candidate_workspace_bytes(
                    &left,
                    right_batch.batch(),
                    &left_indices,
                    &right_indices,
                )?;
                let candidate_memory = context
                    .reserve_memory_while_holding(
                        candidate_bytes,
                        retained
                            .saturating_add(right_batch.memory_size())
                            .saturating_add(optional_memory(&right_values)),
                        "join sort-merge candidate workspace",
                    )
                    .await?;
                let outcomes = predicates.evaluate_candidates(
                    &left,
                    right_batch.batch(),
                    &left_indices,
                    &right_indices,
                    optional_array(&left_values),
                    optional_array(&right_values),
                )?;
                let mut output_left = Vec::new();
                let mut output_right = Vec::new();
                for ((left_row, right_row), outcome) in left_indices
                    .iter()
                    .zip(&right_indices)
                    .zip(outcomes)
                {
                    if !outcome.qualifies {
                        continue;
                    }
                    let state = &mut states[*left_row as usize];
                    if predicates.is_null_aware() {
                        match outcome.membership.expect("null-aware outcome") {
                            SqlTruth::True => state.matches = 1,
                            SqlTruth::False => {}
                            SqlTruth::Unknown => state.unknown = true,
                        }
                    } else {
                        state.matches = state.matches.saturating_add(1);
                        if let Some(matched) = &matched_build {
                            let right_row = right_offset
                                .checked_add(*right_row as usize)
                                .ok_or_else(|| {
                                    Error::ResourceExhausted(
                                        "sort-merge matched row offset overflowed usize".into(),
                                    )
                                })?;
                            matched.mark(u32::try_from(right_row).map_err(|_| {
                                Error::ResourceExhausted(
                                    "sort-merge matched row exceeds UINT32_MAX".into(),
                                )
                            })?);
                        }
                        if state.matches == 1 && join_type == JoinType::LeftSingle {
                            state.first_right = Some(
                                right_offset.checked_add(*right_row as usize).ok_or_else(|| {
                                    Error::ResourceExhausted(
                                        "sort-merge scalar row offset overflowed usize".into(),
                                    )
                                })?,
                            );
                        }
                        if join_type == JoinType::LeftSingle && state.matches > 1 {
                            Err(Error::Execution(
                                "scalar subquery returned more than one row".into(),
                            ))?;
                        }
                        if emit_matches && matches!(
                            join_type,
                            JoinType::Inner
                                | JoinType::Left
                                | JoinType::Right
                                | JoinType::Full
                        ) {
                            output_left.push(*left_row);
                            output_right.push(Some(*right_row));
                        }
                    }
                }
                drop(candidate_memory);
                if !output_left.is_empty() {
                    yield build_output_envelope(
                        &left,
                        right_batch.batch(),
                        &output_left,
                        &output_right,
                        None,
                        join_type,
                        Arc::clone(&schema),
                        &context,
                        retained
                            .saturating_add(right_batch.memory_size())
                            .saturating_add(optional_memory(&right_values)),
                        "join sort-merge output",
                    ).await?;
                }
            }
            right_offset = right_offset.checked_add(right_batch.num_rows()).ok_or_else(|| {
                Error::ResourceExhausted("sort-merge right row count overflowed usize".into())
            })?;
        }

        // A scalar subquery must not expose its first match before the whole
        // equality group has been checked for a second row. Replay the group
        // only after cardinality validation succeeds for every left row.
        if join_type == JoinType::LeftSingle {
            let mut matched = states
                .iter()
                .enumerate()
                .filter_map(|(left_row, state)| {
                    state.first_right.map(|right_row| (left_row as u32, right_row))
                })
                .collect::<Vec<_>>();
            matched.sort_unstable_by_key(|(_, right_row)| *right_row);
            let mut matched_offset = 0usize;
            let mut replay_offset = 0usize;
            for right_batch in context.spill.read_file(&group_file)? {
                let right_batch = BatchEnvelope::try_new(
                    right_batch?,
                    &context.memory,
                    "join sort-merge scalar replay",
                )?;
                let replay_end = replay_offset.checked_add(right_batch.num_rows()).ok_or_else(|| {
                    Error::ResourceExhausted(
                        "sort-merge scalar replay row count overflowed usize".into(),
                    )
                })?;
                while matched_offset < matched.len()
                    && matched[matched_offset].1 < replay_end
                {
                    if matched[matched_offset].1 < replay_offset {
                        Err(Error::Internal(
                            "sort-merge scalar replay moved past a matched row".into(),
                        ))?;
                    }
                    let end = (matched_offset + batch_size.max(1)).min(matched.len());
                    let end = (matched_offset..end)
                        .take_while(|index| matched[*index].1 < replay_end)
                        .last()
                        .map_or(matched_offset, |index| index + 1);
                    let mut output_left = Vec::with_capacity(end - matched_offset);
                    let mut output_right = Vec::with_capacity(end - matched_offset);
                    for (left_row, right_row) in &matched[matched_offset..end] {
                        output_left.push(*left_row);
                        output_right.push(Some(u32::try_from(right_row - replay_offset).map_err(
                            |_| {
                                Error::ResourceExhausted(
                                    "sort-merge scalar replay index exceeds UINT32_MAX".into(),
                                )
                            },
                        )?));
                    }
                    yield build_output_envelope(
                        &left,
                        right_batch.batch(),
                        &output_left,
                        &output_right,
                        None,
                        join_type,
                        Arc::clone(&schema),
                        &context,
                        retained.saturating_add(right_batch.memory_size()),
                        "join sort-merge scalar output",
                    ).await?;
                    matched_offset = end;
                }
                replay_offset = replay_end;
            }
            if matched_offset != matched.len() {
                Err(Error::Internal(
                    "sort-merge scalar replay did not find every matched row".into(),
                ))?;
            }
        }

        let mut output_left = Vec::new();
        let mut output_right = Vec::new();
        let mut markers = Vec::new();
        for (row, state) in states.iter().enumerate() {
            let emit = match join_type {
                JoinType::Inner => false,
                JoinType::Left => state.matches == 0,
                JoinType::Right => false,
                JoinType::Full => state.matches == 0,
                JoinType::Semi => state.matches != 0,
                JoinType::Anti => state.matches == 0,
                JoinType::LeftSingle => state.matches == 0,
                JoinType::Mark => true,
                JoinType::NullAwareAnti => state.matches == 0 && !state.unknown,
            };
            if emit {
                output_left.push(u32::try_from(row).map_err(|_| {
                    Error::ResourceExhausted(
                        "sort-merge join output exceeds UINT32_MAX rows".into(),
                    )
                })?);
                output_right.push(None);
                if join_type == JoinType::Mark {
                    markers.push(if state.matches != 0 {
                        Some(true)
                    } else if state.unknown {
                        None
                    } else {
                        Some(false)
                    });
                }
            }
        }
        for offset in (0..output_left.len()).step_by(batch_size.max(1)) {
            let rows = batch_size.max(1).min(output_left.len() - offset);
            let marker_slice = if join_type == JoinType::Mark {
                Some(&markers[offset..offset + rows])
            } else {
                None
            };
            yield build_output_envelope(
                &left,
                &empty_right,
                &output_left[offset..offset + rows],
                &output_right[offset..offset + rows],
                marker_slice,
                join_type,
                Arc::clone(&schema),
                &context,
                retained,
                "join sort-merge output",
            ).await?;
        }
    })
}

fn spill_input(
    files: Vec<SpillFile>,
    context: Arc<QueryContext>,
    owner: &'static str,
) -> MemoryBatchStream {
    boxed_memory_batch_stream(async_stream::try_stream! {
        for file in files {
            for batch in context.spill.read_file(&file)? {
                context.check_cancelled()?;
                yield BatchEnvelope::try_new(batch?, &context.memory, owner)?;
            }
            context.spill.remove_file(&file)?;
        }
    })
}

async fn spill_right_group(
    right: &mut SortedCursor,
    key: &[CellValue],
    schema: SchemaRef,
    context: &QueryContext,
) -> Result<(SpillFile, usize)> {
    let mut writer = context.spill.writer("join-equal-group", schema)?;
    let mut rows = 0usize;
    while right.ensure_row().await? && right.key()? == key {
        let batch = right.take_equal_run(key)?;
        rows = rows.checked_add(batch.num_rows()).ok_or_else(|| {
            Error::ResourceExhausted("sort-merge equality group row count overflowed".into())
        })?;
        writer.write_batch(&batch)?;
    }
    Ok((writer.finish(1)?, rows))
}

async fn spill_left_group(
    left: &mut SortedCursor,
    key: &[CellValue],
    schema: SchemaRef,
    context: &QueryContext,
) -> Result<SpillFile> {
    let mut writer = context.spill.writer("join-left-equal-group", schema)?;
    while left.ensure_row().await? && left.key()? == key {
        writer.write_batch(&left.take_equal_run(key)?)?;
    }
    writer.finish(1)
}

async fn right_only(
    left_schema: &SchemaRef,
    right: RecordBatch,
    schema: SchemaRef,
    context: &QueryContext,
    held_bytes: usize,
) -> Result<BatchEnvelope> {
    build_unmatched_right_envelope(left_schema, &right, &[0], schema, context, held_bytes).await
}

async fn left_only(
    left: RecordBatch,
    empty_right: &RecordBatch,
    join_type: JoinType,
    schema: SchemaRef,
    context: &QueryContext,
    held_bytes: usize,
) -> Result<BatchEnvelope> {
    let marker = (join_type == JoinType::Mark).then_some([Some(false)]);
    build_output_envelope(
        &left,
        empty_right,
        &[0],
        &[None],
        marker.as_ref().map(|marker| marker.as_slice()),
        join_type,
        schema,
        context,
        held_bytes,
        "join sort-merge output",
    )
    .await
}

fn emits_empty_group(join_type: JoinType) -> bool {
    matches!(
        join_type,
        JoinType::Left
            | JoinType::Full
            | JoinType::Anti
            | JoinType::LeftSingle
            | JoinType::Mark
            | JoinType::NullAwareAnti
    )
}

fn tracks_build_matches(join_type: JoinType) -> bool {
    matches!(join_type, JoinType::Right | JoinType::Full)
}

fn sort_keys(expressions: &[BoundExpr]) -> Vec<SortExpr> {
    expressions
        .iter()
        .cloned()
        .map(|expr| SortExpr {
            expr,
            descending: false,
            nulls_first: false,
        })
        .collect()
}

fn has_null(key: &[CellValue]) -> bool {
    key.iter().any(CellValue::is_null)
}

fn compare_keys(left: &[CellValue], right: &[CellValue]) -> Result<Ordering> {
    if left.len() != right.len() {
        return Err(Error::Internal("sort-merge join key arity changed".into()));
    }
    for (left, right) in left.iter().zip(right) {
        let ordering = match (left, right) {
            (CellValue::Null, CellValue::Null) => Ordering::Equal,
            (CellValue::Null, _) => Ordering::Greater,
            (_, CellValue::Null) => Ordering::Less,
            _ => left.compare(right)?,
        };
        if ordering != Ordering::Equal {
            return Ok(ordering);
        }
    }
    Ok(Ordering::Equal)
}

struct TaskCleanup {
    context: Arc<QueryContext>,
    left: Vec<SpillFile>,
    right: Vec<SpillFile>,
}

impl TaskCleanup {
    fn new(context: Arc<QueryContext>, left: Vec<SpillFile>, right: Vec<SpillFile>) -> Self {
        Self {
            context,
            left,
            right,
        }
    }
}

impl Drop for TaskCleanup {
    fn drop(&mut self) {
        if let Err(error) = remove_files(&self.context, &self.left) {
            tracing::error!(%error, "failed to remove left sort-merge spill files");
        }
        if let Err(error) = remove_files(&self.context, &self.right) {
            tracing::error!(%error, "failed to remove right sort-merge spill files");
        }
    }
}

#[cfg(test)]
mod tests {
    use std::cmp::Ordering;

    use super::compare_keys;
    use crate::execution::value::CellValue;

    #[test]
    fn compares_composite_keys_with_nulls_last() {
        assert_eq!(
            compare_keys(
                &[CellValue::Int64(1), CellValue::Utf8("a".into())],
                &[CellValue::Int64(1), CellValue::Utf8("b".into())],
            )
            .unwrap(),
            Ordering::Less,
        );
        assert_eq!(
            compare_keys(&[CellValue::Null], &[CellValue::Int64(1)]).unwrap(),
            Ordering::Greater,
        );
    }
}
