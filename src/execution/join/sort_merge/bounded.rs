use std::{mem::size_of, sync::Arc};

use arrow::{datatypes::SchemaRef, record_batch::RecordBatch};
use futures::StreamExt;

use crate::{
    Error, Result,
    runtime::{
        BatchEnvelope, MemoryBatchStream, MemoryReservation, QueryContext, SpillFile,
        boxed_memory_batch_stream,
    },
    sql::JoinType,
};

use super::super::{
    condition::JoinPredicates,
    evaluate_optional_values,
    matched::BuildMatchTracker,
    optional_array, optional_memory,
    output::{
        build_output_envelope, build_unmatched_right_envelope, candidate_workspace_bytes,
        grow_workspace,
    },
};
use super::process_equal_run;

#[allow(clippy::too_many_arguments)]
pub(super) fn process(
    left_group: SpillFile,
    right_group: SpillFile,
    left_schema: SchemaRef,
    right_schema: SchemaRef,
    predicates: JoinPredicates,
    join_type: JoinType,
    schema: SchemaRef,
    context: Arc<QueryContext>,
    batch_size: usize,
    cursor_bytes: usize,
) -> MemoryBatchStream {
    boxed_memory_batch_stream(async_stream::try_stream! {
        if !matches!(join_type, JoinType::Right | JoinType::Full) {
            Err(Error::Internal(
                "bounded build-match fallback requires RIGHT or FULL join".into(),
            ))?;
        }
        if predicates.is_null_aware() {
            Err(Error::Internal(
                "RIGHT/FULL bounded fallback cannot use null-aware membership".into(),
            ))?;
        }

        let mut cleanup = GroupCleanup::new(
            Arc::clone(&context),
            vec![left_group.clone(), right_group.clone()],
        );
        for right_batch in context.spill.read_file(&right_group)? {
            context.check_cancelled()?;
            let right_batch = BatchEnvelope::try_new(
                right_batch?,
                &context.memory,
                "join bounded right group",
            )?;
            let mut offset = 0usize;
            while offset < right_batch.num_rows() {
                let held_bytes = cursor_bytes.saturating_add(right_batch.memory_size());
                let (rows, tracker_memory, tracker) = reserve_chunk_tracker(
                    right_batch.num_rows() - offset,
                    &context,
                    held_bytes,
                ).await?;
                let right = right_batch.batch().slice(offset, rows);
                let mut output = process_right_chunk(
                    left_group.clone(),
                    right,
                    Arc::clone(&left_schema),
                    predicates.clone(),
                    tracker,
                    tracker_memory,
                    join_type,
                    Arc::clone(&schema),
                    Arc::clone(&context),
                    batch_size,
                    held_bytes,
                );
                while let Some(batch) = output.next().await {
                    yield batch?;
                }
                offset += rows;
            }
        }

        if join_type == JoinType::Full {
            let empty_right = RecordBatch::new_empty(Arc::clone(&right_schema));
            for left_batch in context.spill.read_file(&left_group)? {
                context.check_cancelled()?;
                let left_batch = BatchEnvelope::try_new(
                    left_batch?,
                    &context.memory,
                    "join bounded unmatched left group",
                )?;
                let mut output = process_equal_run(
                    left_batch.batch().clone(),
                    &right_group,
                    &empty_right,
                    &predicates,
                    None,
                    JoinType::Full,
                    false,
                    Arc::clone(&schema),
                    Arc::clone(&context),
                    batch_size,
                    cursor_bytes.saturating_add(left_batch.memory_size()),
                );
                while let Some(batch) = output.next().await {
                    yield batch?;
                }
            }
        }
        cleanup.finish()?;
    })
}

#[allow(clippy::too_many_arguments)]
fn process_right_chunk(
    left_group: SpillFile,
    right: RecordBatch,
    left_schema: SchemaRef,
    predicates: JoinPredicates,
    tracker: BuildMatchTracker,
    tracker_memory: MemoryReservation,
    join_type: JoinType,
    schema: SchemaRef,
    context: Arc<QueryContext>,
    batch_size: usize,
    parent_bytes: usize,
) -> MemoryBatchStream {
    boxed_memory_batch_stream(async_stream::try_stream! {
        let right_values = evaluate_optional_values(
            predicates.right_value(),
            &right,
            &context,
            "join bounded right membership value",
        )?;
        let retained = parent_bytes
            .saturating_add(tracker_memory.size())
            .saturating_add(optional_memory(&right_values));

        for left_batch in context.spill.read_file(&left_group)? {
            context.check_cancelled()?;
            let left_batch = BatchEnvelope::try_new(
                left_batch?,
                &context.memory,
                "join bounded left group",
            )?;
            let left_values = evaluate_optional_values(
                predicates.left_value(),
                left_batch.batch(),
                &context,
                "join bounded left membership value",
            )?;
            let held_bytes = retained
                .saturating_add(left_batch.memory_size())
                .saturating_add(optional_memory(&left_values));
            let mut left_row = 0usize;
            let mut right_row = 0usize;
            while left_row < left_batch.num_rows() {
                context.check_cancelled()?;
                let capacity = batch_size.max(1);
                let index_bytes = capacity
                    .saturating_mul(size_of::<u32>().saturating_mul(4))
                    .saturating_add(1_024);
                let mut workspace = context
                    .reserve_memory_while_holding(
                        index_bytes,
                        held_bytes,
                        "join bounded candidate indices",
                    )
                    .await?;
                let mut left_indices = Vec::with_capacity(capacity);
                let mut right_indices = Vec::with_capacity(capacity);
                while left_indices.len() < capacity && left_row < left_batch.num_rows() {
                    left_indices.push(u32::try_from(left_row).map_err(|_| {
                        Error::ResourceExhausted(
                            "bounded join left batch exceeds UINT32_MAX rows".into(),
                        )
                    })?);
                    right_indices.push(u32::try_from(right_row).map_err(|_| {
                        Error::ResourceExhausted(
                            "bounded join right chunk exceeds UINT32_MAX rows".into(),
                        )
                    })?);
                    right_row += 1;
                    if right_row == right.num_rows() {
                        left_row += 1;
                        right_row = 0;
                    }
                }
                grow_workspace(
                    &mut workspace,
                    candidate_workspace_bytes(
                        left_batch.batch(),
                        &right,
                        &left_indices,
                        &right_indices,
                        predicates.candidate_projection(),
                    )?,
                    &context,
                    held_bytes,
                )
                .await?;
                let outcomes = predicates.evaluate_candidates(
                    left_batch.batch(),
                    &right,
                    &left_indices,
                    &right_indices,
                    optional_array(&left_values),
                    optional_array(&right_values),
                )?;
                let mut output_left = Vec::with_capacity(left_indices.len());
                let mut output_right = Vec::with_capacity(right_indices.len());
                for ((left, right), outcome) in left_indices
                    .iter()
                    .zip(&right_indices)
                    .zip(outcomes)
                {
                    if outcome.qualifies {
                        tracker.mark(*right);
                        output_left.push(*left);
                        output_right.push(Some(*right));
                    }
                }
                if !output_left.is_empty() {
                    let output = build_output_envelope(
                        left_batch.batch(),
                        &right,
                        &output_left,
                        &output_right,
                        None,
                        join_type,
                        Arc::clone(&schema),
                        &context,
                        held_bytes.saturating_add(workspace.size()),
                        "join bounded matched output",
                    )
                    .await?;
                    drop((workspace, left_indices, right_indices, output_left, output_right));
                    yield output;
                }
            }
        }

        let held_bytes = retained;
        let mut start = 0usize;
        loop {
            let indices = tracker
                .unmatched_from(start, batch_size.max(1), &context, held_bytes)
                .await?;
            let Some(last) = indices.last().copied() else {
                break;
            };
            start = last as usize + 1;
            yield build_unmatched_right_envelope(
                &left_schema,
                &right,
                &indices,
                Arc::clone(&schema),
                &context,
                held_bytes.saturating_add(indices.memory_size()),
            ).await?;
        }
    })
}

async fn reserve_chunk_tracker(
    available_rows: usize,
    context: &QueryContext,
    held_bytes: usize,
) -> Result<(usize, MemoryReservation, BuildMatchTracker)> {
    let mut rows = available_rows.min(u32::MAX as usize).max(1);
    loop {
        let required = BuildMatchTracker::required_bytes(rows);
        match context
            .reserve_memory_while_holding(required, held_bytes, "join bounded match tracker")
            .await
        {
            Ok(memory) => {
                let tracker = BuildMatchTracker::from_reserved(rows, &memory)?;
                return Ok((rows, memory, tracker));
            }
            Err(Error::ResourceExhausted(_)) if rows > 1 => rows = rows.div_ceil(2),
            Err(error) => return Err(error),
        }
    }
}

struct GroupCleanup {
    context: Arc<QueryContext>,
    files: Vec<SpillFile>,
}

impl GroupCleanup {
    fn new(context: Arc<QueryContext>, files: Vec<SpillFile>) -> Self {
        Self { context, files }
    }

    fn finish(&mut self) -> Result<()> {
        while let Some(file) = self.files.pop() {
            self.context.spill.remove_file(&file)?;
        }
        Ok(())
    }
}

impl Drop for GroupCleanup {
    fn drop(&mut self) {
        while let Some(file) = self.files.pop() {
            if let Err(error) = self.context.spill.remove_file(&file)
                && !matches!(error, Error::Cancelled)
            {
                tracing::error!(%error, path = %file.path().display(), "failed to remove bounded join group spill");
            }
        }
    }
}
