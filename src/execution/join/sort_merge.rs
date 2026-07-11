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

use super::{
    probe::build_output_envelope,
    spill::{PartitionTask, remove_files},
};
use crate::execution::{sort, value::CellValue};
use cursor::SortedCursor;

#[allow(clippy::too_many_arguments)]
pub(super) fn fallback(
    task: PartitionTask,
    left_expressions: Vec<BoundExpr>,
    right_expressions: Vec<BoundExpr>,
    left_schema: SchemaRef,
    right_schema: SchemaRef,
    join_type: JoinType,
    schema: SchemaRef,
    context: Arc<QueryContext>,
    batch_size: usize,
) -> MemoryBatchStream {
    boxed_memory_batch_stream(async_stream::try_stream! {
        let cleanup = TaskCleanup::new(
            context.clone(),
            task.left.clone(),
            task.right.clone(),
        );
        let left_input = spill_input(task.left, Arc::clone(&context), "join sort left") ;
        let right_input = spill_input(task.right, Arc::clone(&context), "join sort right");
        let left_sort = sort::sort(
            left_input,
            sort_keys(&left_expressions),
            None,
            Arc::clone(&left_schema),
            Arc::clone(&context),
            batch_size,
        );
        let right_sort = sort::sort(
            right_input,
            sort_keys(&right_expressions),
            None,
            Arc::clone(&right_schema),
            Arc::clone(&context),
            batch_size,
        );
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
            if has_null(&left_key) {
                if matches!(join_type, JoinType::Left | JoinType::Anti) {
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
                if has_null(&right_key) || compare_keys(&right_key, &left_key)? != Ordering::Less {
                    break;
                }
                right.take_row();
            }

            if !right.ensure_row().await? {
                if matches!(join_type, JoinType::Left | JoinType::Anti) {
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
            if has_null(&right_key) || compare_keys(&left_key, &right_key)? == Ordering::Less {
                if matches!(join_type, JoinType::Left | JoinType::Anti) {
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
            let group_file = spill_right_group(
                &mut right,
                &group_key,
                Arc::clone(&right_schema),
                &context,
            ).await?;

            while left.ensure_row().await? && left.key()? == group_key {
                let left_row = left.take_row();
                match join_type {
                    JoinType::Inner | JoinType::Left => {
                        for right_batch in context.spill.read_file(&group_file)? {
                            let right_batch = BatchEnvelope::try_new(
                                right_batch?,
                                &context.memory,
                                "join sort-merge group",
                            )?;
                            let mut offset = 0;
                            while offset < right_batch.num_rows() {
                                let rows = batch_size.max(1).min(right_batch.num_rows() - offset);
                                let right_slice = right_batch.batch().slice(offset, rows);
                                let left_indices = vec![0_u32; rows];
                                let right_indices = (0..rows)
                                    .map(|row| {
                                        u32::try_from(row).map(Some).map_err(|_| {
                                            Error::ResourceExhausted(
                                                "sort-merge join output exceeds UINT32_MAX rows".into(),
                                            )
                                        })
                                    })
                                    .collect::<Result<Vec<_>>>()?;
                                yield build_output_envelope(
                                    &left_row,
                                    &right_slice,
                                    &left_indices,
                                    &right_indices,
                                    join_type,
                                    Arc::clone(&schema),
                                    &context,
                                    left
                                        .held_bytes()
                                        .saturating_add(right.held_bytes())
                                        .saturating_add(right_batch.memory_size()),
                                    "join sort-merge output",
                                ).await?;
                                offset += rows;
                            }
                        }
                    }
                    JoinType::Semi => {
                        let held_bytes = left.held_bytes().saturating_add(right.held_bytes());
                        yield left_only(
                            left_row,
                            &empty_right,
                            join_type,
                            Arc::clone(&schema),
                            &context,
                            held_bytes,
                        ).await?;
                    }
                    JoinType::Anti => {}
                }
            }
            context.spill.remove_file(&group_file)?;
        }
        drop(cleanup);
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
) -> Result<SpillFile> {
    let mut writer = context.spill.writer("join-equal-group", schema)?;
    while right.ensure_row().await? && right.key()? == key {
        writer.write_batch(&right.take_equal_run(key)?)?;
    }
    writer.finish(1)
}

async fn left_only(
    left: RecordBatch,
    empty_right: &RecordBatch,
    join_type: JoinType,
    schema: SchemaRef,
    context: &QueryContext,
    held_bytes: usize,
) -> Result<BatchEnvelope> {
    build_output_envelope(
        &left,
        empty_right,
        &[0],
        &[None],
        join_type,
        schema,
        context,
        held_bytes,
        "join sort-merge output",
    )
    .await
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
