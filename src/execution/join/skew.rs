use std::sync::Arc;

use arrow::{
    array::UInt32Array, compute::take_record_batch, datatypes::SchemaRef, record_batch::RecordBatch,
};

use crate::{
    Error, Result,
    runtime::{QueryContext, RecordBatchStream, boxed_record_batch_stream},
    sql::{BoundExpr, JoinType},
};

use super::{
    ProbeCursor, build_hash_table, build_output, evaluate_keys, row_key,
    spill::{PartitionTask, remove_files},
};

#[allow(clippy::too_many_arguments)]
pub(super) fn fallback(
    task: PartitionTask,
    left_expressions: Vec<BoundExpr>,
    right_expressions: Vec<BoundExpr>,
    right_schema: SchemaRef,
    join_type: JoinType,
    schema: SchemaRef,
    context: Arc<QueryContext>,
    batch_size: usize,
) -> RecordBatchStream {
    boxed_record_batch_stream(async_stream::try_stream! {
        let mut reservation = context.memory.reservation();
        let empty_right = RecordBatch::new_empty(right_schema);

        for left_file in &task.left {
            for left_batch in context.spill.read_batches(left_file)? {
                for row in 0..left_batch.num_rows() {
                    context.check_cancelled()?;
                    let left_row = take_rows(&left_batch, row, 1)?;
                    let left_required = minimum_left_bytes(&left_row);
                    reservation.try_resize(left_required).map_err(|_| {
                        minimum_state_error(left_required, &context)
                    })?;
                    let left_keys = evaluate_keys(&left_expressions, &left_row)?;
                    let key = row_key(&left_keys, 0)?;
                    let mut matched = false;

                    if !key.iter().any(super::CellValue::is_null) {
                        'right_files: for right_file in &task.right {
                            for right_batch in context.spill.read_batches(right_file)? {
                                let mut offset = 0;
                                while offset < right_batch.num_rows() {
                                    context.check_cancelled()?;
                                    let (right_chunk, rows) = reserve_right_chunk(
                                        &left_row,
                                        &right_batch,
                                        offset,
                                        &mut reservation,
                                        &context,
                                        batch_size,
                                    )?;
                                    let right_keys = evaluate_keys(&right_expressions, &right_chunk)?;
                                    let hash_table = build_hash_table(
                                        &right_keys,
                                        right_chunk.num_rows(),
                                        matches!(join_type, JoinType::Semi | JoinType::Anti),
                                    )?;
                                    if hash_table.contains_key(&key) {
                                        matched = true;
                                        if matches!(join_type, JoinType::Inner | JoinType::Left) {
                                            let mut probe = ProbeCursor::new(
                                                &left_row,
                                                &right_chunk,
                                                &left_keys,
                                                &hash_table,
                                                JoinType::Inner,
                                                Arc::clone(&schema),
                                                batch_size,
                                            );
                                            while let Some(output) = probe.next_batch(&context)? {
                                                yield output;
                                            }
                                        }
                                    }
                                    offset += rows;
                                    reservation.try_resize(left_required)?;
                                    if matched && join_type == JoinType::Semi {
                                        break 'right_files;
                                    }
                                }
                            }
                        }
                    }

                    let emit_left_only = match join_type {
                        JoinType::Left => !matched,
                        JoinType::Semi => matched,
                        JoinType::Anti => !matched,
                        JoinType::Inner => false,
                    };
                    if emit_left_only {
                        yield build_output(
                            &left_row,
                            &empty_right,
                            &[0],
                            &[None],
                            join_type,
                            Arc::clone(&schema),
                        )?;
                    }
                    reservation.try_resize(0)?;
                }
            }
            context.spill.remove_file(left_file);
        }
        remove_files(&context, &task.right);
    })
}

fn reserve_right_chunk(
    left: &RecordBatch,
    right: &RecordBatch,
    offset: usize,
    reservation: &mut crate::runtime::MemoryReservation,
    context: &QueryContext,
    batch_size: usize,
) -> Result<(RecordBatch, usize)> {
    let mut rows = (right.num_rows() - offset).min(batch_size.max(1));
    loop {
        let chunk = take_rows(right, offset, rows)?;
        let required = join_state_bytes(left, &chunk);
        if reservation.try_resize(required).is_ok() {
            return Ok((chunk, rows));
        }
        if rows == 1 {
            return Err(minimum_state_error(required, context));
        }
        rows = (rows / 2).max(1);
    }
}

fn take_rows(batch: &RecordBatch, offset: usize, rows: usize) -> Result<RecordBatch> {
    let indices = (offset..offset + rows)
        .map(|index| {
            u32::try_from(index).map_err(|_| {
                Error::ResourceExhausted("skew join fallback batch exceeds UINT32_MAX rows".into())
            })
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(take_record_batch(batch, &UInt32Array::from(indices))?)
}

fn minimum_left_bytes(left: &RecordBatch) -> usize {
    left.get_array_memory_size()
        .saturating_mul(2)
        .saturating_add(256)
}

fn join_state_bytes(left: &RecordBatch, right: &RecordBatch) -> usize {
    minimum_left_bytes(left)
        .saturating_add(right.get_array_memory_size().saturating_mul(2))
        .saturating_add(right.num_rows().saturating_mul(128))
}

fn minimum_state_error(required: usize, context: &QueryContext) -> Error {
    Error::ResourceExhausted(format!(
        "skew join fallback cannot reserve the minimum one-row state: {required} bytes required, \
         query limit {} bytes, currently available {} bytes",
        context.memory.limit(),
        context.memory.available()
    ))
}
