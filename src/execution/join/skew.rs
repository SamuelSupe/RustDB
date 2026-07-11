use std::sync::Arc;

use arrow::{
    array::UInt32Array, compute::take_record_batch, datatypes::SchemaRef, record_batch::RecordBatch,
};

use crate::{
    Error, Result,
    runtime::{MemoryReservation, QueryContext, RecordBatchStream, boxed_record_batch_stream},
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
            for left_batch in context.spill.read_file(left_file)? {
                let left_batch = left_batch?;
                let _left_batch_memory = reserve_decoded_batch(&left_batch, "left", &context)?;
                for row in 0..left_batch.num_rows() {
                    context.check_cancelled()?;
                    let left_estimate = left_state_estimate(&left_batch, row)?;
                    reservation.try_resize(left_estimate).map_err(|_| {
                        minimum_state_error(left_estimate, &context)
                    })?;
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
                            for right_batch in context.spill.read_file(right_file)? {
                                let right_batch = right_batch?;
                                let _right_batch_memory =
                                    reserve_decoded_batch(&right_batch, "right", &context)?;
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
                                            let output_rows = hash_table
                                                .get(&key)
                                                .map_or(0, Vec::len)
                                                .min(batch_size.max(1));
                                            loop {
                                                let Some((output, _output_memory)) =
                                                    next_output(
                                                        &mut probe,
                                                        &left_row,
                                                        &right_chunk,
                                                        output_rows,
                                                        &context,
                                                    )?
                                                else {
                                                    break;
                                                };
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
                        let estimate = output_estimate(&left_row, &empty_right, 1);
                        let mut output_memory = context
                            .memory
                            .try_reserve(estimate)
                            .map_err(|_| output_error(estimate, &context))?;
                        let output = build_output(
                            &left_row,
                            &empty_right,
                            &[0],
                            &[None],
                            join_type,
                            Arc::clone(&schema),
                        )?;
                        let actual = output.get_array_memory_size().max(1);
                        output_memory
                            .try_resize(actual)
                            .map_err(|_| output_error(estimate.max(actual), &context))?;
                        yield output;
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
        let estimate = join_state_estimate(left, right, offset, rows)?;
        if reservation.try_resize(estimate).is_err() {
            if rows == 1 {
                return Err(minimum_state_error(estimate, context));
            }
            rows = rows.div_ceil(2);
            continue;
        }
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

fn reserve_decoded_batch(
    batch: &RecordBatch,
    side: &str,
    context: &QueryContext,
) -> Result<MemoryReservation> {
    let bytes = batch.get_array_memory_size().max(1);
    context.memory.try_reserve(bytes).map_err(|_| {
        Error::ResourceExhausted(format!(
            "skew join fallback cannot reserve decoded {side} spill batch: {bytes} bytes required, \
             query limit {} bytes, currently available {} bytes",
            context.memory.limit(),
            context.memory.available()
        ))
    })
}

fn left_state_estimate(source: &RecordBatch, row: usize) -> Result<usize> {
    Ok(logical_slice_bytes(source, row, 1)?
        .saturating_mul(2)
        .saturating_add(source.num_columns().saturating_mul(512))
        .saturating_add(256)
        .max(1))
}

fn join_state_estimate(
    left: &RecordBatch,
    source: &RecordBatch,
    offset: usize,
    rows: usize,
) -> Result<usize> {
    Ok(minimum_left_bytes(left)
        .saturating_add(logical_slice_bytes(source, offset, rows)?.saturating_mul(2))
        .saturating_add(rows.saturating_mul(source.num_columns()).saturating_mul(16))
        .saturating_add(rows.saturating_mul(128))
        .saturating_add(source.num_columns().saturating_mul(512))
        .max(1))
}

fn logical_slice_bytes(batch: &RecordBatch, offset: usize, rows: usize) -> Result<usize> {
    let slice = batch.slice(offset, rows);
    let buffers = slice.columns().iter().try_fold(0usize, |bytes, column| {
        Ok::<_, arrow::error::ArrowError>(
            bytes.saturating_add(column.to_data().get_slice_memory_size()?),
        )
    })?;
    Ok(buffers
        .saturating_add(batch.num_columns().saturating_mul(256))
        .max(1))
}

fn next_output(
    probe: &mut ProbeCursor<'_>,
    left: &RecordBatch,
    right: &RecordBatch,
    rows: usize,
    context: &QueryContext,
) -> Result<Option<(RecordBatch, MemoryReservation)>> {
    let estimate = output_estimate(left, right, rows);
    let mut memory = context
        .memory
        .try_reserve(estimate)
        .map_err(|_| output_error(estimate, context))?;
    let Some(output) = probe.next_batch(context)? else {
        return Ok(None);
    };
    let actual = output.get_array_memory_size().max(1);
    memory
        .try_resize(actual)
        .map_err(|_| output_error(estimate.max(actual), context))?;
    Ok(Some((output, memory)))
}

fn output_estimate(left: &RecordBatch, right: &RecordBatch, rows: usize) -> usize {
    left.get_array_memory_size()
        .saturating_mul(rows.max(1))
        .saturating_add(right.get_array_memory_size())
        .saturating_add(
            rows.saturating_mul(left.num_columns().saturating_add(right.num_columns()))
                .saturating_mul(16),
        )
        .saturating_add(
            left.num_columns()
                .saturating_add(right.num_columns())
                .saturating_mul(512),
        )
        .max(1)
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

fn output_error(required: usize, context: &QueryContext) -> Error {
    Error::ResourceExhausted(format!(
        "skew join fallback cannot reserve one output batch: {required} bytes required, \
         query limit {} bytes, currently available {} bytes",
        context.memory.limit(),
        context.memory.available()
    ))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::{
        array::Int64Array,
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    };

    use super::{join_state_estimate, reserve_right_chunk};
    use crate::runtime::{MemoryPool, QueryContext};

    #[test]
    fn right_chunk_reservation_shrinks_with_logical_slice_rows() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::Int64, false),
            Field::new("value", DataType::Int64, false),
        ]));
        let left = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(vec![7])),
                Arc::new(Int64Array::from(vec![11])),
            ],
        )
        .unwrap();
        let right = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![7; 8_192])),
                Arc::new(Int64Array::from_iter_values(0..8_192)),
            ],
        )
        .unwrap();
        let decoded_bytes = right.get_array_memory_size().max(1);
        let one_row = join_state_estimate(&left, &right, 0, 1).unwrap();
        let root = tempfile::tempdir().unwrap();
        let context = QueryContext::new(
            MemoryPool::new(decoded_bytes + one_row + 2_048),
            root.path(),
        )
        .unwrap();
        let decoded_memory = context.memory.try_reserve(decoded_bytes).unwrap();
        let mut state_memory = context.memory.reservation();

        let (chunk, rows) =
            reserve_right_chunk(&left, &right, 0, &mut state_memory, &context, 256).unwrap();
        assert!(rows > 0 && rows < 256);
        assert_eq!(chunk.num_rows(), rows);
        assert!(context.memory.peak() <= context.memory.limit());

        drop(chunk);
        drop(state_memory);
        drop(decoded_memory);
        assert_eq!(context.memory.used(), 0);
    }
}
