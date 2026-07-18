use std::{fs::File, sync::Arc};

use arrow::{
    array::{Array, BooleanArray, BooleanBuilder},
    compute::filter_record_batch,
    record_batch::RecordBatch,
};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use super::ReturningProjection;
use crate::{
    Error, Result,
    runtime::{BatchEnvelope, MemoryPool, QueryControl},
    sql::BoundExpr,
    storage::{NativeDeleteSegment, NativeDeleteVector, NativeTableWriter, PreparedSnapshot},
};

#[allow(clippy::too_many_arguments)]
pub(super) fn apply(
    root: &std::path::Path,
    snapshot: &crate::storage::NativeTableSnapshot,
    predicate: &BoundExpr,
    assignments: &[(usize, BoundExpr)],
    matches: Option<&super::super::native_matches::UpdateMatches>,
    returning: Option<&ReturningProjection>,
    memory: &MemoryPool,
    batch_size: usize,
    control: &QueryControl,
    mut writer: NativeTableWriter,
) -> Result<(Option<PreparedSnapshot>, Vec<BatchEnvelope>)> {
    let mut returned = Vec::new();
    let result = (|| {
        let indices = (0..snapshot.segment_paths(root).len()).collect::<Vec<_>>();
        snapshot.verify_segment_indices(root, &indices, || control.check_cancelled())?;
        let mut changed = false;
        for segment in snapshot.delete_segments(root)? {
            control.check_cancelled()?;
            changed |= update_segment(
                &segment,
                snapshot.schema(),
                predicate,
                assignments,
                matches,
                returning,
                memory,
                batch_size,
                control,
                &mut writer,
                &mut returned,
            )?;
        }
        control.check_cancelled()?;
        Ok(changed)
    })();
    match result {
        Ok(true) => writer.finish().map(|prepared| (Some(prepared), returned)),
        Ok(false) => {
            writer.abort()?;
            Ok((None, returned))
        }
        Err(error) => match writer.abort() {
            Ok(()) => Err(error),
            Err(cleanup) => Err(Error::native_storage(
                root,
                format!("{error}; native UPDATE cleanup failed: {cleanup}"),
            )),
        },
    }
}

#[allow(clippy::too_many_arguments)]
fn update_segment(
    segment: &NativeDeleteSegment,
    schema: arrow::datatypes::SchemaRef,
    predicate: &BoundExpr,
    assignments: &[(usize, BoundExpr)],
    matches: Option<&super::super::native_matches::UpdateMatches>,
    returning: Option<&ReturningProjection>,
    memory: &MemoryPool,
    batch_size: usize,
    control: &QueryControl,
    writer: &mut NativeTableWriter,
    returned: &mut Vec<BatchEnvelope>,
) -> Result<bool> {
    let mut vector = segment
        .load_delete_vector()?
        .unwrap_or(NativeDeleteVector::empty(segment.rows())?);
    if vector.deleted_rows() == segment.rows() {
        return Ok(false);
    }
    let previous = vector.deleted_rows();
    let file = File::open(segment.path())
        .map_err(|error| Error::io(Some(segment.path().to_path_buf()), error))?;
    let mut reader = ParquetRecordBatchReaderBuilder::try_new(file)?
        .with_batch_size(batch_size)
        .build()?;
    let mut offset = 0_u64;
    for batch in &mut reader {
        control.check_cancelled()?;
        let batch = crate::storage::decode_native_segment_batch(&batch?, &schema)?;
        let (mask, matched_update) = match matches {
            Some(matches) => {
                let (mask, updated) =
                    matches.select(&batch, Arc::clone(&schema), &vector, offset)?;
                (mask, Some(updated))
            }
            None => (
                visible_update_mask(predicate, &batch, &vector, offset)?,
                None,
            ),
        };
        if mask.true_count() != 0 {
            let updated = match matched_update {
                Some(updated) => updated,
                None => {
                    let selected = filter_record_batch(&batch, &mask)?;
                    apply_assignments(&selected, Arc::clone(&schema), assignments)?
                }
            };
            if let Some(returning) = returning {
                let batch = crate::execution::project_expressions(
                    &returning.expressions,
                    Arc::clone(&returning.schema),
                    &updated,
                )?;
                returned.push(BatchEnvelope::try_new(batch, memory, "UPDATE RETURNING")?);
            }
            writer.write_batch(&updated)?;
            mark_selected(&mut vector, &mask, offset)?;
        }
        offset = add_rows(offset, batch.num_rows())?;
    }
    if offset != segment.rows() {
        return Err(Error::native_storage(
            segment.path(),
            format!(
                "native segment decoded {offset} rows, expected {}",
                segment.rows()
            ),
        ));
    }
    if vector.deleted_rows() == previous {
        return Ok(false);
    }
    writer.write_delete_vector(segment.segment_id(), &vector)?;
    Ok(true)
}

fn visible_update_mask(
    predicate: &BoundExpr,
    batch: &RecordBatch,
    vector: &NativeDeleteVector,
    offset: u64,
) -> Result<BooleanArray> {
    let selected = crate::execution::evaluate_expression(predicate, batch)?;
    let selected = selected
        .as_any()
        .downcast_ref::<BooleanArray>()
        .ok_or_else(|| Error::Internal("UPDATE predicate did not produce Boolean".to_owned()))?;
    let mut mask = BooleanBuilder::with_capacity(batch.num_rows());
    for row in 0..batch.num_rows() {
        let physical = add_rows(offset, row)?;
        mask.append_value(
            selected.is_valid(row) && selected.value(row) && !vector.contains(physical),
        );
    }
    Ok(mask.finish())
}

fn mark_selected(vector: &mut NativeDeleteVector, mask: &BooleanArray, offset: u64) -> Result<()> {
    for row in 0..mask.len() {
        if mask.value(row) {
            vector.mark_deleted(add_rows(offset, row)?)?;
        }
    }
    Ok(())
}

fn add_rows(offset: u64, rows: usize) -> Result<u64> {
    offset
        .checked_add(u64::try_from(rows).map_err(|_| {
            Error::ResourceExhausted("native row offset does not fit in u64".to_owned())
        })?)
        .ok_or_else(|| Error::ResourceExhausted("native row offset overflow".to_owned()))
}

fn apply_assignments(
    batch: &RecordBatch,
    schema: arrow::datatypes::SchemaRef,
    assignments: &[(usize, BoundExpr)],
) -> Result<RecordBatch> {
    let mut columns = batch.columns().to_vec();
    for (index, expression) in assignments {
        columns[*index] = crate::execution::evaluate_expression(expression, batch)?;
    }
    Ok(RecordBatch::try_new(schema, columns)?)
}
