use std::{mem::size_of, sync::Arc};

use arrow::{
    array::{ArrayRef, BooleanArray, UInt32Array, new_null_array},
    compute::take,
    datatypes::SchemaRef,
    record_batch::{RecordBatch, RecordBatchOptions},
};

use crate::{
    Error, Result,
    runtime::{BatchEnvelope, MemoryReservation, QueryContext},
    sql::JoinType,
};

pub(super) fn candidate_workspace_bytes(
    left: &RecordBatch,
    right: &RecordBatch,
    left_indices: &[u32],
    right_indices: &[u32],
) -> Result<usize> {
    let left_bytes = selected_rows_bytes(left, left_indices.iter().copied().map(Some))?;
    let right_bytes = selected_rows_bytes(right, right_indices.iter().copied().map(Some))?;
    Ok(left_bytes
        .saturating_add(right_bytes)
        .saturating_mul(3)
        .saturating_add(
            left_indices
                .len()
                .saturating_mul(size_of::<u32>().saturating_mul(4)),
        )
        .saturating_add(
            left.num_columns()
                .saturating_add(right.num_columns())
                .saturating_mul(512),
        )
        .saturating_add(1_024)
        .max(1))
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn build_output_envelope(
    left: &RecordBatch,
    right: &RecordBatch,
    left_indices: &[u32],
    right_indices: &[Option<u32>],
    markers: Option<&[Option<bool>]>,
    join_type: JoinType,
    schema: SchemaRef,
    context: &QueryContext,
    held_bytes: usize,
    owner: &'static str,
) -> Result<BatchEnvelope> {
    let estimate = output_workspace_bytes(left, right, left_indices, right_indices, join_type)?;
    let workspace = context
        .reserve_memory_while_holding(estimate, held_bytes, owner)
        .await?;
    let output = build_output(
        left,
        right,
        left_indices,
        right_indices,
        markers,
        join_type,
        schema,
    )?;
    BatchEnvelope::from_reservation(output, workspace, owner)
}

pub(super) async fn grow_workspace(
    workspace: &mut MemoryReservation,
    required: usize,
    context: &QueryContext,
    held_bytes: usize,
) -> Result<()> {
    let additional = required.saturating_sub(workspace.size());
    if additional != 0 {
        let more = context
            .reserve_memory_while_holding(
                additional,
                held_bytes.saturating_add(workspace.size()),
                "join output workspace",
            )
            .await?;
        workspace.absorb(more)?;
    }
    Ok(())
}

pub(super) fn output_workspace_bytes(
    left: &RecordBatch,
    right: &RecordBatch,
    left_indices: &[u32],
    right_indices: &[Option<u32>],
    join_type: JoinType,
) -> Result<usize> {
    let left_bytes = selected_rows_bytes(left, left_indices.iter().copied().map(Some))?;
    let right_bytes = if outputs_left_only(join_type) || join_type == JoinType::Mark {
        0
    } else {
        selected_rows_bytes(right, right_indices.iter().copied())?
    };
    let rows = left_indices.len();
    let columns = left.num_columns().saturating_add(
        if outputs_left_only(join_type) || join_type == JoinType::Mark {
            0
        } else {
            right.num_columns()
        },
    );
    let indices = rows.saturating_mul(size_of::<u32>().saturating_add(size_of::<Option<u32>>()));
    Ok(left_bytes
        .saturating_add(right_bytes)
        .saturating_mul(2)
        .saturating_add(indices.saturating_mul(2))
        .saturating_add(columns.saturating_mul(512))
        .saturating_add(1_024)
        .max(1))
}

fn selected_rows_bytes<I>(batch: &RecordBatch, rows: I) -> Result<usize>
where
    I: IntoIterator<Item = Option<u32>>,
{
    rows.into_iter().try_fold(0usize, |total, row| {
        let row_bytes = match row {
            Some(row) => batch.columns().iter().try_fold(0usize, |bytes, column| {
                let data = column.to_data().slice(row as usize, 1);
                Ok::<_, arrow::error::ArrowError>(
                    bytes.saturating_add(data.get_slice_memory_size()?),
                )
            })?,
            None => batch.num_columns().saturating_mul(64),
        };
        Ok(total.saturating_add(row_bytes))
    })
}

pub(super) fn build_output(
    left: &RecordBatch,
    right: &RecordBatch,
    left_indices: &[u32],
    right_indices: &[Option<u32>],
    markers: Option<&[Option<bool>]>,
    join_type: JoinType,
    schema: SchemaRef,
) -> Result<RecordBatch> {
    let left_indices = UInt32Array::from(left_indices.to_vec());
    let mut columns = Vec::with_capacity(left.num_columns() + right.num_columns());
    for column in left.columns() {
        columns.push(take(column.as_ref(), &left_indices, None)?);
    }
    if outputs_left_only(join_type) {
        return build_record_batch(schema, columns, left_indices.len());
    }
    if join_type == JoinType::Mark {
        let markers = markers.ok_or_else(|| {
            Error::Internal("Mark join output is missing its boolean marker".into())
        })?;
        if markers.len() != left_indices.len() {
            return Err(Error::Internal(
                "Mark join marker count does not match output rows".into(),
            ));
        }
        columns.push(Arc::new(BooleanArray::from(markers.to_vec())));
        return build_record_batch(schema, columns, left_indices.len());
    }
    let right_indices = UInt32Array::from(right_indices.to_vec());
    for (index, column) in right.columns().iter().enumerate() {
        if right.num_rows() == 0 {
            columns.push(new_null_array(
                schema.field(left.num_columns() + index).data_type(),
                left_indices.len(),
            ));
        } else {
            columns.push(take(column.as_ref(), &right_indices, None)?);
        }
    }
    build_record_batch(schema, columns, left_indices.len())
}

fn outputs_left_only(join_type: JoinType) -> bool {
    matches!(
        join_type,
        JoinType::Semi | JoinType::Anti | JoinType::NullAwareAnti
    )
}

fn build_record_batch(
    schema: SchemaRef,
    columns: Vec<ArrayRef>,
    rows: usize,
) -> Result<RecordBatch> {
    if columns.is_empty() {
        let options = RecordBatchOptions::new().with_row_count(Some(rows));
        Ok(RecordBatch::try_new_with_options(
            schema, columns, &options,
        )?)
    } else {
        Ok(RecordBatch::try_new(schema, columns)?)
    }
}
