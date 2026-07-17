use std::sync::Arc;

use arrow::{
    array::{ArrayRef, BooleanArray, UInt32Array, new_null_array},
    compute::take,
    datatypes::SchemaRef,
    record_batch::{RecordBatch, RecordBatchOptions},
};

use crate::{
    Error, Result,
    runtime::{BatchEnvelope, QueryContext},
    sql::{JoinType, field_is_materialized},
};

mod memory;
mod target;

#[cfg(test)]
use memory::selected_rows_bytes;
pub(super) use memory::{candidate_workspace_bytes, grow_workspace, output_workspace_bytes};
use memory::{outputs_left_only, unmatched_right_workspace_bytes};
pub(super) use target::BatchOutputTarget;
pub(in crate::execution) use target::{JoinEmission, JoinOutputTarget, JoinSelection};

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
    let estimate =
        output_workspace_bytes(left, right, left_indices, right_indices, join_type, &schema)?;
    let workspace = context
        .reserve_memory_while_holding(estimate, held_bytes, owner)
        .await?;
    let _permit = context.acquire_compute().await?;
    let _active = context.scheduler.enter_lane();
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

pub(super) async fn build_unmatched_right_envelope(
    left_schema: &SchemaRef,
    right: &RecordBatch,
    right_indices: &[u32],
    schema: SchemaRef,
    context: &QueryContext,
    held_bytes: usize,
) -> Result<BatchEnvelope> {
    let estimate = unmatched_right_workspace_bytes(left_schema, right, right_indices, &schema)?;
    let workspace = context
        .reserve_memory_while_holding(estimate, held_bytes, "join unmatched build output")
        .await?;
    let _permit = context.acquire_compute().await?;
    let _active = context.scheduler.enter_lane();
    let rows = right_indices.len();
    let indices = UInt32Array::from(right_indices.to_vec());
    let mut columns = Vec::with_capacity(left_schema.fields().len() + right.num_columns());
    for index in 0..left_schema.fields().len() {
        columns.push(new_null_array(schema.field(index).data_type(), rows));
    }
    for (index, column) in right.columns().iter().enumerate() {
        let output_index = left_schema.fields().len() + index;
        let field = schema.field(output_index);
        if field_is_materialized(field) {
            columns.push(take(column.as_ref(), &indices, None)?);
        } else {
            columns.push(new_null_array(field.data_type(), rows));
        }
    }
    let output = build_record_batch(schema, columns, rows)?;
    BatchEnvelope::from_reservation(output, workspace, "join unmatched build output")
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
    let identity_left = is_identity_selection(left_indices, left.num_rows());
    let left_take_indices = (!identity_left).then(|| UInt32Array::from(left_indices.to_vec()));
    let mut columns = Vec::with_capacity(left.num_columns() + right.num_columns());
    for (index, column) in left.columns().iter().enumerate() {
        let field = schema.field(index);
        if field_is_materialized(field) {
            if identity_left {
                columns.push(Arc::clone(column));
            } else {
                columns.push(take(
                    column.as_ref(),
                    left_take_indices.as_ref().expect("non-identity indices"),
                    None,
                )?);
            }
        } else {
            columns.push(new_null_array(field.data_type(), left_indices.len()));
        }
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
        let output_index = left.num_columns() + index;
        let field = schema.field(output_index);
        if right.num_rows() == 0 {
            columns.push(new_null_array(field.data_type(), left_indices.len()));
        } else if field_is_materialized(field) {
            columns.push(take(column.as_ref(), &right_indices, None)?);
        } else {
            columns.push(new_null_array(field.data_type(), left_indices.len()));
        }
    }
    build_record_batch(schema, columns, left_indices.len())
}

fn is_identity_selection(indices: &[u32], rows: usize) -> bool {
    indices.len() == rows
        && indices
            .iter()
            .enumerate()
            .all(|(position, index)| usize::try_from(*index) == Ok(position))
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

#[cfg(test)]
#[path = "output/tests.rs"]
mod tests;
