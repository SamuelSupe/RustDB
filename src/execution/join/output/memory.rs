use std::mem::size_of;

use arrow::{
    array::ArrayRef,
    datatypes::{DataType, SchemaRef},
    record_batch::RecordBatch,
};

use crate::{
    Result,
    runtime::{MemoryReservation, QueryContext, estimate_array_bytes},
    sql::{JoinType, field_is_materialized},
};

pub(in crate::execution::join) fn candidate_workspace_bytes(
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

pub(in crate::execution::join) async fn grow_workspace(
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

pub(in crate::execution::join) fn output_workspace_bytes(
    left: &RecordBatch,
    right: &RecordBatch,
    left_indices: &[u32],
    right_indices: &[Option<u32>],
    join_type: JoinType,
    schema: &SchemaRef,
) -> Result<usize> {
    let left_bytes = output_side_bytes(
        left,
        schema,
        0,
        || left_indices.iter().copied().map(Some),
        false,
    )?;
    let right_bytes = if outputs_left_only(join_type) || join_type == JoinType::Mark {
        0
    } else {
        output_side_bytes(
            right,
            schema,
            left.num_columns(),
            || right_indices.iter().copied(),
            right.num_rows() == 0,
        )?
    };
    let rows = left_indices.len();
    let marker_bytes = if join_type == JoinType::Mark {
        estimate_array_bytes(&DataType::Boolean, rows)
    } else {
        0
    };
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
        .saturating_add(marker_bytes)
        .saturating_mul(2)
        .saturating_add(indices.saturating_mul(2))
        .saturating_add(columns.saturating_mul(512))
        .saturating_add(1_024)
        .max(1))
}

pub(super) fn unmatched_right_workspace_bytes(
    left_schema: &SchemaRef,
    right: &RecordBatch,
    right_indices: &[u32],
    schema: &SchemaRef,
) -> Result<usize> {
    let rows = right_indices.len();
    let left_bytes = (0..left_schema.fields().len()).fold(0usize, |bytes, index| {
        bytes.saturating_add(estimate_array_bytes(schema.field(index).data_type(), rows))
    });
    let right_bytes = output_side_bytes(
        right,
        schema,
        left_schema.fields().len(),
        || right_indices.iter().copied().map(Some),
        false,
    )?;
    let columns = left_schema
        .fields()
        .len()
        .saturating_add(right.num_columns());
    Ok(left_bytes
        .saturating_add(right_bytes)
        .saturating_mul(2)
        .saturating_add(rows.saturating_mul(size_of::<u32>()).saturating_mul(2))
        .saturating_add(columns.saturating_mul(512))
        .saturating_add(1_024)
        .max(1))
}

fn output_side_bytes<I, F>(
    batch: &RecordBatch,
    schema: &SchemaRef,
    field_offset: usize,
    rows: F,
    force_null: bool,
) -> Result<usize>
where
    I: ExactSizeIterator<Item = Option<u32>>,
    F: Fn() -> I,
{
    let row_count = rows().len();
    batch
        .columns()
        .iter()
        .enumerate()
        .try_fold(0usize, |bytes, (index, column)| {
            let field = schema.field(field_offset + index);
            let column_bytes = if force_null || !field_is_materialized(field) {
                estimate_array_bytes(field.data_type(), row_count)
            } else {
                selected_column_bytes(column, field.data_type(), rows())?
            };
            Ok(bytes.saturating_add(column_bytes))
        })
}

fn selected_column_bytes<I>(column: &ArrayRef, data_type: &DataType, mut rows: I) -> Result<usize>
where
    I: ExactSizeIterator<Item = Option<u32>>,
{
    let row_count = rows.len();
    let baseline = estimate_array_bytes(data_type, row_count);
    if is_fixed_width(data_type) {
        return Ok(baseline);
    }
    let data = column.to_data();
    let selected = rows.try_fold(0usize, |bytes, row| {
        let Some(row) = row else { return Ok(bytes) };
        let slice = data.slice(row as usize, 1);
        Ok::<_, arrow::error::ArrowError>(bytes.saturating_add(slice.get_slice_memory_size()?))
    })?;
    Ok(baseline.max(selected))
}

pub(super) fn selected_rows_bytes<I>(batch: &RecordBatch, rows: I) -> Result<usize>
where
    I: IntoIterator<Item = Option<u32>>,
    I::IntoIter: ExactSizeIterator,
{
    let mut rows = rows.into_iter();
    if batch
        .columns()
        .iter()
        .all(|column| is_fixed_width(column.data_type()))
    {
        return Ok(batch.columns().iter().fold(0usize, |bytes, column| {
            bytes.saturating_add(estimate_array_bytes(column.data_type(), rows.len()))
        }));
    }
    rows.try_fold(0usize, |total, row| {
        let row_bytes = match row {
            Some(row) => batch.columns().iter().try_fold(0usize, |bytes, column| {
                let data = column.to_data().slice(row as usize, 1);
                Ok::<_, arrow::error::ArrowError>(
                    bytes.saturating_add(data.get_slice_memory_size()?),
                )
            })?,
            None => batch.columns().iter().fold(0usize, |bytes, column| {
                bytes.saturating_add(estimate_array_bytes(column.data_type(), 1))
            }),
        };
        Ok(total.saturating_add(row_bytes))
    })
}

fn is_fixed_width(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Null
            | DataType::Boolean
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Float16
            | DataType::Float32
            | DataType::Float64
            | DataType::Date32
            | DataType::Date64
            | DataType::Time32(_)
            | DataType::Time64(_)
            | DataType::Timestamp(_, _)
            | DataType::Duration(_)
            | DataType::Interval(_)
            | DataType::Decimal128(_, _)
            | DataType::Decimal256(_, _)
            | DataType::FixedSizeBinary(_)
    )
}

pub(super) fn outputs_left_only(join_type: JoinType) -> bool {
    matches!(
        join_type,
        JoinType::Semi | JoinType::Anti | JoinType::NullAwareAnti
    )
}
