use std::{collections::HashMap, mem::size_of, sync::Arc};

use arrow::{
    array::{ArrayRef, UInt32Array, new_null_array},
    compute::take,
    datatypes::SchemaRef,
    record_batch::{RecordBatch, RecordBatchOptions},
};

use crate::{
    Error, Result,
    runtime::{BatchEnvelope, MemoryReservation, QueryContext},
    sql::JoinType,
};

use super::{CellValue, row_key};

pub(super) fn try_build_hash_table(
    key_arrays: &[ArrayRef],
    rows: usize,
    deduplicate: bool,
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
        if key.iter().any(CellValue::is_null) {
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
    join_type: JoinType,
    schema: SchemaRef,
    batch_size: usize,
    held_bytes: usize,
    row: usize,
    match_index: usize,
}

impl<'a> ProbeCursor<'a> {
    #[allow(clippy::too_many_arguments)]
    pub(super) fn new(
        left: &'a RecordBatch,
        right: &'a RecordBatch,
        left_keys: &'a [ArrayRef],
        hash_table: &'a HashMap<Vec<CellValue>, Vec<u32>>,
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
            join_type,
            schema,
            batch_size: batch_size.max(1),
            held_bytes,
            row: 0,
            match_index: 0,
        }
    }

    pub(super) async fn next_batch(
        &mut self,
        context: &QueryContext,
    ) -> Result<Option<BatchEnvelope>> {
        context.check_cancelled()?;
        let index_bytes = self
            .batch_size
            .saturating_mul(size_of::<u32>().saturating_add(size_of::<Option<u32>>()))
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
        {
            let _active = context.scheduler.enter_lane();
            while self.row < self.left.num_rows() && left_indices.len() < self.batch_size {
                let key = row_key(self.left_keys, self.row)?;
                let matches = if key.iter().any(CellValue::is_null) {
                    None
                } else {
                    self.hash_table.get(&key)
                };
                if matches!(self.join_type, JoinType::Semi | JoinType::Anti) {
                    let emit = match self.join_type {
                        JoinType::Semi => matches.is_some(),
                        JoinType::Anti => matches.is_none(),
                        _ => unreachable!("checked semi/anti join"),
                    };
                    if emit {
                        left_indices.push(u32::try_from(self.row).map_err(|_| {
                            Error::ResourceExhausted(
                                "join probe batch exceeds UINT32_MAX rows".into(),
                            )
                        })?);
                        right_indices.push(None);
                    }
                    self.row += 1;
                    self.match_index = 0;
                    continue;
                }
                if let Some(matches) = matches {
                    let left_row = u32::try_from(self.row).map_err(|_| {
                        Error::ResourceExhausted("join probe batch exceeds UINT32_MAX rows".into())
                    })?;
                    while self.match_index < matches.len() && left_indices.len() < self.batch_size {
                        let right_row = matches[self.match_index];
                        self.match_index += 1;
                        left_indices.push(left_row);
                        right_indices.push(Some(right_row));
                    }
                    if self.match_index == matches.len() {
                        self.row += 1;
                        self.match_index = 0;
                    }
                } else {
                    if self.join_type == JoinType::Left {
                        left_indices.push(u32::try_from(self.row).map_err(|_| {
                            Error::ResourceExhausted(
                                "join probe batch exceeds UINT32_MAX rows".into(),
                            )
                        })?);
                        right_indices.push(None);
                    }
                    self.row += 1;
                    self.match_index = 0;
                }

                if left_indices.len() % 1_024 == 0 {
                    context.check_cancelled()?;
                }
            }
        }

        if left_indices.is_empty() {
            Ok(None)
        } else {
            grow_output_workspace(
                &mut workspace,
                output_workspace_bytes(
                    self.left,
                    self.right,
                    &left_indices,
                    &right_indices,
                    self.join_type,
                )?,
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
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn build_output_envelope(
    left: &RecordBatch,
    right: &RecordBatch,
    left_indices: &[u32],
    right_indices: &[Option<u32>],
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
    let output = build_output(left, right, left_indices, right_indices, join_type, schema)?;
    BatchEnvelope::from_reservation(output, workspace, owner)
}

async fn grow_output_workspace(
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

fn output_workspace_bytes(
    left: &RecordBatch,
    right: &RecordBatch,
    left_indices: &[u32],
    right_indices: &[Option<u32>],
    join_type: JoinType,
) -> Result<usize> {
    let left_bytes = selected_rows_bytes(left, left_indices.iter().copied().map(Some))?;
    let right_bytes = if matches!(join_type, JoinType::Semi | JoinType::Anti) {
        0
    } else {
        selected_rows_bytes(right, right_indices.iter().copied())?
    };
    let rows = left_indices.len();
    let columns = left.num_columns().saturating_add(
        if matches!(join_type, JoinType::Semi | JoinType::Anti) {
            0
        } else {
            right.num_columns()
        },
    );
    let indices = rows.saturating_mul(size_of::<u32>().saturating_add(size_of::<Option<u32>>()));
    Ok(left_bytes
        .saturating_add(right_bytes)
        // Arrow take retains builder buffers beside the final output briefly.
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
    join_type: JoinType,
    schema: SchemaRef,
) -> Result<RecordBatch> {
    let left_indices = UInt32Array::from(left_indices.to_vec());
    let mut columns = Vec::with_capacity(left.num_columns() + right.num_columns());
    for column in left.columns() {
        columns.push(take(column.as_ref(), &left_indices, None)?);
    }
    if matches!(join_type, JoinType::Semi | JoinType::Anti) {
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
