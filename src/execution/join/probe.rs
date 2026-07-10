use std::{collections::HashMap, sync::Arc};

use arrow::{
    array::{ArrayRef, UInt32Array, new_null_array},
    compute::take,
    datatypes::SchemaRef,
    record_batch::{RecordBatch, RecordBatchOptions},
};

use crate::{Error, Result, runtime::QueryContext, sql::JoinType};

use super::{CellValue, row_key};

pub(super) fn build_hash_table(
    key_arrays: &[ArrayRef],
    rows: usize,
    deduplicate: bool,
) -> Result<HashMap<Vec<CellValue>, Vec<u32>>> {
    let mut hash_table: HashMap<Vec<CellValue>, Vec<u32>> = HashMap::new();
    for row in 0..rows {
        let key = row_key(key_arrays, row)?;
        if key.iter().any(CellValue::is_null) {
            continue;
        }
        let rows = hash_table.entry(key).or_default();
        if deduplicate && !rows.is_empty() {
            continue;
        }
        rows.push(u32::try_from(row).map_err(|_| {
            Error::ResourceExhausted("hash join build side exceeds UINT32_MAX rows".into())
        })?);
    }
    Ok(hash_table)
}

pub(super) struct ProbeCursor<'a> {
    left: &'a RecordBatch,
    right: &'a RecordBatch,
    left_keys: &'a [ArrayRef],
    hash_table: &'a HashMap<Vec<CellValue>, Vec<u32>>,
    join_type: JoinType,
    schema: SchemaRef,
    batch_size: usize,
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
    ) -> Self {
        Self {
            left,
            right,
            left_keys,
            hash_table,
            join_type,
            schema,
            batch_size: batch_size.max(1),
            row: 0,
            match_index: 0,
        }
    }

    pub(super) fn next_batch(&mut self, context: &QueryContext) -> Result<Option<RecordBatch>> {
        context.check_cancelled()?;
        let mut left_indices = Vec::with_capacity(self.batch_size);
        let mut right_indices = Vec::with_capacity(self.batch_size);
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
                        Error::ResourceExhausted("join probe batch exceeds UINT32_MAX rows".into())
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
                        Error::ResourceExhausted("join probe batch exceeds UINT32_MAX rows".into())
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

        if left_indices.is_empty() {
            Ok(None)
        } else {
            Ok(Some(build_output(
                self.left,
                self.right,
                &left_indices,
                &right_indices,
                self.join_type,
                Arc::clone(&self.schema),
            )?))
        }
    }
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
