use std::{mem::size_of, sync::Arc};

use arrow::{
    array::{Array, Date32Array, Decimal128Array, Int8Array, Int16Array, Int32Array, Int64Array},
    datatypes::Schema,
    record_batch::RecordBatch,
};

use super::{EncodedPredicateBlock, PredicateSidecarBlock, PredicateSidecarFile, PredicateType};
use crate::runtime::{MemoryPool, MemoryReservation};

const ROW_GROUP_ROWS: usize = 128 * 1024;
const RETAINED_LIMIT_BYTES: usize = 64 * 1024 * 1024;
// Dictionary + ids + frame deltas can coexist while choosing an encoding.
// This deliberately overclaims the codec's row-sized temporary structures.
const ENCODE_WORKSPACE_BYTES_PER_ROW: usize = 96;

pub(in crate::storage::native::segment) struct PredicateSidecarCollector {
    columns: Vec<Column>,
    blocks: Vec<PredicateSidecarBlock>,
    row_group_rows: Vec<u32>,
    rows_in_group: usize,
    fixed_buffer_bytes: usize,
    retained_block_bytes: usize,
    enabled: bool,
    reservation: Option<MemoryReservation>,
}

struct Column {
    ordinal: u32,
    data_type: PredicateType,
    values: Vec<Option<i64>>,
}

pub(in crate::storage::native) struct PredicateSidecarArtifact {
    pub(in crate::storage::native) bytes: Vec<u8>,
    pub(in crate::storage::native) row_group_count: u64,
    pub(in crate::storage::native) indexed_column_ordinals: Vec<u32>,
    pub(in crate::storage::native) _reservation: Option<MemoryReservation>,
}

impl PredicateSidecarCollector {
    pub(in crate::storage::native::segment) fn new(
        schema: &Arc<Schema>,
        memory: Option<&MemoryPool>,
    ) -> Self {
        let bytes_per_column = ROW_GROUP_ROWS
            .saturating_mul(size_of::<Option<i64>>())
            .saturating_add(size_of::<Column>());
        let max_columns = RETAINED_LIMIT_BYTES / bytes_per_column;
        let column_specs = schema
            .fields()
            .iter()
            .enumerate()
            .filter_map(|(ordinal, field)| {
                Some((
                    u32::try_from(ordinal).ok()?,
                    PredicateType::from_arrow(field.data_type()).ok()?,
                ))
            })
            .take(max_columns)
            .collect::<Vec<_>>();
        let fixed_buffer_bytes = column_specs.len().saturating_mul(bytes_per_column);
        let reservation = memory.and_then(|pool| pool.try_reserve(fixed_buffer_bytes).ok());
        let enabled = !column_specs.is_empty()
            && fixed_buffer_bytes <= RETAINED_LIMIT_BYTES
            && (memory.is_none() || reservation.is_some());
        let columns = if enabled {
            column_specs
                .into_iter()
                .map(|(ordinal, data_type)| Column {
                    ordinal,
                    data_type,
                    values: Vec::with_capacity(ROW_GROUP_ROWS),
                })
                .collect()
        } else {
            Vec::new()
        };
        Self {
            columns,
            blocks: Vec::new(),
            row_group_rows: Vec::new(),
            rows_in_group: 0,
            fixed_buffer_bytes,
            retained_block_bytes: 0,
            enabled,
            reservation,
        }
    }

    pub(in crate::storage::native::segment) fn write_batch(&mut self, batch: &RecordBatch) {
        if !self.enabled || batch.num_rows() == 0 {
            return;
        }
        let mut offset = 0;
        while offset < batch.num_rows() {
            let take = (ROW_GROUP_ROWS - self.rows_in_group).min(batch.num_rows() - offset);
            for column in &mut self.columns {
                let array = batch.column(column.ordinal as usize);
                if append_values(
                    &mut column.values,
                    column.data_type,
                    array.as_ref(),
                    offset,
                    take,
                )
                .is_err()
                {
                    self.disable();
                    return;
                }
            }
            self.rows_in_group += take;
            offset += take;
            if self.rows_in_group == ROW_GROUP_ROWS && !self.flush_row_group() {
                return;
            }
        }
    }

    pub(in crate::storage::native::segment) fn finish(
        mut self,
        schema_fingerprint: &str,
        segment_sha256: &str,
        segment_rows: u64,
    ) -> Option<PredicateSidecarArtifact> {
        if !self.enabled {
            return None;
        }
        if self.rows_in_group != 0 && !self.flush_row_group() {
            return None;
        }
        if self.blocks.is_empty()
            || self
                .row_group_rows
                .iter()
                .map(|rows| u64::from(*rows))
                .sum::<u64>()
                != segment_rows
        {
            return None;
        }
        let file = PredicateSidecarFile::new(
            schema_fingerprint,
            segment_sha256,
            segment_rows,
            self.row_group_rows,
            self.blocks,
        )
        .ok()?;
        let indexed_column_ordinals = file.indexed_column_ordinals().to_vec();
        let row_group_count = u64::try_from(file.row_group_rows().len()).ok()?;
        let encoded_len = file.encoded_len().ok()?;
        if let Some(reservation) = &mut self.reservation
            && reservation.try_grow(encoded_len).is_err()
        {
            return None;
        }
        let bytes = file.to_bytes().ok()?;
        drop(file);
        self.columns.clear();
        self.columns.shrink_to_fit();
        if let Some(reservation) = &mut self.reservation
            && reservation.try_resize(bytes.len()).is_err()
        {
            return None;
        }
        Some(PredicateSidecarArtifact {
            bytes,
            row_group_count,
            indexed_column_ordinals,
            _reservation: self.reservation.take(),
        })
    }

    fn flush_row_group(&mut self) -> bool {
        let row_count = match u32::try_from(self.rows_in_group) {
            Ok(rows) if rows != 0 => rows,
            _ => {
                self.disable();
                return false;
            }
        };
        let row_group = match u32::try_from(self.row_group_rows.len()) {
            Ok(row_group) => row_group,
            Err(_) => {
                self.disable();
                return false;
            }
        };
        for column in &mut self.columns {
            let workspace = self
                .rows_in_group
                .saturating_mul(ENCODE_WORKSPACE_BYTES_PER_ROW);
            if let Some(reservation) = &mut self.reservation
                && reservation.try_grow(workspace).is_err()
            {
                self.disable();
                return false;
            }
            let encoded = match EncodedPredicateBlock::encode(column.data_type, &column.values) {
                Ok(encoded) => encoded,
                Err(_) => {
                    if let Some(reservation) = &mut self.reservation {
                        reservation.shrink(workspace);
                    }
                    self.disable();
                    return false;
                }
            };
            if let Some(block) = encoded {
                let block = match PredicateSidecarBlock::new(row_group, column.ordinal, block) {
                    Ok(block) => block,
                    Err(_) => {
                        if let Some(reservation) = &mut self.reservation {
                            reservation.shrink(workspace);
                        }
                        self.disable();
                        return false;
                    }
                };
                let block_bytes = block.retained_bytes();
                let next = self.retained_block_bytes.saturating_add(block_bytes);
                if block_bytes > workspace
                    || self.fixed_buffer_bytes.saturating_add(next) > RETAINED_LIMIT_BYTES
                {
                    if let Some(reservation) = &mut self.reservation {
                        reservation.shrink(workspace);
                    }
                    self.disable();
                    return false;
                }
                if let Some(reservation) = &mut self.reservation {
                    reservation.shrink(workspace - block_bytes);
                }
                self.retained_block_bytes = next;
                self.blocks.push(block);
            } else if let Some(reservation) = &mut self.reservation {
                reservation.shrink(workspace);
            }
            column.values.clear();
        }
        self.row_group_rows.push(row_count);
        self.rows_in_group = 0;
        true
    }

    fn disable(&mut self) {
        self.enabled = false;
        self.columns.clear();
        self.blocks.clear();
        self.row_group_rows.clear();
        self.rows_in_group = 0;
        self.fixed_buffer_bytes = 0;
        self.retained_block_bytes = 0;
        if let Some(reservation) = &mut self.reservation {
            reservation.shrink(reservation.size());
        }
    }
}

fn append_values(
    output: &mut Vec<Option<i64>>,
    data_type: PredicateType,
    array: &dyn Array,
    offset: usize,
    len: usize,
) -> std::result::Result<(), ()> {
    macro_rules! append {
        ($ty:ty, $convert:expr) => {{
            let values = array.as_any().downcast_ref::<$ty>().ok_or(())?;
            for row in offset..offset + len {
                output.push((!values.is_null(row)).then(|| $convert(values.value(row))));
            }
        }};
    }
    match data_type {
        PredicateType::Int8 => append!(Int8Array, i64::from),
        PredicateType::Int16 => append!(Int16Array, i64::from),
        PredicateType::Int32 => append!(Int32Array, i64::from),
        PredicateType::Int64 => append!(Int64Array, |value| value),
        PredicateType::Date32 => append!(Date32Array, i64::from),
        PredicateType::Decimal128 { .. } => {
            let values = array.as_any().downcast_ref::<Decimal128Array>().ok_or(())?;
            for row in offset..offset + len {
                let value = if values.is_null(row) {
                    None
                } else {
                    Some(i64::try_from(values.value(row)).map_err(|_| ())?)
                };
                output.push(value);
            }
        }
    }
    Ok(())
}
