use std::mem::size_of;

use crate::{Error, Result, runtime::MemoryReservation};
use arrow::{
    array::{Array, ArrayRef},
    datatypes::DataType,
    row::{RowConverter, Rows, SortField},
};

use super::fixed::{DuplicateRows, FixedEntry};

mod arena;
mod memory;
mod multiplicity;

use arena::CompositeEntries;
pub(in crate::execution::join) use multiplicity::CompositeMultiplicityTable;

const BUILD_ENCODE_ROWS: usize = 65_536;

pub(in crate::execution::join) struct CompositeHashTable {
    converter: RowConverter,
    logical_types: Box<[DataType]>,
    _metadata_bytes: usize,
    values: CompositeEntries,
    duplicates: DuplicateRows,
}

pub(in crate::execution::join) struct CompositeProbeRows {
    rows: Rows,
    memory: MemoryReservation,
    null_free: bool,
}

impl CompositeProbeRows {
    pub(in crate::execution::join) fn memory_size(&self) -> usize {
        self.memory.size()
    }
}

impl CompositeHashTable {
    pub(in crate::execution::join) fn probe_workspace_bytes(
        &self,
        arrays: &[ArrayRef],
    ) -> Result<usize> {
        self.validate_arrays(arrays)?;
        memory::encode_peak_bytes(arrays)
    }

    pub(in crate::execution::join) fn encode_probe(
        &self,
        arrays: &[ArrayRef],
        mut workspace: MemoryReservation,
    ) -> Result<CompositeProbeRows> {
        self.validate_arrays(arrays)?;
        let rows = self.converter.convert_columns(arrays)?;
        let actual = rows.size().max(1);
        if let Err(error) = workspace.try_resize(actual) {
            drop(rows);
            return Err(Error::ResourceExhausted(format!(
                "composite Join probe retained {actual} row-encoding bytes: {error}"
            )));
        }
        Ok(CompositeProbeRows {
            rows,
            memory: workspace,
            null_free: arrays.iter().all(|array| array.null_count() == 0),
        })
    }

    pub(in crate::execution::join) fn lookup<'a>(
        &'a self,
        probe: &CompositeProbeRows,
        arrays: &[ArrayRef],
        row: usize,
        null_equal_keys: bool,
    ) -> Result<Option<&'a [u32]>> {
        if row >= probe.rows.num_rows() {
            return Err(Error::Internal(
                "composite Join probe row is out of bounds".into(),
            ));
        }
        if !null_equal_keys && !probe.null_free && arrays.iter().any(|array| array.is_null(row)) {
            return Ok(None);
        }
        Ok(self
            .values
            .get(probe.rows.row(row).data())
            .map(|entry| entry.rows(&self.duplicates)))
    }

    #[cfg(test)]
    pub(super) fn capacity(&self) -> usize {
        self.values.capacity()
    }

    #[cfg(test)]
    pub(super) fn allocated_bytes(&self) -> usize {
        self._metadata_bytes
            .saturating_add(self.values.allocated_bytes())
            .saturating_add(self.duplicates.allocated_bytes())
    }

    #[cfg(test)]
    pub(super) fn distinct_keys(&self) -> usize {
        self.values.len()
    }

    fn validate_arrays(&self, arrays: &[ArrayRef]) -> Result<()> {
        if arrays.len() != self.logical_types.len() {
            return Err(Error::Internal(format!(
                "composite Join expected {} key arrays, received {}",
                self.logical_types.len(),
                arrays.len()
            )));
        }
        for (expected, array) in self.logical_types.iter().zip(arrays) {
            if expected != array.data_type() {
                return Err(Error::Internal(format!(
                    "composite Join expected key type {expected}, found {}",
                    array.data_type()
                )));
            }
        }
        let rows = arrays.first().map_or(0, |array| array.len());
        if arrays.iter().any(|array| array.len() != rows) {
            return Err(Error::Internal(
                "composite Join key arrays have different row counts".into(),
            ));
        }
        Ok(())
    }
}

pub(super) fn eligible(arrays: &[ArrayRef], rows: usize) -> bool {
    arrays.len() >= 2
        && arrays
            .iter()
            .all(|array| array.len() == rows && supports_type(array.data_type()))
}

pub(super) fn build(
    arrays: &[ArrayRef],
    rows: usize,
    deduplicate: bool,
    null_equal_keys: bool,
    reservation: &mut MemoryReservation,
) -> Result<Option<CompositeHashTable>> {
    if !eligible(arrays, rows) {
        return Ok(None);
    }
    let initial = reservation.size();
    let logical_types = arrays
        .iter()
        .map(|array| array.data_type().clone())
        .collect::<Vec<_>>();
    let metadata_estimate = memory::converter_upper_bound(&logical_types)
        .saturating_add(size_of::<CompositeHashTable>());
    if reservation.try_grow(metadata_estimate).is_err() {
        return Ok(None);
    }
    let converter =
        match RowConverter::new(logical_types.iter().cloned().map(SortField::new).collect()) {
            Ok(converter) => converter,
            Err(error) => {
                reservation.try_resize(initial)?;
                return Err(error.into());
            }
        };
    let logical_types = logical_types.into_boxed_slice();
    let mut table = CompositeHashTable {
        converter,
        logical_types,
        _metadata_bytes: metadata_estimate,
        values: CompositeEntries::new(),
        duplicates: DuplicateRows::default(),
    };
    // Most analytical join keys are non-null. Avoid walking every key array
    // for every row when the batch has no NULLs; batches containing NULLs
    // retain the exact per-row SQL null semantics below.
    let check_nulls = !null_equal_keys && arrays.iter().any(|array| array.null_count() != 0);
    let mut offset = 0usize;
    while offset < rows {
        let length = BUILD_ENCODE_ROWS.min(rows - offset);
        let chunk = arrays
            .iter()
            .map(|array| array.slice(offset, length))
            .collect::<Vec<_>>();
        let workspace_estimate = match memory::encode_peak_bytes(&chunk) {
            Ok(bytes) => bytes,
            Err(error) => {
                drop(chunk);
                drop(table);
                reservation.try_resize(initial)?;
                return Err(error);
            }
        };
        if reservation.try_grow(workspace_estimate).is_err() {
            return rollback(table, reservation, initial);
        }
        let encoded = match table.converter.convert_columns(&chunk) {
            Ok(encoded) => encoded,
            Err(error) => {
                drop(chunk);
                drop(table);
                reservation.try_resize(initial)?;
                return Err(error.into());
            }
        };
        let encoded_bytes = encoded.size().max(1);
        if !reconcile_growth(reservation, workspace_estimate, encoded_bytes) {
            drop(encoded);
            drop(chunk);
            return rollback(table, reservation, initial);
        }

        for local_row in 0..length {
            let row = offset + local_row;
            if check_nulls && arrays.iter().any(|array| array.is_null(row)) {
                continue;
            }
            let row_index = match u32::try_from(row) {
                Ok(row) if row != u32::MAX => row,
                _ => {
                    drop(encoded);
                    drop(chunk);
                    drop(table);
                    reservation.try_resize(initial)?;
                    return Err(Error::ResourceExhausted(
                        "hash join build side exceeds UINT32_MAX rows".into(),
                    ));
                }
            };
            if !push_key(
                &mut table,
                encoded.row(local_row).data(),
                row_index,
                deduplicate,
                reservation,
            ) {
                drop(encoded);
                drop(chunk);
                return rollback(table, reservation, initial);
            }
        }
        drop(encoded);
        drop(chunk);
        reservation.shrink(encoded_bytes);
        offset += length;
    }
    Ok(Some(table))
}

fn push_key(
    table: &mut CompositeHashTable,
    key: &[u8],
    row: u32,
    deduplicate: bool,
    reservation: &mut MemoryReservation,
) -> bool {
    let hash = table.values.hash(key);
    if let Some(entry) = table.values.get_mut(hash, key) {
        return push_duplicate(entry, &mut table.duplicates, row, deduplicate, reservation);
    }
    table.values.try_insert(hash, key, row, reservation)
}

fn push_duplicate(
    entry: &mut FixedEntry,
    duplicates: &mut DuplicateRows,
    row: u32,
    deduplicate: bool,
    reservation: &mut MemoryReservation,
) -> bool {
    if deduplicate {
        return true;
    }
    match entry.duplicate_id() {
        Some(id) => duplicates.try_push(id, row, reservation),
        None => match duplicates.try_promote(entry.first(), row, reservation) {
            Some(id) => {
                entry.set_duplicate_id(id);
                true
            }
            None => false,
        },
    }
}

fn reconcile_growth(reservation: &mut MemoryReservation, estimate: usize, actual: usize) -> bool {
    if actual > estimate && reservation.try_grow(actual - estimate).is_err() {
        return false;
    }
    reservation.shrink(estimate.saturating_sub(actual));
    true
}

fn rollback(
    table: CompositeHashTable,
    reservation: &mut MemoryReservation,
    initial: usize,
) -> Result<Option<CompositeHashTable>> {
    drop(table);
    reservation.try_resize(initial)?;
    Ok(None)
}

fn supports_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Boolean
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Binary
            | DataType::LargeBinary
            | DataType::Decimal128(_, _)
            | DataType::Date32
            | DataType::Date64
            | DataType::Time32(_)
            | DataType::Time64(_)
            | DataType::Timestamp(_, _)
            | DataType::Duration(_)
    )
}

#[cfg(test)]
#[path = "composite/tests.rs"]
mod tests;
