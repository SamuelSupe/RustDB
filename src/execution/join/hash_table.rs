use std::collections::HashMap;

use arrow::{
    array::{Array, ArrayRef, Int64Array, UInt64Array},
    datatypes::DataType,
};

use crate::{Error, Result, runtime::MemoryReservation};

use super::{CellValue, row_key};

mod binary;
mod composite;
mod fixed;
mod probe;
mod utf8;

pub(super) use binary::BinaryKeys;
pub(in crate::execution::join) use composite::{
    CompositeHashTable, CompositeMultiplicityTable, CompositeProbeRows,
};
#[cfg(test)]
use fixed::FixedEntry;
use fixed::{FixedHashTable, FixedKeys};
pub(super) use probe::FixedProbe;
pub(super) use utf8::Utf8Keys;

pub(super) enum JoinHashTable {
    Int64(FixedHashTable<i64>),
    UInt64(FixedHashTable<u64>),
    Utf8(utf8::Utf8HashTable),
    Binary(binary::BinaryHashTable),
    Composite(Box<CompositeHashTable>),
    Generic(HashMap<Vec<CellValue>, Vec<u32>>),
}

#[derive(Clone)]
pub(super) enum FixedKeyIter<'a> {
    Int64(FixedKeys<'a, i64>),
    UInt64(FixedKeys<'a, u64>),
}

impl Iterator for FixedKeyIter<'_> {
    type Item = CellValue;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Int64(keys) => keys.next().map(CellValue::Int64),
            Self::UInt64(keys) => keys.next().map(CellValue::UInt64),
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let len = self.len();
        (len, Some(len))
    }
}

impl ExactSizeIterator for FixedKeyIter<'_> {
    fn len(&self) -> usize {
        match self {
            Self::Int64(keys) => keys.len(),
            Self::UInt64(keys) => keys.len(),
        }
    }
}

impl JoinHashTable {
    pub(super) fn generic(values: HashMap<Vec<CellValue>, Vec<u32>>) -> Self {
        Self::Generic(values)
    }

    #[cfg(test)]
    pub(super) fn capacity(&self) -> usize {
        match self {
            Self::Int64(table) => table.capacity(),
            Self::UInt64(table) => table.capacity(),
            Self::Utf8(table) => table.capacity(),
            Self::Binary(table) => table.capacity(),
            Self::Composite(table) => table.capacity(),
            Self::Generic(table) => table.capacity(),
        }
    }

    pub(super) fn generic_values(&self) -> Option<&HashMap<Vec<CellValue>, Vec<u32>>> {
        match self {
            Self::Generic(values) => Some(values),
            Self::Int64(_)
            | Self::UInt64(_)
            | Self::Utf8(_)
            | Self::Binary(_)
            | Self::Composite(_) => None,
        }
    }

    pub(super) fn utf8_keys(&self) -> Option<Utf8Keys<'_>> {
        match self {
            Self::Utf8(table) => Some(table.keys()),
            Self::Int64(_)
            | Self::UInt64(_)
            | Self::Binary(_)
            | Self::Composite(_)
            | Self::Generic(_) => None,
        }
    }

    pub(super) fn binary_keys(&self) -> Option<BinaryKeys<'_>> {
        match self {
            Self::Binary(table) => Some(table.keys()),
            Self::Int64(_)
            | Self::UInt64(_)
            | Self::Utf8(_)
            | Self::Composite(_)
            | Self::Generic(_) => None,
        }
    }

    pub(super) fn fixed_keys(&self) -> Option<FixedKeyIter<'_>> {
        match self {
            Self::Int64(table) => Some(FixedKeyIter::Int64(table.keys())),
            Self::UInt64(table) => Some(FixedKeyIter::UInt64(table.keys())),
            Self::Utf8(_) | Self::Binary(_) | Self::Composite(_) | Self::Generic(_) => None,
        }
    }

    /// Binds a fixed-width probe once per Arrow batch. Generic and multi-key
    /// tables return `None` and keep using the full `ProbeCursor` path.
    pub(super) fn fixed_probe<'a>(
        &'a self,
        key_arrays: &'a [ArrayRef],
    ) -> Result<Option<FixedProbe<'a>>> {
        probe::bind(self, key_arrays)
    }

    pub(super) fn composite(&self) -> Option<&CompositeHashTable> {
        match self {
            Self::Composite(table) => Some(table),
            Self::Int64(_)
            | Self::UInt64(_)
            | Self::Utf8(_)
            | Self::Binary(_)
            | Self::Generic(_) => None,
        }
    }

    pub(super) fn matches<'a>(
        &'a self,
        key_arrays: &[ArrayRef],
        row: usize,
        null_equal_keys: bool,
    ) -> Result<Option<&'a [u32]>> {
        match self {
            Self::Int64(table) => {
                let array = downcast::<Int64Array>(key_arrays, DataType::Int64)?;
                Ok(table.lookup(
                    (!array.is_null(row)).then(|| array.value(row)),
                    null_equal_keys,
                ))
            }
            Self::UInt64(table) => {
                let array = downcast::<UInt64Array>(key_arrays, DataType::UInt64)?;
                Ok(table.lookup(
                    (!array.is_null(row)).then(|| array.value(row)),
                    null_equal_keys,
                ))
            }
            Self::Utf8(table) => {
                let [array] = key_arrays else {
                    return Err(Error::Internal(
                        "UTF-8 join hash table requires exactly one key array".into(),
                    ));
                };
                Ok(table.lookup(utf8::probe_value(array, row)?, null_equal_keys))
            }
            Self::Binary(table) => {
                let [array] = key_arrays else {
                    return Err(Error::Internal(
                        "binary join hash table requires exactly one key array".into(),
                    ));
                };
                Ok(table.lookup(binary::probe_value(array, row)?, null_equal_keys))
            }
            Self::Composite(_) => Err(Error::Internal(
                "composite Join hash table requires a prepared probe batch".into(),
            )),
            Self::Generic(table) => {
                let key = row_key(key_arrays, row)?;
                if !null_equal_keys && key.iter().any(CellValue::is_null) {
                    Ok(None)
                } else {
                    Ok(table.get(&key).map(Vec::as_slice))
                }
            }
        }
    }
}

pub(super) fn try_build_composite(
    keys: &[ArrayRef],
    rows: usize,
    deduplicate: bool,
    null_equal_keys: bool,
    reservation: &mut MemoryReservation,
) -> Result<Option<JoinHashTable>> {
    composite::build(keys, rows, deduplicate, null_equal_keys, reservation)
        .map(|table| table.map(Box::new).map(JoinHashTable::Composite))
}

pub(super) fn composite_eligible(keys: &[ArrayRef], rows: usize) -> bool {
    composite::eligible(keys, rows)
}

pub(super) fn try_build_binary(
    key: &ArrayRef,
    rows: usize,
    deduplicate: bool,
    null_equal_keys: bool,
    reservation: &mut MemoryReservation,
) -> Result<Option<JoinHashTable>> {
    binary::build(key, rows, deduplicate, null_equal_keys, reservation)
        .map(|table| table.map(JoinHashTable::Binary))
}

pub(super) fn try_build_utf8(
    key: &ArrayRef,
    rows: usize,
    deduplicate: bool,
    null_equal_keys: bool,
    reservation: &mut MemoryReservation,
) -> Result<Option<JoinHashTable>> {
    utf8::build(key, rows, deduplicate, null_equal_keys, reservation)
        .map(|table| table.map(JoinHashTable::Utf8))
}

pub(super) fn try_build_fixed(
    key: &ArrayRef,
    rows: usize,
    deduplicate: bool,
    null_equal_keys: bool,
    reservation: &mut MemoryReservation,
) -> Result<Option<JoinHashTable>> {
    match key.data_type() {
        DataType::Int64 => fixed::build(
            downcast::<Int64Array>(std::slice::from_ref(key), DataType::Int64)?,
            rows,
            deduplicate,
            null_equal_keys,
            reservation,
            |array, row| (!array.is_null(row)).then(|| array.value(row)),
        )
        .map(|table| table.map(JoinHashTable::Int64)),
        DataType::UInt64 => fixed::build(
            downcast::<UInt64Array>(std::slice::from_ref(key), DataType::UInt64)?,
            rows,
            deduplicate,
            null_equal_keys,
            reservation,
            |array, row| (!array.is_null(row)).then(|| array.value(row)),
        )
        .map(|table| table.map(JoinHashTable::UInt64)),
        data_type => Err(Error::Internal(format!(
            "fixed join hash table does not support {data_type}"
        ))),
    }
}

fn downcast<A: 'static>(key_arrays: &[ArrayRef], expected: DataType) -> Result<&A> {
    let array = key_arrays.first().ok_or_else(|| {
        Error::Internal("fixed join hash table requires exactly one key array".into())
    })?;
    array.as_any().downcast_ref::<A>().ok_or_else(|| {
        Error::Internal(format!(
            "fixed join hash table expected {expected}, found {}",
            array.data_type()
        ))
    })
}

#[cfg(test)]
#[path = "hash_table/tests.rs"]
mod tests;
