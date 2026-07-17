use std::mem::size_of;

use ahash::RandomState;
use arrow::array::{Array, ArrayRef, BinaryArray, LargeBinaryArray};
use hashbrown::{
    HashMap,
    hash_map::{Keys, RawEntryMut},
};

use crate::{Error, Result, runtime::MemoryReservation};

use super::fixed::{DuplicateRows, FixedEntry, try_push_rows};

pub(in crate::execution::join) struct BinaryHashTable {
    values: HashMap<Vec<u8>, FixedEntry, RandomState>,
    duplicates: DuplicateRows,
    nulls: Vec<u32>,
}

#[derive(Clone)]
pub(in crate::execution::join) struct BinaryKeys<'a> {
    inner: Keys<'a, Vec<u8>, FixedEntry>,
}

impl<'a> Iterator for BinaryKeys<'a> {
    type Item = &'a [u8];

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(Vec::as_slice)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let len = self.len();
        (len, Some(len))
    }
}

impl ExactSizeIterator for BinaryKeys<'_> {
    fn len(&self) -> usize {
        self.inner.len()
    }
}

impl BinaryHashTable {
    pub(super) fn lookup(&self, key: Option<&[u8]>, null_equal_keys: bool) -> Option<&[u32]> {
        match key {
            Some(key) => self
                .values
                .get(key)
                .map(|entry| entry.rows(&self.duplicates)),
            None if null_equal_keys && !self.nulls.is_empty() => Some(&self.nulls),
            None => None,
        }
    }

    pub(super) fn keys(&self) -> BinaryKeys<'_> {
        BinaryKeys {
            inner: self.values.keys(),
        }
    }

    #[cfg(test)]
    pub(super) fn capacity(&self) -> usize {
        self.values.capacity()
    }

    #[cfg(test)]
    pub(super) fn allocated_bytes(&self) -> usize {
        self.values
            .capacity()
            .saturating_mul(bucket_bytes())
            .saturating_add(
                self.values
                    .keys()
                    .map(Vec::capacity)
                    .fold(0usize, usize::saturating_add),
            )
            .saturating_add(self.duplicates.allocated_bytes())
            .saturating_add(self.nulls.capacity().saturating_mul(size_of::<u32>()))
    }
}

pub(super) fn build(
    key: &ArrayRef,
    rows: usize,
    deduplicate: bool,
    null_equal_keys: bool,
    reservation: &mut MemoryReservation,
) -> Result<Option<BinaryHashTable>> {
    let array = BinaryKeyArray::try_new(key)?;
    let initial = reservation.size();
    let mut table = BinaryHashTable {
        values: HashMap::with_hasher(RandomState::new()),
        duplicates: DuplicateRows::default(),
        nulls: Vec::new(),
    };

    for row in 0..rows {
        let row_index = match u32::try_from(row) {
            Ok(row) if row != u32::MAX => row,
            _ => {
                rollback(table, reservation, initial)?;
                return Err(Error::ResourceExhausted(
                    "hash join build side exceeds UINT32_MAX rows".into(),
                ));
            }
        };
        let inserted = match array.value(row) {
            Some(key) => push_key(
                &mut table,
                key,
                row_index,
                deduplicate,
                reservation,
                initial,
            )?,
            None if null_equal_keys => {
                (deduplicate && !table.nulls.is_empty())
                    || try_push_rows(&mut table.nulls, row_index, reservation)
            }
            None => true,
        };
        if !inserted {
            rollback(table, reservation, initial)?;
            return Ok(None);
        }
    }
    Ok(Some(table))
}

fn push_key(
    table: &mut BinaryHashTable,
    key: &[u8],
    row: u32,
    deduplicate: bool,
    reservation: &mut MemoryReservation,
    initial: usize,
) -> Result<bool> {
    let old_capacity = table.values.capacity();
    let needs_growth = table.values.len() == old_capacity;
    let hash = table.values.hasher().hash_one(key);
    let map_estimate = if needs_growth {
        old_capacity
            .max(4)
            .saturating_mul(2)
            .saturating_mul(bucket_bytes())
    } else {
        0
    };
    let estimated = key.len().saturating_add(map_estimate);

    let stored_capacity = if needs_growth {
        match table
            .values
            .raw_entry_mut()
            .from_hash(hash, |stored| stored.as_slice() == key)
        {
            RawEntryMut::Occupied(mut entry) => {
                return Ok(push_duplicate(
                    entry.get_mut(),
                    &mut table.duplicates,
                    row,
                    deduplicate,
                    reservation,
                ));
            }
            RawEntryMut::Vacant(_) => {}
        }
        if reservation.try_grow(estimated).is_err() {
            return Ok(false);
        }
        if table.values.try_reserve(1).is_err() {
            reservation.shrink(estimated);
            return Ok(false);
        }
        let RawEntryMut::Vacant(entry) = table
            .values
            .raw_entry_mut()
            .from_hash(hash, |stored| stored.as_slice() == key)
        else {
            unreachable!("binary key appeared while reserving a single-threaded build table")
        };
        let (stored_key, _) =
            entry.insert_hashed_nocheck(hash, key.to_vec(), FixedEntry::unique(row));
        stored_key.capacity()
    } else {
        let vacant = match table
            .values
            .raw_entry_mut()
            .from_hash(hash, |stored| stored.as_slice() == key)
        {
            RawEntryMut::Occupied(mut entry) => {
                return Ok(push_duplicate(
                    entry.get_mut(),
                    &mut table.duplicates,
                    row,
                    deduplicate,
                    reservation,
                ));
            }
            RawEntryMut::Vacant(entry) => entry,
        };
        if reservation.try_grow(estimated).is_err() {
            return Ok(false);
        }
        let (stored_key, _) =
            vacant.insert_hashed_nocheck(hash, key.to_vec(), FixedEntry::unique(row));
        stored_key.capacity()
    };
    let actual = table
        .values
        .capacity()
        .saturating_sub(old_capacity)
        .saturating_mul(bucket_bytes())
        .saturating_add(stored_capacity);
    if actual > estimated && reservation.try_grow(actual - estimated).is_err() {
        rollback_values(table, reservation, initial)?;
        return Ok(false);
    }
    reservation.shrink(estimated.saturating_sub(actual));
    Ok(true)
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

fn rollback_values(
    table: &mut BinaryHashTable,
    reservation: &mut MemoryReservation,
    initial: usize,
) -> Result<()> {
    table.values = HashMap::with_hasher(RandomState::new());
    table.duplicates = DuplicateRows::default();
    table.nulls = Vec::new();
    reservation.try_resize(initial)
}

fn rollback(
    table: BinaryHashTable,
    reservation: &mut MemoryReservation,
    initial: usize,
) -> Result<()> {
    drop(table);
    reservation.try_resize(initial)
}

fn bucket_bytes() -> usize {
    size_of::<Vec<u8>>()
        .saturating_add(size_of::<FixedEntry>())
        .saturating_add(16)
}

enum BinaryKeyArray<'a> {
    Binary(&'a BinaryArray),
    LargeBinary(&'a LargeBinaryArray),
}

impl<'a> BinaryKeyArray<'a> {
    fn try_new(array: &'a ArrayRef) -> Result<Self> {
        if let Some(array) = array.as_any().downcast_ref::<BinaryArray>() {
            Ok(Self::Binary(array))
        } else if let Some(array) = array.as_any().downcast_ref::<LargeBinaryArray>() {
            Ok(Self::LargeBinary(array))
        } else {
            Err(Error::Internal(format!(
                "binary join hash table expected Binary or LargeBinary, found {}",
                array.data_type()
            )))
        }
    }

    fn value(&self, row: usize) -> Option<&'a [u8]> {
        match self {
            Self::Binary(array) => (!array.is_null(row)).then(|| array.value(row)),
            Self::LargeBinary(array) => (!array.is_null(row)).then(|| array.value(row)),
        }
    }
}

pub(super) fn probe_value(array: &ArrayRef, row: usize) -> Result<Option<&[u8]>> {
    BinaryKeyArray::try_new(array).map(|array| array.value(row))
}

#[cfg(test)]
#[path = "binary/tests.rs"]
mod tests;
