use std::mem::size_of;

use hashbrown::{HashTable, hash_table::Entry as HashEntry};

use crate::{Error, Result, runtime::MemoryReservation};

const INITIAL_KEY_CAPACITY: usize = 8;

pub(super) struct Entries {
    keys: Vec<u8>,
    entries: HashTable<Entry>,
}

struct Entry {
    hash: u64,
    count: u64,
    left_start: u32,
    left_len: u32,
    right_start: u32,
    right_len: u32,
}

impl Entries {
    pub(super) fn new() -> Self {
        Self {
            keys: Vec::new(),
            entries: HashTable::new(),
        }
    }

    pub(super) fn count_hashed(&self, hash: u64, left: &[u8], right: &[u8]) -> u64 {
        self.entries
            .find(hash, |entry| self.entry_matches(entry, hash, left, right))
            .map_or(0, |entry| entry.count)
    }

    #[cfg(test)]
    pub(super) fn is_full(&self) -> bool {
        self.entries.len() == self.entries.capacity()
    }

    #[cfg(test)]
    pub(super) fn entry_capacity(&self) -> usize {
        self.entries.capacity()
    }

    #[cfg(test)]
    pub(super) fn next_entry_reservation_bytes(&self) -> usize {
        self.entry_reservation_bytes()
    }

    pub(super) fn try_increment_hashed(
        &mut self,
        hash: u64,
        left: &[u8],
        right: &[u8],
        reservation: &mut MemoryReservation,
    ) -> Result<bool> {
        if self.entries.len() == self.entries.capacity() {
            if let Some(entry) = self.entries.find_mut(hash, |entry| {
                entry_matches(&self.keys, entry, hash, left, right)
            }) {
                increment(entry)?;
                return Ok(true);
            }
            if !self.reserve_entry(reservation) {
                return Ok(false);
            }
        }

        let Self { keys, entries } = self;
        match entries.entry(
            hash,
            |entry| entry_matches(keys, entry, hash, left, right),
            |entry| entry.hash,
        ) {
            HashEntry::Occupied(mut occupied) => {
                increment(occupied.get_mut())?;
                Ok(true)
            }
            HashEntry::Vacant(vacant) => {
                let Some(entry) = prepare_entry(keys, hash, left, right, reservation) else {
                    return Ok(false);
                };
                vacant.insert(entry);
                Ok(true)
            }
        }
    }

    fn entry_matches(&self, entry: &Entry, hash: u64, left: &[u8], right: &[u8]) -> bool {
        entry_matches(&self.keys, entry, hash, left, right)
    }

    fn reserve_entry(&mut self, reservation: &mut MemoryReservation) -> bool {
        if self.entries.len() < self.entries.capacity() {
            return true;
        }
        let old_bytes = self.entries.allocation_size();
        let estimate = self.entry_reservation_bytes();
        if reservation.try_grow(estimate).is_err() {
            return false;
        }
        if self.entries.try_reserve(1, |entry| entry.hash).is_err() {
            reservation.shrink(estimate);
            return false;
        }
        reconcile_replacement(
            reservation,
            old_bytes,
            estimate,
            self.entries.allocation_size(),
        )
    }

    fn entry_reservation_bytes(&self) -> usize {
        self.entries
            .capacity()
            .max(4)
            .saturating_mul(2)
            .saturating_mul(size_of::<Entry>().saturating_add(16))
    }
}

fn increment(entry: &mut Entry) -> Result<()> {
    entry.count = entry
        .count
        .checked_add(1)
        .ok_or_else(|| Error::Execution("join build multiplicity overflowed UINT64".into()))?;
    Ok(())
}

fn prepare_entry(
    keys: &mut Vec<u8>,
    hash: u64,
    left: &[u8],
    right: &[u8],
    reservation: &mut MemoryReservation,
) -> Option<Entry> {
    let right_start = keys.len().checked_add(left.len())?;
    let end = right_start.checked_add(right.len())?;
    if end > u32::MAX as usize {
        return None;
    }
    let left_start = u32::try_from(keys.len()).ok()?;
    let left_len = u32::try_from(left.len()).ok()?;
    let right_start = u32::try_from(right_start).ok()?;
    let right_len = u32::try_from(right.len()).ok()?;
    let key_bytes = left.len().checked_add(right.len())?;
    if !reserve_key_bytes(keys, key_bytes, reservation) {
        return None;
    }
    keys.extend_from_slice(left);
    keys.extend_from_slice(right);
    Some(Entry {
        hash,
        count: 1,
        left_start,
        left_len,
        right_start,
        right_len,
    })
}

fn reserve_key_bytes(
    keys: &mut Vec<u8>,
    additional: usize,
    reservation: &mut MemoryReservation,
) -> bool {
    let Some(required) = keys.len().checked_add(additional) else {
        return false;
    };
    if required <= keys.capacity() {
        return true;
    }
    let old_bytes = keys.capacity();
    let estimate = old_bytes
        .saturating_mul(2)
        .max(required)
        .max(INITIAL_KEY_CAPACITY);
    if reservation.try_grow(estimate).is_err() {
        return false;
    }
    if keys.try_reserve(additional).is_err() {
        reservation.shrink(estimate);
        return false;
    }
    reconcile_replacement(reservation, old_bytes, estimate, keys.capacity())
}

fn entry_matches(keys: &[u8], entry: &Entry, hash: u64, left: &[u8], right: &[u8]) -> bool {
    entry.hash == hash
        && key_slice(keys, entry.left_start, entry.left_len) == Some(left)
        && key_slice(keys, entry.right_start, entry.right_len) == Some(right)
}

fn key_slice(keys: &[u8], start: u32, len: u32) -> Option<&[u8]> {
    let start = usize::try_from(start).ok()?;
    let end = start.checked_add(usize::try_from(len).ok()?)?;
    keys.get(start..end)
}

fn reconcile_replacement(
    reservation: &mut MemoryReservation,
    old_bytes: usize,
    estimate: usize,
    actual: usize,
) -> bool {
    let reserved = old_bytes.saturating_add(estimate);
    if actual > reserved && reservation.try_grow(actual - reserved).is_err() {
        return false;
    }
    reservation.shrink(reserved.saturating_sub(actual));
    true
}
