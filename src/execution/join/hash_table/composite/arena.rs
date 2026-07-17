use std::mem::size_of;

use ahash::RandomState;
use hashbrown::HashTable;

use crate::runtime::MemoryReservation;

use super::super::fixed::FixedEntry;

const INITIAL_KEY_CAPACITY: usize = 8;

pub(super) struct CompositeEntries {
    hasher: RandomState,
    keys: Vec<u8>,
    entries: HashTable<CompositeEntry>,
}

struct CompositeEntry {
    hash: u64,
    rows: FixedEntry,
    start: u32,
    len: u32,
}

impl CompositeEntries {
    pub(super) fn new() -> Self {
        Self {
            hasher: RandomState::new(),
            keys: Vec::new(),
            entries: HashTable::new(),
        }
    }

    pub(super) fn get(&self, key: &[u8]) -> Option<&FixedEntry> {
        let hash = self.hasher.hash_one(key);
        self.entries
            .find(hash, |entry| self.entry_matches(entry, hash, key))
            .map(|entry| &entry.rows)
    }

    pub(super) fn hash(&self, key: &[u8]) -> u64 {
        self.hasher.hash_one(key)
    }

    pub(super) fn get_mut(&mut self, hash: u64, key: &[u8]) -> Option<&mut FixedEntry> {
        let keys = &self.keys;
        self.entries
            .find_mut(hash, |entry| entry_matches(keys, entry, hash, key))
            .map(|entry| &mut entry.rows)
    }

    pub(super) fn try_insert(
        &mut self,
        hash: u64,
        key: &[u8],
        row: u32,
        reservation: &mut MemoryReservation,
    ) -> bool {
        let Some(end) = self.keys.len().checked_add(key.len()) else {
            return false;
        };
        if end > u32::MAX as usize {
            return false;
        }
        let Ok(start) = u32::try_from(self.keys.len()) else {
            return false;
        };
        let Ok(len) = u32::try_from(key.len()) else {
            return false;
        };
        if !self.reserve_entry(reservation) || !self.reserve_key_bytes(key.len(), reservation) {
            return false;
        }
        self.keys.extend_from_slice(key);
        self.entries.insert_unique(
            hash,
            CompositeEntry {
                hash,
                rows: FixedEntry::unique(row),
                start,
                len,
            },
            |entry| entry.hash,
        );
        true
    }

    #[cfg(test)]
    pub(super) fn capacity(&self) -> usize {
        self.entries.capacity()
    }

    #[cfg(test)]
    pub(super) fn allocated_bytes(&self) -> usize {
        self.keys
            .capacity()
            .saturating_add(self.entries.allocation_size())
    }

    #[cfg(test)]
    pub(super) fn len(&self) -> usize {
        self.entries.len()
    }

    fn entry_matches(&self, entry: &CompositeEntry, hash: u64, key: &[u8]) -> bool {
        entry_matches(&self.keys, entry, hash, key)
    }

    fn reserve_entry(&mut self, reservation: &mut MemoryReservation) -> bool {
        if self.entries.len() < self.entries.capacity() {
            return true;
        }

        let old_bytes = self.entries.allocation_size();
        let estimate = self
            .entries
            .capacity()
            .max(4)
            .saturating_mul(2)
            .saturating_mul(size_of::<CompositeEntry>().saturating_add(16));
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

    fn reserve_key_bytes(
        &mut self,
        additional: usize,
        reservation: &mut MemoryReservation,
    ) -> bool {
        let Some(required) = self.keys.len().checked_add(additional) else {
            return false;
        };
        if required <= self.keys.capacity() {
            return true;
        }

        let old_bytes = self.keys.capacity();
        let estimate = old_bytes
            .saturating_mul(2)
            .max(required)
            .max(INITIAL_KEY_CAPACITY);
        if reservation.try_grow(estimate).is_err() {
            return false;
        }
        if self.keys.try_reserve(additional).is_err() {
            reservation.shrink(estimate);
            return false;
        }
        reconcile_replacement(reservation, old_bytes, estimate, self.keys.capacity())
    }
}

fn entry_matches(keys: &[u8], entry: &CompositeEntry, hash: u64, key: &[u8]) -> bool {
    entry.hash == hash && key_slice(keys, entry.start, entry.len) == Some(key)
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
