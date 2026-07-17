use std::mem::size_of;

use crate::runtime::MemoryReservation;

use super::{DenseKey, DuplicateRows};

// A hard bound keeps a dense candidate from attempting an unexpectedly large
// allocation even when a caller has a very generous query budget.
pub(super) const MAX_DENSE_SLOTS: usize = 1 << 24;
pub(in crate::execution::join::hash_table) type DenseSlot = u32;

const DUPLICATE_TAG: DenseSlot = 1 << 31;
const VALUE_MASK: DenseSlot = DUPLICATE_TAG - 1;
const EMPTY: DenseSlot = u32::MAX;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum DenseValue {
    Unique(u32),
    Duplicate(u32),
}

pub(in crate::execution::join::hash_table) fn encode_unique(row: u32) -> Option<DenseSlot> {
    (row < DUPLICATE_TAG).then_some(row)
}

pub(in crate::execution::join::hash_table) fn encode_duplicate(
    duplicate_id: u32,
) -> Option<DenseSlot> {
    (duplicate_id < VALUE_MASK).then_some(DUPLICATE_TAG | duplicate_id)
}

fn decode(slot: DenseSlot) -> Option<DenseValue> {
    match slot {
        EMPTY => None,
        slot if slot & DUPLICATE_TAG == 0 => Some(DenseValue::Unique(slot)),
        slot => Some(DenseValue::Duplicate(slot & VALUE_MASK)),
    }
}

pub(in crate::execution::join::hash_table) struct DenseTable<T> {
    min: T,
    slots: Vec<DenseSlot>,
    occupied: usize,
}

impl<T: DenseKey> DenseTable<T> {
    pub(in crate::execution::join::hash_table) fn try_new(
        min: T,
        slots: usize,
        reservation: &mut MemoryReservation,
    ) -> Option<Self> {
        let estimate = slots.checked_mul(size_of::<DenseSlot>())?;
        if reservation.try_grow(estimate).is_err() {
            return None;
        }

        let mut entries = Vec::new();
        if entries.try_reserve_exact(slots).is_err() {
            reservation.shrink(estimate);
            return None;
        }
        let actual = entries.capacity().saturating_mul(size_of::<DenseSlot>());
        if actual > estimate && reservation.try_grow(actual - estimate).is_err() {
            drop(entries);
            reservation.shrink(estimate);
            return None;
        }
        reservation.shrink(estimate.saturating_sub(actual));
        entries.resize(slots, EMPTY);
        Some(Self {
            min,
            slots: entries,
            occupied: 0,
        })
    }

    pub(super) fn value(&self, key: T) -> Option<DenseValue> {
        decode(*self.slots.get(key.offset(self.min)?)?)
    }

    pub(super) fn rows<'a>(&'a self, key: T, duplicates: &'a DuplicateRows) -> Option<&'a [u32]> {
        let slot = self.slots.get(key.offset(self.min)?)?;
        match decode(*slot)? {
            DenseValue::Unique(_) => Some(std::slice::from_ref(slot)),
            DenseValue::Duplicate(id) => Some(duplicates.rows(id)),
        }
    }

    pub(in crate::execution::join::hash_table) fn insert_unique(
        &mut self,
        key: T,
        row: u32,
    ) -> bool {
        let Some(row) = encode_unique(row) else {
            return false;
        };
        let Some(slot) = self
            .slots
            .get_mut(key.offset(self.min).unwrap_or(usize::MAX))
        else {
            return false;
        };
        if *slot != EMPTY {
            return false;
        }
        *slot = row;
        self.occupied += 1;
        true
    }

    pub(in crate::execution::join::hash_table) fn set_duplicate(
        &mut self,
        key: T,
        duplicate_id: u32,
    ) -> bool {
        let Some(encoded) = encode_duplicate(duplicate_id) else {
            return false;
        };
        let Some(slot) = self
            .slots
            .get_mut(key.offset(self.min).unwrap_or(usize::MAX))
        else {
            return false;
        };
        if !matches!(decode(*slot), Some(DenseValue::Unique(_))) {
            return false;
        }
        *slot = encoded;
        true
    }

    #[cfg(test)]
    pub(super) fn capacity(&self) -> usize {
        self.slots.capacity()
    }

    #[cfg(test)]
    pub(super) fn allocated_bytes(&self) -> usize {
        self.capacity().saturating_mul(size_of::<DenseSlot>())
    }

    pub(super) fn keys(&self) -> DenseKeys<'_, T> {
        DenseKeys {
            min: self.min,
            slots: &self.slots,
            offset: 0,
            remaining: self.occupied,
        }
    }
}

#[derive(Clone)]
pub(in crate::execution::join) struct DenseKeys<'a, T> {
    min: T,
    slots: &'a [DenseSlot],
    offset: usize,
    remaining: usize,
}

impl<T: DenseKey> Iterator for DenseKeys<'_, T> {
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        while let Some(entry) = self.slots.get(self.offset) {
            let offset = self.offset;
            self.offset += 1;
            if *entry != EMPTY {
                self.remaining -= 1;
                return T::from_offset(self.min, offset);
            }
        }
        None
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.remaining, Some(self.remaining))
    }
}

impl<T: DenseKey> ExactSizeIterator for DenseKeys<'_, T> {
    fn len(&self) -> usize {
        self.remaining
    }
}
