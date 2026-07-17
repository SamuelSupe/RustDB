use std::mem::size_of;

use crate::runtime::MemoryReservation;

const NO_DUPLICATES: u32 = u32::MAX;
const INITIAL_DUPLICATE_CAPACITY: usize = 4;

#[derive(Clone, Copy)]
pub(in crate::execution::join) struct FixedEntry {
    first: u32,
    duplicate_id: u32,
}

impl FixedEntry {
    pub(in crate::execution::join::hash_table) fn unique(row: u32) -> Self {
        debug_assert_ne!(row, u32::MAX);
        Self {
            first: row,
            duplicate_id: NO_DUPLICATES,
        }
    }

    pub(in crate::execution::join::hash_table) fn rows<'a>(
        &'a self,
        duplicates: &'a DuplicateRows,
    ) -> &'a [u32] {
        match self.duplicate_id() {
            Some(id) => duplicates.rows(id),
            None => std::slice::from_ref(&self.first),
        }
    }

    pub(in crate::execution::join::hash_table) fn duplicate_id(&self) -> Option<u32> {
        (self.duplicate_id != NO_DUPLICATES).then_some(self.duplicate_id)
    }

    pub(in crate::execution::join::hash_table) fn first(&self) -> u32 {
        self.first
    }

    pub(in crate::execution::join::hash_table) fn set_duplicate_id(&mut self, id: u32) {
        debug_assert_ne!(id, NO_DUPLICATES);
        self.duplicate_id = id;
    }
}

#[derive(Default)]
pub(in crate::execution::join::hash_table) struct DuplicateRows {
    groups: Vec<Vec<u32>>,
}

impl DuplicateRows {
    pub(in crate::execution::join::hash_table) fn rows(&self, id: u32) -> &[u32] {
        &self.groups[id as usize]
    }

    pub(in crate::execution::join::hash_table) fn try_promote(
        &mut self,
        first: u32,
        row: u32,
        reservation: &mut MemoryReservation,
    ) -> Option<u32> {
        let id = u32::try_from(self.groups.len()).ok()?;
        if id == NO_DUPLICATES {
            return None;
        }

        let outer_before = self.groups.capacity();
        let estimate = predicted_growth::<Vec<u32>>(self.groups.len(), outer_before)
            .saturating_add(INITIAL_DUPLICATE_CAPACITY.saturating_mul(size_of::<u32>()));
        if reservation.try_grow(estimate).is_err() {
            return None;
        }

        let mut rows = Vec::new();
        if rows.try_reserve(INITIAL_DUPLICATE_CAPACITY).is_err()
            || self.groups.try_reserve(1).is_err()
        {
            return None;
        }
        rows.extend([first, row]);
        let inner_bytes = rows.capacity().saturating_mul(size_of::<u32>());
        self.groups.push(rows);
        let actual = self
            .groups
            .capacity()
            .saturating_sub(outer_before)
            .saturating_mul(size_of::<Vec<u32>>())
            .saturating_add(inner_bytes);
        if !reconcile_growth(reservation, estimate, actual) {
            return None;
        }
        Some(id)
    }

    pub(in crate::execution::join::hash_table) fn try_push(
        &mut self,
        id: u32,
        row: u32,
        reservation: &mut MemoryReservation,
    ) -> bool {
        let rows = &mut self.groups[id as usize];
        let before = rows.capacity();
        let estimate = predicted_growth::<u32>(rows.len(), before);
        if reservation.try_grow(estimate).is_err() {
            return false;
        }
        if rows.try_reserve(1).is_err() {
            return false;
        }
        rows.push(row);
        let actual = rows
            .capacity()
            .saturating_sub(before)
            .saturating_mul(size_of::<u32>());
        reconcile_growth(reservation, estimate, actual)
    }

    pub(in crate::execution::join::hash_table) fn group_count(&self) -> usize {
        self.groups.len()
    }

    #[cfg(test)]
    pub(super) fn capacity(&self) -> usize {
        self.groups.capacity()
    }

    #[cfg(test)]
    pub(in crate::execution::join::hash_table) fn allocated_bytes(&self) -> usize {
        self.groups
            .capacity()
            .saturating_mul(size_of::<Vec<u32>>())
            .saturating_add(self.groups.iter().fold(0usize, |bytes, rows| {
                bytes.saturating_add(rows.capacity().saturating_mul(size_of::<u32>()))
            }))
    }
}

pub(in crate::execution::join::hash_table) fn try_push_rows(
    rows: &mut Vec<u32>,
    row: u32,
    reservation: &mut MemoryReservation,
) -> bool {
    let before = rows.capacity();
    let estimate = predicted_growth::<u32>(rows.len(), before);
    if reservation.try_grow(estimate).is_err() {
        return false;
    }
    if rows.try_reserve(1).is_err() {
        return false;
    }
    rows.push(row);
    let actual = rows
        .capacity()
        .saturating_sub(before)
        .saturating_mul(size_of::<u32>());
    reconcile_growth(reservation, estimate, actual)
}

fn predicted_growth<T>(length: usize, capacity: usize) -> usize {
    if length < capacity {
        0
    } else {
        capacity
            .max(INITIAL_DUPLICATE_CAPACITY)
            .saturating_sub(capacity)
            .max(capacity)
            .saturating_mul(size_of::<T>())
    }
}

fn reconcile_growth(reservation: &mut MemoryReservation, estimate: usize, actual: usize) -> bool {
    if actual > estimate && reservation.try_grow(actual - estimate).is_err() {
        return false;
    }
    reservation.shrink(estimate.saturating_sub(actual));
    true
}
