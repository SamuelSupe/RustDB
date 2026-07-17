use std::{collections::HashMap, mem::size_of};

use ahash::RandomState;

use crate::runtime::MemoryReservation;

use super::{DenseKey, FixedEntry, FixedStorage, FixedTable};

pub(super) fn try_new<T: DenseKey>(
    rows: usize,
    reservation: &mut MemoryReservation,
) -> Option<FixedTable<T>> {
    let estimate = estimated_bytes::<T>(rows);
    if reservation.try_grow(estimate).is_err() {
        return None;
    }

    let mut values = HashMap::with_hasher(RandomState::new());
    if values.try_reserve(rows).is_err() {
        reservation.shrink(estimate);
        return None;
    }
    let actual = map_bytes::<T>(values.capacity());
    if actual > estimate && reservation.try_grow(actual - estimate).is_err() {
        drop(values);
        reservation.shrink(estimate);
        return None;
    }
    reservation.shrink(estimate.saturating_sub(actual));
    Some(FixedTable::new(FixedStorage::Hash(values)))
}

pub(super) fn estimated_bytes<T>(rows: usize) -> usize {
    predicted_capacity(rows).saturating_mul(bucket_bytes::<T>())
}

pub(super) fn map_bytes<T>(capacity: usize) -> usize {
    capacity.saturating_mul(bucket_bytes::<T>())
}

fn predicted_capacity(rows: usize) -> usize {
    match rows {
        0 => 0,
        1..=3 => 3,
        _ => {
            let required_buckets = rows
                .checked_mul(8)
                .and_then(|bytes| bytes.checked_add(6))
                .map(|bytes| bytes / 7)
                .unwrap_or(usize::MAX);
            required_buckets
                .checked_next_power_of_two()
                .map(|buckets| buckets / 8 * 7)
                .unwrap_or(usize::MAX)
        }
    }
}

fn bucket_bytes<T>() -> usize {
    size_of::<T>()
        .saturating_add(size_of::<FixedEntry>())
        .saturating_add(16)
}
