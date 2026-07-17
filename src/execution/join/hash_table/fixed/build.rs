use std::mem::size_of;

use crate::{Error, Result, runtime::MemoryReservation};

use super::{
    DenseKey, FixedEntry, FixedStorage, FixedTable,
    dense::{DenseSlot, DenseTable, DenseValue, MAX_DENSE_SLOTS, encode_duplicate},
    hash,
    rows::try_push_rows,
};

pub(in crate::execution::join::hash_table) fn build<A, T, F>(
    array: &A,
    rows: usize,
    deduplicate: bool,
    null_equal_keys: bool,
    reservation: &mut MemoryReservation,
    value: F,
) -> Result<Option<FixedTable<T>>>
where
    T: DenseKey,
    F: Fn(&A, usize) -> Option<T>,
{
    let initial = reservation.size();
    let (bounds, non_null_rows) = scan_bounds(array, rows, &value);
    if let Some((min, max)) = bounds
        && let Some(slots) = T::distance(min, max).and_then(|range| range.checked_add(1))
        && dense_is_smaller::<T>(slots, non_null_rows)
        && let Some(table) = DenseTable::try_new(min, slots, reservation)
            .map(|dense| FixedTable::new(FixedStorage::Dense(dense)))
        && let Some(table) = populate(
            table,
            array,
            rows,
            deduplicate,
            null_equal_keys,
            reservation,
            &value,
            initial,
        )?
    {
        return Ok(Some(table));
    }

    reservation.try_resize(initial)?;
    let Some(table) = hash::try_new(non_null_rows, reservation) else {
        reservation.try_resize(initial)?;
        return Ok(None);
    };
    populate(
        table,
        array,
        rows,
        deduplicate,
        null_equal_keys,
        reservation,
        &value,
        initial,
    )
}

fn dense_is_smaller<T>(slots: usize, rows: usize) -> bool {
    slots <= MAX_DENSE_SLOTS
        && slots.saturating_mul(size_of::<DenseSlot>()) < hash::estimated_bytes::<T>(rows)
}

fn scan_bounds<A, T: DenseKey, F: Fn(&A, usize) -> Option<T>>(
    array: &A,
    rows: usize,
    value: &F,
) -> (Option<(T, T)>, usize) {
    let mut bounds: Option<(T, T)> = None;
    let mut count = 0usize;
    for row in 0..rows {
        let Some(key) = value(array, row) else {
            continue;
        };
        count += 1;
        bounds = Some(match bounds {
            Some((min, max)) => (min.min(key), max.max(key)),
            None => (key, key),
        });
    }
    (bounds, count)
}

#[allow(clippy::too_many_arguments)]
fn populate<A, T, F>(
    mut table: FixedTable<T>,
    array: &A,
    rows: usize,
    deduplicate: bool,
    null_equal_keys: bool,
    reservation: &mut MemoryReservation,
    value: &F,
    initial: usize,
) -> Result<Option<FixedTable<T>>>
where
    T: DenseKey,
    F: Fn(&A, usize) -> Option<T>,
{
    for row in 0..rows {
        let Some(row) = u32::try_from(row).ok().filter(|row| *row != u32::MAX) else {
            rollback(table, reservation, initial)?;
            return Err(Error::ResourceExhausted(
                "hash join build side exceeds UINT32_MAX rows".into(),
            ));
        };
        let inserted = match value(array, row as usize) {
            Some(key) => push_value(&mut table, key, row, deduplicate, reservation),
            None if null_equal_keys => push_null(&mut table, row, deduplicate, reservation),
            None => true,
        };
        if !inserted {
            rollback(table, reservation, initial)?;
            return Ok(None);
        }
    }
    Ok(Some(table))
}

fn push_value<T: DenseKey>(
    table: &mut FixedTable<T>,
    key: T,
    row: u32,
    deduplicate: bool,
    reservation: &mut MemoryReservation,
) -> bool {
    match &mut table.storage {
        FixedStorage::Dense(values) => push_dense_value(
            values,
            &mut table.duplicates,
            key,
            row,
            deduplicate,
            reservation,
        ),
        FixedStorage::Hash(values) => {
            let entry = values.entry(key).or_insert_with(|| FixedEntry::unique(row));
            if entry.first() == row || deduplicate {
                return true;
            }
            match entry.duplicate_id() {
                Some(id) => table.duplicates.try_push(id, row, reservation),
                None => match table
                    .duplicates
                    .try_promote(entry.first(), row, reservation)
                {
                    Some(id) => {
                        entry.set_duplicate_id(id);
                        true
                    }
                    None => false,
                },
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn push_dense_value<T: DenseKey>(
    values: &mut DenseTable<T>,
    duplicates: &mut super::DuplicateRows,
    key: T,
    row: u32,
    deduplicate: bool,
    reservation: &mut MemoryReservation,
) -> bool {
    match values.value(key) {
        None => values.insert_unique(key, row),
        Some(_) if deduplicate => true,
        Some(DenseValue::Duplicate(id)) => duplicates.try_push(id, row, reservation),
        Some(DenseValue::Unique(first)) => {
            let Ok(next_id) = u32::try_from(duplicates.group_count()) else {
                return false;
            };
            if encode_duplicate(next_id).is_none() {
                return false;
            }
            let Some(id) = duplicates.try_promote(first, row, reservation) else {
                return false;
            };
            values.set_duplicate(key, id)
        }
    }
}

fn push_null<T: DenseKey>(
    table: &mut FixedTable<T>,
    row: u32,
    deduplicate: bool,
    reservation: &mut MemoryReservation,
) -> bool {
    (deduplicate && !table.nulls.is_empty()) || try_push_rows(&mut table.nulls, row, reservation)
}

fn rollback<T>(
    table: FixedTable<T>,
    reservation: &mut MemoryReservation,
    initial: usize,
) -> Result<()> {
    drop(table);
    reservation.try_resize(initial)
}
