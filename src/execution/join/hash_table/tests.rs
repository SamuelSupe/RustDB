use std::{mem::size_of, sync::Arc};

use arrow::array::{ArrayRef, Int64Array, StringArray};

use super::{FixedEntry, JoinHashTable, try_build_fixed, try_build_utf8};
use crate::execution::join::probe::try_build_hash_table_with_nulls;
use crate::runtime::MemoryPool;

#[path = "tests/dense.rs"]
mod dense;

#[test]
fn int64_table_keeps_duplicate_rows_without_allocating_generic_keys() {
    let key: ArrayRef = Arc::new(Int64Array::from(vec![Some(1), Some(1), None, Some(2)]));
    let pool = MemoryPool::new(1 << 20);
    let mut reservation = pool.reservation();
    let table = try_build_fixed(&key, 4, false, false, &mut reservation)
        .unwrap()
        .unwrap();

    assert!(matches!(&table, JoinHashTable::Int64(_)));
    let JoinHashTable::Int64(fixed) = &table else {
        unreachable!()
    };
    assert!(fixed.is_dense());
    assert_eq!(
        table
            .matches(&[Arc::clone(&key)], 0, false)
            .unwrap()
            .unwrap(),
        &[0, 1]
    );
    assert!(
        table
            .matches(&[Arc::clone(&key)], 2, false)
            .unwrap()
            .is_none()
    );
    assert_eq!(table.matches(&[key], 3, false).unwrap().unwrap(), &[3]);
}

#[test]
fn null_equal_and_deduplicate_apply_to_fixed_keys() {
    let key: ArrayRef = Arc::new(Int64Array::from(vec![None, None, Some(7), Some(7)]));
    let pool = MemoryPool::new(1 << 20);
    let mut reservation = pool.reservation();
    let table = try_build_fixed(&key, 4, true, true, &mut reservation)
        .unwrap()
        .unwrap();

    assert_eq!(
        table
            .matches(&[Arc::clone(&key)], 0, true)
            .unwrap()
            .unwrap(),
        &[0]
    );
    assert_eq!(table.matches(&[key], 2, true).unwrap().unwrap(), &[2]);
    let JoinHashTable::Int64(fixed) = &table else {
        panic!("expected an Int64 fixed hash table");
    };
    assert_eq!(fixed.duplicate_sidecar(), (0, 0));
}

#[test]
fn fixed_keys_are_exposed_as_a_borrowed_iterator() {
    let key: ArrayRef = Arc::new(Int64Array::from_iter_values(0..10_000));
    let pool = MemoryPool::new(1 << 24);
    let mut reservation = pool.reservation();
    let table = try_build_fixed(&key, 10_000, false, false, &mut reservation)
        .unwrap()
        .unwrap();

    let keys = table.fixed_keys().unwrap();
    assert_eq!(keys.len(), 10_000);
    assert_eq!(keys.count(), 10_000);
}

#[test]
fn unique_fixed_keys_do_not_reserve_a_vector_per_key() {
    const ROWS: usize = 20_000;

    assert_eq!(size_of::<FixedEntry>(), 2 * size_of::<u32>());

    let key: ArrayRef = Arc::new(Int64Array::from_iter_values(0..ROWS as i64));
    let pool = MemoryPool::new(1 << 26);
    let mut reservation = pool.reservation();
    let table = try_build_fixed(&key, ROWS, false, false, &mut reservation)
        .unwrap()
        .unwrap();

    let JoinHashTable::Int64(fixed) = &table else {
        panic!("expected an Int64 fixed hash table");
    };
    assert_eq!(fixed.duplicate_sidecar(), (0, 0));
    assert_eq!(reservation.size(), fixed.allocated_bytes());

    let legacy_bucket_bytes = size_of::<i64>() + size_of::<Vec<u32>>() + 16;
    let legacy_per_key_heap = 4 * size_of::<u32>();
    let legacy_estimate = table
        .capacity()
        .saturating_mul(legacy_bucket_bytes)
        .saturating_add(ROWS.saturating_mul(legacy_per_key_heap));

    assert!(
        reservation.size().saturating_add(ROWS * size_of::<u32>()) < legacy_estimate,
        "inline rows should save well over one u32 per unique key: reserved={}, legacy={legacy_estimate}",
        reservation.size()
    );
    assert_eq!(
        table.matches(&[key], ROWS - 1, false).unwrap().unwrap(),
        &[(ROWS - 1) as u32]
    );
}

#[test]
fn fixed_rows_promote_to_many_only_for_duplicate_keys() {
    let key: ArrayRef = Arc::new(Int64Array::from(vec![3, 9, 3, 3, 9, 12]));
    let pool = MemoryPool::new(1 << 20);
    let mut reservation = pool.reservation();
    let table = try_build_fixed(&key, 6, false, false, &mut reservation)
        .unwrap()
        .unwrap();

    assert_eq!(
        table
            .matches(&[Arc::clone(&key)], 0, false)
            .unwrap()
            .unwrap(),
        &[0, 2, 3]
    );
    assert_eq!(
        table
            .matches(&[Arc::clone(&key)], 1, false)
            .unwrap()
            .unwrap(),
        &[1, 4]
    );
    assert_eq!(table.matches(&[key], 5, false).unwrap().unwrap(), &[5]);
    let JoinHashTable::Int64(fixed) = &table else {
        panic!("expected an Int64 fixed hash table");
    };
    assert_eq!(fixed.duplicate_sidecar().0, 2);
    assert_eq!(reservation.size(), fixed.allocated_bytes());
}

#[test]
fn fixed_table_accounts_and_releases_map_sidecar_and_null_rows() {
    let key: ArrayRef = Arc::new(Int64Array::from(vec![
        Some(4),
        Some(4),
        Some(9),
        None,
        None,
    ]));
    let pool = MemoryPool::new(1 << 20);
    let mut reservation = pool.reservation();
    let table = try_build_fixed(&key, 5, false, true, &mut reservation)
        .unwrap()
        .unwrap();

    let JoinHashTable::Int64(fixed) = &table else {
        panic!("expected an Int64 fixed hash table");
    };
    assert_eq!(fixed.duplicate_sidecar().0, 1);
    assert_eq!(reservation.size(), fixed.allocated_bytes());
    assert_eq!(table.matches(&[key], 3, true).unwrap().unwrap(), &[3, 4]);

    drop(table);
    assert_eq!(pool.used(), reservation.size());
    drop(reservation);
    assert_eq!(pool.used(), 0);
}

#[test]
fn failed_fixed_build_restores_the_initial_reservation() {
    let key: ArrayRef = Arc::new(Int64Array::from_iter_values(0..1_000));
    let pool = MemoryPool::new(128);
    let mut reservation = pool.reservation();
    reservation.try_resize(16).unwrap();

    assert!(
        try_build_fixed(&key, 1_000, false, false, &mut reservation)
            .unwrap()
            .is_none()
    );
    assert_eq!(reservation.size(), 16);
    assert_eq!(pool.used(), 16);
}

#[test]
fn duplicate_sidecar_reservation_failure_rolls_back_the_table() {
    let key: ArrayRef = Arc::new(Int64Array::from(vec![1, 1]));
    let dense_bytes = size_of::<u32>();
    let pool = MemoryPool::new(16 + dense_bytes + 111);
    let mut reservation = pool.reservation();
    reservation.try_resize(16).unwrap();

    assert!(
        try_build_fixed(&key, 2, false, false, &mut reservation)
            .unwrap()
            .is_none()
    );
    assert_eq!(reservation.size(), 16);
    assert_eq!(pool.used(), 16);
}

#[test]
fn utf8_table_owns_each_key_once_and_borrows_probe_values() {
    let build: ArrayRef = Arc::new(StringArray::from(vec![
        Some("north"),
        Some("north"),
        None,
        Some("south"),
    ]));
    let pool = MemoryPool::new(1 << 20);
    let mut reservation = pool.reservation();
    let table = try_build_hash_table_with_nulls(
        std::slice::from_ref(&build),
        4,
        false,
        true,
        &mut reservation,
    )
    .unwrap()
    .unwrap();

    let JoinHashTable::Utf8(utf8) = &table else {
        panic!("expected a borrowed-probe UTF-8 hash table");
    };
    assert_eq!(reservation.size(), utf8.allocated_bytes());

    let probe: ArrayRef = Arc::new(StringArray::from(vec![
        Some("north"),
        Some("missing"),
        None,
        Some("south"),
    ]));
    assert_eq!(
        table.matches(&[Arc::clone(&probe)], 0, true).unwrap(),
        Some(&[0, 1][..])
    );
    assert_eq!(table.matches(&[Arc::clone(&probe)], 1, true).unwrap(), None);
    assert_eq!(
        table.matches(&[Arc::clone(&probe)], 2, true).unwrap(),
        Some(&[2][..])
    );
    assert_eq!(table.matches(&[probe], 3, true).unwrap(), Some(&[3][..]));
}

#[test]
fn failed_utf8_build_restores_the_initial_reservation() {
    let key: ArrayRef = Arc::new(StringArray::from(vec!["a long UTF-8 join key"; 32]));
    let pool = MemoryPool::new(64);
    let mut reservation = pool.reservation();
    reservation.try_resize(16).unwrap();

    assert!(
        try_build_utf8(&key, 32, false, false, &mut reservation)
            .unwrap()
            .is_none()
    );
    assert_eq!(reservation.size(), 16);
    assert_eq!(pool.used(), 16);
}
