use std::{mem::size_of, sync::Arc};

use arrow::array::{ArrayRef, Int64Array, UInt64Array};

use super::super::fixed::{DenseSlot, DenseTable, encode_duplicate, encode_unique};
use super::super::{CellValue, JoinHashTable, try_build_fixed};
use crate::runtime::MemoryPool;

#[test]
fn dense_slots_are_four_bytes_with_disjoint_row_and_duplicate_tags() {
    const TAG: u32 = 1 << 31;

    assert_eq!(size_of::<DenseSlot>(), size_of::<u32>());
    assert_eq!(encode_unique(TAG - 1), Some(TAG - 1));
    assert_eq!(encode_unique(TAG), None);
    assert_eq!(encode_duplicate(TAG - 2), Some(u32::MAX - 1));
    assert_eq!(encode_duplicate(TAG - 1), None);
    assert_eq!(encode_duplicate(TAG), None);
}

#[test]
fn dense_slot_encoding_failure_is_rejected_without_truncation() {
    const TAG: u32 = 1 << 31;

    let pool = MemoryPool::new(1 << 20);
    let mut reservation = pool.reservation();
    let mut table = DenseTable::<i64>::try_new(0, 2, &mut reservation).unwrap();

    assert!(table.insert_unique(0, TAG - 1));
    assert!(!table.insert_unique(1, TAG));
    assert!(!table.set_duplicate(0, TAG));
}

#[test]
fn uint64_max_keys_use_dense_slots_without_overflow() {
    let key: ArrayRef = Arc::new(UInt64Array::from(vec![
        u64::MAX - 2,
        u64::MAX,
        u64::MAX - 2,
    ]));
    let pool = MemoryPool::new(1 << 20);
    let mut reservation = pool.reservation();
    let table = try_build_fixed(&key, 3, false, false, &mut reservation)
        .unwrap()
        .unwrap();

    let JoinHashTable::UInt64(fixed) = &table else {
        panic!("expected UInt64 table");
    };
    assert!(fixed.is_dense());
    assert_eq!(table.matches(&[key], 0, false).unwrap(), Some(&[0, 2][..]));
}

#[test]
fn int64_uses_a_negative_offset_and_borrowed_lookup() {
    let build: ArrayRef = Arc::new(Int64Array::from(vec![-3, -1, -3, 0]));
    let probe: ArrayRef = Arc::new(Int64Array::from(vec![-3, -2, 0]));
    let pool = MemoryPool::new(1 << 20);
    let mut reservation = pool.reservation();
    let table = try_build_fixed(&build, 4, false, false, &mut reservation)
        .unwrap()
        .unwrap();

    let JoinHashTable::Int64(fixed) = &table else {
        panic!("expected Int64 table");
    };
    assert!(fixed.is_dense());
    assert_eq!(
        table.matches(&[Arc::clone(&probe)], 0, false).unwrap(),
        Some(&[0, 2][..])
    );
    assert_eq!(
        table.matches(&[Arc::clone(&probe)], 1, false).unwrap(),
        None
    );
    assert_eq!(table.matches(&[probe], 2, false).unwrap(), Some(&[3][..]));
    assert_eq!(
        table.fixed_keys().unwrap().collect::<Vec<_>>(),
        vec![
            CellValue::Int64(-3),
            CellValue::Int64(-1),
            CellValue::Int64(0),
        ]
    );
}

#[test]
fn sparse_and_overflowing_ranges_fall_back_to_random_hash() {
    let key: ArrayRef = Arc::new(Int64Array::from(vec![i64::MIN, 0, i64::MAX]));
    let pool = MemoryPool::new(1 << 20);
    let mut reservation = pool.reservation();
    let table = try_build_fixed(&key, 3, false, false, &mut reservation)
        .unwrap()
        .unwrap();

    let JoinHashTable::Int64(fixed) = &table else {
        panic!("expected Int64 table");
    };
    assert!(!fixed.is_dense());
    assert_eq!(table.fixed_keys().unwrap().len(), 3);
    assert_eq!(table.matches(&[key], 2, false).unwrap(), Some(&[2][..]));
}

#[test]
fn slots_sidecar_and_null_capacity_are_fully_reserved() {
    let key: ArrayRef = Arc::new(Int64Array::from(vec![
        Some(-1),
        Some(-1),
        Some(1),
        None,
        None,
    ]));
    let pool = MemoryPool::new(1 << 20);
    let mut reservation = pool.reservation();
    let table = try_build_fixed(&key, 5, false, true, &mut reservation)
        .unwrap()
        .unwrap();

    let JoinHashTable::Int64(fixed) = &table else {
        panic!("expected Int64 table");
    };
    assert!(fixed.is_dense());
    assert_eq!(fixed.duplicate_sidecar().0, 1);
    assert_eq!(reservation.size(), fixed.allocated_bytes());
    assert_eq!(table.matches(&[key], 3, true).unwrap(), Some(&[3, 4][..]));
}
