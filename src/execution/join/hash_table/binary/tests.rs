use std::sync::Arc;

use arrow::array::{ArrayRef, BinaryArray, Int64Array, LargeBinaryArray};

use super::super::super::JoinHashTable;
use crate::{
    execution::join::probe::{
        try_build_existence_hash_table_with_nulls, try_build_hash_table_with_nulls,
    },
    runtime::MemoryPool,
};

#[test]
fn binary_keys_borrow_probe_bytes_and_preserve_duplicates_and_nulls() {
    let key = b"\0\xffbinary".as_slice();
    let build: ArrayRef = Arc::new(BinaryArray::from(vec![
        Some(key),
        Some(key),
        None,
        None,
        Some(b"other".as_slice()),
    ]));
    let pool = MemoryPool::new(1 << 20);
    let mut reservation = pool.reservation();
    let table = try_build_hash_table_with_nulls(&[build], 5, false, true, &mut reservation)
        .unwrap()
        .unwrap();

    let JoinHashTable::Binary(binary) = &table else {
        panic!("single Binary key did not use the binary hash table")
    };
    assert_eq!(reservation.size(), binary.allocated_bytes());
    assert_eq!(binary.lookup(Some(key), true), Some(&[0, 1][..]));
    assert_eq!(binary.lookup(None, true), Some(&[2, 3][..]));
    assert_eq!(binary.lookup(None, false), None);
    assert_eq!(binary.keys().len(), 2);
}

#[test]
fn large_binary_dispatches_and_deduplicates_rows() {
    let build: ArrayRef = Arc::new(LargeBinaryArray::from(vec![
        Some(b"alpha".as_slice()),
        Some(b"alpha".as_slice()),
        None,
        None,
    ]));
    let pool = MemoryPool::new(1 << 20);
    let mut reservation = pool.reservation();
    let table = try_build_hash_table_with_nulls(&[build], 4, true, true, &mut reservation)
        .unwrap()
        .unwrap();

    let JoinHashTable::Binary(binary) = table else {
        panic!("single LargeBinary key did not use the binary hash table")
    };
    assert_eq!(binary.lookup(Some(b"alpha"), true), Some(&[0][..]));
    assert_eq!(binary.lookup(None, true), Some(&[2][..]));
}

#[test]
fn binary_probe_accepts_non_utf8_without_allocating_a_key() {
    let build: ArrayRef = Arc::new(BinaryArray::from(vec![b"\xff\0\xfe".as_slice()]));
    let pool = MemoryPool::new(1 << 20);
    let mut reservation = pool.reservation();
    let table = try_build_hash_table_with_nulls(&[build], 1, false, false, &mut reservation)
        .unwrap()
        .unwrap();
    let probe: ArrayRef = Arc::new(BinaryArray::from(vec![b"\xff\0\xfe".as_slice()]));

    assert_eq!(table.matches(&[probe], 0, false).unwrap(), Some(&[0][..]));
}

#[test]
fn existence_summary_keeps_binary_keys_on_the_generic_path() {
    let build: ArrayRef = Arc::new(BinaryArray::from(vec![b"a".as_slice(), b"a".as_slice()]));
    let summary: ArrayRef = Arc::new(Int64Array::from(vec![1, 2]));
    let pool = MemoryPool::new(1 << 20);
    let mut reservation = pool.reservation();
    let table =
        try_build_existence_hash_table_with_nulls(&[build], 2, false, &summary, &mut reservation)
            .unwrap()
            .unwrap();

    assert!(matches!(table, JoinHashTable::Generic(_)));
}

#[test]
fn failed_binary_build_restores_initial_reservation() {
    let values = (0_u8..32).map(|value| vec![value; 64]).collect::<Vec<_>>();
    let build: ArrayRef = Arc::new(BinaryArray::from_iter_values(
        values.iter().map(Vec::as_slice),
    ));
    let pool = MemoryPool::new(128);
    let mut reservation = pool.reservation();
    reservation.try_resize(16).unwrap();

    assert!(
        try_build_hash_table_with_nulls(&[build], 32, false, false, &mut reservation)
            .unwrap()
            .is_none()
    );
    assert_eq!(reservation.size(), 16);
    assert_eq!(pool.used(), 16);
}
