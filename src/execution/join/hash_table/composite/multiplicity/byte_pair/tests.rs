use std::sync::Arc;

use arrow::{
    array::{ArrayRef, BinaryArray, LargeStringArray},
    datatypes::DataType,
};

use crate::runtime::MemoryPool;

use super::{Probe, Table, probe_memory_bytes, supports};

#[test]
fn mixed_large_utf8_and_binary_are_exact_and_null_aware() {
    assert!(supports(&[DataType::LargeUtf8, DataType::Binary]));
    let pool = MemoryPool::new(1 << 20);
    let mut memory = pool.reservation();
    let mut table = Table::new();
    let build = arrays(
        vec![Some("ab"), Some("a"), Some("ab"), None, Some("")],
        vec![Some(b"c"), Some(b"bc"), Some(b"c"), Some(b"x"), Some(b"")],
    );
    assert!(table.try_insert(&build, &mut memory).unwrap());

    let probe_arrays = arrays(
        vec![Some("ab"), Some("a"), Some("abc"), None, Some("")],
        vec![Some(b"c"), Some(b"bc"), Some(b""), Some(b"x"), Some(b"")],
    );
    let probe = Probe::try_new(
        &probe_arrays,
        pool.try_reserve(probe_memory_bytes()).unwrap(),
    )
    .unwrap();
    let counts = (0..probe_arrays[0].len())
        .map(|row| table.count(&probe, row).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(counts, vec![2, 1, 0, 0, 1]);

    drop(probe);
    drop(table);
    drop(memory);
    assert_eq!(pool.used(), 0);
}

#[test]
fn full_table_duplicate_increments_without_available_memory() {
    let pool = MemoryPool::new(1 << 20);
    let mut memory = pool.reservation();
    let mut table = Table::new();
    let duplicate = arrays(vec![Some("duplicate")], vec![Some(b"key")]);
    assert!(table.try_insert(&duplicate, &mut memory).unwrap());

    for index in 0..1_024 {
        if table.entries.is_full() {
            break;
        }
        let left = format!("left-{index}");
        let right = format!("right-{index}").into_bytes();
        let unique = arrays(vec![Some(left.as_str())], vec![Some(right.as_slice())]);
        assert!(table.try_insert(&unique, &mut memory).unwrap());
    }
    assert!(table.entries.is_full());

    let blocker = pool.try_reserve(pool.available()).unwrap();
    assert_eq!(pool.available(), 0);
    assert!(table.try_insert(&duplicate, &mut memory).unwrap());
    drop(blocker);

    let probe =
        Probe::try_new(&duplicate, pool.try_reserve(probe_memory_bytes()).unwrap()).unwrap();
    assert_eq!(table.count(&probe, 0).unwrap(), 2);

    drop(probe);
    drop(table);
    drop(memory);
    assert_eq!(pool.used(), 0);
}

#[test]
fn rejected_vacant_key_is_not_visible() {
    let pool = MemoryPool::new(1 << 20);
    let mut memory = pool.reservation();
    let mut table = Table::new();
    let retained = arrays(vec![Some("retained")], vec![Some(b"key")]);
    assert!(table.try_insert(&retained, &mut memory).unwrap());
    for index in 0..1_024 {
        if table.entries.is_full() {
            break;
        }
        let left = format!("fill-left-{index}");
        let right = format!("fill-right-{index}").into_bytes();
        let unique = arrays(vec![Some(left.as_str())], vec![Some(right.as_slice())]);
        assert!(table.try_insert(&unique, &mut memory).unwrap());
    }
    assert!(table.entries.is_full());

    let original_capacity = table.entries.entry_capacity();
    let entry_reservation = table.entries.next_entry_reservation_bytes();
    assert!(pool.available() > entry_reservation);
    let blocker = pool
        .try_reserve(pool.available() - entry_reservation)
        .unwrap();
    assert_eq!(pool.available(), entry_reservation);
    let large_right = vec![b'x'; 1 << 10];
    let rejected = arrays(vec![Some("different")], vec![Some(large_right.as_slice())]);
    assert!(!table.try_insert(&rejected, &mut memory).unwrap());
    assert!(table.entries.entry_capacity() > original_capacity);
    assert!(table.try_insert(&retained, &mut memory).unwrap());
    drop(blocker);

    let probe_arrays = arrays(
        vec![Some("retained"), Some("different")],
        vec![Some(b"key"), Some(large_right.as_slice())],
    );
    let probe = Probe::try_new(
        &probe_arrays,
        pool.try_reserve(probe_memory_bytes()).unwrap(),
    )
    .unwrap();
    assert_eq!(table.count(&probe, 0).unwrap(), 2);
    assert_eq!(table.count(&probe, 1).unwrap(), 0);

    drop(probe);
    drop(table);
    drop(memory);
    assert_eq!(pool.used(), 0);
}

fn arrays(left: Vec<Option<&str>>, right: Vec<Option<&[u8]>>) -> Vec<ArrayRef> {
    vec![
        Arc::new(LargeStringArray::from(left)),
        Arc::new(BinaryArray::from(right)),
    ]
}
