use std::sync::Arc;

use arrow::{
    array::{ArrayRef, StringArray},
    datatypes::DataType,
};

use crate::runtime::MemoryPool;

use super::CompositeMultiplicityTable;

#[test]
fn counts_composite_keys_across_batches_and_skips_nulls() {
    let pool = MemoryPool::new(1 << 20);
    let mut table_memory = pool.reservation();
    let mut table =
        CompositeMultiplicityTable::try_new(&[DataType::Utf8, DataType::Utf8], &mut table_memory)
            .unwrap()
            .unwrap();
    assert!(table.uses_byte_pair());
    let first = keys(
        vec![Some("ab"), Some("a"), Some("x"), None],
        vec![Some("c"), Some("bc"), Some("y"), Some("ignored")],
    );
    let second = keys(
        vec![Some("x"), Some("ab"), Some("")],
        vec![Some("y"), Some("c"), Some("")],
    );
    assert!(table.try_insert(&first, &mut table_memory).unwrap());
    assert!(table.try_insert(&second, &mut table_memory).unwrap());

    let probe_keys = keys(
        vec![Some("ab"), Some("a"), Some("x"), None, Some("")],
        vec![Some("c"), Some("bc"), Some("y"), Some("ignored"), Some("")],
    );
    let estimate = table.probe_workspace_bytes(&probe_keys).unwrap();
    let probe = table
        .encode_probe(&probe_keys, pool.try_reserve(estimate).unwrap())
        .unwrap();
    let counts = (0..probe_keys[0].len())
        .map(|row| table.count(&probe, &probe_keys, row).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(counts, vec![2, 1, 2, 0, 1]);

    drop(probe);
    drop(table);
    drop(table_memory);
    assert_eq!(pool.used(), 0);
}

#[test]
fn constructor_admission_failure_leaves_reservation_unchanged() {
    let pool = MemoryPool::new(1);
    let mut reservation = pool.reservation();
    let table =
        CompositeMultiplicityTable::try_new(&[DataType::Utf8, DataType::Utf8], &mut reservation)
            .unwrap();
    assert!(table.is_none());
    assert_eq!(reservation.size(), 0);
    assert_eq!(pool.used(), 0);
}

fn keys(left: Vec<Option<&str>>, right: Vec<Option<&str>>) -> Vec<ArrayRef> {
    vec![
        Arc::new(StringArray::from(left)),
        Arc::new(StringArray::from(right)),
    ]
}
