use std::sync::Arc;

use arrow::{
    array::{ArrayRef, BinaryArray, Float64Array, Int64Array, StringArray},
    datatypes::DataType,
};

use super::{build, eligible, supports_type};
use crate::execution::join::{
    hash_table::JoinHashTable,
    probe::{
        try_build_existence_hash_table_with_nulls, try_build_hash_table_with_nulls,
        try_build_primary_hash_table_with_nulls,
    },
};
use crate::runtime::MemoryPool;

fn keys(ids: Vec<Option<i64>>, labels: Vec<Option<&str>>) -> Vec<ArrayRef> {
    vec![
        Arc::new(Int64Array::from(ids)),
        Arc::new(StringArray::from(labels)),
    ]
}

#[test]
fn composite_build_borrows_probe_rows_and_keeps_duplicates() {
    let build_keys = keys(
        vec![Some(1), Some(1), Some(1), Some(2)],
        vec![Some("a"), Some("a"), Some("b"), Some("z")],
    );
    let pool = MemoryPool::new(1 << 20);
    let mut build_memory = pool.reservation();
    let table = build(&build_keys, 4, false, false, &mut build_memory)
        .unwrap()
        .unwrap();
    assert_eq!(table.distinct_keys(), 3);
    let retained = build_memory.size();
    assert!(retained >= table.allocated_bytes());
    let probe_keys = keys(
        vec![Some(1), Some(1), Some(2), Some(9)],
        vec![Some("a"), Some("b"), Some("z"), Some("x")],
    );
    let estimate = table.probe_workspace_bytes(&probe_keys).unwrap();
    let probe = table
        .encode_probe(&probe_keys, pool.try_reserve(estimate).unwrap())
        .unwrap();

    assert_eq!(
        table.lookup(&probe, &probe_keys, 0, false).unwrap(),
        Some(&[0, 1][..])
    );
    assert_eq!(
        table.lookup(&probe, &probe_keys, 1, false).unwrap(),
        Some(&[2][..])
    );
    assert_eq!(
        table.lookup(&probe, &probe_keys, 2, false).unwrap(),
        Some(&[3][..])
    );
    assert_eq!(table.lookup(&probe, &probe_keys, 3, false).unwrap(), None);
    assert!(pool.used() > retained);
    drop(probe);
    assert_eq!(pool.used(), retained);

    let second_keys = keys(vec![Some(2)], vec![Some("z")]);
    let second_estimate = table.probe_workspace_bytes(&second_keys).unwrap();
    let second = table
        .encode_probe(&second_keys, pool.try_reserve(second_estimate).unwrap())
        .unwrap();
    assert_eq!(
        table.lookup(&second, &second_keys, 0, false).unwrap(),
        Some(&[3][..])
    );
    drop(second);
    assert_eq!(pool.used(), retained);
    drop(table);
    build_memory.try_resize(0).unwrap();
    assert_eq!(pool.used(), 0);
}

#[test]
fn null_patterns_and_deduplication_match_join_policy() {
    let build_keys = keys(
        vec![None, None, Some(1), Some(1)],
        vec![Some("x"), Some("x"), None, None],
    );
    let pool = MemoryPool::new(1 << 20);
    let mut memory = pool.reservation();
    let table = build(&build_keys, 4, true, true, &mut memory)
        .unwrap()
        .unwrap();
    let estimate = table.probe_workspace_bytes(&build_keys).unwrap();
    let probe = table
        .encode_probe(&build_keys, pool.try_reserve(estimate).unwrap())
        .unwrap();

    assert_eq!(
        table.lookup(&probe, &build_keys, 0, true).unwrap(),
        Some(&[0][..])
    );
    assert_eq!(
        table.lookup(&probe, &build_keys, 2, true).unwrap(),
        Some(&[2][..])
    );
    assert_eq!(table.lookup(&probe, &build_keys, 0, false).unwrap(), None);
    assert_eq!(table.lookup(&probe, &build_keys, 2, false).unwrap(), None);
}

#[test]
fn binary_tuple_preserves_arbitrary_bytes() {
    let ids: ArrayRef = Arc::new(Int64Array::from(vec![1, 2]));
    let bytes: ArrayRef = Arc::new(BinaryArray::from(vec![
        Some(&b"a\0b"[..]),
        Some(&[0xff, 0x00][..]),
    ]));
    let arrays = vec![ids, bytes];
    let pool = MemoryPool::new(1 << 20);
    let mut memory = pool.reservation();
    let table = build(&arrays, 2, false, false, &mut memory)
        .unwrap()
        .unwrap();
    let estimate = table.probe_workspace_bytes(&arrays).unwrap();
    let probe = table
        .encode_probe(&arrays, pool.try_reserve(estimate).unwrap())
        .unwrap();

    assert_eq!(
        table.lookup(&probe, &arrays, 0, false).unwrap(),
        Some(&[0][..])
    );
    assert_eq!(
        table.lookup(&probe, &arrays, 1, false).unwrap(),
        Some(&[1][..])
    );
}

#[test]
fn eligibility_rejects_float_dictionary_and_nested_types() {
    let floats: Vec<ArrayRef> = vec![
        Arc::new(Float64Array::from(vec![1.0])),
        Arc::new(Int64Array::from(vec![1])),
    ];
    assert!(!eligible(&floats, 1));
    let single: Vec<ArrayRef> = vec![Arc::new(Int64Array::from(vec![1]))];
    assert!(!eligible(&single, 1));
    assert!(!supports_type(&DataType::Dictionary(
        Box::new(DataType::Int32),
        Box::new(DataType::Utf8),
    )));
    assert!(!supports_type(&DataType::List(Arc::new(
        arrow::datatypes::Field::new("item", DataType::Int64, true),
    ))));
}

#[test]
fn only_primary_eligible_builds_use_composite() {
    let arrays = keys(vec![Some(1), Some(2)], vec![Some("a"), Some("b")]);
    let pool = MemoryPool::new(1 << 20);

    let mut generic_memory = pool.reservation();
    let generic = try_build_hash_table_with_nulls(
        &arrays,
        arrays[0].len(),
        false,
        false,
        &mut generic_memory,
    )
    .unwrap()
    .unwrap();
    assert!(matches!(generic, JoinHashTable::Generic(_)));
    drop(generic);
    generic_memory.try_resize(0).unwrap();

    let mut primary_memory = pool.reservation();
    let primary = try_build_primary_hash_table_with_nulls(
        &arrays,
        arrays[0].len(),
        false,
        false,
        &mut primary_memory,
    )
    .unwrap()
    .unwrap();
    assert!(matches!(primary, JoinHashTable::Composite(_)));
    drop(primary);
    primary_memory.try_resize(0).unwrap();

    let summary: ArrayRef = Arc::new(Int64Array::from(vec![10, 20]));
    let mut summary_memory = pool.reservation();
    let summarized = try_build_existence_hash_table_with_nulls(
        &arrays,
        arrays[0].len(),
        false,
        &summary,
        &mut summary_memory,
    )
    .unwrap()
    .unwrap();
    assert!(matches!(summarized, JoinHashTable::Generic(_)));
}

#[test]
fn eligible_primary_reservation_rejection_does_not_fall_back_to_generic() {
    let arrays = keys(vec![Some(1)], vec![Some("a")]);
    let pool = MemoryPool::new(1);
    let mut memory = pool.reservation();

    let table =
        try_build_primary_hash_table_with_nulls(&arrays, 1, false, false, &mut memory).unwrap();

    assert!(table.is_none());
    assert_eq!(memory.size(), 0);
    assert_eq!(pool.used(), 0);
}
