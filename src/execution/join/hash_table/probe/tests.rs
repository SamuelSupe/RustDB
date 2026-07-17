use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array};

use super::bind;
use crate::{execution::join::hash_table::try_build_fixed, runtime::MemoryPool};

#[test]
fn binds_once_and_borrows_fixed_matches() {
    let build: ArrayRef = Arc::new(Int64Array::from(vec![1, 2, 1]));
    let pool = MemoryPool::new(1 << 20);
    let mut reservation = pool.reservation();
    let table = try_build_fixed(&build, 3, false, false, &mut reservation)
        .unwrap()
        .unwrap();
    let probe: ArrayRef = Arc::new(Int64Array::from(vec![Some(1), None, Some(9)]));
    let keys = vec![probe];

    let bound = bind(&table, &keys).unwrap().unwrap();
    assert_eq!(bound.lookup(0), Some(&[0, 2][..]));
    assert_eq!(bound.lookup(1), None);
    assert_eq!(bound.lookup(2), None);
}
