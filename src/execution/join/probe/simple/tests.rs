use std::sync::Arc;

use arrow::{
    array::{ArrayRef, Int64Array, StringArray},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};

use crate::{
    execution::join::{
        condition::JoinPredicates,
        probe::{ProbeCursor, try_build_hash_table, try_build_hash_table_with_nulls},
    },
    runtime::{MemoryPool, QueryContext},
    sql::JoinType,
};

#[test]
fn simple_equality_indices_cover_join_semantics() {
    let schema = Arc::new(Schema::new(vec![Field::new("key", DataType::Int64, false)]));
    let left = batch(Arc::clone(&schema), vec![1, 2, 3]);
    let right = batch(Arc::clone(&schema), vec![1, 1, 2]);
    let left_keys = vec![Arc::clone(left.column(0))];
    let right_keys = vec![Arc::clone(right.column(0))];
    let directory = tempfile::tempdir().unwrap();
    let context = QueryContext::new(MemoryPool::new(1 << 20), directory.path()).unwrap();
    let mut reservation = context.memory.reservation();
    let hash = try_build_hash_table(&right_keys, right.num_rows(), false, &mut reservation)
        .unwrap()
        .unwrap();
    let predicates = JoinPredicates::new(None, None, &schema, &schema);

    assert_eq!(
        indices(
            &left,
            &right,
            &left_keys,
            &hash,
            &predicates,
            JoinType::Inner,
            &context,
        ),
        (vec![0, 0, 1], vec![Some(0), Some(1), Some(2)], vec![])
    );
    assert_eq!(
        indices(
            &left,
            &right,
            &left_keys,
            &hash,
            &predicates,
            JoinType::Left,
            &context,
        ),
        (
            vec![0, 0, 1, 2],
            vec![Some(0), Some(1), Some(2), None],
            vec![],
        )
    );
    assert_eq!(
        indices(
            &left,
            &right,
            &left_keys,
            &hash,
            &predicates,
            JoinType::Semi,
            &context,
        ),
        (vec![0, 1], vec![None, None], vec![])
    );
    assert_eq!(
        indices(
            &left,
            &right,
            &left_keys,
            &hash,
            &predicates,
            JoinType::Anti,
            &context,
        ),
        (vec![2], vec![None], vec![])
    );
    assert_eq!(
        indices(
            &left,
            &right,
            &left_keys,
            &hash,
            &predicates,
            JoinType::Mark,
            &context,
        ),
        (
            vec![0, 1, 2],
            vec![None, None, None],
            vec![Some(true), Some(true), Some(false)],
        )
    );
}

#[test]
fn utf8_simple_equality_preserves_duplicates_nulls_and_join_semantics() {
    let schema = Arc::new(Schema::new(vec![Field::new("key", DataType::Utf8, true)]));
    let left = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(StringArray::from(vec![
            Some("a"),
            Some("c"),
            None,
            Some("b"),
        ]))],
    )
    .unwrap();
    let right = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(StringArray::from(vec![
            Some("a"),
            Some("a"),
            None,
            Some("b"),
        ]))],
    )
    .unwrap();
    let left_keys = vec![Arc::clone(left.column(0))];
    let right_keys = vec![Arc::clone(right.column(0))];
    let directory = tempfile::tempdir().unwrap();
    let context = QueryContext::new(MemoryPool::new(1 << 20), directory.path()).unwrap();
    let mut reservation = context.memory.reservation();
    let hash = try_build_hash_table_with_nulls(
        &right_keys,
        right.num_rows(),
        false,
        false,
        &mut reservation,
    )
    .unwrap()
    .unwrap();
    let predicates = JoinPredicates::new(None, None, &schema, &schema);

    assert_eq!(
        indices(
            &left,
            &right,
            &left_keys,
            &hash,
            &predicates,
            JoinType::Inner,
            &context,
        ),
        (vec![0, 0, 3], vec![Some(0), Some(1), Some(3)], vec![])
    );
    assert_eq!(
        indices(
            &left,
            &right,
            &left_keys,
            &hash,
            &predicates,
            JoinType::Left,
            &context,
        ),
        (
            vec![0, 0, 1, 2, 3],
            vec![Some(0), Some(1), None, None, Some(3)],
            vec![],
        )
    );
    assert_eq!(
        indices(
            &left,
            &right,
            &left_keys,
            &hash,
            &predicates,
            JoinType::Semi,
            &context,
        ),
        (vec![0, 3], vec![None, None], vec![])
    );
    assert_eq!(
        indices(
            &left,
            &right,
            &left_keys,
            &hash,
            &predicates,
            JoinType::Anti,
            &context,
        ),
        (vec![1, 2], vec![None, None], vec![])
    );

    let mut null_reservation = context.memory.reservation();
    let null_equal_hash = try_build_hash_table_with_nulls(
        &right_keys,
        right.num_rows(),
        false,
        true,
        &mut null_reservation,
    )
    .unwrap()
    .unwrap();
    assert_eq!(
        indices_with_nulls(
            &left,
            &right,
            &left_keys,
            &null_equal_hash,
            &predicates,
            JoinType::Inner,
            &context,
            true,
        ),
        (
            vec![0, 0, 2, 3],
            vec![Some(0), Some(1), Some(2), Some(3)],
            vec![],
        )
    );
}

fn indices(
    left: &RecordBatch,
    right: &RecordBatch,
    left_keys: &[ArrayRef],
    hash: &super::super::JoinHashTable,
    predicates: &JoinPredicates,
    join_type: JoinType,
    context: &QueryContext,
) -> (Vec<u32>, Vec<Option<u32>>, Vec<Option<bool>>) {
    indices_with_nulls(
        left, right, left_keys, hash, predicates, join_type, context, false,
    )
}

#[allow(clippy::too_many_arguments)]
fn indices_with_nulls(
    left: &RecordBatch,
    right: &RecordBatch,
    left_keys: &[ArrayRef],
    hash: &super::super::JoinHashTable,
    predicates: &JoinPredicates,
    join_type: JoinType,
    context: &QueryContext,
    null_equal_keys: bool,
) -> (Vec<u32>, Vec<Option<u32>>, Vec<Option<bool>>) {
    let mut cursor = ProbeCursor::new(
        left,
        right,
        left_keys,
        hash,
        predicates,
        None,
        None,
        None,
        null_equal_keys,
        None,
        join_type,
        Arc::new(Schema::empty()),
        32,
        0,
    );
    let mut left_indices = Vec::new();
    let mut right_indices = Vec::new();
    let mut markers = Vec::new();
    cursor
        .fill_simple_indices(&mut left_indices, &mut right_indices, &mut markers, context)
        .unwrap();
    (left_indices, right_indices, markers)
}

fn batch(schema: Arc<Schema>, values: Vec<i64>) -> RecordBatch {
    RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(values))]).unwrap()
}
