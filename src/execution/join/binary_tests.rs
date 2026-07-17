use std::sync::Arc;

use arrow::{
    array::{BinaryArray, Int64Array},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use futures::TryStreamExt;

use super::join;
use crate::{
    runtime::{MemoryPool, QueryContext, boxed_record_batch_stream},
    sql::{BoundExpr, JoinType},
};

#[tokio::test]
async fn binary_inner_join_matches_arbitrary_bytes_and_excludes_nulls() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::Binary, true),
        Field::new("id", DataType::Int64, false),
    ]));
    let repeated = b"\0\xffkey".as_slice();
    let other = b"\xfe\0other".as_slice();
    let left = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(BinaryArray::from(vec![
                Some(repeated),
                Some(repeated),
                Some(other),
                None,
                Some(b"missing".as_slice()),
            ])),
            Arc::new(Int64Array::from(vec![10, 11, 12, 13, 14])),
        ],
    )
    .unwrap();
    let right = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(BinaryArray::from(vec![
                Some(repeated),
                Some(repeated),
                Some(other),
                None,
            ])),
            Arc::new(Int64Array::from(vec![20, 21, 22, 23])),
        ],
    )
    .unwrap();
    let output_schema = Arc::new(Schema::new(vec![
        Field::new("left_key", DataType::Binary, true),
        Field::new("left_id", DataType::Int64, false),
        Field::new("right_key", DataType::Binary, true),
        Field::new("right_id", DataType::Int64, false),
    ]));
    let temp = tempfile::tempdir().unwrap();
    let context = QueryContext::shared(MemoryPool::new(1 << 20), temp.path()).unwrap();
    let batches = join(
        boxed_record_batch_stream(futures::stream::once(async move { Ok(left) })),
        boxed_record_batch_stream(futures::stream::once(async move { Ok(right) })),
        vec![(
            BoundExpr::column(0, DataType::Binary, "left.key"),
            BoundExpr::column(0, DataType::Binary, "right.key"),
        )],
        None,
        None,
        Arc::clone(&schema),
        schema,
        JoinType::Inner,
        output_schema,
        Arc::clone(&context),
        2,
    )
    .map_ok(|batch| batch.into_public())
    .try_collect::<Vec<_>>()
    .await
    .unwrap();

    let mut pairs = batches
        .iter()
        .flat_map(|batch| {
            let left = batch
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            let right = batch
                .column(3)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            (0..batch.num_rows())
                .map(|row| (left.value(row), right.value(row)))
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    pairs.sort_unstable();

    assert_eq!(pairs, [(10, 20), (10, 21), (11, 20), (11, 21), (12, 22)]);
    assert!(
        !pairs
            .iter()
            .any(|(left, right)| *left == 13 || *right == 23)
    );
    assert_eq!(context.memory.used(), 0);
}
