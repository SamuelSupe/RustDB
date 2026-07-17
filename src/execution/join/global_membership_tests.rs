use std::{sync::Arc, time::Duration};

use arrow::{
    array::{Array, BooleanArray, Int64Array},
    datatypes::{DataType, Field, Schema, SchemaRef},
    record_batch::RecordBatch,
};
use futures::{TryStreamExt, stream};
use tokio::time::timeout;

use super::join;
use crate::{
    runtime::{MemoryPool, QueryContext, boxed_record_batch_stream},
    sql::{BoundExpr, JoinType},
};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn global_membership_hash_avoids_cartesian_candidates_and_parallelizes() {
    const LANES: usize = 4;
    const RIGHT_ROWS: i64 = 20_000;
    const LEFT_BATCHES: i64 = 32;
    const ROWS_PER_BATCH: i64 = 2_048;

    let (left_schema, right_schema) = schemas();
    let left_batches = (0..LEFT_BATCHES)
        .map(|batch| {
            let start = batch * ROWS_PER_BATCH;
            let end = start + ROWS_PER_BATCH;
            let values = (start..end).map(|row| {
                if row % 2 == 0 {
                    (row / 2) % RIGHT_ROWS
                } else {
                    RIGHT_ROWS + row
                }
            });
            Ok(RecordBatch::try_new(
                Arc::clone(&left_schema),
                vec![
                    Arc::new(Int64Array::from_iter_values(values)),
                    Arc::new(Int64Array::from_iter_values(start..end)),
                ],
            )
            .unwrap())
        })
        .collect::<Vec<_>>();
    let right_batch = RecordBatch::try_new(
        Arc::clone(&right_schema),
        vec![Arc::new(Int64Array::from_iter_values(0..RIGHT_ROWS))],
    )
    .unwrap();
    let temp = tempfile::tempdir().unwrap();
    let context = QueryContext::shared(MemoryPool::new(128 << 20), temp.path()).unwrap();
    context.configure_compute_lanes(LANES);

    let output = join(
        boxed_record_batch_stream(stream::iter(left_batches)),
        boxed_record_batch_stream(stream::once(async move { Ok(right_batch) })),
        Vec::new(),
        None,
        Some(membership_values()),
        Arc::clone(&left_schema),
        Arc::clone(&right_schema),
        JoinType::Mark,
        output_schema(JoinType::Mark, &left_schema),
        Arc::clone(&context),
        512,
    )
    .map_ok(|batch| batch.into_public())
    .try_collect::<Vec<_>>();
    let batches = timeout(Duration::from_secs(10), output)
        .await
        .expect("global membership probe regressed toward Cartesian candidate work")
        .unwrap();

    let mut true_rows = 0usize;
    let mut false_rows = 0usize;
    for batch in &batches {
        let markers = batch
            .column(2)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        for row in 0..batch.num_rows() {
            assert!(markers.is_valid(row));
            if markers.value(row) {
                true_rows += 1;
            } else {
                false_rows += 1;
            }
        }
    }
    let expected = (LEFT_BATCHES * ROWS_PER_BATCH / 2) as usize;
    assert_eq!((true_rows, false_rows), (expected, expected));
    let metrics = context.metrics.snapshot();
    assert!((2..=LANES as u64).contains(&metrics.peak_active_lanes));
    assert_eq!(metrics.spill_write_bytes, 0);
    assert!(context.memory.peak() <= context.memory.limit());
    assert_eq!(context.memory.used(), 0);
}

#[tokio::test]
async fn global_membership_hash_falls_back_to_spill_when_build_memory_is_rejected() {
    const RIGHT_ROWS: i64 = 4_096;
    for join_type in [JoinType::Mark, JoinType::NullAwareAnti] {
        let (left_schema, right_schema) = schemas();
        let left = RecordBatch::try_new(
            Arc::clone(&left_schema),
            vec![
                Arc::new(Int64Array::from(vec![Some(7), Some(10_000), None])),
                Arc::new(Int64Array::from(vec![0, 1, 2])),
            ],
        )
        .unwrap();
        let mut right = (0..RIGHT_ROWS).map(Some).collect::<Vec<_>>();
        right.push(None);
        let right = RecordBatch::try_new(
            Arc::clone(&right_schema),
            vec![Arc::new(Int64Array::from(right))],
        )
        .unwrap();
        let temp = tempfile::tempdir().unwrap();
        let context = QueryContext::shared(MemoryPool::new(64 << 10), temp.path()).unwrap();

        let batches = join(
            boxed_record_batch_stream(stream::once(async move { Ok(left) })),
            boxed_record_batch_stream(stream::once(async move { Ok(right) })),
            Vec::new(),
            None,
            Some(membership_values()),
            Arc::clone(&left_schema),
            Arc::clone(&right_schema),
            join_type,
            output_schema(join_type, &left_schema),
            Arc::clone(&context),
            8,
        )
        .map_ok(|batch| batch.into_public())
        .try_collect::<Vec<_>>()
        .await
        .unwrap();

        if join_type == JoinType::Mark {
            let mut markers_by_id = [None; 3];
            let mut seen = [false; 3];
            for batch in &batches {
                let ids = batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                let markers = batch
                    .column(2)
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .unwrap();
                for row in 0..batch.num_rows() {
                    let id = usize::try_from(ids.value(row)).unwrap();
                    seen[id] = true;
                    markers_by_id[id] = markers.is_valid(row).then(|| markers.value(row));
                }
            }
            assert_eq!(seen, [true; 3]);
            assert_eq!(markers_by_id, [Some(true), None, None]);
        } else {
            assert!(batches.is_empty());
        }
        assert!(context.metrics.snapshot().spill_write_bytes > 0);
        assert!(context.memory.peak() <= context.memory.limit());
        assert_eq!(context.memory.used(), 0);
    }
}

fn membership_values() -> (BoundExpr, BoundExpr) {
    (
        BoundExpr::column(0, DataType::Int64, "left.value"),
        BoundExpr::column(0, DataType::Int64, "right.value"),
    )
}

fn schemas() -> (SchemaRef, SchemaRef) {
    (
        Arc::new(Schema::new(vec![
            Field::new("value", DataType::Int64, true),
            Field::new("id", DataType::Int64, false),
        ])),
        Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            true,
        )])),
    )
}

fn output_schema(join_type: JoinType, left: &SchemaRef) -> SchemaRef {
    if join_type == JoinType::NullAwareAnti {
        return Arc::clone(left);
    }
    let mut fields = left.fields().iter().cloned().collect::<Vec<_>>();
    fields.push(Arc::new(Field::new("marker", DataType::Boolean, true)));
    Arc::new(Schema::new(fields))
}
