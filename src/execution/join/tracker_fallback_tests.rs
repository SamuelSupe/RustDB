use std::sync::Arc;

use arrow::{
    array::{Array, ArrayRef, Int64Array},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use futures::{TryStreamExt, stream};

use crate::runtime::{MemoryPool, QueryContext, boxed_record_batch_stream};
use crate::sql::{BinaryOp, BoundExpr, ExprKind, JoinType};

use super::{
    BuildMatchTracker, condition::JoinPredicates, join, sort_merge, spill,
    try_build_hash_table_with_nulls,
};

#[tokio::test]
async fn full_join_spills_when_only_the_match_tracker_exceeds_the_budget() {
    const ROWS: usize = 20_000;
    let side_schema = Arc::new(Schema::new(vec![Field::new("key", DataType::Int64, false)]));
    let right_batch = RecordBatch::try_new(
        Arc::clone(&side_schema),
        vec![Arc::new(Int64Array::from_iter_values(0..ROWS as i64))],
    )
    .unwrap();
    let limit = tracker_failure_boundary(&right_batch);
    assert!(right_batch.get_array_memory_size() <= limit / 4);

    let left_batch = RecordBatch::try_new(
        Arc::clone(&side_schema),
        vec![Arc::new(Int64Array::from(vec![0]))],
    )
    .unwrap();
    let output_schema = Arc::new(Schema::new(vec![
        Field::new("left_key", DataType::Int64, true),
        Field::new("right_key", DataType::Int64, true),
    ]));
    let temp = tempfile::tempdir().unwrap();
    let context = QueryContext::shared(MemoryPool::new(limit), temp.path()).unwrap();
    let batches = join(
        boxed_record_batch_stream(stream::once(async move { Ok(left_batch) })),
        boxed_record_batch_stream(stream::once(async move { Ok(right_batch) })),
        vec![(
            BoundExpr::column(0, DataType::Int64, "left.key"),
            BoundExpr::column(0, DataType::Int64, "right.key"),
        )],
        None,
        None,
        Arc::clone(&side_schema),
        side_schema,
        JoinType::Full,
        output_schema,
        Arc::clone(&context),
        512,
    )
    .map_ok(|batch| batch.into_public())
    .try_collect::<Vec<_>>()
    .await
    .unwrap();

    assert_eq!(
        batches.iter().map(RecordBatch::num_rows).sum::<usize>(),
        ROWS
    );
    assert!(context.metrics.snapshot().spill_partitions > 0);
    assert!(context.memory.peak() <= limit);
    assert_eq!(context.memory.used(), 0);
}

fn tracker_failure_boundary(batch: &RecordBatch) -> usize {
    let pool = MemoryPool::new(64 << 20);
    let mut reservation = pool.reservation();
    reservation.try_grow(batch.get_array_memory_size()).unwrap();
    let keys: Vec<ArrayRef> = vec![Arc::clone(batch.column(0))];
    let hash =
        try_build_hash_table_with_nulls(&keys, batch.num_rows(), false, false, &mut reservation)
            .unwrap()
            .unwrap();
    let before = reservation.size();
    let tracker = BuildMatchTracker::new(batch.num_rows(), &mut reservation).unwrap();
    let tracker_bytes = reservation.size() - before;
    drop((tracker, hash, reservation));
    assert_eq!(pool.used(), 0);
    before + tracker_bytes - 1
}

#[tokio::test]
async fn terminal_skew_group_uses_bounded_tracker_with_residual() {
    const RIGHT_ROWS: usize = 97;
    const MEMORY_LIMIT: usize = 2 << 20;

    for join_type in [JoinType::Right, JoinType::Full] {
        let left_schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::Int64, false),
            Field::new("left_id", DataType::Int64, false),
        ]));
        let right_schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::Int64, false),
            Field::new("right_id", DataType::Int64, false),
        ]));
        let left = RecordBatch::try_new(
            Arc::clone(&left_schema),
            vec![
                Arc::new(Int64Array::from(vec![1, 1, 1])),
                Arc::new(Int64Array::from(vec![0, 2, RIGHT_ROWS as i64 + 1])),
            ],
        )
        .unwrap();
        let right = RecordBatch::try_new(
            Arc::clone(&right_schema),
            vec![
                Arc::new(Int64Array::from(vec![1; RIGHT_ROWS])),
                Arc::new(Int64Array::from_iter_values(0..RIGHT_ROWS as i64)),
            ],
        )
        .unwrap();
        let temp = tempfile::tempdir().unwrap();
        let context = QueryContext::shared(MemoryPool::new(MEMORY_LIMIT), temp.path()).unwrap();
        let left_file = context
            .spill
            .write_record_batches("bounded-left", Arc::clone(&left_schema), [left])
            .unwrap();
        let right_file = context
            .spill
            .write_record_batches("bounded-right", Arc::clone(&right_schema), [right])
            .unwrap();
        let initial_spill_files = context.metrics.snapshot().spill_files;
        // This is the terminal Grace task after both hash seeds left the single
        // key partition unchanged. A zero tracker budget deterministically
        // exercises the same branch as an exhausted whole-group reservation.
        let task = spill::PartitionTask {
            left: vec![left_file],
            right: vec![right_file],
            build: spill::BuildPartitionStats::rows_only(RIGHT_ROWS),
            depth: spill::MAX_REPARTITION_DEPTH,
            stagnant_repartitions: 2,
        };
        let residual = BoundExpr {
            kind: ExprKind::Binary {
                left: Box::new(BoundExpr::column(1, DataType::Int64, "left_id")),
                op: BinaryOp::Eq,
                right: Box::new(BoundExpr::column(3, DataType::Int64, "right_id")),
            },
            data_type: DataType::Boolean,
            display_name: "left_id = right_id".into(),
        };
        let output_schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::Int64, true),
            Field::new("left_id", DataType::Int64, true),
            Field::new("key", DataType::Int64, join_type == JoinType::Full),
            Field::new("right_id", DataType::Int64, join_type == JoinType::Full),
        ]));
        let batches = sort_merge::fallback_with_tracker_budget(
            task,
            vec![BoundExpr::column(0, DataType::Int64, "left.key")],
            vec![BoundExpr::column(0, DataType::Int64, "right.key")],
            Arc::clone(&left_schema),
            Arc::clone(&right_schema),
            JoinPredicates::new(Some(residual), None, &left_schema, &right_schema),
            join_type,
            output_schema,
            Arc::clone(&context),
            7,
            0,
        )
        .map_ok(|batch| batch.into_public())
        .try_collect::<Vec<_>>()
        .await
        .unwrap();

        let mut matched = Vec::new();
        let mut unmatched_right = Vec::new();
        let mut unmatched_left = Vec::new();
        for batch in &batches {
            let left_ids = batch
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            let right_ids = batch
                .column(3)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            for row in 0..batch.num_rows() {
                match (left_ids.is_null(row), right_ids.is_null(row)) {
                    (false, false) => matched.push((left_ids.value(row), right_ids.value(row))),
                    (true, false) => unmatched_right.push(right_ids.value(row)),
                    (false, true) => unmatched_left.push(left_ids.value(row)),
                    (true, true) => panic!("join emitted an all-NULL row"),
                }
            }
        }
        matched.sort_unstable();
        unmatched_right.sort_unstable();
        unmatched_left.sort_unstable();
        assert_eq!(matched, [(0, 0), (2, 2)]);
        assert_eq!(
            unmatched_right,
            (0..RIGHT_ROWS as i64)
                .filter(|row| *row != 0 && *row != 2)
                .collect::<Vec<_>>()
        );
        if join_type == JoinType::Full {
            assert_eq!(unmatched_left, [RIGHT_ROWS as i64 + 1]);
        } else {
            assert!(unmatched_left.is_empty());
        }

        let metrics = context.metrics.snapshot();
        assert!(metrics.spill_files > initial_spill_files);
        assert!(context.memory.peak() <= MEMORY_LIMIT);
        assert_eq!(context.memory.used(), 0);
        assert_eq!(
            std::fs::read_dir(context.spill.directory())
                .unwrap()
                .filter(|entry| entry.as_ref().is_ok_and(|entry| {
                    entry
                        .path()
                        .extension()
                        .is_some_and(|extension| extension == "arrow")
                }))
                .count(),
            0
        );
    }
}
