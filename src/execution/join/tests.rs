use std::{collections::HashMap, sync::Arc};

use arrow::{
    array::{Array, Int64Array},
    datatypes::{DataType, Field, Schema, SchemaRef},
    record_batch::RecordBatch,
};
use futures::TryStreamExt;

use super::{join, spill};
use crate::{
    runtime::{MemoryPool, QueryContext, QueryMetricsSnapshot, boxed_record_batch_stream},
    sql::{BoundExpr, JoinType},
};

const MEMORY_LIMIT: usize = 2_048;
const RIGHT_DUPLICATES: i64 = 40;

#[tokio::test]
async fn seeded_repartition_splits_an_initially_colliding_partition() {
    let keys = (0_i64..100_000)
        .filter(|key| spill::partition_for_key(&[super::CellValue::Int64(*key)], 0) == 0)
        .take(48)
        .collect::<Vec<_>>();
    assert_eq!(keys.len(), 48);
    let mut next_seed_counts = [0usize; spill::PARTITIONS];
    for key in &keys {
        let partition =
            spill::partition_for_key(&[super::CellValue::Int64(*key)], 0x9e37_79b9_7f4a_7c15);
        next_seed_counts[partition] += 1;
    }
    assert!(next_seed_counts.into_iter().max().unwrap() < keys.len());

    let (left_schema, right_schema) = schemas();
    let left_batch = RecordBatch::try_new(
        Arc::clone(&left_schema),
        vec![
            Arc::new(Int64Array::from(keys.clone())),
            Arc::new(Int64Array::from_iter_values(0..keys.len() as i64)),
        ],
    )
    .unwrap();
    let right_batch = RecordBatch::try_new(
        Arc::clone(&right_schema),
        vec![
            Arc::new(Int64Array::from(keys)),
            Arc::new(Int64Array::from_iter_values(0..48)),
        ],
    )
    .unwrap();

    let (batches, metrics) = run_join(
        JoinType::Inner,
        left_schema,
        right_schema,
        left_batch,
        right_batch,
    )
    .await;
    assert_eq!(rows(&batches), 48);
    assert!(metrics.spill_partitions > 0);
}

#[tokio::test]
async fn duplicate_build_key_uses_bounded_skew_fallback() {
    let (batches, metrics) = run_skew_join(JoinType::Inner).await;
    assert!(metrics.spill_partitions > 0);
    assert_eq!(rows(&batches), 2 * RIGHT_DUPLICATES as usize);

    let mut by_left = HashMap::<i64, usize>::new();
    let mut by_right = HashMap::<i64, usize>::new();
    for batch in batches {
        let left_ids = int64(&batch, 1);
        let right_ids = int64(&batch, 3);
        for row in 0..batch.num_rows() {
            *by_left.entry(left_ids.value(row)).or_default() += 1;
            *by_right.entry(right_ids.value(row)).or_default() += 1;
        }
    }
    assert_eq!(
        by_left,
        HashMap::from([
            (10, RIGHT_DUPLICATES as usize),
            (11, RIGHT_DUPLICATES as usize),
        ])
    );
    assert!(by_right.values().all(|count| *count == 2));
    assert_eq!(by_right.len(), RIGHT_DUPLICATES as usize);
}

#[tokio::test]
async fn skew_fallback_left_join_emits_unmatched_and_null_keys() {
    let (batches, _) = run_skew_join(JoinType::Left).await;
    assert_eq!(rows(&batches), 2 * RIGHT_DUPLICATES as usize + 2);

    let mut unmatched = Vec::new();
    for batch in batches {
        let left_ids = int64(&batch, 1);
        let right_ids = int64(&batch, 3);
        for row in 0..batch.num_rows() {
            if right_ids.is_null(row) {
                unmatched.push(left_ids.value(row));
            }
        }
    }
    unmatched.sort_unstable();
    assert_eq!(unmatched, [12, 13]);
}

#[tokio::test]
async fn skew_fallback_semi_and_anti_preserve_left_multiplicity() {
    let (semi, _) = run_skew_join(JoinType::Semi).await;
    assert_eq!(left_ids(&semi), [10, 11]);

    let (anti, _) = run_skew_join(JoinType::Anti).await;
    assert_eq!(left_ids(&anti), [12, 13]);
}

async fn run_skew_join(join_type: JoinType) -> (Vec<RecordBatch>, QueryMetricsSnapshot) {
    let (left_schema, right_schema) = schemas();
    let left_batch = RecordBatch::try_new(
        Arc::clone(&left_schema),
        vec![
            Arc::new(Int64Array::from(vec![Some(1), Some(1), Some(2), None])),
            Arc::new(Int64Array::from(vec![10, 11, 12, 13])),
        ],
    )
    .unwrap();
    let right_batch = RecordBatch::try_new(
        Arc::clone(&right_schema),
        vec![
            Arc::new(Int64Array::from(vec![1; RIGHT_DUPLICATES as usize])),
            Arc::new(Int64Array::from_iter_values(0..RIGHT_DUPLICATES)),
        ],
    )
    .unwrap();
    run_join(
        join_type,
        left_schema,
        right_schema,
        left_batch,
        right_batch,
    )
    .await
}

async fn run_join(
    join_type: JoinType,
    left_schema: SchemaRef,
    right_schema: SchemaRef,
    left_batch: RecordBatch,
    right_batch: RecordBatch,
) -> (Vec<RecordBatch>, QueryMetricsSnapshot) {
    let left = boxed_record_batch_stream(futures::stream::once(async move { Ok(left_batch) }));
    let right = boxed_record_batch_stream(futures::stream::once(async move { Ok(right_batch) }));
    let schema = output_schema(join_type, &left_schema, &right_schema);
    let temp = tempfile::tempdir().unwrap();
    let context = QueryContext::shared(MemoryPool::new(MEMORY_LIMIT), temp.path()).unwrap();
    let stream = join(
        left,
        right,
        vec![(
            BoundExpr::column(0, DataType::Int64, "left.key"),
            BoundExpr::column(0, DataType::Int64, "right.key"),
        )],
        left_schema,
        right_schema,
        join_type,
        schema,
        Arc::clone(&context),
        7,
    );
    let batches = stream.try_collect::<Vec<_>>().await.unwrap();
    assert_eq!(context.memory.used(), 0);
    assert_eq!(
        std::fs::read_dir(context.spill.directory())
            .unwrap()
            .count(),
        0
    );
    (batches, context.metrics.snapshot())
}

fn schemas() -> (SchemaRef, SchemaRef) {
    (
        Arc::new(Schema::new(vec![
            Field::new("key", DataType::Int64, true),
            Field::new("left_id", DataType::Int64, false),
        ])),
        Arc::new(Schema::new(vec![
            Field::new("key", DataType::Int64, false),
            Field::new("right_id", DataType::Int64, false),
        ])),
    )
}

fn output_schema(join_type: JoinType, left: &SchemaRef, right: &SchemaRef) -> SchemaRef {
    if matches!(join_type, JoinType::Semi | JoinType::Anti) {
        return Arc::clone(left);
    }
    let mut fields = left.fields().iter().cloned().collect::<Vec<_>>();
    fields.extend(right.fields().iter().map(|field| {
        Arc::new(Field::new(
            field.name(),
            field.data_type().clone(),
            join_type == JoinType::Left || field.is_nullable(),
        ))
    }));
    Arc::new(Schema::new(fields))
}

fn rows(batches: &[RecordBatch]) -> usize {
    batches.iter().map(RecordBatch::num_rows).sum()
}

fn int64(batch: &RecordBatch, column: usize) -> &Int64Array {
    batch
        .column(column)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
}

fn left_ids(batches: &[RecordBatch]) -> Vec<i64> {
    let mut ids = batches
        .iter()
        .flat_map(|batch| int64(batch, 1).values().iter().copied())
        .collect::<Vec<_>>();
    ids.sort_unstable();
    ids
}
