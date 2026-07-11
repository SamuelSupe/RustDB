use std::{collections::HashMap, sync::Arc};

use arrow::{
    array::{Array, Int64Array},
    datatypes::{DataType, Field, Schema, SchemaRef},
    record_batch::RecordBatch,
};
use futures::TryStreamExt;

use super::{join, skew, spill};
use crate::{
    runtime::{MemoryPool, QueryContext, QueryMetricsSnapshot, boxed_record_batch_stream},
    sql::{BoundExpr, JoinType},
};

const MEMORY_LIMIT: usize = 1_536 << 10;
const RIGHT_DUPLICATES: i64 = 12_000;

#[test]
fn partition_spiller_coalesces_many_input_batches_into_bounded_files() {
    const INPUT_BATCHES: i64 = 128;
    const ROWS_PER_BATCH: i64 = 512;

    let temp = tempfile::tempdir().unwrap();
    let context = QueryContext::shared(MemoryPool::new(64 << 20), temp.path()).unwrap();
    let (_, right_schema) = schemas();
    let expressions = [BoundExpr::column(0, DataType::Int64, "right.key")];
    let mut spiller = spill::PartitionSpiller::new(&context, "join-right-test");

    for batch_index in 0..INPUT_BATCHES {
        let start = batch_index * ROWS_PER_BATCH;
        let keys = (start..start + ROWS_PER_BATCH).collect::<Vec<_>>();
        let batch = RecordBatch::try_new(
            Arc::clone(&right_schema),
            vec![
                Arc::new(Int64Array::from(keys.clone())),
                Arc::new(Int64Array::from_iter_values(
                    keys.into_iter().map(|key| key * 3),
                )),
            ],
        )
        .unwrap();
        spill::spill_batch(
            batch,
            &expressions,
            spill::Side::Right,
            JoinType::Inner,
            &mut spiller,
            0,
        )
        .unwrap();
    }

    let right = spiller.finish().unwrap();
    let left = (0..spill::PARTITIONS).map(|_| Vec::new()).collect();
    let tasks = spill::initial_tasks(left, right);
    let physical_files = tasks.iter().map(|task| task.right.len()).sum::<usize>();
    assert!(
        physical_files <= spill::PARTITIONS,
        "{INPUT_BATCHES} input batches created {physical_files} files"
    );
    let spilled_rows = tasks
        .iter()
        .flat_map(|task| &task.right)
        .map(|file| {
            context
                .spill
                .read_batches(file)
                .unwrap()
                .iter()
                .map(RecordBatch::num_rows)
                .sum::<usize>()
        })
        .sum::<usize>();
    assert_eq!(spilled_rows, (INPUT_BATCHES * ROWS_PER_BATCH) as usize);

    context
        .metrics
        .record_spill(0, u64::try_from(tasks.len()).unwrap_or(u64::MAX));
    let metrics = context.metrics.snapshot();
    assert!(metrics.spill_bytes > 0);
    assert_eq!(metrics.spill_partitions, tasks.len() as u64);
    spill::remove_tasks(&context, &tasks);
    assert_eq!(
        std::fs::read_dir(context.spill.directory())
            .unwrap()
            .count(),
        0
    );
    assert_eq!(context.memory.used(), 0);
    assert!(context.memory.peak() > 0);
    assert!(context.memory.peak() <= context.memory.limit());
}

#[test]
fn partition_spiller_chunks_one_batch_larger_than_file_target() {
    const ROWS: usize = 8_192;
    const MEMORY_LIMIT: usize = 64 << 10;
    const TARGET_BYTES: usize = MEMORY_LIMIT / 8;

    let temp = tempfile::tempdir().unwrap();
    let context = QueryContext::shared(MemoryPool::new(MEMORY_LIMIT), temp.path()).unwrap();
    let (_, right_schema) = schemas();
    let batch = RecordBatch::try_new(
        right_schema,
        vec![
            Arc::new(Int64Array::from(vec![7_i64; ROWS])),
            Arc::new(Int64Array::from_iter_values(0..ROWS as i64)),
        ],
    )
    .unwrap();
    assert!(batch.get_array_memory_size() > TARGET_BYTES);

    let mut spiller = spill::PartitionSpiller::new(&context, "join-right-large-batch");
    spill::spill_batch(
        batch,
        &[BoundExpr::column(0, DataType::Int64, "right.key")],
        spill::Side::Right,
        JoinType::Inner,
        &mut spiller,
        0,
    )
    .unwrap();
    let files = spiller
        .finish()
        .unwrap()
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();
    assert!(files.len() > 1, "large partition did not rotate files");

    let mut spilled_rows = 0;
    let mut record_batches = 0;
    for file in &files {
        let batches = context.spill.read_batches(file).unwrap();
        let uncompressed_bytes = batches
            .iter()
            .map(RecordBatch::get_array_memory_size)
            .sum::<usize>();
        let largest_batch = batches
            .iter()
            .map(RecordBatch::get_array_memory_size)
            .max()
            .unwrap_or(0);
        assert!(uncompressed_bytes <= TARGET_BYTES.saturating_add(largest_batch));
        spilled_rows += batches.iter().map(RecordBatch::num_rows).sum::<usize>();
        record_batches += batches.len();
    }
    assert_eq!(spilled_rows, ROWS);
    assert!(files.len() < record_batches, "chunks were not coalesced");
    spill::remove_files(&context, &files);
    assert_eq!(context.memory.used(), 0);
    assert!(context.memory.peak() > 0);
    assert!(context.memory.peak() <= MEMORY_LIMIT);
    assert_eq!(
        std::fs::read_dir(context.spill.directory())
            .unwrap()
            .count(),
        0
    );
}

#[test]
fn partition_spiller_retries_smaller_slices_with_fragmented_budget() {
    const ROWS: usize = 64;
    const MEMORY_LIMIT: usize = 64 << 10;
    const AVAILABLE_FOR_SPILL: usize = 1_024;

    let temp = tempfile::tempdir().unwrap();
    let context = QueryContext::shared(MemoryPool::new(MEMORY_LIMIT), temp.path()).unwrap();
    let (_, right_schema) = schemas();
    let mut spiller = spill::PartitionSpiller::new(&context, "join-fragmented-budget");
    let primer = RecordBatch::try_new(
        Arc::clone(&right_schema),
        vec![
            Arc::new(Int64Array::from(vec![7])),
            Arc::new(Int64Array::from(vec![-1])),
        ],
    )
    .unwrap();
    spill::spill_batch(
        primer,
        &[BoundExpr::column(0, DataType::Int64, "right.key")],
        spill::Side::Right,
        JoinType::Inner,
        &mut spiller,
        0,
    )
    .unwrap();
    let active_file_bytes = context.memory.used();
    let batch = RecordBatch::try_new(
        right_schema,
        vec![
            Arc::new(Int64Array::from(vec![7; ROWS])),
            Arc::new(Int64Array::from_iter_values(
                (0..ROWS as i64).map(|row| row * 3),
            )),
        ],
    )
    .unwrap();
    let held = context
        .memory
        .try_reserve(MEMORY_LIMIT - active_file_bytes - AVAILABLE_FOR_SPILL)
        .unwrap();
    let counts = spill::spill_batch(
        batch,
        &[BoundExpr::column(0, DataType::Int64, "right.key")],
        spill::Side::Right,
        JoinType::Inner,
        &mut spiller,
        0,
    )
    .unwrap();
    assert_eq!(counts.into_iter().sum::<usize>(), ROWS);
    assert!(context.memory.used() > held.size());
    assert!(context.memory.peak() <= MEMORY_LIMIT);
    drop(held);

    let files = spiller
        .finish()
        .unwrap()
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();
    let spilled = files
        .iter()
        .flat_map(|file| context.spill.read_batches(file).unwrap())
        .collect::<Vec<_>>();
    let spilled_rows = spilled.iter().map(RecordBatch::num_rows).sum::<usize>();
    assert_eq!(spilled_rows, ROWS + 1);
    assert!(
        spilled.len() > 4,
        "the constrained reservation did not force smaller spill slices"
    );
    spill::remove_files(&context, &files);
    assert_eq!(context.memory.used(), 0);
}

#[test]
fn partition_spiller_rejects_budget_below_one_row_temporary_state() {
    const MEMORY_LIMIT: usize = 512;

    let temp = tempfile::tempdir().unwrap();
    let context = QueryContext::shared(MemoryPool::new(MEMORY_LIMIT), temp.path()).unwrap();
    let (_, right_schema) = schemas();
    let batch = RecordBatch::try_new(
        right_schema,
        vec![
            Arc::new(Int64Array::from(vec![7])),
            Arc::new(Int64Array::from(vec![21])),
        ],
    )
    .unwrap();
    let mut spiller = spill::PartitionSpiller::new(&context, "join-minimum-state");
    let error = spill::spill_batch(
        batch,
        &[BoundExpr::column(0, DataType::Int64, "right.key")],
        spill::Side::Right,
        JoinType::Inner,
        &mut spiller,
        0,
    )
    .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("minimum one-row temporary state")
    );
    assert_eq!(context.memory.used(), 0);
    assert!(
        spiller
            .finish()
            .unwrap()
            .into_iter()
            .all(|files| files.is_empty())
    );
}

#[test]
fn fragmented_build_batches_use_buffer_footprint_after_compaction() {
    const ROWS: i64 = 2_048;
    const MEMORY_LIMIT: usize = 1 << 20;

    let temp = tempfile::tempdir().unwrap();
    let context = QueryContext::shared(MemoryPool::new(MEMORY_LIMIT), temp.path()).unwrap();
    let (_, right_schema) = schemas();
    let (file, fragmented_array_bytes) = fragmented_build_file(&context, &right_schema, ROWS);
    let old_estimate = fragmented_array_bytes
        .saturating_mul(2)
        .saturating_add(ROWS as usize * 128);
    assert!(
        old_estimate > MEMORY_LIMIT,
        "fixture does not reproduce fragmented array overestimation"
    );

    let mut reservation = context.memory.reservation();
    let loaded = spill::load_build_partition(
        std::slice::from_ref(&file),
        &right_schema,
        &context,
        &mut reservation,
    )
    .unwrap();
    match loaded {
        spill::BuildPartition::Loaded(batch) => assert_eq!(batch.num_rows(), ROWS as usize),
        spill::BuildPartition::TooLarge { .. } => {
            panic!("fragmented buffers caused a false repartition")
        }
    }
    assert!(context.memory.used() <= MEMORY_LIMIT);
    reservation.try_resize(0).unwrap();
    context.spill.remove_file(&file);
}

#[test]
fn build_buffer_footprint_still_rejects_an_oversized_partition() {
    const ROWS: i64 = 2_048;
    const MEMORY_LIMIT: usize = 256 << 10;

    let temp = tempfile::tempdir().unwrap();
    let context = QueryContext::shared(MemoryPool::new(MEMORY_LIMIT), temp.path()).unwrap();
    let (_, right_schema) = schemas();
    let (file, _) = fragmented_build_file(&context, &right_schema, ROWS);
    let mut reservation = context.memory.reservation();
    match spill::load_build_partition(
        std::slice::from_ref(&file),
        &right_schema,
        &context,
        &mut reservation,
    )
    .unwrap()
    {
        spill::BuildPartition::TooLarge { rows } => assert_eq!(rows, ROWS as usize),
        spill::BuildPartition::Loaded(_) => panic!("oversized build partition was loaded"),
    }
    assert!(context.memory.used() > 0);
    context.spill.remove_file(&file);
    assert_eq!(context.memory.used(), 0);
}

#[tokio::test]
async fn seeded_repartition_splits_an_initially_colliding_partition() {
    const ROWS: usize = 12_000;
    let keys = (0_i64..1_000_000)
        .filter(|key| spill::partition_for_key(&[super::CellValue::Int64(*key)], 0) == 0)
        .take(ROWS)
        .collect::<Vec<_>>();
    assert_eq!(keys.len(), ROWS);
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
            Arc::new(Int64Array::from_iter_values(0..ROWS as i64)),
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
    assert_eq!(rows(&batches), ROWS);
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

#[tokio::test]
async fn skew_fallback_rejects_a_decoded_spill_batch_over_budget() {
    const MEMORY_LIMIT: usize = 128 << 10;
    const RIGHT_ROWS: usize = 12_000;
    let (left_schema, right_schema) = schemas();
    let left_batch = RecordBatch::try_new(
        Arc::clone(&left_schema),
        vec![
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(Int64Array::from(vec![10])),
        ],
    )
    .unwrap();
    let right_batch = RecordBatch::try_new(
        Arc::clone(&right_schema),
        vec![
            Arc::new(Int64Array::from(vec![1; RIGHT_ROWS])),
            Arc::new(Int64Array::from_iter_values(0..RIGHT_ROWS as i64)),
        ],
    )
    .unwrap();
    assert!(right_batch.get_array_memory_size() > MEMORY_LIMIT);

    let temp = tempfile::tempdir().unwrap();
    let context = QueryContext::shared(MemoryPool::new(MEMORY_LIMIT), temp.path()).unwrap();
    let left_file = context
        .spill
        .write_record_batches("skew-left-source", Arc::clone(&left_schema), [left_batch])
        .unwrap();
    let right_file = context
        .spill
        .write_record_batches(
            "skew-right-source",
            Arc::clone(&right_schema),
            [right_batch],
        )
        .unwrap();
    let task = spill::PartitionTask {
        left: vec![left_file.clone()],
        right: vec![right_file.clone()],
        depth: spill::MAX_REPARTITION_DEPTH,
    };
    let error = skew::fallback(
        task,
        vec![BoundExpr::column(0, DataType::Int64, "left.key")],
        vec![BoundExpr::column(0, DataType::Int64, "right.key")],
        Arc::clone(&right_schema),
        JoinType::Inner,
        output_schema(JoinType::Inner, &left_schema, &right_schema),
        Arc::clone(&context),
        4,
    )
    .try_collect::<Vec<_>>()
    .await
    .unwrap_err();
    assert!(matches!(
        error,
        crate::Error::ResourceExhausted(message)
            if message.contains("decoded right spill batch")
                && message.contains("query limit 131072 bytes")
    ));

    context.spill.remove_file(&left_file);
    context.spill.remove_file(&right_file);
    assert_eq!(context.memory.used(), 0);
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

fn fragmented_build_file(
    context: &QueryContext,
    schema: &SchemaRef,
    rows: i64,
) -> (crate::runtime::SpillFile, usize) {
    let mut writer = context
        .spill
        .writer("fragmented-build", Arc::clone(schema))
        .unwrap();
    let mut array_bytes = 0usize;
    for row in 0..rows {
        let batch = RecordBatch::try_new(
            Arc::clone(schema),
            vec![
                Arc::new(Int64Array::from(vec![row])),
                Arc::new(Int64Array::from(vec![row * 3])),
            ],
        )
        .unwrap();
        array_bytes = array_bytes.saturating_add(batch.get_array_memory_size());
        writer.write_batch(&batch).unwrap();
    }
    (writer.finish(0).unwrap(), array_bytes)
}
