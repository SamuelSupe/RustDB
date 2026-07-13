use std::{collections::HashMap, sync::Arc};

use arrow::{
    array::{Array, Float64Array, Int64Array, StringArray},
    datatypes::{DataType, Field, Schema, SchemaRef},
    record_batch::RecordBatch,
};
use futures::TryStreamExt;

use super::{evaluate_keys_accounted, join, join_with_null_keys, sort_merge, spill};
use crate::{
    runtime::{
        BatchEnvelope, MemoryPool, QueryContext, QueryMetricsSnapshot, boxed_record_batch_stream,
    },
    sql::{BoundExpr, JoinType},
};

const MEMORY_LIMIT: usize = 1_536 << 10;
const RIGHT_DUPLICATES: i64 = 12_000;

#[test]
fn direct_column_keys_do_not_charge_the_input_buffer_twice() {
    let schema = Arc::new(Schema::new(vec![Field::new("key", DataType::Utf8, false)]));
    let batch = RecordBatch::try_new(
        schema,
        vec![Arc::new(StringArray::from(vec!["x".repeat(256 << 10)]))],
    )
    .unwrap();
    let input_bytes = batch.get_array_memory_size();
    let temp = tempfile::tempdir().unwrap();
    let context = QueryContext::shared(MemoryPool::new(input_bytes + 4_096), temp.path()).unwrap();
    let input = BatchEnvelope::try_new(batch, &context.memory, "join key input").unwrap();
    let before = context.memory.used();

    let keys = evaluate_keys_accounted(
        &[BoundExpr::column(0, DataType::Utf8, "key")],
        input.batch(),
        &context,
        "join key alias",
    )
    .unwrap();

    assert!(keys.memory_size() < 1_024);
    assert_eq!(context.memory.used(), before + keys.memory_size());
    drop(keys);
    drop(input);
    assert_eq!(context.memory.used(), 0);
}

#[tokio::test]
async fn output_materialization_transfers_its_workspace_into_the_batch_lease() {
    let (left_schema, right_schema) = schemas();
    let left = RecordBatch::try_new(
        Arc::clone(&left_schema),
        vec![
            Arc::new(Int64Array::from(vec![7])),
            Arc::new(Int64Array::from(vec![11])),
        ],
    )
    .unwrap();
    let right = RecordBatch::try_new(
        Arc::clone(&right_schema),
        vec![
            Arc::new(Int64Array::from(vec![7, 7])),
            Arc::new(Int64Array::from(vec![21, 22])),
        ],
    )
    .unwrap();
    let temp = tempfile::tempdir().unwrap();
    let context = QueryContext::shared(MemoryPool::new(1 << 20), temp.path()).unwrap();

    let output = super::output::build_output_envelope(
        &left,
        &right,
        &[0, 0],
        &[Some(0), Some(1)],
        None,
        JoinType::Inner,
        output_schema(JoinType::Inner, &left_schema, &right_schema),
        &context,
        0,
        "join test output",
    )
    .await
    .unwrap();
    assert_eq!(context.memory.used(), output.memory_size());
    drop(output);
    assert_eq!(context.memory.used(), 0);
}

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

    let right = spiller.finish_manifest().unwrap();
    let left = (0..spill::PARTITIONS).map(|_| Vec::new()).collect();
    let tasks = spill::initial_tasks(left, right);
    assert!(
        tasks
            .iter()
            .filter(|task| !task.right.is_empty())
            .all(|task| task.build.estimated_bytes > 0),
        "initial partition manifest did not retain build-byte estimates"
    );
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
    spill::remove_tasks(&context, &tasks).unwrap();
    assert_eq!(
        std::fs::read_dir(context.spill.directory())
            .unwrap()
            .filter(|entry| entry.as_ref().is_ok_and(|entry| entry
                .path()
                .extension()
                .is_some_and(|extension| extension == "arrow")))
            .count(),
        0
    );
    assert_eq!(context.memory.used(), 0);
    assert!(context.memory.peak() > 0);
    assert!(context.memory.peak() <= context.memory.limit());
}

#[test]
fn partition_spiller_chunks_large_batch_without_rotating_small_files() {
    const ROWS: usize = 8_192;
    const MEMORY_LIMIT: usize = 64 << 10;

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
    assert!(batch.get_array_memory_size() > MEMORY_LIMIT);

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
    assert_eq!(files.len(), 1, "one partition generation reuses one writer");

    let mut spilled_rows = 0;
    let mut record_batches = 0;
    for file in &files {
        let batches = context.spill.read_batches(file).unwrap();
        let largest_batch = batches
            .iter()
            .map(RecordBatch::get_array_memory_size)
            .max()
            .unwrap_or(0);
        assert!(largest_batch <= MEMORY_LIMIT);
        spilled_rows += batches.iter().map(RecordBatch::num_rows).sum::<usize>();
        record_batches += batches.len();
    }
    assert_eq!(spilled_rows, ROWS);
    assert!(files.len() < record_batches, "chunks were not coalesced");
    spill::remove_files(&context, &files).unwrap();
    assert_eq!(context.memory.used(), 0);
    assert!(context.memory.peak() > 0);
    assert!(context.memory.peak() <= MEMORY_LIMIT);
    assert_eq!(
        std::fs::read_dir(context.spill.directory())
            .unwrap()
            .filter(|entry| entry.as_ref().is_ok_and(|entry| entry
                .path()
                .extension()
                .is_some_and(|extension| extension == "arrow")))
            .count(),
        0
    );
}

#[test]
fn partition_spiller_retries_smaller_slices_with_fragmented_budget() {
    const ROWS: usize = 64;
    const MEMORY_LIMIT: usize = 64 << 10;
    const AVAILABLE_FOR_TEMPORARY: usize = 1_024;

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
        .try_reserve(
            MEMORY_LIMIT
                - active_file_bytes
                - AVAILABLE_FOR_TEMPORARY
                - context.spill.write_copy_headroom_bytes(),
        )
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
    spill::remove_files(&context, &files).unwrap();
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
    let fragmented = fragmented_build_file(&context, &right_schema, ROWS);
    let old_estimate = fragmented
        .array_bytes
        .saturating_mul(2)
        .saturating_add(ROWS as usize * 128);
    assert!(
        old_estimate > MEMORY_LIMIT,
        "fixture does not reproduce fragmented array overestimation"
    );

    let mut reservation = context.memory.reservation();
    let read_before = context.metrics.snapshot().spill_read_bytes;
    let loaded = spill::load_build_partition(
        std::slice::from_ref(&fragmented.file),
        &right_schema,
        &context,
        &mut reservation,
        fragmented.stats(ROWS as usize),
    )
    .unwrap();
    match loaded {
        spill::BuildPartition::Loaded(batch) => assert_eq!(batch.num_rows(), ROWS as usize),
        spill::BuildPartition::TooLarge { .. } => {
            panic!("fragmented buffers caused a false repartition")
        }
    }
    let first_read = context
        .metrics
        .snapshot()
        .spill_read_bytes
        .saturating_sub(read_before);
    assert!(context.memory.used() <= MEMORY_LIMIT);
    reservation.try_resize(0).unwrap();
    let second_read_before = context.metrics.snapshot().spill_read_bytes;
    drop(context.spill.read_batches(&fragmented.file).unwrap());
    let second_read = context
        .metrics
        .snapshot()
        .spill_read_bytes
        .saturating_sub(second_read_before);
    assert_eq!(
        first_read, second_read,
        "build partition was read more than once"
    );
    context.spill.remove_file(&fragmented.file).unwrap();
}

#[test]
fn actual_hash_footprint_rejects_an_oversized_partition() {
    const ROWS: i64 = 2_048;
    const MEMORY_LIMIT: usize = 256 << 10;

    let temp = tempfile::tempdir().unwrap();
    let context = QueryContext::shared(MemoryPool::new(MEMORY_LIMIT), temp.path()).unwrap();
    let (_, right_schema) = schemas();
    let fragmented = fragmented_build_file(&context, &right_schema, ROWS);
    let mut reservation = context.memory.reservation();
    let loaded = spill::load_build_partition(
        std::slice::from_ref(&fragmented.file),
        &right_schema,
        &context,
        &mut reservation,
        fragmented.stats(ROWS as usize),
    )
    .unwrap();
    let spill::BuildPartition::Loaded(batch) = loaded else {
        panic!("the compacted batch itself should fit before hash-key accounting")
    };
    let keys = super::evaluate_keys(
        &[BoundExpr::column(0, DataType::Int64, "right.key")],
        &batch,
    )
    .unwrap();
    assert!(
        super::try_build_hash_table(&keys, batch.num_rows(), false, &mut reservation)
            .unwrap()
            .is_none()
    );
    assert!(context.memory.used() > 0);
    drop(batch);
    reservation.try_resize(0).unwrap();
    context.spill.remove_file(&fragmented.file).unwrap();
    assert_eq!(context.memory.used(), 0);
}

#[tokio::test]
async fn seeded_repartition_splits_an_initially_colliding_partition() {
    const ROWS: usize = 12_000;
    let keys = (0_i64..4_000_000)
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
async fn skew_fallback_full_join_emits_unmatched_build_rows() {
    let (batches, metrics) = run_skew_join(JoinType::Full).await;
    assert!(metrics.spill_partitions > 0);
    assert_eq!(rows(&batches), 2 * RIGHT_DUPLICATES as usize + 3);

    let unmatched_build = batches
        .iter()
        .flat_map(|batch| {
            let left_ids = int64(batch, 1);
            let right_ids = int64(batch, 3);
            (0..batch.num_rows())
                .filter(|row| left_ids.is_null(*row) && !right_ids.is_null(*row))
                .map(|row| right_ids.value(row))
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    assert_eq!(unmatched_build, [RIGHT_DUPLICATES]);
}

#[tokio::test]
async fn right_and_full_join_emit_unmatched_build_rows() {
    let (left_schema, right_schema) = schemas();
    let left = RecordBatch::try_new(
        Arc::clone(&left_schema),
        vec![
            Arc::new(Int64Array::from(vec![Some(1), Some(2), None])),
            Arc::new(Int64Array::from(vec![10, 20, 30])),
        ],
    )
    .unwrap();
    let right = RecordBatch::try_new(
        Arc::clone(&right_schema),
        vec![
            Arc::new(Int64Array::from(vec![2, 3, 4])),
            Arc::new(Int64Array::from(vec![200, 300, 400])),
        ],
    )
    .unwrap();

    let (right_rows, _) = run_join(
        JoinType::Right,
        Arc::clone(&left_schema),
        Arc::clone(&right_schema),
        left.clone(),
        right.clone(),
    )
    .await;
    assert_eq!(rows(&right_rows), 3);
    assert_eq!(right_ids(&right_rows), [200, 300, 400]);

    let (full_rows, _) = run_join(JoinType::Full, left_schema, right_schema, left, right).await;
    assert_eq!(rows(&full_rows), 5);
    assert_eq!(right_ids(&full_rows), [200, 300, 400]);
    let unmatched_left = full_rows
        .iter()
        .flat_map(|batch| {
            let left_ids = int64(batch, 1);
            let right_ids = int64(batch, 3);
            (0..batch.num_rows())
                .filter(|row| right_ids.is_null(*row))
                .map(|row| left_ids.value(row))
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    assert_eq!(unmatched_left, [10, 30]);
}

#[tokio::test]
async fn null_equal_semi_join_preserves_null_through_grace_spill() {
    const ROWS: i64 = 24_000;
    let schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::Int64, true),
        Field::new("value", DataType::Int64, false),
    ]));
    let keys = (0..ROWS)
        .map(Some)
        .chain(std::iter::once(None))
        .collect::<Vec<_>>();
    let batch = || {
        RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(keys.clone())),
                Arc::new(Int64Array::from_iter_values(0..=ROWS)),
            ],
        )
        .unwrap()
    };
    let left = boxed_record_batch_stream(futures::stream::once({
        let batch = batch();
        async move { Ok(batch) }
    }));
    let right = boxed_record_batch_stream(futures::stream::once({
        let batch = batch();
        async move { Ok(batch) }
    }));
    let temp = tempfile::tempdir().unwrap();
    let context = QueryContext::shared(MemoryPool::new(2 << 20), temp.path()).unwrap();
    let batches = join_with_null_keys(
        left,
        right,
        vec![(
            BoundExpr::column(0, DataType::Int64, "left.key"),
            BoundExpr::column(0, DataType::Int64, "right.key"),
        )],
        true,
        None,
        None,
        Arc::clone(&schema),
        Arc::clone(&schema),
        JoinType::Semi,
        schema,
        Arc::clone(&context),
        256,
    )
    .map_ok(|batch| batch.into_public())
    .try_collect::<Vec<_>>()
    .await
    .unwrap();
    assert_eq!(rows(&batches), ROWS as usize + 1);
    assert!(context.metrics.snapshot().spill_partitions > 0);
    assert_eq!(
        batches
            .iter()
            .map(|batch| int64(batch, 0).null_count())
            .sum::<usize>(),
        1
    );
    assert_eq!(context.memory.used(), 0);
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
        build: spill::BuildPartitionStats::rows_only(RIGHT_ROWS),
        depth: spill::MAX_REPARTITION_DEPTH,
        stagnant_repartitions: 0,
    };
    let error = sort_merge::fallback(
        task,
        vec![BoundExpr::column(0, DataType::Int64, "left.key")],
        vec![BoundExpr::column(0, DataType::Int64, "right.key")],
        Arc::clone(&left_schema),
        Arc::clone(&right_schema),
        super::condition::JoinPredicates::new(None, None, &left_schema, &right_schema),
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
            if message.contains("join sort right batch")
                && message.contains("query limit 131072")
    ));

    context.spill.remove_file(&left_file).unwrap();
    context.spill.remove_file(&right_file).unwrap();
    assert_eq!(context.memory.used(), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn small_frozen_build_is_probed_by_multiple_lanes() {
    const LANES: usize = 4;
    const KEYS: i64 = 1_024;
    const LEFT_BATCHES: i64 = 32;
    const ROWS_PER_BATCH: i64 = 2_048;

    let (left_schema, right_schema) = schemas();
    let left_batches = (0..LEFT_BATCHES)
        .map(|batch| {
            let start = batch * ROWS_PER_BATCH;
            Ok(RecordBatch::try_new(
                Arc::clone(&left_schema),
                vec![
                    Arc::new(Int64Array::from_iter_values(
                        (start..start + ROWS_PER_BATCH).map(|row| row % KEYS),
                    )),
                    Arc::new(Int64Array::from_iter_values(start..start + ROWS_PER_BATCH)),
                ],
            )
            .unwrap())
        })
        .collect::<Vec<_>>();
    let right_batch = RecordBatch::try_new(
        Arc::clone(&right_schema),
        vec![
            Arc::new(Int64Array::from_iter_values(0..KEYS)),
            Arc::new(Int64Array::from_iter_values(0..KEYS)),
        ],
    )
    .unwrap();
    let left = boxed_record_batch_stream(futures::stream::iter(left_batches));
    let right = boxed_record_batch_stream(futures::stream::once(async move { Ok(right_batch) }));
    let schema = output_schema(JoinType::Inner, &left_schema, &right_schema);
    let temp = tempfile::tempdir().unwrap();
    let context = QueryContext::shared(MemoryPool::new(128 << 20), temp.path()).unwrap();
    context.configure_compute_lanes(LANES);

    let batches = join(
        left,
        right,
        vec![(
            BoundExpr::column(0, DataType::Int64, "left.key"),
            BoundExpr::column(0, DataType::Int64, "right.key"),
        )],
        None,
        None,
        Arc::clone(&left_schema),
        Arc::clone(&right_schema),
        JoinType::Inner,
        schema,
        Arc::clone(&context),
        512,
    )
    .map_ok(|batch| batch.into_public())
    .try_collect::<Vec<_>>()
    .await
    .unwrap();

    assert_eq!(rows(&batches), (LEFT_BATCHES * ROWS_PER_BATCH) as usize);
    let peak = context.metrics.snapshot().peak_active_lanes;
    assert!((2..=LANES as u64).contains(&peak), "unexpected peak {peak}");
    assert_eq!(context.memory.used(), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn grace_partitions_preserve_every_join_type_with_parallel_workers() {
    for (join_type, expected_rows) in [
        (JoinType::Inner, 12_000),
        (JoinType::Left, 12_002),
        (JoinType::Semi, 12_000),
        (JoinType::Anti, 2),
    ] {
        let (actual_rows, metrics) = run_parallel_grace_join(join_type).await;
        assert_eq!(actual_rows, expected_rows, "unexpected {join_type:?} rows");
        assert!((2..=4).contains(&metrics.peak_active_lanes));
        assert!(metrics.spill_partitions > 1);
    }
}

#[tokio::test]
async fn long_string_hash_keys_are_accounted_and_switch_to_grace_join() {
    const ROWS: usize = 128;
    const KEY_BYTES: usize = 16 << 10;
    let left_schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::Utf8, false),
        Field::new("left_id", DataType::Int64, false),
    ]));
    let right_schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::Utf8, false),
        Field::new("right_id", DataType::Int64, false),
    ]));
    let keys = (0..ROWS)
        .map(|row| format!("{row:04}-{}", "x".repeat(KEY_BYTES)))
        .collect::<Vec<_>>();
    let left_batch = RecordBatch::try_new(
        Arc::clone(&left_schema),
        vec![
            Arc::new(StringArray::from(vec![keys[3].as_str(), keys[97].as_str()])),
            Arc::new(Int64Array::from(vec![3, 97])),
        ],
    )
    .unwrap();
    let right_batch = RecordBatch::try_new(
        Arc::clone(&right_schema),
        vec![
            Arc::new(StringArray::from(keys)),
            Arc::new(Int64Array::from_iter_values(0..ROWS as i64)),
        ],
    )
    .unwrap();
    let memory_limit = right_batch
        .get_array_memory_size()
        .saturating_mul(2)
        .saturating_add(1 << 10);
    let left = boxed_record_batch_stream(futures::stream::once(async move { Ok(left_batch) }));
    let right = boxed_record_batch_stream(futures::stream::once(async move { Ok(right_batch) }));
    let output_schema = output_schema(JoinType::Inner, &left_schema, &right_schema);
    let temp = tempfile::tempdir().unwrap();
    let context = QueryContext::shared(MemoryPool::new(memory_limit), temp.path()).unwrap();
    let batches = join(
        left,
        right,
        vec![(
            BoundExpr::column(0, DataType::Utf8, "left.key"),
            BoundExpr::column(0, DataType::Utf8, "right.key"),
        )],
        None,
        None,
        Arc::clone(&left_schema),
        Arc::clone(&right_schema),
        JoinType::Inner,
        output_schema,
        Arc::clone(&context),
        64,
    )
    .map_ok(|batch| batch.into_public())
    .try_collect::<Vec<_>>()
    .await
    .unwrap();

    assert_eq!(rows(&batches), 2);
    assert!(context.metrics.snapshot().spill_partitions > 0);
    assert!(context.memory.peak() <= memory_limit);
    assert_eq!(context.memory.used(), 0);
}

#[tokio::test]
async fn float_key_sort_merge_matches_hash_join_for_zero_and_nan() {
    let negative_nan = f64::from_bits(0xfff8_0000_0000_0001);
    let nan_a = f64::from_bits(0x7ff8_0000_0000_0001);
    let nan_b = f64::from_bits(0x7ff8_0000_0000_0002);
    let left_schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::Float64, false),
        Field::new("left_id", DataType::Int64, false),
    ]));
    let right_schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::Float64, false),
        Field::new("right_id", DataType::Int64, false),
    ]));
    let left_batch = RecordBatch::try_new(
        Arc::clone(&left_schema),
        vec![
            Arc::new(Float64Array::from(vec![
                negative_nan,
                -0.0,
                0.0,
                nan_a,
                nan_b,
                1.0,
            ])),
            Arc::new(Int64Array::from(vec![9, 10, 11, 12, 13, 14])),
        ],
    )
    .unwrap();
    let right_batch = RecordBatch::try_new(
        Arc::clone(&right_schema),
        vec![
            Arc::new(Float64Array::from(vec![0.0, -0.0, nan_b, nan_a])),
            Arc::new(Int64Array::from(vec![20, 21, 22, 23])),
        ],
    )
    .unwrap();
    let schema = output_schema(JoinType::Inner, &left_schema, &right_schema);

    let hash_temp = tempfile::tempdir().unwrap();
    let hash_context = QueryContext::shared(MemoryPool::new(64 << 20), hash_temp.path()).unwrap();
    let hash = join(
        boxed_record_batch_stream(futures::stream::once({
            let left_batch = left_batch.clone();
            async move { Ok(left_batch) }
        })),
        boxed_record_batch_stream(futures::stream::once({
            let right_batch = right_batch.clone();
            async move { Ok(right_batch) }
        })),
        vec![(
            BoundExpr::column(0, DataType::Float64, "left.key"),
            BoundExpr::column(0, DataType::Float64, "right.key"),
        )],
        None,
        None,
        Arc::clone(&left_schema),
        Arc::clone(&right_schema),
        JoinType::Inner,
        Arc::clone(&schema),
        hash_context,
        4,
    )
    .map_ok(|batch| batch.into_public())
    .try_collect::<Vec<_>>()
    .await
    .unwrap();

    let merge_temp = tempfile::tempdir().unwrap();
    let merge_context = QueryContext::shared(MemoryPool::new(64 << 20), merge_temp.path()).unwrap();
    let left_file = merge_context
        .spill
        .write_record_batches("float-left", Arc::clone(&left_schema), [left_batch])
        .unwrap();
    let right_file = merge_context
        .spill
        .write_record_batches("float-right", Arc::clone(&right_schema), [right_batch])
        .unwrap();
    let task = spill::PartitionTask {
        left: vec![left_file],
        right: vec![right_file],
        build: spill::BuildPartitionStats::rows_only(4),
        depth: spill::MAX_REPARTITION_DEPTH,
        stagnant_repartitions: 0,
    };
    let merged = sort_merge::fallback(
        task,
        vec![BoundExpr::column(0, DataType::Float64, "left.key")],
        vec![BoundExpr::column(0, DataType::Float64, "right.key")],
        Arc::clone(&left_schema),
        Arc::clone(&right_schema),
        super::condition::JoinPredicates::new(None, None, &left_schema, &right_schema),
        JoinType::Inner,
        schema,
        Arc::clone(&merge_context),
        4,
    )
    .map_ok(|batch| batch.into_public())
    .try_collect::<Vec<_>>()
    .await
    .unwrap();

    assert_eq!(join_pairs(&merged), join_pairs(&hash));
    assert_eq!(join_pairs(&hash).len(), 10);
    assert_eq!(merge_context.memory.used(), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn abandoning_parallel_probe_releases_frozen_build_memory() {
    let (left_schema, right_schema) = schemas();
    let input_left_schema = Arc::clone(&left_schema);
    let left_batches = (0..32_i64).map(move |batch| {
        let start = batch * 2_048;
        Ok(RecordBatch::try_new(
            Arc::clone(&input_left_schema),
            vec![
                Arc::new(Int64Array::from(vec![1; 2_048])),
                Arc::new(Int64Array::from_iter_values(start..start + 2_048)),
            ],
        )
        .unwrap())
    });
    let right_batch = RecordBatch::try_new(
        Arc::clone(&right_schema),
        vec![
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(Int64Array::from(vec![7])),
        ],
    )
    .unwrap();
    let left = boxed_record_batch_stream(futures::stream::iter(left_batches));
    let right = boxed_record_batch_stream(futures::stream::once(async move { Ok(right_batch) }));
    let schema = output_schema(JoinType::Inner, &left_schema, &right_schema);
    let temp = tempfile::tempdir().unwrap();
    let context = QueryContext::shared(MemoryPool::new(128 << 20), temp.path()).unwrap();
    context.configure_compute_lanes(4);
    let mut output = join(
        left,
        right,
        vec![(
            BoundExpr::column(0, DataType::Int64, "left.key"),
            BoundExpr::column(0, DataType::Int64, "right.key"),
        )],
        None,
        None,
        left_schema,
        right_schema,
        JoinType::Inner,
        schema,
        Arc::clone(&context),
        64,
    );

    let first = output.try_next().await.unwrap().unwrap();
    drop(first);
    drop(output);
    context.cleanup_spill_after_tasks().await.unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while context.memory.used() != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("parallel join lanes did not release their batch/build leases");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn abandoning_parallel_grace_join_cleans_files_and_leases() {
    const ROWS: i64 = 20_000;
    let (left_schema, right_schema) = schemas();
    let left_batch = RecordBatch::try_new(
        Arc::clone(&left_schema),
        vec![
            Arc::new(Int64Array::from_iter_values(0..ROWS)),
            Arc::new(Int64Array::from_iter_values(0..ROWS)),
        ],
    )
    .unwrap();
    let right_batch = RecordBatch::try_new(
        Arc::clone(&right_schema),
        vec![
            Arc::new(Int64Array::from_iter_values(0..ROWS)),
            Arc::new(Int64Array::from_iter_values(0..ROWS)),
        ],
    )
    .unwrap();
    let left = boxed_record_batch_stream(futures::stream::once(async move { Ok(left_batch) }));
    let right = boxed_record_batch_stream(futures::stream::once(async move { Ok(right_batch) }));
    let schema = output_schema(JoinType::Inner, &left_schema, &right_schema);
    let temp = tempfile::tempdir().unwrap();
    let context = QueryContext::shared(MemoryPool::new(2 << 20), temp.path()).unwrap();
    context.configure_compute_lanes_unbounded_for_test(4);
    let spill_directory = context.spill.directory().to_path_buf();
    let mut output = join(
        left,
        right,
        vec![(
            BoundExpr::column(0, DataType::Int64, "left.key"),
            BoundExpr::column(0, DataType::Int64, "right.key"),
        )],
        None,
        None,
        left_schema,
        right_schema,
        JoinType::Inner,
        schema,
        Arc::clone(&context),
        64,
    );

    let first = output.try_next().await.unwrap().unwrap();
    drop(first);
    drop(output);
    context.cleanup_spill_after_tasks().await.unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while context.memory.used() != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("parallel Grace join lanes did not release their reservations");
    drop(context);
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
    while spill_directory.exists() && std::time::Instant::now() < deadline {
        std::thread::sleep(std::time::Duration::from_millis(5));
    }
    assert!(!spill_directory.exists());
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
    let mut right_keys = vec![1; RIGHT_DUPLICATES as usize];
    right_keys.push(3);
    let right_batch = RecordBatch::try_new(
        Arc::clone(&right_schema),
        vec![
            Arc::new(Int64Array::from(right_keys)),
            Arc::new(Int64Array::from_iter_values(0..=RIGHT_DUPLICATES)),
        ],
    )
    .unwrap();
    run_join_with_limit(
        join_type,
        left_schema,
        right_schema,
        left_batch,
        right_batch,
        384 << 10,
    )
    .await
}

async fn run_parallel_grace_join(join_type: JoinType) -> (usize, QueryMetricsSnapshot) {
    const RIGHT_ROWS: i64 = 12_000;
    let (left_schema, right_schema) = schemas();
    let mut left_batches = (0..RIGHT_ROWS)
        .step_by(512)
        .map(|start| {
            let end = (start + 512).min(RIGHT_ROWS);
            RecordBatch::try_new(
                Arc::clone(&left_schema),
                vec![
                    Arc::new(Int64Array::from_iter_values(start..end)),
                    Arc::new(Int64Array::from_iter_values(start..end)),
                ],
            )
            .unwrap()
        })
        .collect::<Vec<_>>();
    left_batches.push(
        RecordBatch::try_new(
            Arc::clone(&left_schema),
            vec![
                Arc::new(Int64Array::from(vec![Some(RIGHT_ROWS), None])),
                Arc::new(Int64Array::from(vec![RIGHT_ROWS, RIGHT_ROWS + 1])),
            ],
        )
        .unwrap(),
    );
    let right_batch = RecordBatch::try_new(
        Arc::clone(&right_schema),
        vec![
            Arc::new(Int64Array::from_iter_values(0..RIGHT_ROWS)),
            Arc::new(Int64Array::from_iter_values(0..RIGHT_ROWS)),
        ],
    )
    .unwrap();
    let left = boxed_record_batch_stream(futures::stream::iter(left_batches.into_iter().map(Ok)));
    let right = boxed_record_batch_stream(futures::stream::once(async move { Ok(right_batch) }));
    let schema = output_schema(join_type, &left_schema, &right_schema);
    let temp = tempfile::tempdir().unwrap();
    let mut query = QueryContext::new(MemoryPool::new(MEMORY_LIMIT), temp.path()).unwrap();
    // This case verifies concurrent Grace workers. Use enough partitions for
    // two conservative build-footprint permits to coexist under the shared
    // half-query build-admission budget.
    query.execution.spill_partition_target_bytes = Some(64 << 10);
    let context = Arc::new(query);
    context.configure_compute_lanes_unbounded_for_test(4);
    let batches = join(
        left,
        right,
        vec![(
            BoundExpr::column(0, DataType::Int64, "left.key"),
            BoundExpr::column(0, DataType::Int64, "right.key"),
        )],
        None,
        None,
        left_schema,
        right_schema,
        join_type,
        schema,
        Arc::clone(&context),
        128,
    )
    .map_ok(|batch| batch.into_public())
    .try_collect::<Vec<_>>()
    .await
    .unwrap();
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
    (rows(&batches), context.metrics.snapshot())
}

async fn run_join(
    join_type: JoinType,
    left_schema: SchemaRef,
    right_schema: SchemaRef,
    left_batch: RecordBatch,
    right_batch: RecordBatch,
) -> (Vec<RecordBatch>, QueryMetricsSnapshot) {
    run_join_with_limit(
        join_type,
        left_schema,
        right_schema,
        left_batch,
        right_batch,
        MEMORY_LIMIT,
    )
    .await
}

async fn run_join_with_limit(
    join_type: JoinType,
    left_schema: SchemaRef,
    right_schema: SchemaRef,
    left_batch: RecordBatch,
    right_batch: RecordBatch,
    memory_limit: usize,
) -> (Vec<RecordBatch>, QueryMetricsSnapshot) {
    let left = boxed_record_batch_stream(futures::stream::once(async move { Ok(left_batch) }));
    let right = boxed_record_batch_stream(futures::stream::once(async move { Ok(right_batch) }));
    let schema = output_schema(join_type, &left_schema, &right_schema);
    let temp = tempfile::tempdir().unwrap();
    let context = QueryContext::shared(MemoryPool::new(memory_limit), temp.path()).unwrap();
    let stream = join(
        left,
        right,
        vec![(
            BoundExpr::column(0, DataType::Int64, "left.key"),
            BoundExpr::column(0, DataType::Int64, "right.key"),
        )],
        None,
        None,
        left_schema,
        right_schema,
        join_type,
        schema,
        Arc::clone(&context),
        7,
    );
    let batches = stream
        .map_ok(|batch| batch.into_public())
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_eq!(context.memory.used(), 0);
    assert_eq!(
        std::fs::read_dir(context.spill.directory())
            .unwrap()
            .filter(|entry| entry.as_ref().is_ok_and(|entry| entry
                .path()
                .extension()
                .is_some_and(|extension| extension == "arrow")))
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
    if matches!(
        join_type,
        JoinType::Semi | JoinType::Anti | JoinType::NullAwareAnti
    ) {
        return Arc::clone(left);
    }
    let mut fields = left
        .fields()
        .iter()
        .map(|field| {
            Arc::new(Field::new(
                field.name(),
                field.data_type().clone(),
                matches!(join_type, JoinType::Right | JoinType::Full) || field.is_nullable(),
            ))
        })
        .collect::<Vec<_>>();
    if join_type == JoinType::Mark {
        fields.push(Arc::new(Field::new("marker", DataType::Boolean, true)));
        return Arc::new(Schema::new(fields));
    }
    fields.extend(right.fields().iter().map(|field| {
        Arc::new(Field::new(
            field.name(),
            field.data_type().clone(),
            matches!(
                join_type,
                JoinType::Left | JoinType::Full | JoinType::LeftSingle
            ) || field.is_nullable(),
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

fn right_ids(batches: &[RecordBatch]) -> Vec<i64> {
    let mut ids = batches
        .iter()
        .flat_map(|batch| {
            let values = int64(batch, 3);
            (0..values.len())
                .filter(|row| !values.is_null(*row))
                .map(|row| values.value(row))
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    ids.sort_unstable();
    ids
}

fn join_pairs(batches: &[RecordBatch]) -> Vec<(i64, i64)> {
    let mut pairs = batches
        .iter()
        .flat_map(|batch| {
            let left = int64(batch, 1);
            let right = int64(batch, 3);
            (0..batch.num_rows())
                .map(|row| (left.value(row), right.value(row)))
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    pairs.sort_unstable();
    pairs
}

struct FragmentedBuildFile {
    file: crate::runtime::SpillFile,
    array_bytes: usize,
    data_bytes: usize,
    batches: usize,
    max_batch_bytes: usize,
}

impl FragmentedBuildFile {
    fn stats(&self, rows: usize) -> spill::BuildPartitionStats {
        spill::BuildPartitionStats {
            estimated_bytes: usize::try_from(spill::estimated_build_footprint(
                u64::try_from(self.data_bytes).unwrap(),
                u64::try_from(rows).unwrap(),
            ))
            .unwrap(),
            data_bytes: self.data_bytes,
            batches: self.batches,
            max_batch_bytes: self.max_batch_bytes,
            rows,
        }
    }
}

fn fragmented_build_file(
    context: &QueryContext,
    schema: &SchemaRef,
    rows: i64,
) -> FragmentedBuildFile {
    let mut writer = context
        .spill
        .writer("fragmented-build", Arc::clone(schema))
        .unwrap();
    let mut array_bytes = 0usize;
    let mut data_bytes = 0usize;
    let mut batches = 0usize;
    let mut max_batch_bytes = 0usize;
    for row in 0..rows {
        let batch = RecordBatch::try_new(
            Arc::clone(schema),
            vec![
                Arc::new(Int64Array::from(vec![row])),
                Arc::new(Int64Array::from(vec![row * 3])),
            ],
        )
        .unwrap();
        let batch_bytes = batch.get_array_memory_size();
        array_bytes = array_bytes.saturating_add(batch_bytes);
        data_bytes = data_bytes.saturating_add(spill::batch_logical_buffer_bytes(&batch));
        batches = batches.saturating_add(1);
        max_batch_bytes = max_batch_bytes.max(batch_bytes);
        writer.write_batch(&batch).unwrap();
    }
    FragmentedBuildFile {
        file: writer.finish(0).unwrap(),
        array_bytes,
        data_bytes,
        batches,
        max_batch_bytes,
    }
}
