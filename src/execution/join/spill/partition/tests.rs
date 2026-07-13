use std::sync::Arc;

use arrow::{
    array::{ArrayRef, Int64Array},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use futures::stream;

use super::{
    PartitionSpiller, Side, batch_logical_buffer_bytes, estimated_build_footprint, spill_stream,
};
use crate::{
    Error,
    runtime::{BatchEnvelope, MemoryPool, QueryContext, boxed_memory_batch_stream},
    sql::{BoundExpr, JoinType},
};

fn batch(value: i64) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("key", DataType::Int64, false)])),
        vec![Arc::new(Int64Array::from(vec![value]))],
    )
    .unwrap()
}

#[test]
fn small_batches_reuse_one_file_per_populated_partition() {
    let root = tempfile::tempdir().unwrap();
    let context = QueryContext::new(MemoryPool::new(32 << 20), root.path()).unwrap();
    let mut spiller = PartitionSpiller::with_partitions(&context, "small-batches", 4);
    for value in 0..100 {
        spiller.write(2, batch(value)).unwrap();
    }
    let partitions = spiller.finish().unwrap();
    assert_eq!(partitions.iter().map(Vec::len).sum::<usize>(), 1);
    assert_eq!(partitions[2].len(), 1);
    for file in partitions.into_iter().flatten() {
        context.spill.remove_file(&file).unwrap();
    }
}

#[tokio::test]
async fn spill_stream_reuses_one_ipc_stream_across_input_batches_for_one_generation() {
    const INPUT_BATCHES: usize = 16;

    let root = tempfile::tempdir().unwrap();
    let context = QueryContext::new(MemoryPool::new(32 << 20), root.path()).unwrap();
    let input = (0..INPUT_BATCHES)
        .map(|value| {
            BatchEnvelope::try_new(
                batch(i64::try_from(value).unwrap()),
                &context.memory,
                "join spill stream input",
            )
        })
        .collect::<crate::Result<Vec<_>>>()
        .unwrap();
    let mut input = boxed_memory_batch_stream(stream::iter(input.into_iter().map(Ok::<_, Error>)));

    let partitions = spill_stream(
        &mut input,
        &[BoundExpr::column(0, DataType::Int64, "key")],
        Side::Right,
        JoinType::Inner,
        false,
        &context,
        "stream-reuse",
        1,
    )
    .await
    .unwrap();
    assert_eq!(partitions.len(), 1);
    assert_eq!(partitions[0].len(), 1);

    let file = &partitions[0][0];
    let spilled = context.spill.read_batches(file).unwrap();
    assert_eq!(spilled.len(), INPUT_BATCHES);
    assert_eq!(
        spilled.iter().map(RecordBatch::num_rows).sum::<usize>(),
        INPUT_BATCHES
    );

    let metrics = context.metrics.snapshot();
    assert_eq!(metrics.spill_files, 1);
    assert_eq!(metrics.active_spill_files, 1);
    assert_eq!(metrics.peak_active_spill_files, 1);

    context.spill.remove_file(file).unwrap();
    assert_eq!(context.metrics.snapshot().active_spill_files, 0);
    assert_eq!(context.memory.used(), 0);
}

#[test]
fn write_amplification_counts_pending_batches_in_one_writer() {
    let root = tempfile::tempdir().unwrap();
    let mut context = QueryContext::new(MemoryPool::new(32 << 20), root.path()).unwrap();
    context.execution.max_spill_write_amplification = Some(5.0);
    let mut spiller = PartitionSpiller::with_partitions(&context, "amplification", 1);
    let logical = batch(1).get_array_memory_size().max(1);
    context.record_spill_logical_input_bytes(u64::try_from(logical).unwrap());
    spiller.write(0, batch(1)).unwrap();
    let error = spiller.write(0, batch(2)).unwrap_err();
    assert!(
        matches!(error, Error::ResourceExhausted(message) if message.contains("write amplification"))
    );
}

#[test]
fn manifest_estimate_includes_retained_data_hash_and_row_keys() {
    let root = tempfile::tempdir().unwrap();
    let context = QueryContext::new(MemoryPool::new(32 << 20), root.path()).unwrap();
    let input = batch(1);
    let physical = u64::try_from(input.get_array_memory_size()).unwrap();
    let data_bytes = u64::try_from(batch_logical_buffer_bytes(&input)).unwrap();
    let mut spiller = PartitionSpiller::with_partitions(&context, "footprint", 2);
    spiller.write(1, input).unwrap();
    let manifest = spiller.finish_manifest().unwrap();
    let stats = manifest.build[1];
    assert_eq!(stats.rows, 1);
    assert!(
        stats.estimated_bytes
            >= usize::try_from(estimated_build_footprint(data_bytes, 1, 1)).unwrap()
    );
    assert_eq!(stats.data_bytes, usize::try_from(data_bytes).unwrap());
    assert_eq!(stats.batches, 1);
    assert_eq!(stats.max_batch_bytes, usize::try_from(physical).unwrap());
    assert!(stats.estimated_bytes > usize::try_from(physical).unwrap());
    for file in manifest.files.into_iter().flatten() {
        context.spill.remove_file(&file).unwrap();
    }
}

#[test]
fn manifest_estimate_covers_the_actual_unique_integer_hash_build() {
    const ROWS: usize = 4_096;

    let root = tempfile::tempdir().unwrap();
    let context = QueryContext::new(MemoryPool::new(64 << 20), root.path()).unwrap();
    let input = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("key", DataType::Int64, false)])),
        vec![Arc::new(Int64Array::from_iter_values(0..ROWS as i64))],
    )
    .unwrap();
    let data_bytes = u64::try_from(batch_logical_buffer_bytes(&input)).unwrap();
    let keys = vec![Arc::clone(input.column(0)) as ArrayRef];
    let mut reservation = context.memory.reservation();
    reservation
        .try_resize(input.get_array_memory_size())
        .unwrap();
    let hash_table =
        crate::execution::join::probe::try_build_hash_table(&keys, ROWS, false, &mut reservation)
            .unwrap()
            .unwrap();
    let estimated = usize::try_from(estimated_build_footprint(data_bytes, ROWS as u64, 1)).unwrap();
    assert!(
        estimated >= reservation.size(),
        "manifest estimate {estimated} is smaller than actual build reservation {}; \
         map capacity {}, CellValue {}, bucket {}",
        reservation.size(),
        hash_table.capacity(),
        std::mem::size_of::<crate::execution::value::CellValue>(),
        std::mem::size_of::<Vec<crate::execution::value::CellValue>>()
            + std::mem::size_of::<Vec<u32>>()
            + 16,
    );
    drop(hash_table);
    reservation.try_resize(0).unwrap();
    assert_eq!(context.memory.used(), 0);
}
