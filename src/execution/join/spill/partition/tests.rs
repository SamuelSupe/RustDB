use std::sync::Arc;

use arrow::{
    array::Int64Array,
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};

use super::{PartitionSpiller, batch_logical_buffer_bytes, estimated_build_footprint};
use crate::{
    Error,
    runtime::{MemoryPool, QueryContext},
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
    assert_eq!(
        stats.estimated_bytes,
        usize::try_from(estimated_build_footprint(data_bytes, 1)).unwrap()
    );
    assert_eq!(stats.data_bytes, usize::try_from(data_bytes).unwrap());
    assert_eq!(stats.batches, 1);
    assert_eq!(stats.max_batch_bytes, usize::try_from(physical).unwrap());
    assert!(stats.estimated_bytes > usize::try_from(physical).unwrap());
    for file in manifest.files.into_iter().flatten() {
        context.spill.remove_file(&file).unwrap();
    }
}
