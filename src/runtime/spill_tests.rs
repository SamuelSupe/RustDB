use std::{collections::HashMap, sync::Arc};

use arrow::{
    array::{ArrayRef, Int32Array, StringArray},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use futures::StreamExt;

use super::metadata::{ACTIVE_FILE_METADATA_BASE_BYTES, active_file_metadata_bytes};
use super::{MemoryPool, QueryControl, QueryMetrics, SpillManager, writer_memory_bytes};
use crate::Error;

fn batch() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef,
            Arc::new(StringArray::from(vec!["a", "b", "c"])) as ArrayRef,
        ],
    )
    .expect("valid batch")
}

#[test]
fn round_trips_lz4_ipc_and_cleans_on_drop() {
    let root = tempfile::tempdir().expect("tempdir");
    let batch = batch();
    let directory;
    let memory = MemoryPool::new(64 * ACTIVE_FILE_METADATA_BASE_BYTES);
    {
        let manager = SpillManager::new(root.path(), memory.clone()).expect("spill manager");
        directory = manager.directory().to_owned();
        let spill = manager
            .write_record_batches("sort/run", batch.schema(), vec![batch.clone()])
            .expect("write spill");
        let read = manager.read_batches(&spill).expect("read spill");
        assert_eq!(read, vec![batch]);
        assert!(spill.path().starts_with(&directory));
        assert_eq!(memory.used(), active_file_metadata_bytes(spill.path()));

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(spill.path())
                .expect("metadata")
                .permissions()
                .mode()
                & 0o777;
            assert_eq!(mode, 0o600);
        }
    }
    assert!(!directory.exists());
    assert_eq!(memory.used(), 0);
    assert!(memory.peak() <= memory.limit());
}

#[test]
fn incremental_writer_appends_batches_and_drops_unfinished_files() {
    let root = tempfile::tempdir().expect("tempdir");
    let control = QueryControl::new();
    let metrics = QueryMetrics::new();
    let memory = MemoryPool::new(64 * ACTIVE_FILE_METADATA_BASE_BYTES);
    let manager = SpillManager::for_query(
        root.path(),
        uuid::Uuid::new_v4(),
        &control,
        memory.clone(),
        Some(metrics.clone()),
    )
    .expect("spill manager");
    let batch = batch();
    let writer_bytes = writer_memory_bytes(batch.schema().as_ref());

    let mut writer = manager
        .writer("join-p0", batch.schema())
        .expect("incremental writer");
    writer.write_batch(&batch).expect("first batch");
    writer.write_batch(&batch).expect("second batch");
    let completed = writer.finish(0).expect("finish spill file");
    let completed_bytes = active_file_metadata_bytes(completed.path());
    assert_eq!(memory.used(), completed_bytes);
    let read = manager.read_batches(&completed).expect("read spill");
    assert_eq!(read, vec![batch.clone(), batch.clone()]);
    assert!(metrics.snapshot().spill_bytes > 0);
    assert_eq!(metrics.snapshot().spill_partitions, 0);

    let mut unfinished = manager
        .writer("join-p1", batch.schema())
        .expect("second incremental writer");
    unfinished.write_batch(&batch).expect("unfinished batch");
    let unfinished_path = unfinished.path().to_owned();
    assert!(unfinished_path.exists());
    let unfinished_bytes = active_file_metadata_bytes(&unfinished_path);
    assert_eq!(
        memory.used(),
        completed_bytes + unfinished_bytes + writer_bytes
    );
    drop(unfinished);
    assert!(!unfinished_path.exists());
    assert_eq!(memory.used(), completed_bytes);

    manager.remove_file(&completed);
    assert_eq!(memory.used(), 0);
    assert_eq!(
        std::fs::read_dir(manager.directory())
            .expect("spill directory")
            .count(),
        0
    );
}

#[test]
fn streaming_writer_keeps_many_batch_blocks_out_of_a_file_footer() {
    const BATCHES: usize = 4_096;
    let root = tempfile::tempdir().expect("tempdir");
    let memory = MemoryPool::new(2 << 20);
    let manager = SpillManager::new(root.path(), memory.clone()).expect("spill manager");
    let batch = batch();
    let mut writer = manager
        .writer("many-stream-batches", batch.schema())
        .expect("stream writer");
    let writer_charge = memory.used();
    for _ in 0..BATCHES {
        writer.write_batch(&batch).expect("stream batch");
        assert_eq!(memory.used(), writer_charge);
    }
    let spill = writer.finish(1).expect("finish stream");

    let prefix = std::fs::read(spill.path()).expect("read stream prefix");
    assert_eq!(&prefix[..4], &[0xff, 0xff, 0xff, 0xff]);
    assert_ne!(&prefix[..6], b"ARROW1");
    assert_eq!(manager.read_batches(&spill).unwrap().len(), BATCHES);
    manager.remove_file(&spill);
    assert_eq!(memory.used(), 0);
}

#[test]
fn streaming_writer_rejects_dictionary_state_before_creating_a_file() {
    let root = tempfile::tempdir().expect("tempdir");
    let memory = MemoryPool::new(1 << 20);
    let manager = SpillManager::new(root.path(), memory.clone()).expect("spill manager");
    let schema = Arc::new(Schema::new(vec![Field::new(
        "dictionary",
        DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
        false,
    )]));

    let error = manager
        .writer("dictionary", schema)
        .err()
        .expect("dictionary schema must be rejected");
    assert!(matches!(
        error,
        Error::InvalidArgument(message)
            if message.contains("decode dictionaries before spilling")
    ));
    assert_eq!(memory.used(), 0);
    assert_eq!(std::fs::read_dir(manager.directory()).unwrap().count(), 0);
}

#[test]
fn writer_charge_includes_schema_and_nested_field_metadata() {
    let plain_child = Arc::new(Field::new("child", DataType::Utf8, false));
    let plain = Schema::new(vec![Field::new(
        "nested",
        DataType::Struct(vec![plain_child].into()),
        false,
    )]);

    let schema_metadata = HashMap::from([("schema-key".into(), "s".repeat(2_048))]);
    let field_metadata = HashMap::from([("field-key".into(), "f".repeat(1_024))]);
    let child_metadata = HashMap::from([("child-key".into(), "c".repeat(512))]);
    let child =
        Arc::new(Field::new("child", DataType::Utf8, false).with_metadata(child_metadata.clone()));
    let rich = Schema::new_with_metadata(
        vec![
            Field::new("nested", DataType::Struct(vec![child].into()), false)
                .with_metadata(field_metadata.clone()),
        ],
        schema_metadata.clone(),
    );
    let payload_bytes = schema_metadata
        .iter()
        .chain(field_metadata.iter())
        .chain(child_metadata.iter())
        .map(|(key, value)| key.len() + value.len())
        .sum::<usize>();

    assert!(
        writer_memory_bytes(&rich)
            >= writer_memory_bytes(&plain).saturating_add(payload_bytes.saturating_mul(2))
    );
}

#[test]
fn active_file_metadata_obeys_budget_and_reuses_released_capacity() {
    let root = tempfile::tempdir().expect("tempdir");
    let query_id = uuid::Uuid::nil();
    let directory = root.path().join(format!("query-{query_id}"));
    let first_path = directory.join("00000000-first.arrow");
    let second_path = directory.join("00000001-second.arrow");
    let first_bytes = active_file_metadata_bytes(&first_path);
    let second_bytes = active_file_metadata_bytes(&second_path);
    let batch = batch();
    let writer_bytes = writer_memory_bytes(batch.schema().as_ref());
    let memory = MemoryPool::new(first_bytes + second_bytes + writer_bytes);
    let control = QueryControl::new();
    let manager = SpillManager::for_query(root.path(), query_id, &control, memory.clone(), None)
        .expect("spill manager");

    let first = manager
        .write_record_batches("first", batch.schema(), [batch.clone()])
        .expect("first spill file");
    let second = manager
        .write_record_batches("second", batch.schema(), [batch.clone()])
        .expect("second spill file");
    assert_eq!(memory.used(), first_bytes + second_bytes);
    assert_eq!(std::fs::read_dir(manager.directory()).unwrap().count(), 2);

    let error = manager
        .write_record_batches("over-budget", batch.schema(), [batch.clone()])
        .unwrap_err();
    assert!(matches!(
        error,
        Error::ResourceExhausted(message)
            if message.contains("spill active-file metadata")
                && message.contains("active files 2")
    ));
    assert_eq!(memory.used(), first_bytes + second_bytes);
    assert_eq!(std::fs::read_dir(manager.directory()).unwrap().count(), 2);

    manager.remove_file(&first);
    assert_eq!(memory.used(), second_bytes);
    let replacement = manager
        .write_record_batches("first", batch.schema(), [batch])
        .expect("replacement spill file");
    assert_eq!(memory.used(), first_bytes + second_bytes);
    assert!(memory.peak() <= memory.limit());

    manager.cleanup().expect("cleanup");
    assert_eq!(memory.used(), 0);
    assert!(!manager.directory().exists());
    drop(second);
    drop(replacement);
}

#[test]
fn writer_budget_fails_before_file_creation_and_releases_on_finish() {
    let root = tempfile::tempdir().expect("tempdir");
    let batch = batch();
    let writer_bytes = writer_memory_bytes(batch.schema().as_ref());
    let memory = MemoryPool::new(writer_bytes - 1);
    let manager = SpillManager::new(root.path(), memory.clone()).expect("spill manager");

    let error = manager
        .writer("over-budget", batch.schema())
        .err()
        .expect("writer must respect its memory charge");
    assert!(matches!(
        error,
        Error::ResourceExhausted(message)
            if message.contains("spill writer requires")
                && message.contains("buffered streaming IPC/LZ4 state")
    ));
    assert_eq!(memory.used(), 0);
    assert_eq!(std::fs::read_dir(manager.directory()).unwrap().count(), 0);

    let root = tempfile::tempdir().expect("tempdir");
    let memory = MemoryPool::new(64 * ACTIVE_FILE_METADATA_BASE_BYTES);
    let manager = SpillManager::new(root.path(), memory.clone()).expect("spill manager");
    let mut writer = manager
        .writer("accounted", batch.schema())
        .expect("accounted writer");
    let path = writer.path().to_owned();
    assert_eq!(
        memory.used(),
        writer_bytes + active_file_metadata_bytes(&path)
    );
    writer.write_batch(&batch).expect("write batch");
    let completed = writer.finish(1).expect("finish writer");
    assert_eq!(memory.used(), active_file_metadata_bytes(completed.path()));
    manager.remove_file(&completed);
    assert_eq!(memory.used(), 0);
    assert!(memory.peak() <= memory.limit());
}

#[tokio::test]
async fn cancellation_stops_reading_and_removes_query_directory() {
    let root = tempfile::tempdir().expect("tempdir");
    let control = QueryControl::new();
    let metrics = QueryMetrics::new();
    let memory = MemoryPool::new(64 * ACTIVE_FILE_METADATA_BASE_BYTES);
    let manager = SpillManager::for_query(
        root.path(),
        uuid::Uuid::new_v4(),
        &control,
        memory.clone(),
        Some(metrics.clone()),
    )
    .expect("spill manager");
    let batch = batch();
    let spill = manager
        .write_record_batches("aggregate", batch.schema(), vec![batch])
        .expect("write spill");
    let mut stream = manager.read_stream(&spill).expect("read stream");
    assert!(manager.directory().exists());
    assert_eq!(metrics.snapshot().spill_partitions, 1);

    control.cancel();
    assert!(!manager.directory().exists());
    assert_eq!(memory.used(), 0);
    assert!(matches!(stream.next().await, Some(Err(Error::Cancelled))));
}
