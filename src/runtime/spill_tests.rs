use std::{
    collections::HashMap,
    io,
    path::Path,
    sync::{Arc, atomic::Ordering, mpsc},
    thread,
    time::{Duration, Instant},
};

use arrow::{
    array::{ArrayRef, BinaryArray, Int32Array, StringArray},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use futures::StreamExt;
use rand::{RngCore, SeedableRng, rngs::StdRng};

use super::io::copy_memory_bytes;
use super::metadata::{ACTIVE_FILE_METADATA_BASE_BYTES, active_file_metadata_bytes};
use super::{
    DiskSpace, DiskSpaceProbe, MemoryPool, QueryControl, QueryMetrics, SpillIoPool, SpillManager,
    SpillQuotaPool, writer_memory_bytes,
};
use crate::{Error, config::SpillConfig};

#[derive(Debug)]
struct FixedProbe(io::Result<DiskSpace>);

impl DiskSpaceProbe for FixedProbe {
    fn probe(&self, _path: &Path) -> io::Result<DiskSpace> {
        match &self.0 {
            Ok(space) => Ok(*space),
            Err(error) => Err(error.raw_os_error().map_or_else(
                || io::Error::new(error.kind(), error.to_string()),
                io::Error::from_raw_os_error,
            )),
        }
    }
}

fn spill_file_count(manager: &SpillManager) -> usize {
    std::fs::read_dir(manager.directory())
        .expect("spill directory")
        .filter_map(std::result::Result::ok)
        .filter(|entry| {
            entry
                .path()
                .extension()
                .is_some_and(|extension| extension == "arrow")
        })
        .count()
}

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

fn binary_batch(bytes: usize) -> RecordBatch {
    let mut payload = vec![0_u8; bytes];
    StdRng::seed_from_u64(0x5eed).fill_bytes(&mut payload);
    let schema = Arc::new(Schema::new(vec![Field::new(
        "payload",
        DataType::Binary,
        false,
    )]));
    RecordBatch::try_new(
        schema,
        vec![Arc::new(BinaryArray::from(vec![payload.as_slice()]))],
    )
    .expect("valid binary batch")
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
fn cleanup_waits_for_cancelled_in_flight_io_and_its_reservation() {
    let root = tempfile::tempdir().expect("tempdir");
    let control = QueryControl::new();
    let memory = MemoryPool::new(1 << 20);
    let manager = SpillManager::for_task_group_query(
        root.path(),
        uuid::Uuid::new_v4(),
        &control,
        memory.clone(),
        None,
    )
    .unwrap();
    let directory = manager.directory().to_path_buf();
    let reservation = memory.try_reserve(4_096).unwrap();
    let (started_sender, started_receiver) = mpsc::sync_channel(1);
    let (release_sender, release_receiver) = mpsc::sync_channel(1);
    let running_manager = manager.clone();
    let io_thread = thread::spawn(move || {
        running_manager.run_tracked_io_for_test(move || {
            let _reservation = reservation;
            started_sender.send(()).unwrap();
            release_receiver.recv().unwrap();
            Ok(())
        })
    });
    started_receiver
        .recv_timeout(Duration::from_secs(1))
        .expect("tracked I/O must start");

    control.cancel();
    let cleanup_manager = manager.clone();
    let (cleaned_sender, cleaned_receiver) = mpsc::sync_channel(1);
    let cleanup_thread = thread::spawn(move || {
        cleaned_sender.send(cleanup_manager.cleanup()).unwrap();
    });

    assert!(
        cleaned_receiver
            .recv_timeout(Duration::from_millis(20))
            .is_err()
    );
    assert_eq!(memory.used(), 4_096);
    assert!(directory.exists());

    release_sender.send(()).unwrap();
    assert!(matches!(io_thread.join().unwrap(), Err(Error::Cancelled)));
    cleaned_receiver
        .recv_timeout(Duration::from_secs(2))
        .expect("cleanup must finish after I/O quiesces")
        .unwrap();
    cleanup_thread.join().unwrap();
    assert_eq!(memory.used(), 0);
    assert!(!directory.exists());
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

    manager.remove_file(&completed).unwrap();
    assert_eq!(memory.used(), 0);
    assert_eq!(spill_file_count(&manager), 0);
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
    writer.write_batch(&batch).expect("first stream batch");
    assert!(memory.used() >= writer_charge);
    for _ in 1..BATCHES {
        writer.write_batch(&batch).expect("stream batch");
        assert!(memory.used() >= writer_charge);
        assert!(memory.used() <= writer_charge + (256 << 10));
    }
    let spill = writer.finish(1).expect("finish stream");

    let prefix = std::fs::read(spill.path()).expect("read stream prefix");
    assert_eq!(&prefix[..4], &[0xff, 0xff, 0xff, 0xff]);
    assert_ne!(&prefix[..6], b"ARROW1");
    assert_eq!(manager.read_batches(&spill).unwrap().len(), BATCHES);
    manager.remove_file(&spill).unwrap();
    assert_eq!(memory.used(), 0);
}

#[test]
fn write_copy_reserves_before_allocation_and_cancel_releases_a_queued_copy() {
    let root = tempfile::tempdir().unwrap();
    let control = QueryControl::new();
    let memory = MemoryPool::new(8 << 20);
    let io_pool = SpillIoPool::new(1).unwrap();
    let config = SpillConfig {
        directory: root.path().to_path_buf(),
        min_free_ratio: 0.0,
        min_free_bytes: 0,
        io_threads: 1,
        ..SpillConfig::default()
    };
    let quota = SpillQuotaPool::with_probe(
        config,
        Arc::new(FixedProbe(Ok(DiskSpace {
            available_bytes: 64 << 20,
            total_bytes: 64 << 20,
        }))),
    )
    .unwrap()
    .start_query();
    let manager = SpillManager::for_query_with_resources(
        root.path(),
        uuid::Uuid::new_v4(),
        &control,
        memory.clone(),
        None,
        quota.clone(),
        io_pool.clone(),
    )
    .unwrap();
    let batch = binary_batch(2 << 20);
    let writer = manager.writer("queued-copy", batch.schema()).unwrap();
    let expected_writer_bytes = writer_memory_bytes(batch.schema().as_ref());
    let baseline = memory.used();

    let (blocked_sender, blocked_receiver) = mpsc::sync_channel(1);
    let (release_sender, release_receiver) = mpsc::sync_channel(1);
    let blocking_pool = io_pool.clone();
    let blocker = thread::spawn(move || {
        blocking_pool.run(move || {
            blocked_sender.send(()).unwrap();
            release_receiver.recv().unwrap();
            Ok(())
        })
    });
    blocked_receiver.recv().unwrap();

    let (result_sender, result_receiver) = mpsc::sync_channel(1);
    let writer_thread = thread::spawn(move || {
        let mut writer = writer;
        let result = writer.write_batch(&batch);
        result_sender.send(result).unwrap();
    });
    let deadline = Instant::now() + Duration::from_secs(2);
    while memory.used() == baseline && Instant::now() < deadline {
        thread::sleep(Duration::from_millis(2));
    }
    assert_eq!(
        quota.pending_bytes(),
        0,
        "queued writes are not disk writes"
    );
    assert_eq!(memory.used(), baseline + copy_memory_bytes(memory.limit()));
    assert!(baseline >= expected_writer_bytes);
    assert!(memory.used() <= memory.limit());

    let cancel_started = Instant::now();
    control.cancel();
    assert!(cancel_started.elapsed() < Duration::from_secs(1));
    assert!(matches!(
        result_receiver
            .recv_timeout(Duration::from_secs(1))
            .expect("queued writer must observe cancellation"),
        Err(Error::Cancelled)
    ));
    writer_thread.join().unwrap();
    assert_eq!(memory.used(), 0);

    release_sender.send(()).unwrap();
    blocker.join().unwrap().unwrap();
    drop(manager);
}

#[test]
fn write_copy_uses_protected_headroom_when_ordinary_budget_is_full() {
    let root = tempfile::tempdir().unwrap();
    let batch = binary_batch(128 << 10);
    let memory = MemoryPool::new(1 << 20);
    let manager = SpillManager::new(root.path(), memory.clone()).unwrap();
    let mut writer = manager.writer("copy-budget", batch.schema()).unwrap();
    let ordinary = memory.try_reserve(memory.available()).unwrap();

    writer.write_batch(&batch).unwrap();
    let file = writer.finish(1).unwrap();
    manager.remove_file(&file).unwrap();

    drop(ordinary);
    assert_eq!(memory.used(), 0);
    assert_eq!(spill_file_count(&manager), 0);
}

#[test]
fn query_manager_protects_one_forward_progress_copy_slot() {
    let root = tempfile::tempdir().unwrap();
    let memory = MemoryPool::new(1 << 20);
    let manager = SpillManager::new(root.path(), memory.clone()).unwrap();
    let copy_bytes = copy_memory_bytes(memory.limit());

    assert_eq!(memory.emergency_headroom(), copy_bytes);
    let normal = memory.try_reserve(memory.available()).unwrap();
    assert_eq!(memory.available(), 0);
    let copy = memory.try_reserve_emergency(copy_bytes).unwrap();
    assert!(memory.used() <= memory.limit());

    drop(copy);
    drop(normal);
    drop(manager);
    assert_eq!(memory.used(), 0);
}

#[test]
fn engine_pool_protects_one_copy_slot_per_io_worker() {
    let memory = MemoryPool::new(1 << 20);
    let copy_bytes = copy_memory_bytes(memory.limit());
    let io_threads = SpillConfig::default().io_threads;

    SpillManager::protect_io_headroom(&memory, io_threads).unwrap();

    assert_eq!(
        memory.emergency_headroom(),
        copy_bytes.saturating_mul(io_threads)
    );
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
    assert_eq!(spill_file_count(&manager), 0);
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
    let memory =
        MemoryPool::new(first_bytes + second_bytes + writer_bytes + copy_memory_bytes(32 << 10));
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
    assert_eq!(spill_file_count(&manager), 2);

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
    assert_eq!(spill_file_count(&manager), 2);

    manager.remove_file(&first).unwrap();
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
    assert_eq!(spill_file_count(&manager), 0);

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
    manager.remove_file(&completed).unwrap();
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

#[test]
fn production_resources_charge_bytes_until_physical_deletion() {
    let root = tempfile::tempdir().unwrap();
    let config = SpillConfig {
        directory: root.path().to_path_buf(),
        engine_limit_bytes: Some(1 << 20),
        query_limit_bytes: Some(1 << 20),
        min_free_ratio: 0.0,
        min_free_bytes: 0,
        io_threads: 1,
        ..SpillConfig::default()
    };
    let quota_pool = SpillQuotaPool::with_probe(
        config,
        Arc::new(FixedProbe(Ok(DiskSpace {
            available_bytes: 10 << 20,
            total_bytes: 10 << 20,
        }))),
    )
    .unwrap();
    let query_quota = quota_pool.start_query();
    let metrics = QueryMetrics::new();
    let control = QueryControl::new();
    let manager = SpillManager::for_query_with_resources(
        root.path(),
        uuid::Uuid::new_v4(),
        &control,
        MemoryPool::new(1 << 20),
        Some(metrics.clone()),
        query_quota.clone(),
        SpillIoPool::new(1).unwrap(),
    )
    .unwrap();
    assert!(manager.directory().join(".rustdb-spill").is_file());

    let batch = batch();
    let file = manager
        .write_record_batches("quota", batch.schema(), [batch.clone()])
        .unwrap();
    let charged = query_quota.committed_bytes();
    assert!(charged > 0);
    assert_eq!(charged, std::fs::metadata(file.path()).unwrap().len());
    assert_eq!(quota_pool.committed_bytes(), charged);
    assert_eq!(manager.read_batches(&file).unwrap(), vec![batch]);

    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.spill_files, 1);
    assert_eq!(snapshot.spill_write_bytes, charged);
    assert!(snapshot.spill_read_bytes >= charged);
    assert_eq!(snapshot.spill_quota_rejections, 0);

    manager.remove_file(&file).unwrap();
    assert_eq!(query_quota.committed_bytes(), 0);
    assert_eq!(quota_pool.committed_bytes(), 0);
}

#[test]
fn quota_and_injected_enospc_rejections_are_reported_and_cleaned() {
    for (probe, query_limit) in [
        (
            FixedProbe(Ok(DiskSpace {
                available_bytes: 10 << 20,
                total_bytes: 10 << 20,
            })),
            Some(1),
        ),
        (FixedProbe(Err(io::Error::from_raw_os_error(28))), None),
    ] {
        let root = tempfile::tempdir().unwrap();
        let config = SpillConfig {
            directory: root.path().to_path_buf(),
            query_limit_bytes: query_limit,
            min_free_ratio: 0.0,
            min_free_bytes: 0,
            io_threads: 1,
            ..SpillConfig::default()
        };
        let quota_pool = SpillQuotaPool::with_probe(config, Arc::new(probe)).unwrap();
        let query_quota = quota_pool.start_query();
        let metrics = QueryMetrics::new();
        let control = QueryControl::new();
        let manager = SpillManager::for_query_with_resources(
            root.path(),
            uuid::Uuid::new_v4(),
            &control,
            MemoryPool::new(1 << 20),
            Some(metrics.clone()),
            query_quota.clone(),
            SpillIoPool::new(1).unwrap(),
        )
        .unwrap();
        let batch = batch();
        let error = manager
            .write_record_batches("rejected", batch.schema(), [batch])
            .unwrap_err();
        assert!(matches!(
            error,
            Error::ResourceExhausted(_) | Error::Io { .. }
        ));
        assert_eq!(metrics.snapshot().spill_quota_rejections, 1);
        assert_eq!(query_quota.committed_bytes(), 0);
        assert_eq!(query_quota.pending_bytes(), 0);
        assert_eq!(spill_file_count(&manager), 0);
        manager.cleanup().unwrap();
        assert!(!manager.directory().exists());
    }
}

#[test]
fn deletion_failure_is_returned_and_retains_the_quota_charge() {
    let root = tempfile::tempdir().unwrap();
    let config = SpillConfig {
        directory: root.path().to_path_buf(),
        min_free_ratio: 0.0,
        min_free_bytes: 0,
        io_threads: 1,
        ..SpillConfig::default()
    };
    let quota_pool = SpillQuotaPool::with_probe(
        config,
        Arc::new(FixedProbe(Ok(DiskSpace {
            available_bytes: 10 << 20,
            total_bytes: 10 << 20,
        }))),
    )
    .unwrap();
    let query_quota = quota_pool.start_query();
    let control = QueryControl::new();
    let manager = SpillManager::for_query_with_resources(
        root.path(),
        uuid::Uuid::new_v4(),
        &control,
        MemoryPool::new(1 << 20),
        None,
        query_quota.clone(),
        SpillIoPool::new(1).unwrap(),
    )
    .unwrap();
    let batch = batch();
    let file = manager
        .write_record_batches("delete-error", batch.schema(), [batch])
        .unwrap();
    let charged = query_quota.committed_bytes();
    std::fs::remove_file(file.path()).unwrap();
    std::fs::create_dir(file.path()).unwrap();

    assert!(matches!(manager.remove_file(&file), Err(Error::Io { .. })));
    assert_eq!(query_quota.committed_bytes(), charged);
    manager.cleanup().unwrap();
    assert_eq!(query_quota.committed_bytes(), 0);
}

#[test]
fn permanent_cleanup_failure_retains_engine_quota_after_manager_drop() {
    let root = tempfile::tempdir().unwrap();
    let config = SpillConfig {
        directory: root.path().to_path_buf(),
        engine_limit_bytes: Some(1 << 20),
        query_limit_bytes: Some(1 << 20),
        min_free_ratio: 0.0,
        min_free_bytes: 0,
        io_threads: 1,
        ..SpillConfig::default()
    };
    let quota_pool = SpillQuotaPool::with_probe(
        config,
        Arc::new(FixedProbe(Ok(DiskSpace {
            available_bytes: 10 << 20,
            total_bytes: 10 << 20,
        }))),
    )
    .unwrap();
    let query_quota = quota_pool.start_query();
    let control = QueryControl::new();
    let io_pool = SpillIoPool::new(1).unwrap();
    let manager = SpillManager::for_query_with_resources(
        root.path(),
        uuid::Uuid::new_v4(),
        &control,
        MemoryPool::new(1 << 20),
        None,
        query_quota.clone(),
        io_pool.clone(),
    )
    .unwrap();
    let directory = manager.directory().to_path_buf();
    let file = manager
        .write_record_batches("retained-orphan", batch().schema(), [batch()])
        .unwrap();
    let charged = std::fs::metadata(file.path()).unwrap().len();
    assert_eq!(quota_pool.committed_bytes(), charged);

    io_pool.shutdown_for_test();
    drop(manager);
    assert!(directory.exists());
    assert_eq!(query_quota.committed_bytes(), 0);
    assert_eq!(quota_pool.committed_bytes(), charged);

    std::fs::remove_dir_all(directory).unwrap();
}

#[test]
fn production_drop_leaves_cleanup_to_the_reaper() {
    let root = tempfile::tempdir().unwrap();
    let config = SpillConfig {
        directory: root.path().to_path_buf(),
        min_free_ratio: 0.0,
        min_free_bytes: 0,
        io_threads: 1,
        ..SpillConfig::default()
    };
    let quota = SpillQuotaPool::new(config).unwrap().start_query();
    let control = QueryControl::new();
    let manager = SpillManager::for_query_with_resources(
        root.path(),
        uuid::Uuid::new_v4(),
        &control,
        MemoryPool::new(1 << 20),
        None,
        quota,
        SpillIoPool::new(1).unwrap(),
    )
    .unwrap();
    let directory = manager.directory().to_path_buf();

    drop(manager);

    assert!(directory.exists());
    std::fs::remove_dir_all(directory).unwrap();
}

#[test]
fn completed_cleanup_is_a_no_io_fast_path() {
    let root = tempfile::tempdir().unwrap();
    let config = SpillConfig {
        directory: root.path().to_path_buf(),
        min_free_ratio: 0.0,
        min_free_bytes: 0,
        io_threads: 1,
        ..SpillConfig::default()
    };
    let quota = SpillQuotaPool::new(config).unwrap().start_query();
    let control = QueryControl::new();
    let io_pool = SpillIoPool::new(1).unwrap();
    let manager = SpillManager::for_query_with_resources(
        root.path(),
        uuid::Uuid::new_v4(),
        &control,
        MemoryPool::new(1 << 20),
        None,
        quota,
        io_pool.clone(),
    )
    .unwrap();

    manager.cleanup().unwrap();
    assert!(manager.state.cleanup_completed.load(Ordering::Acquire));
    io_pool.shutdown_for_test();
    manager.cleanup().unwrap();
    drop(manager);
}
