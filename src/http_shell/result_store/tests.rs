use std::{
    fs,
    io::Cursor,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use arrow::{
    array::Int64Array,
    datatypes::{DataType, Field, Schema},
    ipc::reader::FileReader,
    record_batch::RecordBatch,
};

use super::{
    ResultStore, ResultStoreConfig, StoredResultState,
    manifest::{self, MANIFEST_FILE, ManifestState},
};
use crate::http_shell::result_read::ResultReadTracker;

fn fixture() -> (Arc<Schema>, RecordBatch) {
    let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
    )
    .unwrap();
    (schema, batch)
}

#[test]
fn beta2_hardening_oversized_manifest_is_rejected_without_unbounded_reading() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join(MANIFEST_FILE);
    let file = fs::File::create(&path).unwrap();
    file.set_len((manifest::MAX_MANIFEST_BYTES as u64).saturating_add(1))
        .unwrap();

    let error = manifest::load(&path).unwrap_err();
    assert!(error.to_string().contains("exceeds"));
}

#[tokio::test]
async fn persists_incremental_chunks_and_pages_results() {
    let directory = tempfile::tempdir().unwrap();
    let store =
        ResultStore::open(ResultStoreConfig::new(directory.path().join("results"))).unwrap();
    let (schema, batch) = fixture();
    let writer = store.writer("test", schema).unwrap();
    writer.write(batch).await.unwrap();

    let snapshot = store.snapshot("test").unwrap().unwrap();
    assert_eq!(snapshot.state(), StoredResultState::Running);
    assert_eq!(snapshot.next_batch_seq(), 1);
    let chunks = snapshot.chunks_from(0).unwrap();
    assert_eq!(chunks.len(), 1);
    let reads = ResultReadTracker::new();
    let bytes = snapshot
        .read_chunk_bytes(0, reads.start().unwrap())
        .await
        .unwrap();
    let mut ipc = FileReader::try_new(Cursor::new(bytes), None).unwrap();
    assert_eq!(ipc.next().unwrap().unwrap().num_rows(), 3);

    let result = writer.finish().await.unwrap();
    let batches = result.read(1, 1, reads.start().unwrap()).await.unwrap();
    assert_eq!(batches[0].num_rows(), 1);
    assert_eq!(result.rows(), 3);
    assert_eq!(result.chunks_from(1).unwrap(), Vec::new());
    result.delete().unwrap();
}

#[tokio::test]
async fn explicit_interruption_seals_committed_batches_without_completing_them() {
    let directory = tempfile::tempdir().unwrap();
    let store =
        ResultStore::open(ResultStoreConfig::new(directory.path().join("results"))).unwrap();
    let (schema, batch) = fixture();
    let writer = store.writer("interrupted", schema).unwrap();
    writer.write(batch).await.unwrap();
    let result = writer.interrupt("shutdown deadline elapsed").await.unwrap();

    assert_eq!(result.rows(), 3);
    let snapshot = result.snapshot().unwrap();
    assert_eq!(snapshot.state(), StoredResultState::Interrupted);
    assert_eq!(snapshot.chunks_from(0).unwrap().len(), 1);
    assert_eq!(snapshot.error(), Some("shutdown deadline elapsed"));
}

#[tokio::test]
async fn dropped_interrupted_writer_preserves_and_seals_committed_prefix() {
    let directory = tempfile::tempdir().unwrap();
    let store =
        ResultStore::open(ResultStoreConfig::new(directory.path().join("results"))).unwrap();
    let (schema, batch) = fixture();
    let interrupted = Arc::new(AtomicBool::new(false));
    let writer = store
        .writer("drop-interrupted", schema)
        .unwrap()
        .preserve_on_drop_when(Arc::clone(&interrupted));
    writer.write(batch).await.unwrap();
    interrupted.store(true, Ordering::Release);
    drop(writer);

    assert!(
        store
            .io
            .wait_idle_until(tokio::time::Instant::now() + Duration::from_secs(1))
            .await
    );
    store
        .seal_interrupted_prefix("drop-interrupted", "bounded shutdown")
        .await
        .unwrap();
    let snapshot = store.snapshot("drop-interrupted").unwrap().unwrap();
    assert_eq!(snapshot.state(), StoredResultState::Interrupted);
    assert_eq!(snapshot.rows(), 3);
    assert_eq!(snapshot.chunks_from(0).unwrap().len(), 1);
}

#[tokio::test]
async fn failed_physical_delete_keeps_quota_accounting_conservative() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().join("results");
    let store = ResultStore::open(ResultStoreConfig::new(&root)).unwrap();
    let (schema, batch) = fixture();
    let writer = store.writer("undeletable", schema).unwrap();
    writer.write(batch).await.unwrap();
    let result = writer.finish().await.unwrap();
    let bytes = result.bytes();
    assert!(bytes > 0);
    assert_eq!(store.quota.used_bytes(), bytes);

    fs::write(root.join("q-undeletable").join("OWNER"), b"foreign\n").unwrap();
    assert!(result.delete().is_err());
    drop(result);

    assert_eq!(store.quota.used_bytes(), bytes);
}

#[tokio::test]
async fn completed_batches_can_be_reclassified_by_a_shutdown_race() {
    let directory = tempfile::tempdir().unwrap();
    let store =
        ResultStore::open(ResultStoreConfig::new(directory.path().join("results"))).unwrap();
    let (schema, batch) = fixture();
    let writer = store.writer("late-interruption", schema).unwrap();
    writer.write(batch).await.unwrap();
    let result = writer.finish().await.unwrap();
    result
        .mark_interrupted("shutdown won publication race")
        .unwrap();

    let snapshot = result.snapshot().unwrap();
    assert_eq!(snapshot.state(), StoredResultState::Interrupted);
    assert_eq!(snapshot.rows(), 3);
    assert_eq!(snapshot.error(), Some("shutdown won publication race"));
}

#[tokio::test]
async fn completed_results_survive_store_restart() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().join("results");
    {
        let store = ResultStore::open(ResultStoreConfig::new(&root)).unwrap();
        let (schema, batch) = fixture();
        let writer = store.writer("recover-me", schema).unwrap();
        writer.write(batch).await.unwrap();
        drop(writer.finish().await.unwrap());
    }

    let store = ResultStore::open(ResultStoreConfig::new(&root)).unwrap();
    let recovered = store.take_recovered();
    assert_eq!(recovered.len(), 1);
    assert_eq!(recovered[0].query_id(), "recover-me");
    assert_eq!(recovered[0].state(), StoredResultState::Completed);
    let result = recovered[0].result().unwrap();
    let reads = ResultReadTracker::new();
    assert_eq!(
        result.read(0, 10, reads.start().unwrap()).await.unwrap()[0].num_rows(),
        3
    );
}

#[tokio::test]
async fn restart_preserves_batches_committed_before_interruption() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().join("results");
    let retained_bytes = {
        let store = ResultStore::open(ResultStoreConfig::new(&root)).unwrap();
        let (schema, batch) = fixture();
        let writer = store.writer("interrupted", schema).unwrap();
        writer.write(batch).await.unwrap();
        drop(writer.finish().await.unwrap());

        // Model a process exit after a batch manifest was committed but before
        // the query journal could publish a terminal state.
        let query = root.join("q-interrupted");
        let mut stored = manifest::load(&query.join(MANIFEST_FILE)).unwrap();
        stored.state = ManifestState::Running;
        manifest::persist(&query, &stored).unwrap();
        stored.bytes
    };

    let mut config = ResultStoreConfig::new(&root);
    config.global_limit_bytes = Some(retained_bytes.saturating_add(1));
    config.query_limit_bytes = config.global_limit_bytes;
    let store = ResultStore::open(config).unwrap();
    let recovered = store.take_recovered();
    assert_eq!(recovered.len(), 1);
    assert_eq!(recovered[0].state(), StoredResultState::Interrupted);
    assert_eq!(recovered[0].summary().rows, 3);
    assert_eq!(recovered[0].summary().batches, 1);
    assert!(recovered[0].result().is_some());

    let snapshot = store.snapshot("interrupted").unwrap().unwrap();
    assert_eq!(snapshot.state(), StoredResultState::Interrupted);
    assert_eq!(snapshot.rows(), 3);
    assert_eq!(snapshot.chunks_from(0).unwrap().len(), 1);
    let reads = ResultReadTracker::new();
    let bytes = snapshot
        .read_chunk_bytes(0, reads.start().unwrap())
        .await
        .unwrap();
    let mut ipc = FileReader::try_new(Cursor::new(bytes), None).unwrap();
    assert_eq!(ipc.next().unwrap().unwrap().num_rows(), 3);

    let (schema, batch) = fixture();
    let writer = store.writer("quota-probe", schema).unwrap();
    let error = writer.write(batch).await.unwrap_err();
    assert!(error.to_string().contains("quota"));
}

#[tokio::test]
async fn same_size_result_corruption_is_detected_before_delivery() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().join("results");
    {
        let store = ResultStore::open(ResultStoreConfig::new(&root)).unwrap();
        let (schema, batch) = fixture();
        let writer = store.writer("corrupt", schema).unwrap();
        writer.write(batch).await.unwrap();
        let result = writer.finish().await.unwrap();
        let path = root
            .join("q-corrupt")
            .join("batches")
            .join("batch-00000000000000000000.arrow");
        let mut bytes = fs::read(&path).unwrap();
        let last = bytes.len() - 1;
        bytes[last] ^= 1;
        fs::write(&path, bytes).unwrap();

        let reads = ResultReadTracker::new();
        let error = result
            .read_chunk_bytes(0, reads.start().unwrap())
            .await
            .unwrap_err();
        assert!(error.to_string().contains("SHA-256"));
    }

    let store = ResultStore::open(ResultStoreConfig::new(&root)).unwrap();
    let recovered = store.take_recovered();
    assert_eq!(recovered[0].state(), StoredResultState::Failed);
    assert!(recovered[0].error().unwrap().contains("result_corrupt"));
}

#[tokio::test]
async fn producer_version_is_diagnostic_within_the_result_epoch() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().join("results");
    {
        let store = ResultStore::open(ResultStoreConfig::new(&root)).unwrap();
        let (schema, batch) = fixture();
        let writer = store.writer("old", schema).unwrap();
        writer.write(batch).await.unwrap();
        drop(writer.finish().await.unwrap());
    }
    let query = root.join("q-old");
    let mut stored = manifest::load(&query.join(MANIFEST_FILE)).unwrap();
    stored.producer_version = "0.9.0-alpha.1".into();
    manifest::persist(&query, &stored).unwrap();

    let store = ResultStore::open(ResultStoreConfig::new(&root)).unwrap();
    let recovered = store.take_recovered();
    assert_eq!(recovered[0].state(), StoredResultState::Completed);
    assert!(recovered[0].result().is_some());
    assert!(recovered[0].error().is_none());
    assert!(
        fs::read_dir(query.join("batches"))
            .unwrap()
            .next()
            .is_some()
    );
    let stored = manifest::load(&query.join(MANIFEST_FILE)).unwrap();
    assert_eq!(stored.state, ManifestState::Completed);
}

#[tokio::test]
async fn explicitly_aborted_results_become_failed() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().join("results");
    {
        let store = ResultStore::open(ResultStoreConfig::new(&root)).unwrap();
        let (schema, batch) = fixture();
        let writer = store.writer("aborted", schema).unwrap();
        writer.write(batch).await.unwrap();
        writer.abort().await.unwrap();
    }
    let store = ResultStore::open(ResultStoreConfig::new(&root)).unwrap();
    let recovered = store.take_recovered();
    assert_eq!(recovered[0].state(), StoredResultState::Failed);
    assert_eq!(
        manifest::load(&root.join("q-aborted").join(MANIFEST_FILE))
            .unwrap()
            .bytes,
        0
    );
}

#[tokio::test]
async fn startup_removes_expired_terminal_results_and_stale_temporary_files() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().join("results");
    {
        let store = ResultStore::open(ResultStoreConfig::new(&root)).unwrap();
        let (schema, _) = fixture();
        let writer = store.writer("expired", schema).unwrap();
        drop(writer);
    }
    let query = root.join("q-expired");
    let mut stored = manifest::load(&query.join(MANIFEST_FILE)).unwrap();
    stored.updated_at_ms = 0;
    manifest::persist(&query, &stored).unwrap();
    fs::write(query.join(".manifest.json.partial"), b"stale").unwrap();

    let mut config = ResultStoreConfig::new(&root);
    config.ttl = Duration::from_secs(1);
    let store = ResultStore::open(config).unwrap();
    assert!(store.take_recovered().is_empty());
    assert!(!query.exists());
}

#[test]
fn result_root_has_one_process_owner() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().join("results");
    let first = ResultStore::open(ResultStoreConfig::new(&root)).unwrap();
    assert!(ResultStore::open(ResultStoreConfig::new(&root)).is_err());
    drop(first);
    ResultStore::open(ResultStoreConfig::new(&root)).unwrap();
}

#[test]
fn offline_state_lock_rejects_a_live_result_store() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().join("results");
    let store = ResultStore::open(ResultStoreConfig::new(&root)).unwrap();
    assert!(super::lock_state(&root).is_err());
    drop(store);
    assert!(super::lock_state(&root).is_ok());
}

#[cfg(unix)]
#[test]
fn result_root_rejects_a_symlinked_owner_marker() {
    use std::os::unix::fs::{PermissionsExt, symlink};

    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().join("results");
    fs::create_dir(&root).unwrap();
    fs::set_permissions(&root, fs::Permissions::from_mode(0o700)).unwrap();
    let outside = directory.path().join("outside-owner");
    fs::write(&outside, b"rustdb-http-results-v2\n").unwrap();
    symlink(&outside, root.join("OWNER")).unwrap();

    let error = match ResultStore::open(ResultStoreConfig::new(&root)) {
        Ok(_) => panic!("a symlinked result owner marker must be rejected"),
        Err(error) => error,
    };
    assert!(error.to_string().contains("not a regular file"));
}
