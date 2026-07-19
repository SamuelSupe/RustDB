use std::{fs, io::Cursor, sync::Arc, time::Duration};

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
async fn incompatible_producer_is_invalidated_without_being_read() {
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
    assert_eq!(recovered[0].state(), StoredResultState::Invalidated);
    assert!(recovered[0].result().is_none());
    assert!(recovered[0].error().unwrap().contains("differs"));
    assert!(
        fs::read_dir(query.join("batches"))
            .unwrap()
            .next()
            .is_none()
    );
    let stored = manifest::load(&query.join(MANIFEST_FILE)).unwrap();
    assert_eq!(stored.state, ManifestState::Invalidated);
}

#[tokio::test]
async fn interrupted_and_aborted_results_become_failed() {
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

#[cfg(unix)]
#[test]
fn result_root_rejects_a_symlinked_owner_marker() {
    use std::os::unix::fs::{PermissionsExt, symlink};

    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().join("results");
    fs::create_dir(&root).unwrap();
    fs::set_permissions(&root, fs::Permissions::from_mode(0o700)).unwrap();
    let outside = directory.path().join("outside-owner");
    fs::write(&outside, b"rustdb-http-results-v1\n").unwrap();
    symlink(&outside, root.join("OWNER")).unwrap();

    let error = match ResultStore::open(ResultStoreConfig::new(&root)) {
        Ok(_) => panic!("a symlinked result owner marker must be rejected"),
        Err(error) => error,
    };
    assert!(error.to_string().contains("not a regular file"));
}
