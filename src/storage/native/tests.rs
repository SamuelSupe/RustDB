use std::{
    fs::{self, OpenOptions},
    os::unix::fs::{PermissionsExt, symlink},
    sync::Arc,
};

use arrow::{
    array::Int64Array,
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};

use super::{INIT_FILE, MARKER_FILE, NativeDatabase, NativeWriteMode, format, marker};
use crate::Error;

#[test]
fn creates_and_reopens_database() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("database");

    let database = NativeDatabase::open(&path).unwrap();
    assert_eq!(database.path(), fs::canonicalize(&path).unwrap());
    assert!(path.join(MARKER_FILE).is_file());
    assert_eq!(
        marker::read(&path.join(MARKER_FILE)).unwrap().version(),
        format::CURRENT_DATABASE_VERSION
    );
    assert!(path.join("catalog").join("CURRENT").is_file());
    assert!(path.join("tables").is_dir());
    assert!(path.join("staging").is_dir());
    assert_eq!(
        fs::metadata(&path).unwrap().permissions().mode() & 0o777,
        0o700
    );
    assert_eq!(
        fs::metadata(path.join(MARKER_FILE))
            .unwrap()
            .permissions()
            .mode()
            & 0o777,
        0o600
    );
    assert_eq!(
        fs::metadata(path.join("catalog").join("CURRENT"))
            .unwrap()
            .permissions()
            .mode()
            & 0o777,
        0o600
    );

    drop(database);
    NativeDatabase::open(&path).unwrap();
}

#[test]
fn refuses_unknown_nonempty_directory_without_deleting_it() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("database");
    fs::create_dir(&path).unwrap();
    let unknown = path.join("keep-me");
    fs::write(&unknown, b"unknown").unwrap();

    let error = NativeDatabase::open(&path).unwrap_err();
    assert!(matches!(error, Error::NativeStorage { .. }));
    assert_eq!(fs::read(unknown).unwrap(), b"unknown");
    assert!(!path.join(".lock").exists());
}

#[test]
fn secures_a_preexisting_empty_database_root_before_initialization() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("database");
    fs::create_dir(&path).unwrap();
    fs::set_permissions(&path, fs::Permissions::from_mode(0o755)).unwrap();

    drop(NativeDatabase::open(&path).unwrap());

    assert_eq!(
        fs::metadata(path).unwrap().permissions().mode() & 0o777,
        0o700
    );
}

#[test]
fn rejects_corrupt_marker_and_current_generation() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("database");
    NativeDatabase::open(&path).unwrap();

    fs::write(path.join(MARKER_FILE), b"{not-json").unwrap();
    assert!(matches!(
        NativeDatabase::open(&path).unwrap_err(),
        Error::NativeStorage { .. }
    ));

    let path = directory.path().join("database-current");
    NativeDatabase::open(&path).unwrap();
    fs::write(path.join("catalog").join("CURRENT"), b"not-a-generation").unwrap();
    assert!(matches!(
        NativeDatabase::open(&path).unwrap_err(),
        Error::NativeStorage { .. }
    ));
}

#[test]
fn rejects_oversized_database_and_catalog_metadata() {
    let directory = tempfile::tempdir().unwrap();

    let marker_database = directory.path().join("oversized-marker");
    drop(NativeDatabase::open(&marker_database).unwrap());
    OpenOptions::new()
        .write(true)
        .open(marker_database.join(MARKER_FILE))
        .unwrap()
        .set_len((super::marker::MAX_DATABASE_MARKER_BYTES + 1) as u64)
        .unwrap();
    assert!(matches!(
        NativeDatabase::open(&marker_database).unwrap_err(),
        Error::NativeStorage { message, .. } if message.contains("database marker exceeds")
    ));

    let current_database = directory.path().join("oversized-current");
    drop(NativeDatabase::open(&current_database).unwrap());
    OpenOptions::new()
        .write(true)
        .open(current_database.join("catalog").join("CURRENT"))
        .unwrap()
        .set_len((super::manifest::MAX_CURRENT_BYTES + 1) as u64)
        .unwrap();
    assert!(matches!(
        NativeDatabase::open(&current_database).unwrap_err(),
        Error::NativeStorage { message, .. } if message.contains("catalog CURRENT exceeds")
    ));

    let catalog_database = directory.path().join("oversized-catalog");
    drop(NativeDatabase::open(&catalog_database).unwrap());
    let generation = catalog_database
        .join("catalog")
        .join("generations")
        .join("00000000000000000000.json");
    OpenOptions::new()
        .write(true)
        .open(generation)
        .unwrap()
        .set_len((super::manifest::MAX_CATALOG_MANIFEST_BYTES + 1) as u64)
        .unwrap();
    assert!(matches!(
        NativeDatabase::open(&catalog_database).unwrap_err(),
        Error::NativeStorage { message, .. }
            if message.contains("catalog generation manifest exceeds")
    ));
}

#[test]
fn resumes_recognized_interrupted_initialization() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("database");
    fs::create_dir(&path).unwrap();
    let initial = marker::DatabaseMarker::new();
    marker::write_new(&path.join(INIT_FILE), &initial).unwrap();

    NativeDatabase::open(&path).unwrap();
    assert!(path.join(MARKER_FILE).is_file());
    assert!(!path.join(INIT_FILE).exists());
    NativeDatabase::open(&path).unwrap();
}

#[test]
fn holds_an_exclusive_database_lock_for_its_lifetime() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("database");
    let database = NativeDatabase::open(&path).unwrap();

    assert!(matches!(
        NativeDatabase::open(&path).unwrap_err(),
        Error::NativeStorage { .. }
    ));
    drop(database);
    NativeDatabase::open(&path).unwrap();
}

#[test]
fn rejects_managed_directory_symlinks() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("database");
    drop(NativeDatabase::open(&path).unwrap());

    let outside = directory.path().join("outside");
    fs::create_dir(&outside).unwrap();
    fs::remove_dir(path.join("staging")).unwrap();
    symlink(&outside, path.join("staging")).unwrap();

    assert!(matches!(
        NativeDatabase::open(&path).unwrap_err(),
        Error::NativeStorage { .. }
    ));
    assert!(outside.is_dir());
}

#[test]
fn reopen_cleans_only_a_valid_abandoned_staging_transaction() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("database");
    let database = NativeDatabase::open(&path).unwrap();
    let staged = super::StagedSnapshot::begin(database.path(), database.database_id()).unwrap();
    let staged_path = staged.path().to_owned();
    std::mem::forget(staged);
    drop(database);
    assert!(staged_path.exists());

    let reopened = NativeDatabase::open(&path).unwrap();
    assert!(!staged_path.exists());
    drop(reopened);
}

#[test]
fn reopen_removes_a_valid_unpublished_snapshot() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("database");
    let database = NativeDatabase::open(&path).unwrap();
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let plan = database
        .plan_write(
            "orphan",
            NativeWriteMode::Create,
            database.catalog_generation(),
            Arc::clone(&schema),
            1024 * 1024,
        )
        .unwrap();
    let mut writer = database.start_write(plan).unwrap();
    writer
        .write_batch(
            &RecordBatch::try_new(
                schema,
                vec![Arc::new(Int64Array::from_iter_values(0..100_i64))],
            )
            .unwrap(),
        )
        .unwrap();
    let prepared = writer.finish().unwrap();
    let destination = prepared.snapshot.final_directory(database.path());
    prepared.staging.publish(&destination).unwrap();
    assert!(destination.exists());
    drop(database);

    let reopened = NativeDatabase::open(&path).unwrap();
    assert!(!destination.exists());
    drop(reopened);
}

#[test]
fn concurrent_writes_rebase_disjoint_tables_and_conflict_on_the_same_table() {
    let directory = tempfile::tempdir().unwrap();
    let database = NativeDatabase::open(directory.path().join("database")).unwrap();
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from(vec![1_i64]))],
    )
    .unwrap();

    let left = prepared_create(&database, "left_table", Arc::clone(&schema), &batch);
    let right = prepared_create(&database, "right_table", Arc::clone(&schema), &batch);
    assert_eq!(database.commit_write(left).unwrap().generation(), 1);
    assert_eq!(database.commit_write(right).unwrap().generation(), 2);
    assert_eq!(database.table_snapshots().len(), 2);

    let first = prepared_create(&database, "same_table", Arc::clone(&schema), &batch);
    let second = prepared_create(&database, "same_table", schema, &batch);
    assert_eq!(database.commit_write(first).unwrap().generation(), 3);
    assert!(matches!(
        database.commit_write(second).unwrap_err(),
        Error::TransactionConflict { .. }
    ));
    assert_eq!(database.catalog_generation(), 3);
}

#[test]
fn publication_gate_keeps_quota_check_and_publish_atomic() {
    use std::{
        sync::mpsc::{RecvTimeoutError, channel, sync_channel},
        thread,
        time::Duration,
    };

    let directory = tempfile::tempdir().unwrap();
    let mut database =
        NativeDatabase::open(directory.path().join("quota-publication-gate")).unwrap();
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from(vec![1_i64]))],
    )
    .unwrap();
    let first = prepared_create(&database, "events", Arc::clone(&schema), &batch);
    let second = prepared_create(&database, "events", schema, &batch);
    let first_bytes =
        super::disk_budget::directories_storage_bytes(first.quota_directories(database.path()))
            .unwrap();
    let second_bytes =
        super::disk_budget::directories_storage_bytes(second.quota_directories(database.path()))
            .unwrap();
    database.quota.default_table_limit_bytes = Some(first_bytes.max(second_bytes));
    let database = Arc::new(database);

    let (entered_tx, entered_rx) = sync_channel(0);
    let (release_tx, release_rx) = channel();
    let (first_ready_tx, first_ready_rx) = channel();
    let (cleanup_tx, cleanup_rx) = channel();
    let first_database = Arc::clone(&database);
    let first_thread = thread::spawn(move || {
        super::quota_publication_test_hook::arm(entered_tx, release_rx);
        let published = first_database.publish_transaction_write(first).unwrap();
        first_ready_tx.send(()).unwrap();
        cleanup_rx.recv().unwrap();
        super::transaction_commit::abort(&first_database, vec![published]).unwrap();
        super::quota::prune_published(&first_database);
    });
    entered_rx.recv().unwrap();

    let (second_tx, second_rx) = channel();
    let second_database = Arc::clone(&database);
    let second_thread = thread::spawn(move || {
        let result = second_database.publish_transaction_write(second);
        second_tx.send(result).unwrap();
    });
    assert!(matches!(
        second_rx.recv_timeout(Duration::from_millis(100)),
        Err(RecvTimeoutError::Timeout)
    ));

    release_tx.send(()).unwrap();
    first_ready_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    let error = match second_rx.recv_timeout(Duration::from_secs(5)).unwrap() {
        Err(error) => error,
        Ok(published) => {
            super::transaction_commit::abort(&database, vec![published]).unwrap();
            panic!("second publication unexpectedly passed the shared table quota");
        }
    };
    assert!(matches!(
        error,
        Error::NativeDiskQuotaExceeded {
            table: Some(table),
            ..
        } if table == "events"
    ));
    cleanup_tx.send(()).unwrap();
    first_thread.join().unwrap();
    second_thread.join().unwrap();
    assert!(database.active_quota_writes.lock().is_empty());
    assert_eq!(
        super::disk_budget::directories_storage_bytes([database.path().join("tables")]).unwrap(),
        0
    );
    assert_eq!(
        fs::read_dir(database.path().join("staging"))
            .unwrap()
            .count(),
        0
    );
}

fn prepared_create(
    database: &NativeDatabase,
    name: &str,
    schema: Arc<Schema>,
    batch: &RecordBatch,
) -> super::PreparedSnapshot {
    let plan = database
        .plan_write(
            name,
            NativeWriteMode::Create,
            database.catalog_generation(),
            schema,
            1024 * 1024,
        )
        .unwrap();
    let mut writer = database.start_write(plan).unwrap();
    writer.write_batch(batch).unwrap();
    writer.finish().unwrap()
}

#[test]
fn rejects_catalog_manifest_from_another_database() {
    let directory = tempfile::tempdir().unwrap();
    let first = directory.path().join("first");
    let second = directory.path().join("second");
    drop(NativeDatabase::open(&first).unwrap());
    drop(NativeDatabase::open(&second).unwrap());

    let generation = "00000000000000000000.json";
    fs::copy(
        first.join("catalog").join("generations").join(generation),
        second.join("catalog").join("generations").join(generation),
    )
    .unwrap();

    assert!(matches!(
        NativeDatabase::open(&second).unwrap_err(),
        Error::NativeStorage { .. }
    ));
}
