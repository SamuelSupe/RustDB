use std::sync::{Arc, atomic::Ordering};

use arrow::array::{Decimal128Array, Int64Array};
use futures::TryStreamExt;
use object_store::{ObjectStore, ObjectStoreExt, memory::InMemory, path::Path as ObjectPath};
use tokio::sync::Notify;

use super::collect;
use crate::{Engine, EngineConfig};

#[test]
fn engine_startup_scavenges_expired_remote_temporary_directories() {
    let directory = tempfile::tempdir().unwrap();
    let spill = directory.path().join("spill");
    std::fs::create_dir(&spill).unwrap();
    let temporary =
        crate::storage::RemoteTempDir::create(&spill, crate::storage::RemoteTempKind::Backup)
            .unwrap();
    let path = temporary.path().to_owned();
    drop(temporary);
    std::thread::sleep(std::time::Duration::from_millis(5));
    let mut config = EngineConfig::builder().spill_directory(&spill).build();
    config.spill.orphan_ttl = std::time::Duration::from_millis(1);

    let _engine = Engine::new(config).unwrap();

    assert!(!path.exists());
    assert!(std::fs::read_dir(&spill).unwrap().all(|entry| {
        !entry
            .unwrap()
            .file_name()
            .to_string_lossy()
            .contains("rustdb-remote")
    }));
}

#[test]
fn backup_rejects_a_poisoned_native_engine() {
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("native");
    let backup = directory.path().join("backup");
    let engine = Engine::open(
        &database,
        EngineConfig::builder()
            .spill_directory(directory.path().join("spill"))
            .build(),
    )
    .unwrap();
    engine.inner.native_poisoned.store(true, Ordering::Release);

    let error = engine.backup_to(&backup).unwrap_err();

    assert!(matches!(error, crate::Error::NativeStorage { .. }));
    assert!(!backup.exists());
}

#[tokio::test]
async fn abandoned_remote_backup_future_finishes_on_the_engine_runtime() {
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("native");
    let spill = directory.path().join("spill");
    let engine = Engine::open(
        &database,
        EngineConfig::builder()
            .compute_threads(1)
            .spill_directory(&spill)
            .build(),
    )
    .unwrap();
    let temporary =
        crate::storage::RemoteTempDir::create(&spill, crate::storage::RemoteTempKind::Backup)
            .unwrap();
    let temporary_path = temporary.path().to_owned();
    let snapshot = temporary.path().join("snapshot");
    engine.backup_to(&snapshot).unwrap();

    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let started = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let worker_engine = engine.clone();
    let worker_store = Arc::clone(&store);
    let worker_started = Arc::clone(&started);
    let worker_release = Arc::clone(&release);
    let engine_lifetime = Arc::downgrade(&engine.inner);
    let abandoned = tokio::spawn(async move {
        worker_engine
            .run_owned_remote_backup(async move {
                worker_started.notify_one();
                worker_release.notified().await;
                let result = crate::storage::upload_remote_backup_to_store(
                    &snapshot,
                    worker_store,
                    "cancelled-backup",
                )
                .await;
                temporary.finish(result)
            })
            .await
    });

    started.notified().await;
    abandoned.abort();
    assert!(abandoned.await.unwrap_err().is_cancelled());
    // The detached worker owns the final Engine handle and therefore its
    // runtime until manifest publication and cleanup have completed.
    drop(engine);
    release.notify_one();

    let manifest = ObjectPath::from("cancelled-backup/manifest.json");
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            match store.head(&manifest).await {
                Ok(_) => break,
                Err(object_store::Error::NotFound { .. }) => tokio::task::yield_now().await,
                Err(error) => panic!("failed to inspect completed backup: {error}"),
            }
        }
    })
    .await
    .expect("engine-owned backup did not publish its manifest");
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while engine_lifetime.strong_count() != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("completed backup retained its Engine/runtime keepalive");
    let objects = store
        .list(Some(&ObjectPath::from("cancelled-backup")))
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert!(objects.iter().any(|object| object.location == manifest));
    assert!(
        objects
            .iter()
            .any(|object| object.location.as_ref().contains("/data/")),
        "published manifest must retain its immutable data objects"
    );
    assert!(!temporary_path.exists());
    assert!(std::fs::read_dir(&spill).unwrap().all(|entry| {
        !entry
            .unwrap()
            .file_name()
            .to_string_lossy()
            .contains("rustdb-remote")
    }));
}

#[tokio::test]
async fn local_backup_restores_after_the_source_database_is_removed() {
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("native");
    let backup = directory.path().join("backup");
    let restored = directory.path().join("restored");
    let csv = directory.path().join("input.csv");
    let mut contents = String::from("id,value\n");
    for id in 0..5_000_i64 {
        contents.push_str(&format!("{id},{}\n", id * 7 + 3));
    }
    std::fs::write(&csv, contents).unwrap();
    let config = EngineConfig::builder()
        .compute_threads(2)
        .spill_directory(directory.path().join("spill"))
        .build();
    let engine = Engine::open(&database, config.clone()).unwrap();
    collect(
        engine
            .session()
            .execute(&format!(
                "CREATE TABLE facts AS SELECT id, value FROM read_csv('{}', header = true)",
                csv.display()
            ))
            .await
            .unwrap(),
    )
    .await;
    engine.backup_to(&backup).unwrap();
    drop(engine);
    std::fs::remove_file(csv).unwrap();
    std::fs::remove_dir_all(database).unwrap();

    let restored_engine = Engine::restore_from(&backup, &restored, config).unwrap();
    let batches = collect(
        restored_engine
            .session()
            .execute("SELECT count(*), sum(id), sum(value) FROM facts")
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(value(&batches[0], 0), 5_000);
    assert_eq!(
        decimal_value(&batches[0], 1),
        (0..5_000_i64).map(i128::from).sum()
    );
    assert_eq!(
        decimal_value(&batches[0], 2),
        (0..5_000_i64).map(|id| i128::from(id * 7 + 3)).sum()
    );
}

#[tokio::test]
async fn local_backup_restores_delete_vectors() {
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("native");
    let backup = directory.path().join("backup");
    let restored = directory.path().join("restored");
    let config = EngineConfig::builder()
        .compute_threads(1)
        .spill_directory(directory.path().join("spill"))
        .build();
    let engine = Engine::open(&database, config.clone()).unwrap();
    let session = engine.session();
    collect(
        session
            .execute("CREATE TABLE events AS SELECT * FROM (VALUES (1), (2), (3)) AS v(id)")
            .await
            .unwrap(),
    )
    .await;
    collect(
        session
            .execute("DELETE FROM events WHERE id = 2")
            .await
            .unwrap(),
    )
    .await;

    engine.backup_to(&backup).unwrap();
    drop(session);
    drop(engine);
    std::fs::remove_dir_all(database).unwrap();

    let restored_engine = Engine::restore_from(&backup, &restored, config).unwrap();
    let rows = collect(
        restored_engine
            .session()
            .execute("SELECT id FROM events ORDER BY id")
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(
        rows[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values(),
        &[1, 3]
    );
}

fn decimal_value(batch: &arrow::record_batch::RecordBatch, column: usize) -> i128 {
    batch
        .column(column)
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .unwrap()
        .value(0)
}

fn value(batch: &arrow::record_batch::RecordBatch, column: usize) -> i64 {
    batch
        .column(column)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0)
}
