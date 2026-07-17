use std::{
    sync::{Arc, atomic::Ordering},
    time::Duration,
};

use arrow::array::StringArray;
use futures::StreamExt;

use super::{collect, query_count};
use crate::{Engine, EngineConfig, Error};

#[test]
fn persistent_engine_creates_and_reopens_database_directory() {
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("native");
    let config = EngineConfig::builder()
        .spill_directory(directory.path().join("spill"))
        .build();

    let engine = Engine::open(&database, config.clone()).unwrap();
    assert_eq!(
        engine.database_path(),
        Some(std::fs::canonicalize(&database).unwrap().as_path())
    );
    drop(engine);

    let reopened = Engine::open(&database, config).unwrap();
    assert_eq!(
        reopened.database_path(),
        Some(std::fs::canonicalize(&database).unwrap().as_path())
    );
}

#[tokio::test]
async fn poisoned_engine_rejects_queued_and_prepared_queries() {
    let directory = tempfile::tempdir().unwrap();
    let engine = Engine::new(
        EngineConfig::builder()
            .max_concurrent_queries(1)
            .spill_directory(directory.path().join("spill"))
            .build(),
    )
    .unwrap();
    let session = engine.session();
    let prepared = session.prepare("SELECT 1").unwrap();
    let held = Arc::clone(&engine.inner.admission)
        .acquire_owned()
        .await
        .unwrap();
    let queued_session = session.clone();
    let queued = tokio::spawn(async move { queued_session.execute("SELECT 1").await });
    tokio::task::yield_now().await;
    engine.inner.native_poisoned.store(true, Ordering::Release);
    drop(held);

    let Err(queued_error) = queued.await.unwrap() else {
        panic!("queued query unexpectedly succeeded on a poisoned engine");
    };
    assert!(matches!(queued_error, Error::NativeStorage { .. }));
    let Err(prepared_error) = prepared.execute(&[]).await else {
        panic!("prepared query unexpectedly succeeded on a poisoned engine");
    };
    assert!(matches!(prepared_error, Error::NativeStorage { .. }));
}

#[tokio::test]
async fn native_ctas_survives_reopen_and_uses_one_query_permit() {
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("native");
    let csv = directory.path().join("input.csv");
    let mut contents = String::from("id,label\n");
    for id in 0..5_000_i64 {
        contents.push_str(&format!("{id},value-{id:08}\n"));
    }
    std::fs::write(&csv, contents).unwrap();
    let config = EngineConfig::builder()
        .compute_threads(1)
        .max_concurrent_queries(1)
        .spill_directory(directory.path().join("spill"))
        .build();

    let engine = Engine::open(&database, config.clone()).unwrap();
    let session = engine.session();
    let sql = format!(
        "CREATE TABLE events AS SELECT id, label FROM read_csv('{}', header = true)",
        csv.display()
    );
    let status = tokio::time::timeout(Duration::from_secs(10), async {
        collect(session.execute(&sql).await.unwrap()).await
    })
    .await
    .expect("CTAS must not wait for a second admission permit");
    assert_eq!(
        status[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        "CREATE TABLE"
    );
    assert_eq!(query_count(&session, "events").await, 5_000);
    drop(session);
    drop(engine);

    let reopened = Engine::open(&database, config).unwrap();
    assert_eq!(query_count(&reopened.session(), "events").await, 5_000);
}

#[tokio::test]
async fn tiny_and_empty_native_tables_fit_the_bounded_metadata_allowance() {
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("native");
    let csv = directory.path().join("tiny.csv");
    write_numbered_csv(&csv, 0, 10);
    let config = EngineConfig::builder()
        .compute_threads(1)
        .spill_directory(directory.path().join("spill"))
        .build();
    let engine = Engine::open(&database, config.clone()).unwrap();
    let session = engine.session();

    collect(
        session
            .execute(&format!(
                "CREATE TABLE tiny AS SELECT id, label FROM read_csv('{}', header = true)",
                csv.display()
            ))
            .await
            .unwrap(),
    )
    .await;
    collect(
        session
            .execute("CREATE TABLE empty AS SELECT 1 AS id WHERE false")
            .await
            .unwrap(),
    )
    .await;
    collect(
        session
            .execute("CREATE TABLE singleton AS SELECT 1 AS id, 'value' AS label")
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(query_count(&session, "tiny").await, 10);
    assert_eq!(query_count(&session, "empty").await, 0);
    assert_eq!(query_count(&session, "singleton").await, 1);
    drop(session);
    drop(engine);

    let reopened = Engine::open(&database, config).unwrap();
    let session = reopened.session();
    assert_eq!(query_count(&session, "tiny").await, 10);
    assert_eq!(query_count(&session, "empty").await, 0);
    assert_eq!(query_count(&session, "singleton").await, 1);
}

#[tokio::test]
async fn native_append_and_replace_publish_atomic_snapshots() {
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("native");
    let first = directory.path().join("first.csv");
    let appended = directory.path().join("appended.csv");
    let replacement = directory.path().join("replacement.csv");
    write_numbered_csv(&first, 0, 3_000);
    write_numbered_csv(&appended, 3_000, 2_000);
    write_numbered_csv(&replacement, 10_000, 2_500);
    let config = EngineConfig::builder()
        .compute_threads(1)
        .max_concurrent_queries(1)
        .spill_directory(directory.path().join("spill"))
        .build();
    let engine = Engine::open(&database, config.clone()).unwrap();
    let session = engine.session();

    collect(
        session
            .execute(&format!(
                "CREATE TABLE events AS SELECT id, label FROM read_csv('{}', header = true)",
                first.display()
            ))
            .await
            .unwrap(),
    )
    .await;
    collect(
        session
            .execute(&format!(
                "INSERT INTO events SELECT id, label FROM read_csv('{}', header = true)",
                appended.display()
            ))
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(query_count(&session, "events").await, 5_000);
    let table = std::fs::read_dir(database.join("tables"))
        .unwrap()
        .next()
        .unwrap()
        .unwrap();
    let snapshots = std::fs::read_dir(table.path().join("snapshots"))
        .unwrap()
        .collect::<std::result::Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(snapshots.len(), 2, "append keeps inherited segment owners");
    assert_eq!(
        snapshots
            .iter()
            .filter(|entry| entry.path().join("manifest.json").exists())
            .count(),
        1,
        "only the current snapshot keeps a full manifest"
    );
    assert_eq!(catalog_generation_count(&database), 1);
    let imported_source_bytes =
        std::fs::metadata(&first).unwrap().len() + std::fs::metadata(&appended).unwrap().len();
    assert!(managed_file_bytes(&database) <= imported_source_bytes * 2);

    collect(
        session
            .execute(&format!(
                "CREATE OR REPLACE TABLE events AS SELECT id, label FROM read_csv('{}', header = true)",
                replacement.display()
            ))
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(query_count(&session, "events").await, 2_500);

    let tables = std::fs::read_dir(database.join("tables"))
        .unwrap()
        .collect::<std::result::Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(tables.len(), 1);
    let snapshots = std::fs::read_dir(tables[0].path().join("snapshots"))
        .unwrap()
        .collect::<std::result::Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(snapshots.len(), 1, "replaced snapshots must be reclaimed");
    assert_eq!(catalog_generation_count(&database), 1);
    assert!(managed_file_bytes(&database) <= std::fs::metadata(&replacement).unwrap().len() * 2);

    drop(session);
    drop(engine);
    let reopened = Engine::open(&database, config).unwrap();
    assert_eq!(query_count(&reopened.session(), "events").await, 2_500);
}

#[tokio::test]
async fn unknown_source_replace_is_planned_before_its_first_batch() {
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("native");
    let csv = directory.path().join("input.csv");
    write_randomized_csv(&csv, 4_096);
    let config = EngineConfig::builder()
        .compute_threads(1)
        .spill_directory(directory.path().join("spill"))
        .build();
    let engine = Engine::open(&database, config).unwrap();
    let session = engine.session();

    collect(
        session
            .execute(&format!(
                "CREATE TABLE events AS SELECT id, label FROM read_csv('{}', header = true)",
                csv.display()
            ))
            .await
            .unwrap(),
    )
    .await;
    let segment_bytes = std::fs::read_dir(database.join("tables"))
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .path()
        .join("snapshots");
    assert!(managed_file_bytes(&segment_bytes) > 64 * 1024);

    collect(
        session
            .execute("CREATE OR REPLACE TABLE events AS SELECT id, label FROM events")
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(query_count(&session, "events").await, 4_096);
}

#[tokio::test]
async fn native_replace_keeps_an_in_flight_query_on_its_old_snapshot() {
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("native");
    let first = directory.path().join("first.csv");
    let replacement = directory.path().join("replacement.csv");
    write_numbered_csv(&first, 0, 10_000);
    write_numbered_csv(&replacement, 20_000, 4_000);
    let config = EngineConfig::builder()
        .batch_size(256)
        .compute_threads(2)
        .max_concurrent_queries(2)
        .spill_directory(directory.path().join("spill"))
        .build();
    let engine = Engine::open(&database, config).unwrap();
    let session = engine.session();
    collect(
        session
            .execute(&format!(
                "CREATE TABLE events AS SELECT id, label FROM read_csv('{}', header = true)",
                first.display()
            ))
            .await
            .unwrap(),
    )
    .await;

    let mut old = session.execute("SELECT id FROM events").await.unwrap();
    let first_batch = old.stream().next().await.unwrap().unwrap();
    assert!(first_batch.num_rows() < 10_000);

    collect(
        session
            .execute(&format!(
                "CREATE OR REPLACE TABLE events AS SELECT id, label FROM read_csv('{}', header = true)",
                replacement.display()
            ))
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(query_count(&session, "events").await, 4_000);

    let mut old_rows = first_batch.num_rows();
    while let Some(batch) = old.stream().next().await {
        old_rows += batch.unwrap().num_rows();
    }
    assert_eq!(old_rows, 10_000);
    drop(old);

    let table = std::fs::read_dir(database.join("tables"))
        .unwrap()
        .next()
        .unwrap()
        .unwrap();
    assert_eq!(
        std::fs::read_dir(table.path().join("snapshots"))
            .unwrap()
            .count(),
        1
    );
}

#[tokio::test]
async fn cancelled_native_write_does_not_publish_or_leave_staging() {
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("native");
    let csv = directory.path().join("large.csv");
    write_numbered_csv(&csv, 0, 200_000);
    let config = EngineConfig::builder()
        .batch_size(256)
        .compute_threads(1)
        .max_concurrent_queries(1)
        .spill_directory(directory.path().join("spill"))
        .build();
    let engine = Engine::open(&database, config).unwrap();
    let session = engine.session();
    let mut result = session
        .execute(&format!(
            "CREATE TABLE cancelled AS SELECT id, label FROM read_csv('{}', header = true)",
            csv.display()
        ))
        .await
        .unwrap();
    result.cancel();
    let error = result.stream().next().await.unwrap().unwrap_err();
    assert!(matches!(error, Error::Cancelled));
    assert!(!session.table_names().contains(&"cancelled".to_owned()));
    assert_eq!(
        std::fs::read_dir(database.join("staging")).unwrap().count(),
        0
    );
}

fn write_numbered_csv(path: &std::path::Path, start: i64, rows: i64) {
    let mut contents = String::from("id,label\n");
    for id in start..start + rows {
        contents.push_str(&format!("{id},value-{id:08}\n"));
    }
    std::fs::write(path, contents).unwrap();
}

fn write_randomized_csv(path: &std::path::Path, rows: usize) {
    let mut contents = String::from("id,label\n");
    let mut state = 0x1234_5678_9abc_def0_u64;
    for id in 0..rows {
        let mut label = String::new();
        for _ in 0..4 {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            label.push_str(&format!("{state:016x}"));
        }
        contents.push_str(&format!("{id},{label}\n"));
    }
    std::fs::write(path, contents).unwrap();
}

fn catalog_generation_count(database: &std::path::Path) -> usize {
    std::fs::read_dir(database.join("catalog/generations"))
        .unwrap()
        .count()
}

fn managed_file_bytes(directory: &std::path::Path) -> u64 {
    std::fs::read_dir(directory)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .map(|path| {
            let metadata = std::fs::symlink_metadata(&path).unwrap();
            if metadata.is_dir() {
                managed_file_bytes(&path)
            } else {
                metadata.len()
            }
        })
        .sum()
}
