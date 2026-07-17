use std::time::Duration;

use futures::StreamExt;

use super::collect;
use crate::{Engine, EngineConfig};

#[tokio::test]
async fn abandoned_query_releases_its_retired_native_snapshot() {
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("native");
    let first = directory.path().join("first.csv");
    let replacement = directory.path().join("replacement.csv");
    write_csv(&first, 0, 6_000);
    write_csv(&replacement, 10_000, 2_000);
    let engine = Engine::open(
        &database,
        EngineConfig::builder()
            .batch_size(128)
            .compute_threads(2)
            .max_concurrent_queries(2)
            .spill_directory(directory.path().join("spill"))
            .build(),
    )
    .unwrap();
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
    assert!(old.stream().next().await.unwrap().unwrap().num_rows() < 6_000);
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
    assert_eq!(snapshot_count(&database), 2);

    drop(old);
    tokio::time::timeout(Duration::from_secs(2), async {
        while snapshot_count(&database) != 1 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("abandoned query snapshot was not reclaimed");
}

#[tokio::test]
async fn append_then_replace_keeps_an_older_query_snapshot_alive() {
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("native");
    let first = directory.path().join("first.csv");
    let appended = directory.path().join("appended.csv");
    let replacement = directory.path().join("replacement.csv");
    write_csv(&first, 0, 6_000);
    write_csv(&appended, 6_000, 1_000);
    write_csv(&replacement, 20_000, 2_000);
    let engine = Engine::open(
        &database,
        EngineConfig::builder()
            .batch_size(128)
            .compute_threads(2)
            .max_concurrent_queries(2)
            .spill_directory(directory.path().join("spill"))
            .build(),
    )
    .unwrap();
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
    let original = only_snapshot(&database);

    let mut old = session.execute("SELECT id FROM events").await.unwrap();
    let first_batch = old.stream().next().await.unwrap().unwrap();
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

    assert!(
        original.is_dir(),
        "the oldest pinned snapshot was reclaimed"
    );
    let mut rows = first_batch.num_rows();
    while let Some(batch) = old.stream().next().await {
        rows += batch.unwrap().num_rows();
    }
    assert_eq!(rows, 6_000);
    drop(old);

    tokio::time::timeout(Duration::from_secs(2), async {
        while snapshot_count(&database) != 1 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("old append lineage was not reclaimed after its query finished");
}

fn write_csv(path: &std::path::Path, start: i64, rows: i64) {
    let mut contents = String::from("id,label\n");
    for id in start..start + rows {
        contents.push_str(&format!("{id},value-{id:08}\n"));
    }
    std::fs::write(path, contents).unwrap();
}

fn snapshot_count(database: &std::path::Path) -> usize {
    let table = std::fs::read_dir(database.join("tables"))
        .unwrap()
        .next()
        .unwrap()
        .unwrap();
    std::fs::read_dir(table.path().join("snapshots"))
        .unwrap()
        .count()
}

fn only_snapshot(database: &std::path::Path) -> std::path::PathBuf {
    let table = std::fs::read_dir(database.join("tables"))
        .unwrap()
        .next()
        .unwrap()
        .unwrap();
    let snapshots = std::fs::read_dir(table.path().join("snapshots"))
        .unwrap()
        .collect::<std::result::Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(snapshots.len(), 1);
    snapshots[0].path()
}
