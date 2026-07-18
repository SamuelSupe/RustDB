use std::{
    sync::{Arc, atomic::Ordering},
    time::Duration,
};

use arrow::array::{Array, Int64Array, StringArray};
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
async fn native_insert_values_appends_a_typed_values_relation() {
    let directory = tempfile::tempdir().unwrap();
    let engine = Engine::open(
        directory.path().join("native"),
        EngineConfig::builder()
            .compute_threads(1)
            .spill_directory(directory.path().join("spill"))
            .build(),
    )
    .unwrap();
    let session = engine.session();

    collect(
        session
            .execute("CREATE TABLE events AS SELECT 1 AS id, 'a' AS label")
            .await
            .unwrap(),
    )
    .await;
    let returned = collect(
        session
            .execute(
                "INSERT INTO events VALUES (2, 'b'), (3, 'c') RETURNING id, upper(label) AS label",
            )
            .await
            .unwrap(),
    )
    .await;
    let returned_rows = returned
        .iter()
        .flat_map(|batch| {
            let ids = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            let labels = batch
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            (0..batch.num_rows())
                .map(|row| (ids.value(row), labels.value(row).to_owned()))
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    assert_eq!(returned_rows, [(2, "B".to_owned()), (3, "C".to_owned())]);

    assert_eq!(query_count(&session, "events").await, 3);
    let batches = collect(
        session
            .execute("SELECT sum(id) AS total FROM events")
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(
        batches.iter().map(|batch| batch.num_rows()).sum::<usize>(),
        1
    );
}

#[tokio::test]
async fn cancelling_returning_after_commit_does_not_poison_the_engine() {
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("native");
    let csv = directory.path().join("input.csv");
    write_numbered_csv(&csv, 1, 32);
    let engine = Engine::open(
        &database,
        EngineConfig::builder()
            .batch_size(1)
            .compute_threads(1)
            .spill_directory(directory.path().join("spill"))
            .build(),
    )
    .unwrap();
    let session = engine.session();

    collect(
        session
            .execute("CREATE TABLE events AS SELECT 0 AS id, 'seed' AS label")
            .await
            .unwrap(),
    )
    .await;
    let mut result = session
        .execute(&format!(
            "INSERT INTO events SELECT id, label FROM read_csv('{}', header = true) RETURNING id",
            csv.display()
        ))
        .await
        .unwrap();
    let first = result.stream().next().await.unwrap().unwrap();
    assert_eq!(first.num_rows(), 1);
    result.cancel();

    let terminal = loop {
        match result.stream().next().await {
            Some(Ok(_)) => continue,
            Some(Err(error)) => break error,
            None => panic!("cancelled RETURNING stream ended without a durable-commit error"),
        }
    };
    assert!(
        matches!(terminal, Error::NativeCommitPostCommitFailure { .. }),
        "unexpected terminal result: {terminal}"
    );
    assert!(!engine.inner.native_poisoned.load(Ordering::Acquire));
    drop(result);
    assert_eq!(query_count(&session, "events").await, 33);
}

#[tokio::test]
async fn native_delete_persists_visible_rows_and_delete_vectors() {
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("native");
    let config = EngineConfig::builder()
        .compute_threads(1)
        .spill_directory(directory.path().join("spill"))
        .build();
    let engine = Engine::open(&database, config.clone()).unwrap();
    let session = engine.session();

    collect(
        session
            .execute("CREATE TABLE events AS SELECT * FROM (VALUES (1, 'a'), (2, 'b'), (3, 'c')) AS v(id, label)")
            .await
            .unwrap(),
    )
    .await;
    let returned = collect(
        session
            .execute("DELETE FROM events WHERE id = 2 RETURNING id, upper(label) AS label")
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(
        returned[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        2
    );
    assert_eq!(
        returned[0]
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        "B"
    );
    assert_eq!(query_count(&session, "events").await, 2);
    let rows = collect(
        session
            .execute("SELECT id FROM events ORDER BY id")
            .await
            .unwrap(),
    )
    .await;
    let ids = rows[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(ids.values(), &[1, 3]);

    collect(
        session
            .execute("DELETE FROM events WHERE id = 2")
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(query_count(&session, "events").await, 2);
    let delete_vectors = walk_files(&database)
        .into_iter()
        .filter(|path| {
            path.extension()
                .is_some_and(|extension| extension == "rdbdel")
        })
        .count();
    assert_eq!(
        delete_vectors, 1,
        "zero-match DELETE must not publish a version"
    );
    drop(session);
    drop(engine);

    let reopened = Engine::open(&database, config).unwrap();
    assert_eq!(query_count(&reopened.session(), "events").await, 2);
}

#[tokio::test]
async fn native_update_versions_rows_and_survives_reopen() {
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("native");
    let config = EngineConfig::builder()
        .compute_threads(1)
        .spill_directory(directory.path().join("spill"))
        .build();
    let engine = Engine::open(&database, config.clone()).unwrap();
    let session = engine.session();

    collect(
        session
            .execute("CREATE TABLE events AS SELECT * FROM (VALUES (1, 'a'), (2, 'b'), (3, 'c')) AS v(id, label)")
            .await
            .unwrap(),
    )
    .await;
    collect(
        session
            .execute("UPDATE events SET id = id + 10, label = upper(label) WHERE id >= 2")
            .await
            .unwrap(),
    )
    .await;
    let rows = collect(
        session
            .execute("SELECT id, label FROM events ORDER BY id")
            .await
            .unwrap(),
    )
    .await;
    let ids = rows[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let labels = rows[0]
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(ids.values(), &[1, 12, 13]);
    assert_eq!(
        (0..labels.len())
            .map(|row| labels.value(row))
            .collect::<Vec<_>>(),
        ["a", "B", "C"]
    );
    let returned = collect(
        session
            .execute("UPDATE events SET label = lower(label) WHERE id = 12 RETURNING id, label")
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(returned.len(), 1);
    assert_eq!(
        returned[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        12
    );
    assert_eq!(
        returned[0]
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        "b"
    );
    let generations = catalog_generation_count(&database);
    collect(
        session
            .execute("UPDATE events SET label = 'missing' WHERE id = 999")
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(catalog_generation_count(&database), generations);
    drop(session);
    drop(engine);

    let reopened = Engine::open(&database, config).unwrap();
    assert_eq!(query_count(&reopened.session(), "events").await, 3);
}

#[tokio::test]
async fn native_truncate_publishes_an_empty_snapshot() {
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("native");
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
    collect(session.execute("TRUNCATE TABLE events").await.unwrap()).await;
    assert_eq!(query_count(&session, "events").await, 0);
    drop(session);
    drop(engine);

    let reopened = Engine::open(&database, config).unwrap();
    assert_eq!(query_count(&reopened.session(), "events").await, 0);
}

#[tokio::test]
async fn native_alter_rewrites_columns_and_renames_the_catalog_entry() {
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("native");
    let config = EngineConfig::builder()
        .compute_threads(1)
        .spill_directory(directory.path().join("spill"))
        .build();
    let engine = Engine::open(&database, config.clone()).unwrap();
    let session = engine.session();
    collect(
        session
            .execute("CREATE TABLE events (id BIGINT, label VARCHAR)")
            .await
            .unwrap(),
    )
    .await;
    collect(
        session
            .execute("INSERT INTO events VALUES (1, 'a'), (2, 'b')")
            .await
            .unwrap(),
    )
    .await;
    collect(
        session
            .execute("ALTER TABLE events ADD COLUMN score BIGINT DEFAULT 7")
            .await
            .unwrap(),
    )
    .await;
    let rows = collect(
        session
            .execute("SELECT score FROM events ORDER BY id")
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(
        rows.iter()
            .flat_map(|batch| {
                let values = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                (0..values.len())
                    .map(|index| values.value(index))
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>(),
        vec![7, 7]
    );
    collect(
        session
            .execute("ALTER TABLE events RENAME COLUMN label TO name")
            .await
            .unwrap(),
    )
    .await;
    collect(
        session
            .execute("ALTER TABLE events DROP COLUMN score")
            .await
            .unwrap(),
    )
    .await;
    collect(
        session
            .execute("ALTER TABLE events RENAME TO archived_events")
            .await
            .unwrap(),
    )
    .await;
    assert!(session.execute("SELECT * FROM events").await.is_err());
    let rows = collect(
        session
            .execute("SELECT name FROM archived_events WHERE id = 2")
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(
        rows[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        "b"
    );
    drop(session);
    drop(engine);

    let reopened = Engine::open(&database, config).unwrap();
    assert_eq!(query_count(&reopened.session(), "archived_events").await, 2);
}

#[tokio::test]
async fn native_update_from_and_delete_using_apply_join_matches_once() {
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("native");
    let config = EngineConfig::builder()
        .compute_threads(1)
        .spill_directory(directory.path().join("spill"))
        .build();
    let engine = Engine::open(&database, config.clone()).unwrap();
    let session = engine.session();
    for sql in [
        "CREATE TABLE events AS SELECT * FROM (VALUES (1, 'a'), (2, 'b'), (3, 'c')) AS v(id, label)",
        "CREATE TABLE changes AS SELECT * FROM (VALUES (2, 'updated'), (2, 'ignored'), (3, 'three')) AS v(id, label)",
        "CREATE TABLE deletions AS SELECT * FROM (VALUES (1), (3)) AS v(id)",
    ] {
        collect(session.execute(sql).await.unwrap()).await;
    }
    let returned = collect(
        session
            .execute(
                "UPDATE events AS e SET label = c.label FROM changes AS c WHERE e.id = c.id RETURNING id, label",
            )
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(
        returned.iter().map(|batch| batch.num_rows()).sum::<usize>(),
        2
    );
    assert_eq!(
        query_count_where(
            &session,
            "events",
            "label IN ('updated', 'ignored', 'three')"
        )
        .await,
        2
    );
    let returned = collect(
        session
            .execute("DELETE FROM events AS e USING deletions AS d WHERE e.id = d.id RETURNING id")
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(
        returned.iter().map(|batch| batch.num_rows()).sum::<usize>(),
        2
    );
    assert_eq!(query_count(&session, "events").await, 1);
    drop(session);
    drop(engine);

    let reopened = Engine::open(&database, config).unwrap();
    assert_eq!(query_count(&reopened.session(), "events").await, 1);
}

async fn query_count_where(session: &crate::Session, table: &str, predicate: &str) -> i64 {
    let batches = collect(
        session
            .execute(&format!("SELECT count(*) FROM {table} WHERE {predicate}"))
            .await
            .unwrap(),
    )
    .await;
    batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0)
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
async fn cancelled_and_abandoned_native_writes_release_wal_memory_and_staging() {
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("native");
    let csv = directory.path().join("large.csv");
    write_numbered_csv(&csv, 0, 200_000);
    let config = EngineConfig::builder()
        .batch_size(64)
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
    wait_for_native_writes(&engine, 1).await;
    result.cancel();
    let error = result.stream().next().await.unwrap().unwrap_err();
    assert!(matches!(error, Error::Cancelled));
    drop(result);
    wait_for_native_writes(&engine, 0).await;
    assert!(!session.table_names().contains(&"cancelled".to_owned()));
    assert_eq!(
        std::fs::read_dir(database.join("staging")).unwrap().count(),
        0
    );
    assert_eq!(engine.memory_snapshot().current_bytes, 0);
    engine
        .inner
        .database
        .as_ref()
        .unwrap()
        .checkpoint()
        .unwrap();

    let abandoned = session
        .execute(&format!(
            "CREATE TABLE abandoned AS SELECT id, label FROM read_csv('{}', header = true)",
            csv.display()
        ))
        .await
        .unwrap();
    wait_for_native_writes(&engine, 1).await;
    drop(abandoned);
    wait_for_native_writes(&engine, 0).await;
    assert!(!session.table_names().contains(&"abandoned".to_owned()));
    assert_eq!(
        std::fs::read_dir(database.join("staging")).unwrap().count(),
        0
    );
    assert_eq!(engine.memory_snapshot().current_bytes, 0);
    engine
        .inner
        .database
        .as_ref()
        .unwrap()
        .checkpoint()
        .unwrap();
}

async fn wait_for_native_writes(engine: &Engine, expected: u64) {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let active = engine
                .inner
                .database
                .as_ref()
                .unwrap()
                .wal_info()
                .unwrap()
                .active_writes;
            if active == expected {
                return;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("native WAL did not reach {expected} active write(s)"));
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

fn walk_files(directory: &std::path::Path) -> Vec<std::path::PathBuf> {
    std::fs::read_dir(directory)
        .unwrap()
        .flat_map(|entry| {
            let path = entry.unwrap().path();
            if path.is_dir() {
                walk_files(&path)
            } else {
                vec![path]
            }
        })
        .collect()
}
