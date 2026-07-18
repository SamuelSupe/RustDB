use std::path::Path;

use futures::StreamExt;

use super::query_count;
use crate::{Engine, EngineConfig, Error, QueryResult, Result, Session, TransactionOptions};

#[tokio::test]
async fn create_rejects_before_publication_and_cleans_its_wal_write() {
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("database");
    let config = config(&directory.path().join("spill"))
        .native_default_table_limit_bytes(Some(1))
        .build();
    let engine = Engine::open(&database, config).unwrap();
    let error = execute_error(
        &engine.session(),
        "CREATE TABLE events AS SELECT 1 AS id, 'a' AS label",
    )
    .await;

    assert_quota(error, Some("events"));
    assert!(
        engine
            .inner
            .database
            .as_ref()
            .unwrap()
            .table_infos()
            .is_empty()
    );
    assert_eq!(
        engine
            .inner
            .database
            .as_ref()
            .unwrap()
            .wal_info()
            .unwrap()
            .active_writes,
        0
    );
    assert_staging_empty(&database);
}

#[tokio::test]
async fn table_override_rejects_append_and_default_limit_rejects_update() {
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("database");
    let spill = directory.path().join("spill");
    let engine = Engine::open(&database, config(&spill).build()).unwrap();
    execute(
        &engine.session(),
        "CREATE TABLE events AS SELECT 1 AS id, 'a' AS label",
    )
    .await
    .unwrap();
    let storage = table_storage(&engine, "events");
    drop(engine);

    let limited = Engine::open(
        &database,
        config(&spill)
            .native_default_table_limit_bytes(Some(u64::MAX))
            .native_table_limit_bytes("main.events", storage)
            .build(),
    )
    .unwrap();
    let error = execute_error(&limited.session(), "INSERT INTO events VALUES (2, 'b')").await;
    assert_quota(error, Some("events"));
    assert_eq!(query_count(&limited.session(), "events").await, 1);
    assert_native_write_clean(&limited);
    execute(
        &limited.session(),
        "CREATE TABLE other_table AS SELECT 9 AS id",
    )
    .await
    .unwrap();
    drop(limited);

    let writable = Engine::open(
        &database,
        config(&spill)
            .native_default_table_limit_bytes(Some(u64::MAX))
            .build(),
    )
    .unwrap();
    execute(&writable.session(), "INSERT INTO events VALUES (2, 'b')")
        .await
        .unwrap();
    let storage = table_storage(&writable, "events");
    drop(writable);

    let limited = Engine::open(
        &database,
        config(&spill)
            .native_default_table_limit_bytes(Some(storage))
            .build(),
    )
    .unwrap();
    let error = execute_error(
        &limited.session(),
        "UPDATE events SET label = 'updated' WHERE id = 1",
    )
    .await;
    assert_quota(error, Some("events"));
    assert_eq!(query_count(&limited.session(), "events").await, 2);
    assert_native_write_clean(&limited);
}

#[tokio::test]
async fn table_peak_keeps_a_retired_snapshot_pinned_by_an_active_reader() {
    let directory = tempfile::tempdir().unwrap();
    let reference_path = directory.path().join("reference-retired");
    let reference = Engine::open(
        &reference_path,
        config(&directory.path().join("reference-retired-spill")).build(),
    )
    .unwrap();
    create_events(&reference.session()).await;
    let _pinned = reference
        .inner
        .database
        .as_ref()
        .unwrap()
        .table_snapshot("events")
        .unwrap();
    replace_events(&reference.session()).await;
    let table_id = table_id(&reference, "events");
    let first_peak = directory_bytes(&reference_path.join("tables").join(table_id));

    let database = directory.path().join("retired-database");
    let engine = Engine::open(
        &database,
        config(&directory.path().join("retired-spill"))
            .native_default_table_limit_bytes(Some(first_peak))
            .build(),
    )
    .unwrap();
    create_events(&engine.session()).await;
    let _pinned = engine
        .inner
        .database
        .as_ref()
        .unwrap()
        .table_snapshot("events")
        .unwrap();
    replace_events(&engine.session()).await;
    let error = execute_error(&engine.session(), "INSERT INTO events VALUES (3, 'c')").await;
    assert_quota(error, Some("events"));
    assert_eq!(query_count(&engine.session(), "events").await, 2);
    assert_native_write_clean(&engine);
}

#[tokio::test]
async fn multi_table_transaction_is_aggregated_at_commit() {
    let directory = tempfile::tempdir().unwrap();
    let reference_path = directory.path().join("reference");
    let reference = Engine::open(
        &reference_path,
        config(&directory.path().join("reference-spill")).build(),
    )
    .unwrap();
    let mut transaction = reference
        .session()
        .begin_transaction(TransactionOptions::read_write())
        .unwrap();
    stage_two_tables(&transaction).await;
    let staged_root_bytes = directory_bytes(&reference_path);
    transaction.rollback().unwrap();
    drop(reference);

    let database = directory.path().join("database");
    drop(Engine::open(&database, config(&directory.path().join("spill")).build()).unwrap());
    // Prepared checks reserve one 8 KiB table entry plus a 64 KiB WAL record.
    // The two-table commit needs another 8 KiB, so this admits both snapshots
    // and rejects their aggregate at the durable commit boundary.
    let limit = staged_root_bytes + 64 * 1024 + 12 * 1024 + 32;
    let engine = Engine::open(
        &database,
        config(&directory.path().join("spill"))
            .native_engine_limit_bytes(Some(limit))
            .build(),
    )
    .unwrap();
    let mut transaction = engine
        .session()
        .begin_transaction(TransactionOptions::read_write())
        .unwrap();
    stage_two_tables(&transaction).await;
    let error = transaction.commit().unwrap_err();
    assert_quota(error, None);
    assert!(
        engine
            .inner
            .database
            .as_ref()
            .unwrap()
            .table_infos()
            .is_empty()
    );
    assert_native_write_clean(&engine);
    assert_eq!(directory_bytes(&database.join("tables")), 0);
    execute(&engine.session(), "CHECKPOINT").await.unwrap();
    drop(transaction);
    drop(engine);

    let reopened = Engine::open(
        &database,
        config(&directory.path().join("reopen-spill")).build(),
    )
    .unwrap();
    assert!(
        reopened
            .inner
            .database
            .as_ref()
            .unwrap()
            .table_infos()
            .is_empty()
    );
    assert_eq!(directory_bytes(&database.join("tables")), 0);
    assert_staging_empty(&database);
}

#[tokio::test]
async fn concurrent_same_name_creates_share_one_table_quota() {
    let directory = tempfile::tempdir().unwrap();
    let reference_path = directory.path().join("same-name-reference");
    let reference = Engine::open(
        &reference_path,
        config(&directory.path().join("same-name-reference-spill")).build(),
    )
    .unwrap();
    let mut reference_transaction = reference
        .session()
        .begin_transaction(TransactionOptions::read_write())
        .unwrap();
    consume(
        reference_transaction
            .execute("CREATE TABLE events AS SELECT 1 AS id, 'a' AS label")
            .await
            .unwrap(),
    )
    .await
    .unwrap();
    let one_snapshot = directory_bytes(&reference_path.join("tables"));
    reference_transaction.rollback().unwrap();

    let database = directory.path().join("same-name-database");
    let engine = Engine::open(
        &database,
        config(&directory.path().join("same-name-spill"))
            .native_default_table_limit_bytes(Some(one_snapshot + one_snapshot / 2))
            .build(),
    )
    .unwrap();
    let mut first = engine
        .session()
        .begin_transaction(TransactionOptions::read_write())
        .unwrap();
    let mut second = engine
        .session()
        .begin_transaction(TransactionOptions::read_write())
        .unwrap();
    consume(
        first
            .execute("CREATE TABLE events AS SELECT 1 AS id, 'a' AS label")
            .await
            .unwrap(),
    )
    .await
    .unwrap();
    let error = match second
        .execute("CREATE TABLE events AS SELECT 2 AS id, 'b' AS label")
        .await
    {
        Ok(result) => consume(result).await.unwrap_err(),
        Err(error) => error,
    };
    assert_quota(error, Some("events"));
    second.rollback().unwrap();
    first.rollback().unwrap();
    assert_native_write_clean(&engine);
    assert_eq!(directory_bytes(&database.join("tables")), 0);
}

#[tokio::test]
async fn catalog_only_commit_and_rename_target_are_quota_checked() {
    let directory = tempfile::tempdir().unwrap();
    let catalog_database = directory.path().join("catalog-database");
    drop(
        Engine::open(
            &catalog_database,
            config(&directory.path().join("catalog-spill")).build(),
        )
        .unwrap(),
    );
    let current = directory_bytes(&catalog_database);
    let catalog_limited = Engine::open(
        &catalog_database,
        config(&directory.path().join("catalog-spill"))
            .native_engine_limit_bytes(Some(current))
            .build(),
    )
    .unwrap();
    let error = execute_error(
        &catalog_limited.session(),
        "CREATE VIEW answer AS SELECT 42 AS value",
    )
    .await;
    assert_quota(error, None);
    assert_native_write_clean(&catalog_limited);

    let rename_database = directory.path().join("rename-database");
    let rename_limited = Engine::open(
        &rename_database,
        config(&directory.path().join("rename-spill"))
            .native_default_table_limit_bytes(Some(u64::MAX))
            .native_table_limit_bytes("archived", 1)
            .build(),
    )
    .unwrap();
    execute(
        &rename_limited.session(),
        "CREATE TABLE events AS SELECT 1 AS id",
    )
    .await
    .unwrap();
    let error = execute_error(
        &rename_limited.session(),
        "ALTER TABLE events RENAME TO archived",
    )
    .await;
    assert_quota(error, Some("archived"));
    assert_eq!(query_count(&rename_limited.session(), "events").await, 1);
    assert_native_write_clean(&rename_limited);
}

#[tokio::test]
async fn large_persistent_view_is_counted_in_engine_catalog_headroom() {
    let directory = tempfile::tempdir().unwrap();
    let reference_path = directory.path().join("view-reference");
    let reference = Engine::open(
        &reference_path,
        config(&directory.path().join("view-reference-spill")).build(),
    )
    .unwrap();
    let payload = "x".repeat(256 * 1024);
    execute(
        &reference.session(),
        &format!("CREATE VIEW huge AS SELECT '{payload}' AS value"),
    )
    .await
    .unwrap();
    drop(reference);

    let database = directory.path().join("view-limited");
    drop(
        Engine::open(
            &database,
            config(&directory.path().join("view-limited-spill")).build(),
        )
        .unwrap(),
    );
    let current = directory_bytes(&database);
    let limit = current + 128 * 1024;
    let limited = Engine::open(
        &database,
        config(&directory.path().join("view-limited-spill"))
            .native_engine_limit_bytes(Some(limit))
            .build(),
    )
    .unwrap();
    let error = execute_error(
        &limited.session(),
        &format!("CREATE VIEW huge AS SELECT '{payload}' AS value"),
    )
    .await;
    assert_quota(error, None);
    assert!(directory_bytes(&database) <= limit);
    assert_native_write_clean(&limited);
    assert!(
        !limited
            .inner
            .native_poisoned
            .load(std::sync::atomic::Ordering::Acquire)
    );
    execute(&limited.session(), "CREATE VIEW tiny AS SELECT 1 AS value")
        .await
        .unwrap();
}

#[tokio::test]
async fn reopen_applies_new_engine_policy_without_persisting_it() {
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("database");
    let spill = directory.path().join("spill");
    let engine = Engine::open(&database, config(&spill).build()).unwrap();
    execute(&engine.session(), "CREATE TABLE events AS SELECT 1 AS id")
        .await
        .unwrap();
    drop(engine);

    let current = directory_bytes(&database);
    let limited = Engine::open(
        &database,
        config(&spill)
            .native_engine_limit_bytes(Some(current))
            .build(),
    )
    .unwrap();
    let error = execute_error(&limited.session(), "INSERT INTO events VALUES (2)").await;
    assert_quota(error, None);
    drop(limited);

    let reopened = Engine::open(
        &database,
        config(&spill)
            .native_engine_limit_bytes(Some(u64::MAX))
            .build(),
    )
    .unwrap();
    execute(&reopened.session(), "INSERT INTO events VALUES (2)")
        .await
        .unwrap();
    assert_eq!(query_count(&reopened.session(), "events").await, 2);
}

fn config(spill: &Path) -> crate::EngineConfigBuilder {
    EngineConfig::builder()
        .compute_threads(1)
        .spill_directory(spill)
}

async fn stage_two_tables(transaction: &crate::Transaction) {
    for sql in [
        "CREATE TABLE first_table AS SELECT 1 AS id, 'first' AS label",
        "CREATE TABLE second_table AS SELECT 2 AS id, 'second' AS label",
    ] {
        consume(transaction.execute(sql).await.unwrap())
            .await
            .unwrap();
    }
}

async fn execute(session: &Session, sql: &str) -> Result<()> {
    consume(session.execute(sql).await?).await
}

async fn execute_error(session: &Session, sql: &str) -> Error {
    match execute(session, sql).await {
        Ok(()) => panic!("{sql} unexpectedly succeeded"),
        Err(error) => error,
    }
}

async fn consume(result: QueryResult) -> Result<()> {
    let mut stream = result.into_stream();
    while let Some(batch) = stream.next().await {
        batch?;
    }
    Ok(())
}

fn table_storage(engine: &Engine, name: &str) -> u64 {
    engine
        .inner
        .database
        .as_ref()
        .unwrap()
        .table_infos()
        .into_iter()
        .find(|table| table.name == name)
        .unwrap()
        .storage_bytes
}

fn table_id(engine: &Engine, name: &str) -> String {
    engine
        .inner
        .database
        .as_ref()
        .unwrap()
        .table_infos()
        .into_iter()
        .find(|table| table.name == name)
        .unwrap()
        .table_id
}

async fn create_events(session: &Session) {
    execute(
        session,
        "CREATE TABLE events AS SELECT * FROM (VALUES (1, 'a'), (2, 'b')) AS v(id, label)",
    )
    .await
    .unwrap();
}

async fn replace_events(session: &Session) {
    execute(
        session,
        "CREATE OR REPLACE TABLE events AS SELECT id, label FROM events",
    )
    .await
    .unwrap();
}

fn assert_quota(error: Error, expected_table: Option<&str>) {
    let Error::NativeDiskQuotaExceeded {
        table,
        current_bytes,
        added_bytes,
        peak_bytes,
        limit_bytes,
        ..
    } = error
    else {
        panic!("expected structured Native quota error, got {error}");
    };
    assert_eq!(table.as_deref(), expected_table);
    assert_eq!(current_bytes.checked_add(added_bytes), Some(peak_bytes));
    assert!(peak_bytes > limit_bytes);
}

fn assert_native_write_clean(engine: &Engine) {
    assert_eq!(
        engine
            .inner
            .database
            .as_ref()
            .unwrap()
            .wal_info()
            .unwrap()
            .active_writes,
        0
    );
    assert_staging_empty(engine.database_path().unwrap());
}

fn assert_staging_empty(database: &Path) {
    assert_eq!(
        std::fs::read_dir(database.join("staging")).unwrap().count(),
        0
    );
}

fn directory_bytes(path: &Path) -> u64 {
    std::fs::read_dir(path)
        .unwrap()
        .map(|entry| {
            let path = entry.unwrap().path();
            let metadata = std::fs::symlink_metadata(&path).unwrap();
            if metadata.is_dir() {
                directory_bytes(&path)
            } else {
                metadata.len()
            }
        })
        .sum()
}
