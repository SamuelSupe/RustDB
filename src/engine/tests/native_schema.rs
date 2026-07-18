use arrow::array::{Array, Int64Array, StringArray};
use futures::StreamExt;

use super::collect;
use crate::{CsvOptions, Engine, EngineConfig, Error, TransactionOptions};

#[tokio::test]
async fn qualified_native_objects_survive_reopen_and_backup_restore() {
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("native");
    let backup = directory.path().join("backup");
    let restored = directory.path().join("restored");
    let csv = directory.path().join("analytics-events.csv");
    let config = EngineConfig::builder()
        .compute_threads(1)
        .spill_directory(directory.path().join("spill"))
        .build();
    let engine = Engine::open(&database, config.clone()).unwrap();
    let session = engine.session();

    execute(&session, "CREATE SCHEMA analytics").await;
    execute(&session, "CREATE TABLE events AS SELECT 1 AS id").await;
    execute(
        &session,
        "CREATE TABLE analytics.events AS SELECT * FROM (VALUES (10), (20)) AS v(id)",
    )
    .await;
    execute(&session, "INSERT INTO analytics.events VALUES (30)").await;
    execute(
        &session,
        "UPDATE analytics.events SET id = id + 1 WHERE id = 30",
    )
    .await;
    execute(&session, "DELETE FROM analytics.events WHERE id = 10").await;
    execute(
        &session,
        "ALTER TABLE analytics.events ADD COLUMN label VARCHAR DEFAULT 'kept'",
    )
    .await;
    execute(&session, "ANALYZE analytics.events").await;
    execute(&session, "COMPACT TABLE analytics.events").await;

    execute(
        &session,
        &format!(
            "COPY analytics.events TO '{}' (FORMAT CSV, HEADER TRUE)",
            csv.display()
        ),
    )
    .await;
    execute(
        &session,
        "CREATE TABLE analytics.imported (id BIGINT, label VARCHAR)",
    )
    .await;
    execute(
        &session,
        &format!(
            "COPY analytics.imported FROM '{}' (FORMAT CSV, HEADER TRUE)",
            csv.display()
        ),
    )
    .await;
    execute(
        &session,
        "CREATE VIEW analytics.event_ids AS SELECT id FROM analytics.imported",
    )
    .await;

    assert_eq!(scalar(&session, "SELECT count(*) FROM events").await, 1);
    assert_eq!(
        scalar(&session, "SELECT count(*) FROM analytics.events").await,
        2
    );
    assert_eq!(
        scalar(
            &session,
            "SELECT count(*) FROM analytics.event_ids WHERE id IN (20, 31)",
        )
        .await,
        2
    );
    assert_information_schema(&session).await;

    let prepared = session
        .prepare("SELECT count(*) FROM analytics.imported WHERE label = 'kept'")
        .unwrap();
    assert_eq!(scalar_result(prepared.execute(&[]).await.unwrap()).await, 2);
    drop(prepared);

    let error = execute_error(&session, "DROP SCHEMA main").await;
    assert!(error.to_string().contains("cannot be dropped"));
    let error = execute_error(&session, "DROP SCHEMA analytics").await;
    assert!(error.to_string().contains("not empty"));

    engine.backup_to(&backup).unwrap();
    drop(session);
    drop(engine);

    let reopened = Engine::open(&database, config.clone()).unwrap();
    assert_eq!(
        scalar(
            &reopened.session(),
            "SELECT count(*) FROM analytics.event_ids",
        )
        .await,
        2
    );
    assert_information_schema(&reopened.session()).await;
    drop(reopened);

    let restored_engine = Engine::restore_from(&backup, &restored, config).unwrap();
    assert_eq!(
        scalar(
            &restored_engine.session(),
            "SELECT count(*) FROM analytics.imported",
        )
        .await,
        2
    );
    execute(&restored_engine.session(), "TRUNCATE analytics.events").await;
    assert_eq!(
        scalar(
            &restored_engine.session(),
            "SELECT count(*) FROM analytics.events",
        )
        .await,
        0
    );
}

#[tokio::test]
async fn schema_ddl_is_transactional_and_detects_catalog_conflicts() {
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

    let mut transaction = session
        .begin_transaction(TransactionOptions::read_write())
        .unwrap();
    consume_transaction(&transaction, "CREATE SCHEMA staged").await;
    assert_eq!(
        transaction_scalar(
            &transaction,
            "SELECT count(*) FROM information_schema.schemata WHERE schema_name = 'staged'",
        )
        .await,
        1
    );
    let shown = collect(transaction.execute("SHOW SCHEMAS").await.unwrap()).await;
    assert!(string_values(&shown).contains(&"staged".to_owned()));
    consume_transaction(&transaction, "CREATE TABLE staged.items AS SELECT 7 AS id").await;
    consume_transaction(
        &transaction,
        "CREATE VIEW staged.item_ids AS SELECT id FROM staged.items",
    )
    .await;
    assert_eq!(
        transaction_scalar(&transaction, "SELECT count(*) FROM staged.item_ids").await,
        1
    );
    transaction.commit().unwrap();
    assert_eq!(
        scalar(&session, "SELECT count(*) FROM staged.items").await,
        1
    );

    let mut create_race = session
        .begin_transaction(TransactionOptions::read_write())
        .unwrap();
    consume_transaction(&create_race, "CREATE SCHEMA race").await;
    execute(&session, "CREATE SCHEMA race").await;
    assert!(matches!(
        create_race.commit().unwrap_err(),
        Error::TransactionConflict { .. }
    ));

    execute(&session, "CREATE SCHEMA drop_race").await;
    let mut drop_race = session
        .begin_transaction(TransactionOptions::read_write())
        .unwrap();
    consume_transaction(&drop_race, "DROP SCHEMA drop_race").await;
    execute(
        &session,
        "CREATE TABLE drop_race.concurrent AS SELECT 1 AS id",
    )
    .await;
    assert!(matches!(
        drop_race.commit().unwrap_err(),
        Error::TransactionConflict { .. }
    ));

    let mut cleanup = session
        .begin_transaction(TransactionOptions::read_write())
        .unwrap();
    consume_transaction(&cleanup, "DROP TABLE drop_race.concurrent").await;
    consume_transaction(&cleanup, "DROP SCHEMA drop_race").await;
    cleanup.commit().unwrap();
    execute(&session, "CREATE SCHEMA IF NOT EXISTS drop_race").await;
    execute(&session, "DROP SCHEMA IF EXISTS drop_race").await;

    let error = execute_error(&session, "DROP SCHEMA staged").await;
    assert!(error.to_string().contains("not empty"));
    execute(&session, "DROP VIEW staged.item_ids").await;
    execute(&session, "DROP TABLE staged.items").await;
    execute(&session, "DROP SCHEMA staged").await;
    assert!(
        session
            .execute("CREATE TABLE staged.missing AS SELECT 1 AS id")
            .await
            .is_err()
    );
}

#[tokio::test]
async fn qualified_temporary_views_and_identifier_case_remain_resolvable() {
    let directory = tempfile::tempdir().unwrap();
    let csv = directory.path().join("external.csv");
    std::fs::write(&csv, "1\n").unwrap();
    let session = Engine::open(
        directory.path().join("native"),
        EngineConfig::builder()
            .spill_directory(directory.path().join("spill"))
            .build(),
    )
    .unwrap()
    .session();
    execute(&session, "CREATE SCHEMA analytics").await;
    assert!(
        session
            .register_csv(
                "missing.external",
                [csv.to_string_lossy().into_owned()],
                CsvOptions::default(),
            )
            .await
            .is_err()
    );
    session
        .register_csv(
            "analytics.external",
            [csv.to_string_lossy().into_owned()],
            CsvOptions::default(),
        )
        .await
        .unwrap();
    execute(&session, "REFRESH TABLE analytics.external").await;
    assert!(session.catalog().table("analytics.external").is_some());
    execute(
        &session,
        "CREATE TABLE analytics.\"MixedCase\" AS SELECT 9 AS id",
    )
    .await;
    let error = execute_error(
        &session,
        "CREATE TEMP VIEW missing.temp_values AS SELECT 1 AS id",
    )
    .await;
    assert!(
        error
            .to_string()
            .contains("schema 'missing' does not exist")
    );
    execute(
        &session,
        "CREATE TEMP VIEW analytics.temp_values AS SELECT id FROM analytics.\"MixedCase\"",
    )
    .await;
    assert_eq!(
        scalar(&session, "SELECT id FROM analytics.temp_values").await,
        9
    );
    assert_eq!(
        scalar(&session, "SELECT id FROM ANALYTICS.MIXEDCASE").await,
        9
    );
    assert_eq!(
        scalar(
            &session,
            "SELECT analytics.\"MixedCase\".* FROM analytics.\"MixedCase\"",
        )
        .await,
        9
    );
    assert!(session.execute("SELECT * FROM a.b.c").await.is_err());
}

#[tokio::test]
async fn same_named_tables_resolve_full_relation_qualifiers_and_wildcards() {
    let directory = tempfile::tempdir().unwrap();
    let session = Engine::open(
        directory.path().join("native"),
        EngineConfig::builder()
            .compute_threads(1)
            .spill_directory(directory.path().join("spill"))
            .build(),
    )
    .unwrap()
    .session();
    execute(&session, "CREATE SCHEMA analytics").await;
    execute(&session, "CREATE SCHEMA sales").await;
    execute(
        &session,
        "CREATE TABLE analytics.events AS \
         SELECT * FROM (VALUES (1, 10), (2, 20)) AS v(id, metric)",
    )
    .await;
    execute(
        &session,
        "CREATE TABLE sales.events AS \
         SELECT * FROM (VALUES (1, 100), (3, 300)) AS v(id, amount)",
    )
    .await;

    let inserted = collect(
        session
            .execute(
                "INSERT INTO analytics.events VALUES (3, 30) \
                 RETURNING analytics.events.id, analytics.events.metric",
            )
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(int64_pair(&inserted), (3, 30));
    let inserted = collect(
        session
            .execute(
                "INSERT INTO sales.events VALUES (4, 400) \
                 RETURNING sales.events.*",
            )
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(int64_pair(&inserted), (4, 400));

    let rows = collect(
        session
            .execute(
                "SELECT analytics.events.*, sales.events.* \
                 FROM analytics.events JOIN sales.events \
                 ON analytics.events.id = sales.events.id",
            )
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(rows[0].num_columns(), 4);
    let values = rows[0]
        .columns()
        .iter()
        .map(|column| {
            column
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0)
        })
        .collect::<Vec<_>>();
    assert_eq!(values, vec![1, 10, 1, 100]);

    assert_eq!(
        scalar(
            &session,
            "SELECT events.id FROM analytics.events WHERE events.id = 1",
        )
        .await,
        1
    );
    assert_eq!(
        scalar(
            &session,
            "SELECT a.id FROM analytics.events AS a WHERE a.id = 2",
        )
        .await,
        2
    );
    let error = execute_error(
        &session,
        "SELECT events.id FROM analytics.events JOIN sales.events \
         ON analytics.events.id = sales.events.id",
    )
    .await;
    assert!(error.to_string().contains("ambiguous"), "{error}");
}

#[tokio::test]
async fn qualified_native_update_and_delete_keep_the_target_namespace() {
    let directory = tempfile::tempdir().unwrap();
    let session = Engine::open(
        directory.path().join("native"),
        EngineConfig::builder()
            .compute_threads(1)
            .spill_directory(directory.path().join("spill"))
            .build(),
    )
    .unwrap()
    .session();
    execute(&session, "CREATE SCHEMA analytics").await;
    execute(&session, "CREATE SCHEMA sales").await;
    execute(
        &session,
        "CREATE TABLE analytics.events AS \
         SELECT * FROM (VALUES (1, 10), (2, 20)) AS v(id, metric)",
    )
    .await;
    execute(
        &session,
        "CREATE TABLE sales.events AS \
         SELECT * FROM (VALUES (1, 100), (2, 200)) AS v(id, metric)",
    )
    .await;

    let updated = collect(
        session
            .execute(
                "UPDATE analytics.events \
                 SET analytics.events.metric = analytics.events.metric + 5 \
                 WHERE analytics.events.id = 1 \
                 RETURNING analytics.events.id, analytics.events.metric",
            )
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(int64_pair(&updated), (1, 15));

    let deleted = collect(
        session
            .execute(
                "DELETE FROM analytics.events \
                 WHERE analytics.events.id = 2 RETURNING analytics.events.*",
            )
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(int64_pair(&deleted), (2, 20));

    let updated = collect(
        session
            .execute(
                "UPDATE sales.events AS s SET s.metric = s.metric + 7 \
                 WHERE s.id = 1 RETURNING s.id, s.metric",
            )
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(int64_pair(&updated), (1, 107));

    let deleted = collect(
        session
            .execute("DELETE FROM sales.events AS s WHERE s.id = 2 RETURNING s.*")
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(int64_pair(&deleted), (2, 200));

    assert_eq!(
        scalar(&session, "SELECT metric FROM analytics.events WHERE id = 1",).await,
        15
    );
    assert_eq!(
        scalar(&session, "SELECT metric FROM sales.events WHERE id = 1").await,
        107
    );
    assert_eq!(
        scalar(&session, "SELECT count(*) FROM analytics.events").await,
        1
    );
    assert_eq!(
        scalar(&session, "SELECT count(*) FROM sales.events").await,
        1
    );
}

#[tokio::test]
async fn durable_ddl_and_sql_commit_status_survive_late_cancellation() {
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("native");
    let config = EngineConfig::builder()
        .compute_threads(1)
        .spill_directory(directory.path().join("spill"))
        .build();
    let engine = Engine::open(&database, config.clone()).unwrap();
    let session = engine.session();

    cancel_after_durable_status(&session, "CREATE SCHEMA keep", "CREATE SCHEMA").await;
    execute(&session, "CREATE TABLE keep.events AS SELECT 1 AS id").await;
    cancel_after_durable_status(
        &session,
        "ALTER TABLE keep.events RENAME TO archived",
        "ALTER TABLE",
    )
    .await;
    cancel_after_durable_status(
        &session,
        "CREATE VIEW keep.ids AS SELECT id FROM keep.archived",
        "CREATE VIEW",
    )
    .await;
    cancel_after_durable_status(&session, "DROP VIEW keep.ids", "DROP VIEW").await;
    cancel_after_durable_status(&session, "DROP TABLE keep.archived", "DROP TABLE").await;
    execute(&session, "CREATE SCHEMA ephemeral").await;
    cancel_after_durable_status(&session, "DROP SCHEMA ephemeral", "DROP SCHEMA").await;

    execute(&session, "BEGIN").await;
    execute(&session, "CREATE TABLE keep.committed AS SELECT 7 AS id").await;
    cancel_after_durable_status(&session, "COMMIT", "COMMIT").await;
    assert_eq!(scalar(&session, "SELECT id FROM keep.committed").await, 7);
    assert!(
        !engine
            .inner
            .native_poisoned
            .load(std::sync::atomic::Ordering::Acquire)
    );
    drop(session);
    drop(engine);

    let reopened = Engine::open(&database, config).unwrap();
    let session = reopened.session();
    assert_eq!(scalar(&session, "SELECT id FROM keep.committed").await, 7);
    assert_eq!(
        scalar(
            &session,
            "SELECT count(*) FROM information_schema.schemata WHERE schema_name = 'keep'",
        )
        .await,
        1
    );
    assert_eq!(
        scalar(
            &session,
            "SELECT count(*) FROM information_schema.schemata WHERE schema_name = 'ephemeral'",
        )
        .await,
        0
    );
}

async fn cancel_after_durable_status(session: &crate::Session, sql: &str, expected: &str) {
    let mut result = session.execute(sql).await.unwrap();
    result.cancel();
    let batch = result.stream().next().await.unwrap().unwrap();
    assert_eq!(
        batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        expected
    );
    assert!(result.stream().next().await.is_none());
}

async fn assert_information_schema(session: &crate::Session) {
    assert_eq!(
        scalar(
            session,
            "SELECT count(*) FROM information_schema.schemata \
             WHERE schema_name IN ('main', 'analytics')",
        )
        .await,
        2
    );
    let shown = collect(session.execute("SHOW SCHEMAS").await.unwrap()).await;
    assert_eq!(string_values(&shown), vec!["analytics", "main"]);
    let rows = collect(
        session
            .execute(
                "SELECT table_schema, table_name FROM information_schema.tables \
                 WHERE table_name IN ('events', 'imported', 'event_ids') \
                 ORDER BY table_schema, table_name",
            )
            .await
            .unwrap(),
    )
    .await;
    let schemas = rows[0]
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let names = rows[0]
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let actual = (0..rows[0].num_rows())
        .map(|index| (schemas.value(index), names.value(index)))
        .collect::<Vec<_>>();
    assert_eq!(
        actual,
        vec![
            ("analytics", "event_ids"),
            ("analytics", "events"),
            ("analytics", "imported"),
            ("main", "events"),
        ]
    );
    assert_eq!(
        scalar(
            session,
            "SELECT count(*) FROM information_schema.columns \
             WHERE table_schema = 'analytics' AND table_name = 'imported'",
        )
        .await,
        2
    );
}

fn string_values(batches: &[arrow::record_batch::RecordBatch]) -> Vec<String> {
    let values = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    (0..values.len())
        .map(|index| values.value(index).to_owned())
        .collect()
}

fn int64_pair(batches: &[arrow::record_batch::RecordBatch]) -> (i64, i64) {
    let left = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    let right = batches[0]
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    (left, right)
}

async fn execute(session: &crate::Session, sql: &str) {
    collect(session.execute(sql).await.unwrap()).await;
}

async fn execute_error(session: &crate::Session, sql: &str) -> Error {
    match session.execute(sql).await {
        Ok(_) => panic!("{sql} unexpectedly succeeded"),
        Err(error) => error,
    }
}

async fn scalar(session: &crate::Session, sql: &str) -> i64 {
    scalar_result(session.execute(sql).await.unwrap()).await
}

async fn scalar_result(result: crate::QueryResult) -> i64 {
    let batches = collect(result).await;
    batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0)
}

async fn consume_transaction(transaction: &crate::Transaction, sql: &str) {
    collect(transaction.execute(sql).await.unwrap()).await;
}

async fn transaction_scalar(transaction: &crate::Transaction, sql: &str) -> i64 {
    scalar_result(transaction.execute(sql).await.unwrap()).await
}
