use arrow::array::{Int64Array, StringArray};
use futures::StreamExt;

use super::*;
use crate::{Engine, EngineConfig, ParameterValue};

async fn consume(mut result: QueryResult) -> Vec<arrow::record_batch::RecordBatch> {
    let mut batches = Vec::new();
    while let Some(batch) = result.stream().next().await {
        batches.push(batch.unwrap());
    }
    batches
}

#[tokio::test]
async fn pins_one_snapshot_and_blocks_commit_with_an_active_result() {
    let directory = tempfile::tempdir().unwrap();
    let engine = Engine::open(directory.path().join("database"), EngineConfig::default()).unwrap();
    let session = engine.session();
    let mut transaction = session
        .begin_transaction(TransactionOptions::read_write())
        .unwrap();
    assert_eq!(transaction.snapshot_generation(), 0);
    assert_eq!(engine.transaction_counts_for_test(), (1, 1));

    let result = transaction.execute("SELECT 1 AS value").await.unwrap();
    let error = transaction.commit().unwrap_err();
    assert!(error.to_string().contains("active result stream"));
    drop(result);
    drop(transaction);
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while engine.transaction_counts_for_test() != (0, 0) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("dropped result must release its transaction after query quiescence");
}

#[tokio::test]
async fn transaction_commit_state_matches_the_durable_boundary_after_restart() {
    use crate::storage::{NativeCommitTestBoundary, arm_native_commit_test_failpoint};

    for (boundary, expected_state, committed) in [
        (
            NativeCommitTestBoundary::CatalogPrepared,
            "rolled back",
            false,
        ),
        (
            NativeCommitTestBoundary::WalCommitted,
            "commit outcome unknown",
            true,
        ),
        (
            NativeCommitTestBoundary::CatalogPublished,
            "committed",
            true,
        ),
    ] {
        let directory = tempfile::tempdir().unwrap();
        let database = directory.path().join("database");
        let config = EngineConfig::builder()
            .compute_threads(1)
            .spill_directory(directory.path().join("spill"))
            .build();
        let engine = Engine::open(&database, config.clone()).unwrap();
        let session = engine.session();
        let mut transaction = session
            .begin_transaction(TransactionOptions::read_write())
            .unwrap();
        consume(
            transaction
                .execute("CREATE TABLE events AS SELECT 1 AS id")
                .await
                .unwrap(),
        )
        .await;

        arm_native_commit_test_failpoint(boundary);
        let error = transaction.commit().unwrap_err();
        match boundary {
            NativeCommitTestBoundary::CatalogPrepared => {
                assert!(matches!(error, Error::NativeStorage { .. }), "{error:?}");
                assert!(transaction.commit_info().is_none());
            }
            NativeCommitTestBoundary::WalCommitted => {
                assert!(
                    matches!(error, Error::CommitOutcomeUnknown { .. }),
                    "{error:?}"
                );
                assert!(transaction.commit_info().is_none());
            }
            NativeCommitTestBoundary::CatalogPublished => {
                assert!(
                    matches!(error, Error::NativeCommitPostCommitFailure { .. }),
                    "{error:?}"
                );
                assert_eq!(
                    transaction
                        .commit_info()
                        .and_then(|info| info.committed_generation()),
                    Some(1)
                );
            }
            NativeCommitTestBoundary::SnapshotPublished => unreachable!(),
        }
        assert!(matches!(
            transaction.rollback().unwrap_err(),
            Error::TransactionClosed { state, .. } if state == expected_state
        ));

        drop(transaction);
        drop(session);
        drop(engine);
        let reopened = Engine::open(&database, config).unwrap();
        let visible = reopened.session().execute("SELECT id FROM events").await;
        assert_eq!(
            visible.is_ok(),
            committed,
            "wrong visibility at {boundary:?}"
        );
        if let Ok(result) = visible {
            assert_eq!(consume(result).await[0].num_rows(), 1);
        }
    }
}

#[tokio::test]
async fn sql_commit_preserves_durable_failure_classification_and_visibility() {
    use crate::storage::{NativeCommitTestBoundary, arm_native_commit_test_failpoint};

    for boundary in [
        NativeCommitTestBoundary::WalCommitted,
        NativeCommitTestBoundary::CatalogPublished,
    ] {
        let directory = tempfile::tempdir().unwrap();
        let database = directory.path().join("database");
        let config = EngineConfig::builder()
            .compute_threads(1)
            .spill_directory(directory.path().join("spill"))
            .build();
        let engine = Engine::open(&database, config.clone()).unwrap();
        let session = engine.session();
        consume(session.execute("BEGIN").await.unwrap()).await;
        consume(
            session
                .execute("CREATE TABLE events AS SELECT 1 AS id")
                .await
                .unwrap(),
        )
        .await;

        arm_native_commit_test_failpoint(boundary);
        let error = match session.execute("COMMIT").await {
            Ok(_) => panic!("COMMIT unexpectedly succeeded at {boundary:?}"),
            Err(error) => error,
        };
        match boundary {
            NativeCommitTestBoundary::WalCommitted => {
                assert!(
                    matches!(error, Error::CommitOutcomeUnknown { .. }),
                    "{error:?}"
                );
            }
            NativeCommitTestBoundary::CatalogPublished => {
                assert!(
                    matches!(error, Error::NativeCommitPostCommitFailure { .. }),
                    "{error:?}"
                );
            }
            NativeCommitTestBoundary::SnapshotPublished
            | NativeCommitTestBoundary::CatalogPrepared => unreachable!(),
        }
        let no_active = match session.execute("ROLLBACK").await {
            Ok(_) => panic!("ROLLBACK unexpectedly found an active transaction"),
            Err(error) => error,
        };
        assert!(no_active.to_string().contains("no transaction is active"));

        drop(session);
        drop(engine);
        let reopened = Engine::open(&database, config).unwrap();
        assert_eq!(
            query_session_scalar(&reopened.session(), "SELECT id FROM events").await,
            1,
            "durable SQL COMMIT disappeared at {boundary:?}"
        );
    }
}

#[tokio::test]
async fn ambiguous_wal_publication_leaves_transaction_indeterminate_and_recovers() {
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("database");
    let config = EngineConfig::builder()
        .compute_threads(1)
        .spill_directory(directory.path().join("spill"))
        .build();
    let engine = Engine::open(&database, config.clone()).unwrap();
    let session = engine.session();
    let mut transaction = session
        .begin_transaction(TransactionOptions::read_write())
        .unwrap();
    consume(
        transaction
            .execute("CREATE TABLE events AS SELECT 1 AS id")
            .await
            .unwrap(),
    )
    .await;

    crate::storage::arm_native_wal_ambiguous_reconciliation();
    let error = transaction.commit().unwrap_err();
    assert!(
        matches!(error, Error::CommitOutcomeUnknown { .. }),
        "{error:?}"
    );
    assert!(transaction.commit_info().is_none());
    assert!(matches!(
        transaction.rollback().unwrap_err(),
        Error::TransactionClosed {
            state: "commit outcome unknown",
            ..
        }
    ));

    drop(transaction);
    drop(session);
    drop(engine);
    let reopened = Engine::open(&database, config).unwrap();
    assert_eq!(
        query_session_scalar(&reopened.session(), "SELECT id FROM events").await,
        1
    );
}

#[tokio::test]
async fn failed_cancelled_and_abandoned_mutations_rollback_only_the_statement() {
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("database");
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

    let mut schema_transaction = session
        .begin_transaction(TransactionOptions::read_write())
        .unwrap();
    let mut schema_result = schema_transaction
        .execute("CREATE SCHEMA cancelled_schema")
        .await
        .unwrap();
    schema_result.cancel();
    assert!(matches!(
        schema_result.stream().next().await.unwrap().unwrap_err(),
        Error::Cancelled
    ));
    drop(schema_result);
    wait_for_transaction_results(&schema_transaction.shared).await;
    consume(
        schema_transaction
            .execute("CREATE SCHEMA kept_schema")
            .await
            .unwrap(),
    )
    .await;
    schema_transaction.commit().unwrap();
    assert_eq!(
        query_session_scalar(
            &session,
            "SELECT count(*) FROM information_schema.schemata WHERE schema_name = 'cancelled_schema'",
        )
        .await,
        0
    );
    assert_eq!(
        query_session_scalar(
            &session,
            "SELECT count(*) FROM information_schema.schemata WHERE schema_name = 'kept_schema'",
        )
        .await,
        1
    );

    consume(
        session
            .execute("CREATE TABLE events AS SELECT 1 AS id")
            .await
            .unwrap(),
    )
    .await;
    let mut insert_transaction = session
        .begin_transaction(TransactionOptions::read_write())
        .unwrap();
    consume(
        insert_transaction
            .execute("INSERT INTO events VALUES (10)")
            .await
            .unwrap(),
    )
    .await;
    let values = (2..=65)
        .map(|value| format!("({value})"))
        .collect::<Vec<_>>()
        .join(", ");
    let mut insert_result = insert_transaction
        .execute(&format!("INSERT INTO events VALUES {values} RETURNING id"))
        .await
        .unwrap();
    assert_eq!(
        insert_result
            .stream()
            .next()
            .await
            .unwrap()
            .unwrap()
            .num_rows(),
        1
    );
    drop(insert_result);
    wait_for_transaction_results(&insert_transaction.shared).await;
    assert!(
        insert_transaction
            .execute("INSERT INTO missing_table VALUES (1)")
            .await
            .is_err()
    );
    consume(
        insert_transaction
            .execute("INSERT INTO events VALUES (99)")
            .await
            .unwrap(),
    )
    .await;
    insert_transaction.commit().unwrap();
    assert_eq!(
        query_session_scalar(&session, "SELECT count(*) FROM events").await,
        3
    );
    assert_eq!(
        std::fs::read_dir(database.join("staging")).unwrap().count(),
        0
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
}

async fn wait_for_transaction_results(shared: &Shared) {
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while {
            let state = shared.state.lock();
            state.active_results != 0 || state.rollback_pending
        } {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("transaction result guard did not quiesce");
}

#[tokio::test]
async fn prepared_queries_obey_the_transaction_lifetime() {
    let directory = tempfile::tempdir().unwrap();
    let engine = Engine::open(directory.path().join("database"), EngineConfig::default()).unwrap();
    let mut transaction = engine
        .session()
        .begin_transaction(TransactionOptions::read_write())
        .unwrap();
    let prepared = transaction.prepare("SELECT $1 AS value").unwrap();
    assert_eq!(prepared.parameter_count(), 1);

    let mut result = prepared
        .execute(&[ParameterValue::Int64(42)])
        .await
        .unwrap();
    let batch = result.stream().next().await.unwrap().unwrap();
    assert_eq!(batch.num_rows(), 1);
    assert!(result.stream().next().await.is_none());
    drop(result);

    let info = transaction.commit().unwrap();
    assert_eq!(info.snapshot_generation(), 0);
    assert_eq!(info.committed_generation(), None);
    assert!(prepared.execute(&[ParameterValue::Int64(7)]).await.is_err());
}

#[tokio::test]
async fn transaction_drop_rolls_back_surviving_prepared_statements() {
    let directory = tempfile::tempdir().unwrap();
    let engine = Engine::open(directory.path().join("database"), EngineConfig::default()).unwrap();
    let transaction = engine
        .session()
        .begin_transaction(TransactionOptions::read_write())
        .unwrap();
    let prepared = transaction.prepare("SELECT 1").unwrap();
    drop(transaction);

    let error = match prepared.execute(&[]).await {
        Ok(_) => panic!("prepared statement unexpectedly survived transaction rollback"),
        Err(error) => error,
    };
    assert!(matches!(error, Error::TransactionClosed { .. }));
}

#[tokio::test]
async fn transaction_drop_defers_staged_cleanup_until_result_quiescence() {
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("database");
    let engine = Engine::open(
        &database,
        EngineConfig::builder()
            .compute_threads(1)
            .spill_directory(directory.path().join("spill"))
            .build(),
    )
    .unwrap();
    let session = engine.session();
    consume(
        session
            .execute("CREATE TABLE events AS SELECT 1 AS id")
            .await
            .unwrap(),
    )
    .await;
    let baseline_snapshots = snapshot_directory_count(&database);

    let transaction = session
        .begin_transaction(TransactionOptions::read_write())
        .unwrap();
    let mut result = transaction
        .execute("INSERT INTO events VALUES (2)")
        .await
        .unwrap();
    result.stream().next().await.unwrap().unwrap();
    assert!(snapshot_directory_count(&database) > baseline_snapshots);
    drop(transaction);
    drop(result);

    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while snapshot_directory_count(&database) != baseline_snapshots {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("deferred rollback should clean staged snapshots");
    assert_eq!(
        query_session_scalar(&session, "SELECT count(*) FROM events").await,
        1
    );
}

#[tokio::test]
async fn sql_transaction_pins_catalog_until_commit() {
    let directory = tempfile::tempdir().unwrap();
    let engine = Engine::open(directory.path().join("database"), EngineConfig::default()).unwrap();
    let reader = engine.session();
    let writer = engine.session();

    consume(reader.execute("BEGIN READ ONLY").await.unwrap()).await;
    consume(
        writer
            .execute("CREATE TABLE visible_later AS SELECT 1 AS id")
            .await
            .unwrap(),
    )
    .await;

    let inside = consume(reader.execute("SHOW TABLES").await.unwrap()).await;
    assert!(
        !table_names(&inside)
            .iter()
            .any(|name| name == "visible_later")
    );
    consume(reader.execute("COMMIT").await.unwrap()).await;

    let outside = consume(reader.execute("SHOW TABLES").await.unwrap()).await;
    assert!(
        table_names(&outside)
            .iter()
            .any(|name| name == "visible_later")
    );
}

#[tokio::test]
async fn sql_commit_rejects_an_unconsumed_result_without_closing_the_transaction() {
    let directory = tempfile::tempdir().unwrap();
    let engine = Engine::open(directory.path().join("database"), EngineConfig::default()).unwrap();
    let session = engine.session();

    consume(session.execute("BEGIN").await.unwrap()).await;
    let result = session.execute("SELECT 1").await.unwrap();
    let error = match session.execute("COMMIT").await {
        Ok(_) => panic!("commit unexpectedly accepted an active result"),
        Err(error) => error,
    };
    assert!(error.to_string().contains("active result stream"));
    let error = match session.execute("ROLLBACK").await {
        Ok(_) => panic!("rollback unexpectedly accepted an active result"),
        Err(error) => error,
    };
    assert!(error.to_string().contains("active result stream"));
    let shared = Arc::clone(
        &session
            .sql_transaction
            .lock()
            .await
            .as_ref()
            .expect("SQL transaction should remain active")
            .shared,
    );
    drop(result);
    wait_for_transaction_results(&shared).await;
    consume(session.execute("ROLLBACK").await.unwrap()).await;
}

#[tokio::test]
async fn read_write_transaction_has_read_your_writes_and_atomic_visibility() {
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("database");
    let config = EngineConfig::builder()
        .compute_threads(1)
        .spill_directory(directory.path().join("spill"))
        .build();
    let engine = Engine::open(&database, config.clone()).unwrap();
    let outside = engine.session();
    consume(
        outside
            .execute("CREATE TABLE events AS SELECT 1 AS id")
            .await
            .unwrap(),
    )
    .await;

    let mut transaction = outside
        .begin_transaction(TransactionOptions::read_write())
        .unwrap();
    consume(
        transaction
            .execute("INSERT INTO events VALUES (2)")
            .await
            .unwrap(),
    )
    .await;
    consume(
        transaction
            .execute("UPDATE events SET id = id + 10 WHERE id = 2")
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(
        query_scalar(&transaction, "SELECT count(*) FROM events").await,
        2
    );
    assert_eq!(
        query_scalar(&transaction, "SELECT count(*) FROM events WHERE id = 12").await,
        1
    );
    assert_eq!(
        query_session_scalar(&outside, "SELECT count(*) FROM events").await,
        1
    );

    let info = transaction.commit().unwrap();
    assert_eq!(info.committed_generation(), Some(2));
    assert_eq!(
        query_session_scalar(&outside, "SELECT count(*) FROM events").await,
        2
    );
    assert_eq!(
        query_session_scalar(&outside, "SELECT count(*) FROM events WHERE id = 12").await,
        1
    );
    drop(transaction);
    drop(outside);
    drop(engine);

    let reopened = Engine::open(&database, config).unwrap();
    assert_eq!(
        query_session_scalar(&reopened.session(), "SELECT count(*) FROM events").await,
        2
    );
}

#[tokio::test]
async fn system_tables_use_the_transaction_snapshot_and_include_own_writes() {
    let directory = tempfile::tempdir().unwrap();
    let engine = Engine::open(
        directory.path().join("database"),
        EngineConfig::builder()
            .compute_threads(1)
            .spill_directory(directory.path().join("spill"))
            .build(),
    )
    .unwrap();
    let session = engine.session();
    consume(
        session
            .execute("CREATE TABLE events AS SELECT 1 AS id")
            .await
            .unwrap(),
    )
    .await;

    let mut transaction = session
        .begin_transaction(TransactionOptions::read_write())
        .unwrap();
    consume(
        session
            .execute("CREATE TABLE visible_later AS SELECT 9 AS id")
            .await
            .unwrap(),
    )
    .await;
    consume(
        session
            .execute("INSERT INTO events VALUES (2)")
            .await
            .unwrap(),
    )
    .await;

    assert!(
        transaction
            .execute("ALTER TABLE visible_later RENAME TO leaked")
            .await
            .is_err()
    );

    assert_eq!(
        query_scalar(
            &transaction,
            "SELECT count(*) FROM information_schema.tables WHERE table_name = 'visible_later'",
        )
        .await,
        0
    );
    assert_eq!(
        query_scalar(
            &transaction,
            "SELECT CAST(rows AS BIGINT) FROM rustdb_system.tables WHERE table_name = 'events'",
        )
        .await,
        1
    );

    consume(
        transaction
            .execute("CREATE TABLE staged AS SELECT 10 AS id")
            .await
            .unwrap(),
    )
    .await;
    consume(
        transaction
            .execute("INSERT INTO staged VALUES (11)")
            .await
            .unwrap(),
    )
    .await;
    consume(
        transaction
            .execute("CREATE VIEW staged_view AS SELECT id FROM staged")
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(
        query_scalar(
            &transaction,
            "SELECT count(*) FROM information_schema.tables WHERE table_name IN ('staged', 'staged_view')",
        )
        .await,
        2
    );
    assert_eq!(
        query_scalar(
            &transaction,
            "SELECT CAST(rows AS BIGINT) FROM rustdb_system.tables WHERE table_name = 'staged'",
        )
        .await,
        2
    );

    consume(
        transaction
            .execute("ALTER TABLE staged RENAME TO staged_renamed")
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(
        query_scalar(
            &transaction,
            "SELECT count(*) FROM information_schema.tables WHERE table_name = 'staged'",
        )
        .await,
        0
    );
    assert_eq!(
        query_scalar(
            &transaction,
            "SELECT count(*) FROM information_schema.tables WHERE table_name = 'staged_renamed'",
        )
        .await,
        1
    );

    transaction.rollback().unwrap();
    assert_eq!(
        query_session_scalar(&session, "SELECT count(*) FROM events").await,
        2
    );
    assert!(
        session
            .execute("SELECT * FROM staged_renamed")
            .await
            .is_err()
    );
}

#[tokio::test]
async fn table_and_view_replacement_is_transactional_and_conflict_checked() {
    let directory = tempfile::tempdir().unwrap();
    let engine = Engine::open(
        directory.path().join("database"),
        EngineConfig::builder()
            .compute_threads(1)
            .spill_directory(directory.path().join("spill"))
            .build(),
    )
    .unwrap();
    let session = engine.session();
    consume(
        session
            .execute("CREATE VIEW object_name AS SELECT 1 AS id")
            .await
            .unwrap(),
    )
    .await;

    let mut replacement = session
        .begin_transaction(TransactionOptions::read_write())
        .unwrap();
    consume(replacement.execute("DROP VIEW object_name").await.unwrap()).await;
    consume(
        replacement
            .execute("CREATE TABLE object_name AS SELECT 2 AS id")
            .await
            .unwrap(),
    )
    .await;
    consume(
        replacement
            .execute("INSERT INTO object_name VALUES (3)")
            .await
            .unwrap(),
    )
    .await;
    consume(
        replacement
            .execute("CREATE TABLE fresh_name AS SELECT 6 AS id")
            .await
            .unwrap(),
    )
    .await;
    consume(
        replacement
            .execute("ALTER TABLE fresh_name RENAME TO renamed_fresh")
            .await
            .unwrap(),
    )
    .await;
    replacement.commit().unwrap();
    assert_eq!(
        query_session_scalar(&session, "SELECT count(*) FROM object_name").await,
        2
    );
    assert_eq!(
        query_session_scalar(&session, "SELECT id FROM renamed_fresh").await,
        6
    );

    let mut conflict = session
        .begin_transaction(TransactionOptions::read_write())
        .unwrap();
    consume(
        session
            .execute("CREATE TABLE concurrent_name AS SELECT 4 AS id")
            .await
            .unwrap(),
    )
    .await;
    consume(
        conflict
            .execute("CREATE VIEW concurrent_name AS SELECT 5 AS id")
            .await
            .unwrap(),
    )
    .await;
    assert!(matches!(
        conflict.commit().unwrap_err(),
        Error::TransactionConflict { .. }
    ));
}

#[tokio::test]
async fn failed_commit_preflight_aborts_staged_snapshots_and_wal_entries() {
    let directory = tempfile::tempdir().unwrap();
    let database_path = directory.path().join("database");
    let engine = Engine::open(
        &database_path,
        EngineConfig::builder()
            .compute_threads(1)
            .spill_directory(directory.path().join("spill"))
            .build(),
    )
    .unwrap();
    let session = engine.session();
    consume(
        session
            .execute("CREATE TABLE events AS SELECT 1 AS id")
            .await
            .unwrap(),
    )
    .await;
    let baseline_snapshots = snapshot_directory_count(&database_path);

    let mut transaction = session
        .begin_transaction(TransactionOptions::read_write())
        .unwrap();
    consume(
        transaction
            .execute("INSERT INTO events VALUES (2)")
            .await
            .unwrap(),
    )
    .await;
    assert!(snapshot_directory_count(&database_path) > baseline_snapshots);
    engine
        .inner
        .native_poisoned
        .store(true, std::sync::atomic::Ordering::Release);

    assert!(transaction.commit().is_err());
    assert_eq!(snapshot_directory_count(&database_path), baseline_snapshots);
    let wal = engine.inner.database.as_ref().unwrap().wal_info().unwrap();
    assert_eq!(wal.active_writes, 0);

    engine
        .inner
        .native_poisoned
        .store(false, std::sync::atomic::Ordering::Release);
    assert_eq!(
        query_session_scalar(&session, "SELECT count(*) FROM events").await,
        1
    );
}

#[tokio::test]
async fn concurrent_appends_rebase_without_losing_rows() {
    let directory = tempfile::tempdir().unwrap();
    let engine = Engine::open(
        directory.path().join("database"),
        EngineConfig::builder()
            .compute_threads(1)
            .spill_directory(directory.path().join("spill"))
            .build(),
    )
    .unwrap();
    let session = engine.session();
    consume(
        session
            .execute("CREATE TABLE events AS SELECT 1 AS id")
            .await
            .unwrap(),
    )
    .await;
    let mut first = session
        .begin_transaction(TransactionOptions::read_write())
        .unwrap();
    let mut second = session
        .begin_transaction(TransactionOptions::read_write())
        .unwrap();
    consume(
        first
            .execute("INSERT INTO events VALUES (2)")
            .await
            .unwrap(),
    )
    .await;
    consume(
        second
            .execute("INSERT INTO events VALUES (3)")
            .await
            .unwrap(),
    )
    .await;

    assert_eq!(first.commit().unwrap().committed_generation(), Some(2));
    assert_eq!(second.commit().unwrap().committed_generation(), Some(3));
    assert_eq!(
        query_session_scalar(&session, "SELECT count(*) FROM events").await,
        3
    );
}

#[tokio::test]
async fn disjoint_row_updates_rebase_and_same_row_updates_conflict() {
    let directory = tempfile::tempdir().unwrap();
    let engine = Engine::open(
        directory.path().join("database"),
        EngineConfig::builder()
            .compute_threads(1)
            .spill_directory(directory.path().join("spill"))
            .build(),
    )
    .unwrap();
    let session = engine.session();
    consume(
        session
            .execute(
                "CREATE TABLE events AS SELECT * FROM (VALUES (1, 'a'), (2, 'b')) AS v(id, label)",
            )
            .await
            .unwrap(),
    )
    .await;
    let mut first = session
        .begin_transaction(TransactionOptions::read_write())
        .unwrap();
    let mut second = session
        .begin_transaction(TransactionOptions::read_write())
        .unwrap();
    consume(
        first
            .execute("UPDATE events SET label = 'x' WHERE id = 1")
            .await
            .unwrap(),
    )
    .await;
    consume(
        second
            .execute("UPDATE events SET label = 'y' WHERE id = 2")
            .await
            .unwrap(),
    )
    .await;
    first.commit().unwrap();
    second.commit().unwrap();
    assert_eq!(
        query_session_scalar(
            &session,
            "SELECT count(*) FROM events WHERE (id = 1 AND label = 'x') OR (id = 2 AND label = 'y')",
        )
        .await,
        2
    );
    drop(first);
    drop(second);

    let mut winner = session
        .begin_transaction(TransactionOptions::read_write())
        .unwrap();
    let mut loser = session
        .begin_transaction(TransactionOptions::read_write())
        .unwrap();
    consume(
        winner
            .execute("UPDATE events SET label = 'winner' WHERE id = 1")
            .await
            .unwrap(),
    )
    .await;
    consume(
        loser
            .execute("DELETE FROM events WHERE id = 1")
            .await
            .unwrap(),
    )
    .await;
    winner.commit().unwrap();
    let loser_id = loser.id().to_string();
    assert!(matches!(
        loser.commit().unwrap_err(),
        Error::TransactionConflict { transaction_id, .. } if transaction_id == loser_id
    ));
}

#[tokio::test]
async fn rollback_discards_private_snapshots_and_wal_state() {
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("database");
    let config = EngineConfig::builder()
        .compute_threads(1)
        .spill_directory(directory.path().join("spill"))
        .build();
    let engine = Engine::open(&database, config.clone()).unwrap();
    let session = engine.session();
    consume(
        session
            .execute("CREATE TABLE events AS SELECT 1 AS id")
            .await
            .unwrap(),
    )
    .await;
    let baseline_snapshots = snapshot_directory_count(&database);

    let mut transaction = session
        .begin_transaction(TransactionOptions::read_write())
        .unwrap();
    consume(
        transaction
            .execute("INSERT INTO events VALUES (2), (3)")
            .await
            .unwrap(),
    )
    .await;
    consume(
        transaction
            .execute("DELETE FROM events WHERE id = 1")
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(
        query_scalar(&transaction, "SELECT count(*) FROM events").await,
        2
    );
    assert!(snapshot_directory_count(&database) > baseline_snapshots);
    assert_eq!(
        query_session_scalar(&session, "SELECT count(*) FROM events").await,
        1
    );

    transaction.rollback().unwrap();
    assert_eq!(snapshot_directory_count(&database), baseline_snapshots);
    assert_eq!(
        query_session_scalar(&session, "SELECT count(*) FROM events").await,
        1
    );
    drop(transaction);
    drop(session);
    drop(engine);

    let reopened = Engine::open(&database, config).unwrap();
    assert_eq!(
        query_session_scalar(&reopened.session(), "SELECT count(*) FROM events").await,
        1
    );
}

#[tokio::test]
async fn drop_table_is_transactional_and_durable() {
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("database");
    let config = EngineConfig::builder()
        .compute_threads(1)
        .spill_directory(directory.path().join("spill"))
        .build();
    let engine = Engine::open(&database, config.clone()).unwrap();
    let session = engine.session();
    consume(
        session
            .execute("CREATE TABLE events AS SELECT 1 AS id")
            .await
            .unwrap(),
    )
    .await;

    let mut rollback = session
        .begin_transaction(TransactionOptions::read_write())
        .unwrap();
    consume(rollback.execute("DROP TABLE events").await.unwrap()).await;
    assert!(rollback.execute("SELECT * FROM events").await.is_err());
    assert_eq!(
        query_session_scalar(&session, "SELECT count(*) FROM events").await,
        1
    );
    rollback.rollback().unwrap();
    assert_eq!(
        query_session_scalar(&session, "SELECT count(*) FROM events").await,
        1
    );
    drop(rollback);

    let mut commit = session
        .begin_transaction(TransactionOptions::read_write())
        .unwrap();
    consume(commit.execute("DROP TABLE events").await.unwrap()).await;
    assert_eq!(commit.commit().unwrap().committed_generation(), Some(2));
    assert!(session.execute("SELECT * FROM events").await.is_err());
    consume(
        session
            .execute("DROP TABLE IF EXISTS events")
            .await
            .unwrap(),
    )
    .await;
    drop(commit);
    drop(session);
    drop(engine);

    let reopened = Engine::open(&database, config).unwrap();
    assert!(
        reopened
            .session()
            .execute("SELECT * FROM events")
            .await
            .is_err()
    );
}

#[tokio::test]
async fn cyclic_table_renames_commit_without_losing_snapshots() {
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("database");
    let config = EngineConfig::builder()
        .compute_threads(1)
        .spill_directory(directory.path().join("spill"))
        .build();
    let engine = Engine::open(&database, config.clone()).unwrap();
    let session = engine.session();
    consume(
        session
            .execute("CREATE TABLE a AS SELECT 1 AS id")
            .await
            .unwrap(),
    )
    .await;
    consume(
        session
            .execute("CREATE TABLE b AS SELECT 2 AS id")
            .await
            .unwrap(),
    )
    .await;

    let mut transaction = session
        .begin_transaction(TransactionOptions::read_write())
        .unwrap();
    consume(
        transaction
            .execute("ALTER TABLE a RENAME TO tmp")
            .await
            .unwrap(),
    )
    .await;
    consume(
        transaction
            .execute("ALTER TABLE b RENAME TO a")
            .await
            .unwrap(),
    )
    .await;
    consume(
        transaction
            .execute("ALTER TABLE tmp RENAME TO b")
            .await
            .unwrap(),
    )
    .await;
    transaction.commit().unwrap();

    assert_eq!(query_session_scalar(&session, "SELECT id FROM a").await, 2);
    assert_eq!(query_session_scalar(&session, "SELECT id FROM b").await, 1);
    drop(transaction);
    drop(session);
    drop(engine);

    let reopened = Engine::open(&database, config).unwrap();
    let session = reopened.session();
    assert_eq!(query_session_scalar(&session, "SELECT id FROM a").await, 2);
    assert_eq!(query_session_scalar(&session, "SELECT id FROM b").await, 1);
}

#[tokio::test]
async fn persistent_views_are_snapshot_isolated_and_survive_reopen() {
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("database");
    let config = EngineConfig::builder()
        .compute_threads(1)
        .spill_directory(directory.path().join("spill"))
        .build();
    let engine = Engine::open(&database, config.clone()).unwrap();
    let session = engine.session();
    consume(
        session
            .execute("CREATE TABLE events AS SELECT * FROM (VALUES (1), (2)) AS v(id)")
            .await
            .unwrap(),
    )
    .await;
    consume(
        session
            .execute("CREATE VIEW visible_events AS SELECT id FROM events WHERE id > 1")
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(
        query_session_scalar(&session, "SELECT count(*) FROM visible_events").await,
        1
    );

    let mut rollback = session
        .begin_transaction(TransactionOptions::read_write())
        .unwrap();
    consume(
        rollback
            .execute("CREATE OR REPLACE VIEW visible_events AS SELECT id FROM events WHERE id > 0")
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(
        query_scalar(&rollback, "SELECT count(*) FROM visible_events").await,
        2
    );
    assert_eq!(
        query_session_scalar(&session, "SELECT count(*) FROM visible_events").await,
        1
    );
    rollback.rollback().unwrap();
    drop(rollback);

    let mut commit = session
        .begin_transaction(TransactionOptions::read_write())
        .unwrap();
    consume(
        commit
            .execute("CREATE OR REPLACE VIEW visible_events AS SELECT id FROM events WHERE id > 0")
            .await
            .unwrap(),
    )
    .await;
    commit.commit().unwrap();
    assert_eq!(
        query_session_scalar(&session, "SELECT count(*) FROM visible_events").await,
        2
    );
    drop(commit);
    drop(session);
    drop(engine);

    let reopened = Engine::open(&database, config.clone()).unwrap();
    let session = reopened.session();
    assert_eq!(
        query_session_scalar(&session, "SELECT count(*) FROM visible_events").await,
        2
    );
    consume(session.execute("DROP VIEW visible_events").await.unwrap()).await;
    assert!(
        session
            .execute("SELECT * FROM visible_events")
            .await
            .is_err()
    );
    drop(session);
    drop(reopened);

    let reopened = Engine::open(&database, config).unwrap();
    assert!(
        reopened
            .session()
            .execute("SELECT * FROM visible_events")
            .await
            .is_err()
    );
}

fn snapshot_directory_count(database: &std::path::Path) -> usize {
    let tables = database.join("tables");
    let Ok(table_entries) = std::fs::read_dir(tables) else {
        return 0;
    };
    table_entries
        .filter_map(std::result::Result::ok)
        .filter_map(|table| std::fs::read_dir(table.path().join("snapshots")).ok())
        .map(|snapshots| snapshots.filter_map(std::result::Result::ok).count())
        .sum()
}

fn table_names(batches: &[arrow::record_batch::RecordBatch]) -> Vec<String> {
    batches
        .iter()
        .flat_map(|batch| {
            let names = batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            (0..batch.num_rows())
                .map(|row| names.value(row).to_owned())
                .collect::<Vec<_>>()
        })
        .collect()
}

async fn query_scalar(transaction: &Transaction, sql: &str) -> i64 {
    let batches = consume(transaction.execute(sql).await.unwrap()).await;
    batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0)
}

async fn query_session_scalar(session: &Session, sql: &str) -> i64 {
    let batches = consume(session.execute(sql).await.unwrap()).await;
    batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0)
}

#[path = "sql_tests.rs"]
mod sql_tests;
