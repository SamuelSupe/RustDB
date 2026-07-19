use super::*;

#[tokio::test]
async fn rollback_clears_an_automatically_rolled_back_transaction() {
    let directory = tempfile::tempdir().unwrap();
    let engine = Engine::open(directory.path().join("database"), EngineConfig::default()).unwrap();
    let session = engine.session();

    consume(
        session
            .execute("CREATE TABLE events AS SELECT 1 AS id")
            .await
            .unwrap(),
    )
    .await;
    consume(session.execute("BEGIN").await.unwrap()).await;
    let mut result = session
        .execute("INSERT INTO events VALUES (2)")
        .await
        .unwrap();
    result.stream().next().await.unwrap().unwrap();
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

    let error = match session.execute("ROLLBACK").await {
        Ok(_) => panic!("automatically rolled back transaction accepted ROLLBACK"),
        Err(error) => error,
    };
    assert!(matches!(error, Error::TransactionClosed { .. }));

    consume(session.execute("BEGIN").await.unwrap()).await;
    consume(session.execute("ROLLBACK").await.unwrap()).await;
    assert_eq!(
        query_session_scalar(&session, "SELECT count(*) FROM events").await,
        1
    );
}

#[tokio::test]
async fn terminal_transaction_retains_session_until_sibling_result_quiesces() {
    let directory = tempfile::tempdir().unwrap();
    let engine = Engine::open(
        directory.path().join("database"),
        EngineConfig::builder().max_concurrent_queries(2).build(),
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
    consume(session.execute("BEGIN").await.unwrap()).await;
    let mut mutation = session
        .execute("INSERT INTO events VALUES (2)")
        .await
        .unwrap();
    mutation.stream().next().await.unwrap().unwrap();
    let sibling = session.execute("SELECT 1").await.unwrap();
    let shared = Arc::clone(
        &session
            .sql_transaction
            .lock()
            .await
            .as_ref()
            .expect("SQL transaction should remain active")
            .shared,
    );
    drop(mutation);

    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        loop {
            let ready = {
                let state = shared.state.lock();
                state.lifecycle == Lifecycle::RolledBack
                    && state.active_results == 1
                    && state.rollback_pending
            };
            if ready {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("mutation abandonment did not enter deferred rollback");

    let error = match session.execute("ROLLBACK").await {
        Ok(_) => panic!("terminal transaction accepted ROLLBACK with a live sibling result"),
        Err(error) => error,
    };
    assert!(matches!(error, Error::TransactionClosed { .. }));
    assert!(session.sql_transaction.lock().await.is_some());
    let error = match session.execute("BEGIN").await {
        Ok(_) => panic!("terminal transaction released its live sibling result"),
        Err(error) => error,
    };
    assert!(error.to_string().contains("already active"));

    drop(sibling);
    wait_for_transaction_results(&shared).await;
    consume(session.execute("BEGIN").await.unwrap()).await;
    consume(session.execute("ROLLBACK").await.unwrap()).await;
    assert_eq!(
        query_session_scalar(&session, "SELECT count(*) FROM events").await,
        1
    );
}
