use super::*;

#[test]
fn copy_to_is_rejected_when_its_external_side_effect_cannot_be_rolled_back() {
    let result = classify_transaction_statement(
        "COPY (SELECT 1) TO '/tmp/rustdb-copy.parquet' (FORMAT PARQUET)",
        TransactionAccessMode::ReadWrite,
    );
    assert!(
        matches!(result, Err(Error::Unsupported(message)) if message.contains("cannot be rolled back"))
    );
}

#[tokio::test]
async fn abandoned_statement_keeps_sql_transaction_usable() {
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

    consume(
        session
            .execute("INSERT INTO events VALUES (3)")
            .await
            .unwrap(),
    )
    .await;
    consume(session.execute("COMMIT").await.unwrap()).await;
    assert_eq!(
        query_session_scalar(&session, "SELECT count(*) FROM events").await,
        2
    );
}

#[tokio::test]
async fn active_mutation_result_blocks_sibling_statement_until_rollback_finishes() {
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
    let error = match session.execute("SELECT count(*) FROM events").await {
        Ok(_) => panic!("transaction allowed a sibling read of staged mutation data"),
        Err(error) => error,
    };
    assert!(error.to_string().contains("active statement result"));
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

    wait_for_transaction_results(&shared).await;
    consume(
        session
            .execute("INSERT INTO events VALUES (3)")
            .await
            .unwrap(),
    )
    .await;
    consume(session.execute("COMMIT").await.unwrap()).await;
    assert_eq!(
        query_session_scalar(&session, "SELECT count(*) FROM events").await,
        2
    );
}
