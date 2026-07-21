use std::{
    fs,
    path::Path,
    sync::{Arc, mpsc},
    thread,
    time::Duration,
};

use arrow::datatypes::Schema;

use super::{QueryManager, QueryManagerConfig, QueryRecord, cancel_and_persist, now_ms};
use crate::{
    Engine, EngineConfig, QueryMetrics, RetryClass, RssGuardian,
    http_shell::{
        QueryListRequest, QueryRequest, QueryState,
        result_store::ResultStore,
        result_store::ResultStoreConfig,
        security::{AuthenticatedActor, PrincipalId, QueryOwner, Role},
    },
    runtime::MemoryPool,
};

fn actor(id: &str, role: Role) -> AuthenticatedActor {
    AuthenticatedActor::Principal {
        id: PrincipalId::new(id).unwrap(),
        role,
    }
}

fn open_manager(root: &Path) -> QueryManager {
    QueryManager::new(
        Engine::new(EngineConfig::default()).unwrap(),
        QueryManagerConfig {
            max_running: 1,
            max_queued: 8,
            max_query_time: Duration::from_secs(10),
            ..QueryManagerConfig::default()
        },
        ResultStoreConfig::new(root),
    )
    .unwrap()
}

fn http_ok<T>(value: std::result::Result<T, super::HttpError>) -> T {
    match value {
        Ok(value) => value,
        Err(error) => panic!("HTTP error {}: {}", error.body.error, error.body.message),
    }
}

async fn wait_for_terminal(
    manager: &QueryManager,
    actor: &AuthenticatedActor,
    query_id: &str,
) -> QueryState {
    for _ in 0..200 {
        let state = http_ok(manager.status(actor, query_id, "wait")).state;
        if matches!(
            state,
            QueryState::Succeeded | QueryState::Failed | QueryState::Cancelled
        ) {
            return state;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("query {query_id} did not finish");
}

#[tokio::test]
async fn idempotency_and_query_access_are_principal_scoped() {
    let temporary = tempfile::tempdir().unwrap();
    let manager = open_manager(&temporary.path().join("results"));
    let alice = actor("alice", Role::Query);
    let bob = actor("bob", Role::Query);
    let admin = actor("admin", Role::Admin);
    let request = QueryRequest {
        sql: "SELECT 1".into(),
        parameters: Vec::new(),
        timeout_ms: None,
    };

    let alice_query =
        http_ok(manager.submit(&alice, "same-idempotency-key", request.clone(), "request-a"));
    let alice_replay =
        http_ok(manager.submit(&alice, "same-idempotency-key", request.clone(), "request-b"));
    let bob_query = http_ok(manager.submit(&bob, "same-idempotency-key", request, "request-c"));

    assert_eq!(alice_query.query_id, alice_replay.query_id);
    assert_ne!(alice_query.query_id, bob_query.query_id);
    assert!(
        manager
            .status(&bob, &alice_query.query_id, "request-d")
            .is_err()
    );
    assert!(
        manager
            .status(&admin, &alice_query.query_id, "request-e")
            .is_ok()
    );

    manager.shutdown().await.unwrap();
}

#[tokio::test]
async fn query_listing_is_owner_scoped_filterable_and_cursor_paginated() {
    let temporary = tempfile::tempdir().unwrap();
    let manager = open_manager(&temporary.path().join("results"));
    let alice = actor("alice", Role::Query);
    let bob = actor("bob", Role::Query);
    let admin = actor("admin", Role::Admin);

    let first = http_ok(manager.submit(
        &alice,
        "list-alice-first",
        QueryRequest {
            sql: "SELECT 1".into(),
            parameters: Vec::new(),
            timeout_ms: None,
        },
        "submit-a1",
    ));
    let second = http_ok(manager.submit(
        &alice,
        "list-alice-second",
        QueryRequest {
            sql: "SELECT 2".into(),
            parameters: Vec::new(),
            timeout_ms: None,
        },
        "submit-a2",
    ));
    let bob_query = http_ok(manager.submit(
        &bob,
        "list-bob-only-query",
        QueryRequest {
            sql: "SELECT 3".into(),
            parameters: Vec::new(),
            timeout_ms: None,
        },
        "submit-b",
    ));
    for (actor, id) in [
        (&alice, first.query_id.as_str()),
        (&alice, second.query_id.as_str()),
        (&bob, bob_query.query_id.as_str()),
    ] {
        assert_eq!(
            wait_for_terminal(&manager, actor, id).await,
            QueryState::Succeeded
        );
    }

    let alice_page = http_ok(manager.list(
        &alice,
        QueryListRequest {
            state: Some(QueryState::Succeeded),
            created_after_ms: None,
            limit: Some(1),
            cursor: None,
        },
        "list-a1",
    ));
    assert_eq!(alice_page.queries.len(), 1);
    assert!(alice_page.next_cursor.is_some());
    assert_ne!(alice_page.queries[0].query_id, bob_query.query_id);

    let alice_next = http_ok(manager.list(
        &alice,
        QueryListRequest {
            state: Some(QueryState::Succeeded),
            created_after_ms: None,
            limit: Some(1),
            cursor: alice_page.next_cursor,
        },
        "list-a2",
    ));
    assert_eq!(alice_next.queries.len(), 1);
    assert_ne!(
        alice_page.queries[0].query_id,
        alice_next.queries[0].query_id
    );
    assert!(alice_next.next_cursor.is_none());

    let all = http_ok(manager.list(
        &admin,
        QueryListRequest {
            state: Some(QueryState::Succeeded),
            created_after_ms: None,
            limit: Some(10),
            cursor: None,
        },
        "list-admin",
    ));
    assert_eq!(all.queries.len(), 3);
    assert!(
        all.queries
            .iter()
            .any(|query| query.query_id == bob_query.query_id)
    );

    let invalid = manager.list(
        &alice,
        QueryListRequest {
            state: None,
            created_after_ms: None,
            limit: Some(1),
            cursor: Some("not-base64!".into()),
        },
        "list-invalid",
    );
    assert_eq!(invalid.unwrap_err().body.error, "request.invalid");
    manager.shutdown().await.unwrap();
}

#[tokio::test]
async fn delete_does_not_reveal_foreign_query_ownership() {
    let temporary = tempfile::tempdir().unwrap();
    let manager = open_manager(&temporary.path().join("results"));
    let alice = actor("alice", Role::Query);
    let bob = actor("bob", Role::Query);
    let submitted = http_ok(manager.submit(
        &alice,
        "alice-delete-side-channel",
        QueryRequest {
            sql: "SELECT 1".into(),
            parameters: Vec::new(),
            timeout_ms: None,
        },
        "submit",
    ));
    wait_for_terminal(&manager, &alice, &submitted.query_id).await;

    http_ok(manager.delete(&bob, &submitted.query_id, "foreign-delete"));
    http_ok(manager.delete(&bob, "01J00000000000000000000000", "missing-delete"));
    assert!(
        manager
            .status(&alice, &submitted.query_id, "owner-status")
            .is_ok()
    );

    http_ok(manager.delete(&alice, &submitted.query_id, "owner-delete"));
    manager.shutdown().await.unwrap();
}

#[tokio::test]
async fn terminal_result_and_idempotency_survive_restart() {
    let temporary = tempfile::tempdir().unwrap();
    let root = temporary.path().join("results");
    let alice = actor("alice", Role::Query);
    let bob = actor("bob", Role::Query);
    let key = "restart-secret-idempotency-key";
    let request = QueryRequest {
        sql: "SELECT 42 AS answer".into(),
        parameters: Vec::new(),
        timeout_ms: None,
    };

    let manager = open_manager(&root);
    let submitted = http_ok(manager.submit(&alice, key, request.clone(), "before-restart"));
    assert_eq!(
        wait_for_terminal(&manager, &alice, &submitted.query_id).await,
        QueryState::Succeeded
    );
    assert_eq!(
        http_ok(manager.completed_result(&alice, &submitted.query_id, "before-read")).rows(),
        1
    );
    manager.shutdown().await.unwrap();
    drop(manager);

    let journal = root.join("query-journal");
    let persisted = ["snapshot.json", "journal.jsonl"]
        .into_iter()
        .filter_map(|name| fs::read(journal.join(name)).ok())
        .flatten()
        .collect::<Vec<_>>();
    let persisted = String::from_utf8_lossy(&persisted);
    assert!(!persisted.contains(key));
    assert!(!persisted.contains("SELECT 42"));

    let manager = open_manager(&root);
    assert_eq!(
        http_ok(manager.status(&alice, &submitted.query_id, "after-restart")).state,
        QueryState::Succeeded
    );
    assert!(
        manager
            .completed_result(&bob, &submitted.query_id, "denied")
            .is_err()
    );
    assert_eq!(
        http_ok(manager.completed_result(&alice, &submitted.query_id, "after-read")).rows(),
        1
    );
    let replay = http_ok(manager.submit(&alice, key, request, "after-replay"));
    assert!(replay.replayed);
    assert_eq!(replay.query_id, submitted.query_id);

    http_ok(manager.delete(&alice, &submitted.query_id, "delete"));
    assert!(!root.join(format!("q-{}", submitted.query_id)).exists());
    manager.shutdown().await.unwrap();
}

#[tokio::test]
async fn successful_result_is_not_published_before_its_journal_record() {
    let temporary = tempfile::tempdir().unwrap();
    let root = temporary.path().join("results");
    let manager = open_manager(&root);
    manager.inner.journal.fail_next_success();
    let alice = actor("alice", Role::Query);
    let submitted = http_ok(manager.submit(
        &alice,
        "durable-success-boundary",
        QueryRequest {
            sql: "SELECT 1 AS value".into(),
            parameters: Vec::new(),
            timeout_ms: None,
        },
        "submit",
    ));
    assert_eq!(
        wait_for_terminal(&manager, &alice, &submitted.query_id).await,
        QueryState::Failed
    );
    let status = http_ok(manager.status(&alice, &submitted.query_id, "status"));
    assert_eq!(
        status.error.as_ref().map(|error| error.error.as_str()),
        Some("query.journal_failed")
    );
    assert!(
        manager
            .completed_result(&alice, &submitted.query_id, "result")
            .is_err()
    );
    assert!(!root.join(format!("q-{}", submitted.query_id)).exists());
    manager.shutdown().await.unwrap();
}

#[tokio::test]
async fn shutdown_forced_abort_seals_and_quiesces_before_deadline() {
    let temporary = tempfile::tempdir().unwrap();
    let manager = open_manager(&temporary.path().join("results"));
    let record = Arc::new(QueryRecord::new(
        QueryOwner::AuthenticationDisabled,
        "e".repeat(64),
        "f".repeat(64),
    ));
    {
        let mut state = record.state.write();
        state.phase = QueryState::Running;
        state.started_at_ms = Some(now_ms());
    }
    manager.inner.journal.upsert(record.persisted()).unwrap();
    manager
        .inner
        .records
        .write()
        .insert(record.id.clone(), Arc::clone(&record));
    manager.inner.tasks.spawn(std::future::pending());

    manager
        .shutdown_until(tokio::time::Instant::now() + Duration::from_secs(1))
        .await
        .unwrap();
    assert_eq!(record.state.read().phase, QueryState::Interrupted);
    let persisted = manager
        .inner
        .journal
        .load()
        .into_iter()
        .find(|query| query.query_id == record.id)
        .unwrap();
    assert_eq!(persisted.state, QueryState::Interrupted);
    assert_eq!(persisted.error.unwrap().error, "query.interrupted");
    assert_eq!(manager.inner.tasks.active(), 0);

    manager.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancel_cannot_regress_a_durable_success_to_running() {
    let temporary = tempfile::tempdir().unwrap();
    let journal = super::QueryJournal::open(super::QueryJournalConfig::new(
        temporary.path().join("journal"),
    ))
    .unwrap();
    let store =
        ResultStore::open(ResultStoreConfig::new(temporary.path().join("results"))).unwrap();
    let record = Arc::new(QueryRecord::new(
        QueryOwner::AuthenticationDisabled,
        "a".repeat(64),
        "b".repeat(64),
    ));
    {
        let mut state = record.state.write();
        state.phase = QueryState::Running;
        state.started_at_ms = Some(record.created_at_ms);
    }
    journal.upsert(record.persisted()).unwrap();
    let result = store
        .writer(&record.id, Arc::new(Schema::empty()))
        .unwrap()
        .finish()
        .await
        .unwrap();

    let transition = record.lock_transition().await;
    let finished_at_ms = now_ms();
    journal
        .upsert(record.pending_success(
            finished_at_ms,
            crate::http_shell::HttpQueryMetrics::default(),
        ))
        .unwrap();

    let (started_tx, started_rx) = mpsc::channel();
    let (done_tx, done_rx) = mpsc::channel();
    let cancel_record = Arc::clone(&record);
    let cancel_journal = Arc::clone(&journal);
    let cancel_thread = thread::spawn(move || {
        started_tx.send(()).unwrap();
        done_tx
            .send(cancel_and_persist(&cancel_record, &cancel_journal))
            .unwrap();
    });
    started_rx.recv().unwrap();
    assert!(done_rx.recv_timeout(Duration::from_millis(20)).is_err());

    record.publish_success(
        finished_at_ms,
        crate::http_shell::HttpQueryMetrics::default(),
        result,
    );
    drop(transition);
    done_rx
        .recv_timeout(Duration::from_secs(1))
        .unwrap()
        .unwrap();
    cancel_thread.join().unwrap();

    let durable = journal.load().pop().unwrap();
    assert_eq!(durable.state, QueryState::Succeeded);
    assert!(durable.result_available);
}

#[test]
fn queued_shutdown_cancellation_is_durable_after_reopen() {
    let temporary = tempfile::tempdir().unwrap();
    let journal_directory = temporary.path().join("journal");
    let journal =
        super::QueryJournal::open(super::QueryJournalConfig::new(&journal_directory)).unwrap();
    let record = Arc::new(QueryRecord::new(
        QueryOwner::AuthenticationDisabled,
        "c".repeat(64),
        "d".repeat(64),
    ));
    journal.upsert(record.persisted()).unwrap();

    let transition = record.blocking_lock_transition();
    let (started_tx, started_rx) = mpsc::channel();
    let (done_tx, done_rx) = mpsc::channel();
    let cancel_record = Arc::clone(&record);
    let cancel_journal = Arc::clone(&journal);
    let cancel_thread = thread::spawn(move || {
        started_tx.send(()).unwrap();
        done_tx
            .send(cancel_and_persist(&cancel_record, &cancel_journal))
            .unwrap();
    });
    started_rx.recv().unwrap();
    assert!(done_rx.recv_timeout(Duration::from_millis(20)).is_err());
    drop(transition);
    done_rx
        .recv_timeout(Duration::from_secs(1))
        .unwrap()
        .unwrap();
    cancel_thread.join().unwrap();
    drop(journal);

    let reopened =
        super::QueryJournal::open(super::QueryJournalConfig::new(journal_directory)).unwrap();
    let durable = reopened.load().pop().unwrap();
    assert_eq!(durable.state, QueryState::Cancelled);
    assert!(!durable.result_available);
}

#[tokio::test]
async fn rss_pressure_throttles_or_rejects_only_new_submissions() {
    let temporary = tempfile::tempdir().unwrap();
    let manager = open_manager(&temporary.path().join("results"));
    let alice = actor("alice", Role::Query);
    let request = QueryRequest {
        sql: "SELECT 1".into(),
        parameters: Vec::new(),
        timeout_ms: None,
    };
    let accepted = http_ok(manager.submit(&alice, "rss-existing-key", request.clone(), "normal"));
    assert_eq!(
        wait_for_terminal(&manager, &alice, &accepted.query_id).await,
        QueryState::Succeeded
    );

    manager
        .inner
        .rss_pressure
        .observe(RssGuardian::default().assess(750, 1_000, None));
    let replay = http_ok(manager.submit(&alice, "rss-existing-key", request.clone(), "replay"));
    assert!(replay.replayed);
    assert_eq!(replay.query_id, accepted.query_id);
    let throttled = manager
        .submit(&alice, "rss-throttle-key", request.clone(), "throttle")
        .unwrap_err();
    assert_eq!(throttled.status, axum::http::StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(throttled.body.error, "admission.rss_throttled");
    assert_eq!(throttled.body.retry, RetryClass::Safe);
    assert_eq!(throttled.retry_after, Some(1));

    manager
        .inner
        .rss_pressure
        .observe(RssGuardian::default().assess(850, 1_000, None));
    let rejected = manager
        .submit(&alice, "rss-rejected-key", request, "reject")
        .unwrap_err();
    assert_eq!(rejected.status, axum::http::StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(rejected.body.error, "admission.rss_rejected");
    assert_eq!(rejected.body.retry, RetryClass::Safe);
    assert_eq!(manager.inner.records.read().len(), 1);
    manager.shutdown().await.unwrap();
}

#[tokio::test]
async fn critical_pressure_prefers_largest_live_reservation_then_earliest_running() {
    let temporary = tempfile::tempdir().unwrap();
    let manager = open_manager(&temporary.path().join("results"));
    let running = |started_at_ms| {
        let record = Arc::new(QueryRecord::new(
            QueryOwner::AuthenticationDisabled,
            "a".repeat(64),
            "b".repeat(64),
        ));
        let mut state = record.state.write();
        state.phase = QueryState::Running;
        state.started_at_ms = Some(started_at_ms);
        drop(state);
        record
    };

    let small = running(10);
    let large = running(20);
    let small_pool = MemoryPool::new(1_024);
    let large_pool = MemoryPool::new(1_024);
    let _small_reservation = small_pool.try_reserve(100).unwrap();
    let _large_reservation = large_pool.try_reserve(700).unwrap();
    let small_live = small.register_live_metrics(QueryMetrics::with_memory_pool(small_pool));
    let large_live = large.register_live_metrics(QueryMetrics::with_memory_pool(large_pool));
    manager.inner.records.write().extend([
        (small.id.clone(), Arc::clone(&small)),
        (large.id.clone(), Arc::clone(&large)),
    ]);

    assert_eq!(
        manager.cancel_largest_for_rss_pressure().as_deref(),
        Some(large.id.as_str())
    );
    assert!(large.cancel.is_cancelled());
    assert!(!small.cancel.is_cancelled());
    drop(small_live);
    drop(large_live);
    assert_eq!(small.current_memory_bytes(), None);
    assert_eq!(large.current_memory_bytes(), None);

    let earliest = running(100);
    let later = running(200);
    manager.inner.records.write().clear();
    manager.inner.records.write().extend([
        (later.id.clone(), Arc::clone(&later)),
        (earliest.id.clone(), Arc::clone(&earliest)),
    ]);
    assert_eq!(
        manager.cancel_largest_for_rss_pressure().as_deref(),
        Some(earliest.id.as_str())
    );
    manager.inner.records.write().clear();
    manager.shutdown().await.unwrap();
}
