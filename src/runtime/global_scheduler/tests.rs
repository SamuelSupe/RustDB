use std::{collections::HashMap, sync::Arc, time::Duration};

use tokio::sync::{Barrier, mpsc, oneshot};
use uuid::Uuid;

use super::GlobalComputeScheduler;
use crate::{Engine, EngineConfig, Error, runtime::QueryControl};

#[test]
fn rejects_zero_slots() {
    assert!(matches!(
        GlobalComputeScheduler::new(0),
        Err(Error::InvalidArgument(_))
    ));
}

#[tokio::test]
async fn uncontended_acquire_does_not_enter_the_wait_queue() {
    let scheduler = GlobalComputeScheduler::new(2).unwrap();
    let query_id = Uuid::new_v4();

    let permit = scheduler
        .acquire(query_id, &QueryControl::new())
        .await
        .unwrap();

    assert_eq!(permit.query_id(), query_id);
    assert_eq!(permit.wait_time(), Duration::ZERO);
    assert_eq!(scheduler.snapshot().queued_waiters, 0);
    drop(permit);
    assert_eq!(scheduler.snapshot().active_slots, 0);
}

#[tokio::test]
async fn permit_drop_releases_slot_without_exceeding_limit() {
    let scheduler = GlobalComputeScheduler::new(4).unwrap();
    let control = QueryControl::new();
    let query_id = Uuid::new_v4();
    let mut permits = Vec::new();
    for _ in 0..4 {
        permits.push(scheduler.acquire(query_id, &control).await.unwrap());
    }
    assert_eq!(scheduler.snapshot().active_slots, 4);

    let waiting_scheduler = scheduler.clone();
    let waiting_control = control.clone();
    let waiter =
        tokio::spawn(async move { waiting_scheduler.acquire(query_id, &waiting_control).await });
    wait_for_queued(&scheduler, 1).await;
    drop(permits.pop());
    permits.push(timeout(waiter).await.unwrap());

    let snapshot = scheduler.snapshot();
    assert_eq!(snapshot.active_slots, 4);
    assert_eq!(snapshot.peak_active_slots, 4);
    assert!(snapshot.active_slots <= snapshot.slots);
    drop(permits);
    assert_eq!(scheduler.snapshot().active_slots, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn four_slots_are_shared_across_two_queries() {
    assert_fair_first_wave(4, 2).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn eight_slots_are_shared_across_four_queries() {
    assert_fair_first_wave(8, 4).await;
}

#[tokio::test]
async fn waiters_rotate_queries_and_preserve_query_fifo() {
    let scheduler = GlobalComputeScheduler::new(1).unwrap();
    let blocker = scheduler
        .acquire(Uuid::new_v4(), &QueryControl::new())
        .await
        .unwrap();
    let first_query = Uuid::new_v4();
    let second_query = Uuid::new_v4();
    let (order_tx, mut order_rx) = mpsc::unbounded_channel();
    let mut releases = HashMap::new();
    let mut tasks = Vec::new();

    for (label, query_id) in [
        ("a1", first_query),
        ("a2", first_query),
        ("b1", second_query),
        ("b2", second_query),
    ] {
        let (release_tx, release_rx) = oneshot::channel();
        releases.insert(label, release_tx);
        tasks.push(spawn_waiter(
            scheduler.clone(),
            query_id,
            label,
            order_tx.clone(),
            release_rx,
        ));
        wait_for_queued(&scheduler, tasks.len()).await;
    }
    drop(order_tx);
    drop(blocker);

    for expected in ["a1", "b1", "a2", "b2"] {
        let actual = tokio::time::timeout(Duration::from_secs(2), order_rx.recv())
            .await
            .expect("waiter should acquire a slot")
            .expect("order channel should stay open");
        assert_eq!(actual, expected);
        releases.remove(actual).unwrap().send(()).unwrap();
    }
    for task in tasks {
        timeout(task).await;
    }
    assert_eq!(scheduler.snapshot().active_slots, 0);
}

#[tokio::test]
async fn cancellation_removes_waiter_and_does_not_leak_slot() {
    let scheduler = GlobalComputeScheduler::new(1).unwrap();
    let blocker = scheduler
        .acquire(Uuid::new_v4(), &QueryControl::new())
        .await
        .unwrap();
    let control = QueryControl::new();
    let waiting_scheduler = scheduler.clone();
    let waiting_control = control.clone();
    let waiter = tokio::spawn(async move {
        waiting_scheduler
            .acquire(Uuid::new_v4(), &waiting_control)
            .await
    });
    wait_for_queued(&scheduler, 1).await;

    control.cancel();
    assert!(matches!(timeout(waiter).await, Err(Error::Cancelled)));
    let snapshot = scheduler.snapshot();
    assert_eq!(snapshot.active_slots, 1);
    assert_eq!(snapshot.queued_waiters, 0);
    assert_eq!(snapshot.waiting_queries, 0);
    assert_eq!(snapshot.cancelled_waiters, 1);

    drop(blocker);
    assert_eq!(scheduler.snapshot().active_slots, 0);
}

#[tokio::test]
async fn contexts_from_one_engine_share_compute_slots() {
    let directory = tempfile::tempdir().unwrap();
    let engine = Engine::new(
        EngineConfig::builder()
            .compute_threads(1)
            .spill_directory(directory.path().join("spill"))
            .build(),
    )
    .unwrap();
    let first = engine.query_context_for_test().unwrap();
    let second = engine.query_context_for_test().unwrap();
    let second_id = second.query_id;
    let second_metrics = second.metrics.clone();

    let first_permit = first.acquire_compute().await.unwrap();
    let waiting_context = Arc::clone(&second);
    let waiter = tokio::spawn(async move { waiting_context.acquire_compute().await });

    tokio::time::timeout(Duration::from_secs(2), async {
        while engine.compute_scheduler_counts_for_test().2 != 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("second context should wait for the engine slot");
    assert_eq!(engine.compute_scheduler_counts_for_test(), (1, 1, 1, 1));

    drop(first_permit);
    let second_permit = tokio::time::timeout(Duration::from_secs(2), waiter)
        .await
        .expect("second context should acquire the released slot")
        .expect("compute waiter should not panic")
        .expect("compute acquisition should succeed");
    assert_eq!(second_permit.query_id(), second_id);
    assert!(second_metrics.snapshot().scheduler_wait > Duration::ZERO);
    assert_eq!(engine.compute_scheduler_counts_for_test().0, 1);

    drop(second_permit);
    assert_eq!(engine.compute_scheduler_counts_for_test().0, 0);
}

#[tokio::test]
async fn local_cancellation_removes_a_context_compute_waiter() {
    let directory = tempfile::tempdir().unwrap();
    let engine = Engine::new(
        EngineConfig::builder()
            .compute_threads(1)
            .spill_directory(directory.path().join("spill"))
            .build(),
    )
    .unwrap();
    let first = engine.query_context_for_test().unwrap();
    let second = engine.query_context_for_test().unwrap();
    let blocker = first.acquire_compute().await.unwrap();
    let cancellation = tokio_util::sync::CancellationToken::new();
    let waiting_context = Arc::clone(&second);
    let waiting_cancellation = cancellation.clone();
    let waiter = tokio::spawn(async move {
        waiting_context
            .acquire_compute_until_cancelled(&waiting_cancellation)
            .await
    });

    tokio::time::timeout(Duration::from_secs(2), async {
        while engine.compute_scheduler_counts_for_test().2 != 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("second context should enter the compute queue");
    cancellation.cancel();
    assert!(matches!(timeout(waiter).await, Err(Error::Cancelled)));
    assert_eq!(engine.compute_scheduler_counts_for_test(), (1, 1, 0, 0));

    drop(blocker);
    assert_eq!(engine.compute_scheduler_counts_for_test().0, 0);
}

async fn assert_fair_first_wave(slots: usize, query_count: usize) {
    let scheduler = GlobalComputeScheduler::new(slots).unwrap();
    let blocker_control = QueryControl::new();
    let mut blockers = Vec::new();
    for _ in 0..slots {
        blockers.push(
            scheduler
                .acquire(Uuid::nil(), &blocker_control)
                .await
                .unwrap(),
        );
    }

    let query_ids = (0..query_count).map(|_| Uuid::new_v4()).collect::<Vec<_>>();
    let ready_barrier = Arc::new(Barrier::new(slots + 1));
    let release_barrier = Arc::new(Barrier::new(slots + 1));
    let (acquired_tx, mut acquired_rx) = mpsc::unbounded_channel();
    let mut tasks = Vec::new();
    for index in 0..slots {
        let query_id = query_ids[index % query_count];
        let worker_scheduler = scheduler.clone();
        let ready_barrier = Arc::clone(&ready_barrier);
        let release_barrier = Arc::clone(&release_barrier);
        let acquired_tx = acquired_tx.clone();
        tasks.push(tokio::spawn(async move {
            let permit = worker_scheduler
                .acquire(query_id, &QueryControl::new())
                .await
                .unwrap();
            assert_eq!(permit.query_id(), query_id);
            assert!(permit.wait_time() > Duration::ZERO);
            acquired_tx.send(query_id).unwrap();
            ready_barrier.wait().await;
            release_barrier.wait().await;
            drop(permit);
        }));
        wait_for_queued(&scheduler, index + 1).await;
    }
    drop(acquired_tx);
    drop(blockers);
    ready_barrier.wait().await;

    let snapshot = scheduler.snapshot();
    assert_eq!(snapshot.active_slots, slots);
    assert_eq!(snapshot.peak_active_slots, slots);
    assert!(snapshot.active_slots <= snapshot.slots);
    assert_eq!(snapshot.queued_waiters, 0);
    assert!(snapshot.total_wait > Duration::ZERO);

    let mut counts = HashMap::new();
    for _ in 0..slots {
        let query_id = acquired_rx.recv().await.unwrap();
        *counts.entry(query_id).or_insert(0usize) += 1;
    }
    for query_id in query_ids {
        assert_eq!(counts.get(&query_id), Some(&(slots / query_count)));
    }
    release_barrier.wait().await;
    for task in tasks {
        timeout(task).await;
    }
    assert_eq!(scheduler.snapshot().active_slots, 0);
}

fn spawn_waiter(
    scheduler: GlobalComputeScheduler,
    query_id: Uuid,
    label: &'static str,
    acquired: mpsc::UnboundedSender<&'static str>,
    release: oneshot::Receiver<()>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let permit = scheduler
            .acquire(query_id, &QueryControl::new())
            .await
            .unwrap();
        acquired.send(label).unwrap();
        release.await.unwrap();
        drop(permit);
    })
}

async fn wait_for_queued(scheduler: &GlobalComputeScheduler, expected: usize) {
    tokio::time::timeout(Duration::from_secs(2), async {
        while scheduler.snapshot().queued_waiters != expected {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("waiter should enter the scheduler queue");
}

async fn timeout<T>(task: tokio::task::JoinHandle<T>) -> T {
    tokio::time::timeout(Duration::from_secs(2), task)
        .await
        .expect("task should finish")
        .expect("task should not panic")
}
