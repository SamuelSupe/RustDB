use std::{
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    task::{Context, Poll, Waker},
    time::Duration,
};

use futures::{Stream, stream};
use tokio::sync::Notify;

use super::next_with_compute;
use crate::{Engine, EngineConfig, Error};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pending_io_releases_the_slot_without_busy_polling() {
    let directory = tempfile::tempdir().unwrap();
    let engine = test_engine(directory.path());
    let scan_context = engine.query_context_for_test().unwrap();
    let scan_metrics = scan_context.metrics.clone();
    let competing_context = engine.query_context_for_test().unwrap();
    let gate = Gate::new();
    let task_gate = gate.clone();

    let task = tokio::spawn(async move {
        let mut input = GateStream::new(task_gate);
        next_with_compute(&mut input, scan_context).await
    });
    timeout(gate.first_poll.notified()).await;
    assert_eq!(gate.polls.load(Ordering::Acquire), 1);

    let competing_permit = timeout(competing_context.acquire_compute()).await.unwrap();
    for _ in 0..16 {
        tokio::task::yield_now().await;
    }
    assert_eq!(gate.polls.load(Ordering::Acquire), 1);
    drop(competing_permit);

    gate.release();
    let (value, permit) = timeout(task).await.unwrap().unwrap().unwrap();
    assert_eq!(value, 7);
    assert_eq!(engine.compute_scheduler_counts_for_test().0, 1);
    drop(permit);
    assert_eq!(engine.compute_scheduler_counts_for_test().0, 0);
    let metrics = scan_metrics.snapshot();
    assert_eq!(metrics.parquet_decode_polls, 2);
    assert_eq!(metrics.parquet_decode_pending_polls, 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn decoder_waits_for_a_slot_and_returns_it_with_a_ready_item() {
    let directory = tempfile::tempdir().unwrap();
    let engine = test_engine(directory.path());
    let blocker_context = engine.query_context_for_test().unwrap();
    let decode_context = engine.query_context_for_test().unwrap();
    let decode_metrics = decode_context.metrics.clone();
    let competing_context = engine.query_context_for_test().unwrap();
    let blocker = blocker_context.acquire_compute().await.unwrap();
    let polls = Arc::new(AtomicUsize::new(0));
    let task_polls = Arc::clone(&polls);

    let task = tokio::spawn(async move {
        let mut input = stream::poll_fn(move |_| {
            task_polls.fetch_add(1, Ordering::AcqRel);
            Poll::Ready(Some(11_usize))
        });
        next_with_compute(&mut input, decode_context).await
    });
    wait_for_queued(&engine, 1).await;
    assert_eq!(polls.load(Ordering::Acquire), 0);

    drop(blocker);
    let (value, decode_permit) = timeout(task).await.unwrap().unwrap().unwrap();
    assert_eq!(value, 11);
    assert_eq!(polls.load(Ordering::Acquire), 1);

    let competitor = tokio::spawn(async move { competing_context.acquire_compute().await });
    wait_for_queued(&engine, 1).await;
    drop(decode_permit);
    let competing_permit = timeout(competitor).await.unwrap().unwrap();
    drop(competing_permit);
    assert_eq!(engine.compute_scheduler_counts_for_test().0, 0);
    let metrics = decode_metrics.snapshot();
    assert_eq!(metrics.parquet_decode_polls, 1);
    assert_eq!(metrics.parquet_decode_pending_polls, 0);
    assert!(!metrics.parquet_decode_compute_permit_wait.is_zero());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancellation_removes_a_pending_decoder_waiter() {
    let directory = tempfile::tempdir().unwrap();
    let engine = test_engine(directory.path());
    let blocker_context = engine.query_context_for_test().unwrap();
    let decode_context = engine.query_context_for_test().unwrap();
    let blocker = blocker_context.acquire_compute().await.unwrap();
    let polls = Arc::new(AtomicUsize::new(0));
    let task_polls = Arc::clone(&polls);
    let task_context = Arc::clone(&decode_context);

    let task = tokio::spawn(async move {
        let mut input = stream::poll_fn(move |_| {
            task_polls.fetch_add(1, Ordering::AcqRel);
            Poll::Ready(Some(13_usize))
        });
        next_with_compute(&mut input, task_context).await
    });
    wait_for_queued(&engine, 1).await;
    decode_context.cancel();

    assert!(matches!(
        timeout(task).await.unwrap(),
        Err(Error::Cancelled)
    ));
    assert_eq!(polls.load(Ordering::Acquire), 0);
    assert_eq!(engine.compute_scheduler_counts_for_test().2, 0);
    drop(blocker);
    assert_eq!(engine.compute_scheduler_counts_for_test().0, 0);
}

fn test_engine(directory: &std::path::Path) -> Engine {
    Engine::new(
        EngineConfig::builder()
            .compute_threads(1)
            .spill_directory(directory.join("spill"))
            .build(),
    )
    .unwrap()
}

async fn wait_for_queued(engine: &Engine, expected: usize) {
    timeout(async {
        while engine.compute_scheduler_counts_for_test().2 != expected {
            tokio::task::yield_now().await;
        }
    })
    .await;
}

async fn timeout<T>(future: impl std::future::Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(2), future)
        .await
        .expect("operation timed out")
}

#[derive(Clone)]
struct Gate {
    first_poll: Arc<Notify>,
    polls: Arc<AtomicUsize>,
    ready: Arc<AtomicBool>,
    waker: Arc<Mutex<Option<Waker>>>,
}

impl Gate {
    fn new() -> Self {
        Self {
            first_poll: Arc::new(Notify::new()),
            polls: Arc::new(AtomicUsize::new(0)),
            ready: Arc::new(AtomicBool::new(false)),
            waker: Arc::new(Mutex::new(None)),
        }
    }

    fn release(&self) {
        self.ready.store(true, Ordering::Release);
        if let Some(waker) = self.waker.lock().unwrap().take() {
            waker.wake();
        }
    }
}

struct GateStream {
    gate: Gate,
    emitted: bool,
}

impl GateStream {
    fn new(gate: Gate) -> Self {
        Self {
            gate,
            emitted: false,
        }
    }
}

impl Stream for GateStream {
    type Item = usize;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.gate.polls.fetch_add(1, Ordering::AcqRel);
        self.gate.first_poll.notify_one();
        if self.emitted {
            return Poll::Ready(None);
        }
        if self.gate.ready.load(Ordering::Acquire) {
            self.emitted = true;
            return Poll::Ready(Some(7));
        }
        *self.gate.waker.lock().unwrap() = Some(cx.waker().clone());
        Poll::Pending
    }
}
