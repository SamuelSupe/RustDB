use std::{
    collections::HashMap,
    future::Future,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    },
};

use parking_lot::Mutex;
use tokio::{sync::Notify, task::AbortHandle, time::Instant};

pub(super) struct TaskRegistry {
    next_id: AtomicU64,
    active: AtomicUsize,
    handles: Mutex<HashMap<u64, Option<AbortHandle>>>,
    idle: Notify,
    aborting: AtomicBool,
}

struct TaskLease {
    id: u64,
    registry: Arc<TaskRegistry>,
}

impl TaskRegistry {
    pub(super) fn new() -> Arc<Self> {
        Arc::new(Self {
            next_id: AtomicU64::new(0),
            active: AtomicUsize::new(0),
            handles: Mutex::new(HashMap::new()),
            idle: Notify::new(),
            aborting: AtomicBool::new(false),
        })
    }

    pub(super) fn spawn<F>(self: &Arc<Self>, future: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        self.active.fetch_add(1, Ordering::AcqRel);
        self.handles.lock().insert(id, None);
        let lease = TaskLease {
            id,
            registry: Arc::clone(self),
        };
        let handle = tokio::spawn(async move {
            let _lease = lease;
            future.await;
        });
        let abort = handle.abort_handle();
        if let Some(slot) = self.handles.lock().get_mut(&id) {
            *slot = Some(abort.clone());
        }
        if self.aborting.load(Ordering::Acquire) {
            abort.abort();
        }
    }

    pub(super) fn active(&self) -> usize {
        self.active.load(Ordering::Acquire)
    }

    pub(super) fn abort_all(&self) {
        self.aborting.store(true, Ordering::Release);
        for handle in self.handles.lock().values().flatten() {
            handle.abort();
        }
    }

    pub(super) async fn wait_idle_until(&self, deadline: Instant) -> bool {
        loop {
            if self.active() == 0 {
                return true;
            }
            let notified = self.idle.notified();
            if tokio::time::timeout_at(deadline, notified).await.is_err() {
                return self.active() == 0;
            }
        }
    }
}

impl Drop for TaskLease {
    fn drop(&mut self) {
        self.registry.handles.lock().remove(&self.id);
        if self.registry.active.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.registry.idle.notify_waiters();
            self.registry.idle.notify_one();
        }
    }
}
