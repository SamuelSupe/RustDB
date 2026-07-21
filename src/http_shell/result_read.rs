use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};

use parking_lot::Mutex;
use tokio::sync::Notify;

use crate::{Error, Result};

pub(crate) struct ResultReadTracker {
    accepting: Mutex<bool>,
    active: AtomicUsize,
    idle: Notify,
}

impl ResultReadTracker {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            accepting: Mutex::new(true),
            active: AtomicUsize::new(0),
            idle: Notify::new(),
        })
    }

    pub(crate) fn start(self: &Arc<Self>) -> Option<ResultReadGuard> {
        let accepting = self.accepting.lock();
        if !*accepting {
            return None;
        }
        self.active.fetch_add(1, Ordering::AcqRel);
        Some(ResultReadGuard {
            tracker: Arc::clone(self),
        })
    }

    pub(crate) fn close(&self) {
        *self.accepting.lock() = false;
    }

    pub(crate) async fn wait_idle(&self) {
        loop {
            let idle = self.idle.notified();
            if self.active.load(Ordering::Acquire) == 0 {
                return;
            }
            idle.await;
        }
    }

    pub(crate) fn active(&self) -> usize {
        self.active.load(Ordering::Acquire)
    }
}

pub(crate) struct ResultReadGuard {
    tracker: Arc<ResultReadTracker>,
}

impl Drop for ResultReadGuard {
    fn drop(&mut self) {
        if self.tracker.active.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.tracker.idle.notify_waiters();
        }
    }
}

pub(crate) struct ResultAccess {
    deleting: AtomicBool,
    lock: Arc<tokio::sync::RwLock<()>>,
}

impl ResultAccess {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            deleting: AtomicBool::new(false),
            lock: Arc::new(tokio::sync::RwLock::new(())),
        })
    }

    pub(crate) async fn read(&self) -> Result<tokio::sync::OwnedRwLockReadGuard<()>> {
        if self.deleting.load(Ordering::Acquire) {
            return Err(Error::Execution("HTTP result is being deleted".into()));
        }
        let guard = Arc::clone(&self.lock).read_owned().await;
        if self.deleting.load(Ordering::Acquire) {
            return Err(Error::Execution("HTTP result is being deleted".into()));
        }
        Ok(guard)
    }

    pub(crate) fn delete<T>(&self, operation: impl FnOnce() -> Result<T>) -> Result<T> {
        if self
            .deleting
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return Err(Error::Execution(
                "HTTP result deletion is already active".into(),
            ));
        }
        let guard = match Arc::clone(&self.lock).try_write_owned() {
            Ok(guard) => guard,
            Err(_) => {
                self.deleting.store(false, Ordering::Release);
                return Err(Error::Execution(
                    "HTTP result is currently being read".into(),
                ));
            }
        };
        let result = operation();
        drop(guard);
        if result.is_err() {
            self.deleting.store(false, Ordering::Release);
        }
        result
    }
}
