use std::{fmt, future::Future, sync::Arc};

use parking_lot::Mutex;
use tokio_util::sync::CancellationToken;

use crate::{
    Error, Result,
    runtime::{QueryLocalFileHandle, QueryLocalFiles},
};

type Cleanup = Box<dyn FnOnce() + Send + 'static>;

#[derive(Clone)]
pub struct QueryControl {
    inner: Arc<Inner>,
}

struct Inner {
    token: CancellationToken,
    cleanups: Mutex<Vec<Cleanup>>,
    local_files: QueryLocalFiles,
}

impl QueryControl {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Inner {
                token: CancellationToken::new(),
                cleanups: Mutex::new(Vec::new()),
                local_files: QueryLocalFiles::default(),
            }),
        }
    }

    pub(crate) fn local_file_handle(&self, uri: &str) -> Arc<QueryLocalFileHandle> {
        self.inner.local_files.handle(uri)
    }

    pub(crate) fn clear_local_files(&self) {
        self.inner.local_files.clear();
    }

    pub fn cancel(&self) {
        self.inner.token.cancel();
        self.clear_local_files();
        let cleanups = std::mem::take(&mut *self.inner.cleanups.lock());
        for cleanup in cleanups {
            cleanup();
        }
    }

    pub fn is_cancelled(&self) -> bool {
        self.inner.token.is_cancelled()
    }

    pub fn check_cancelled(&self) -> Result<()> {
        if self.is_cancelled() {
            Err(Error::Cancelled)
        } else {
            Ok(())
        }
    }

    pub fn cancelled(&self) -> impl Future<Output = ()> + Send + 'static {
        let token = self.inner.token.clone();
        async move { token.cancelled_owned().await }
    }

    /// Registers idempotent resource cleanup to run when the query is cancelled.
    /// If cancellation already happened, the callback runs immediately.
    pub fn register_cleanup(&self, cleanup: impl FnOnce() + Send + 'static) {
        let mut cleanups = self.inner.cleanups.lock();
        if self.is_cancelled() {
            drop(cleanups);
            cleanup();
        } else {
            cleanups.push(Box::new(cleanup));
        }
    }
}

impl Default for QueryControl {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for QueryControl {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("QueryControl")
            .field("cancelled", &self.is_cancelled())
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    use super::QueryControl;

    #[test]
    fn local_file_handles_are_query_scoped_and_released_on_cancel() {
        let control = QueryControl::new();
        let first = control.local_file_handle("file:///data.parquet");
        let same = control.local_file_handle("file:///data.parquet");
        assert!(Arc::ptr_eq(&first, &same));

        let other = QueryControl::new().local_file_handle("file:///data.parquet");
        assert!(!Arc::ptr_eq(&first, &other));

        let retained = Arc::downgrade(&first);
        drop(first);
        drop(same);
        assert!(retained.upgrade().is_some());
        control.cancel();
        assert!(retained.upgrade().is_none());
    }

    #[tokio::test]
    async fn cancellation_notifies_waiters_and_runs_cleanup_once() {
        let control = QueryControl::new();
        let waiter = tokio::spawn(control.cancelled());
        let cleaned = Arc::new(AtomicUsize::new(0));
        let counter = Arc::clone(&cleaned);
        control.register_cleanup(move || {
            counter.fetch_add(1, Ordering::Relaxed);
        });

        control.cancel();
        control.cancel();

        waiter.await.expect("waiter must finish");
        assert!(control.check_cancelled().is_err());
        assert_eq!(cleaned.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn cleanup_registered_after_cancel_runs_immediately() {
        let control = QueryControl::new();
        control.cancel();
        let cleaned = Arc::new(AtomicUsize::new(0));
        let counter = Arc::clone(&cleaned);
        control.register_cleanup(move || {
            counter.fetch_add(1, Ordering::Relaxed);
        });
        assert_eq!(cleaned.load(Ordering::Relaxed), 1);
    }
}
