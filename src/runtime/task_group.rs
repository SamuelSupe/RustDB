use std::{
    any::Any,
    future::Future,
    panic::AssertUnwindSafe,
    path::PathBuf,
    sync::{Arc, Weak},
};

#[cfg(test)]
use std::sync::atomic::{AtomicBool, Ordering};

use futures::FutureExt;
use parking_lot::{Condvar, Mutex};
use tokio::{runtime::Handle, sync::Notify};

use crate::{Error, Result};

use super::QueryControl;

/// Owns every asynchronous worker started for one query.
///
/// The group records the first worker failure, cancels sibling work, and
/// exposes both async and background-thread quiescence paths. It deliberately
/// does not abort Tokio tasks: workers retain Arrow reservations and spill
/// handles until their futures have actually unwound.
#[derive(Clone)]
pub(crate) struct TaskGroup {
    inner: Arc<Inner>,
}

#[derive(Clone)]
pub(crate) struct WeakTaskGroup {
    inner: Weak<Inner>,
}

struct Inner {
    control: QueryControl,
    state: Mutex<State>,
    quiescent: Condvar,
    notified: Notify,
    #[cfg(test)]
    fail_next_reaper_spawn: AtomicBool,
}

struct State {
    accepting: bool,
    active: usize,
    first_failure: Option<TaskFailure>,
    reaper_started: bool,
    reaper_cleanups: Vec<ReaperCleanup>,
}

type ReaperCleanup = Box<dyn FnOnce() + Send + 'static>;

struct ActiveTask {
    inner: Arc<Inner>,
    name: &'static str,
    completed: bool,
}

#[derive(Clone)]
enum TaskFailure {
    InvalidArgument(String),
    Unsupported(String),
    ResourceExhausted(String),
    NativeDiskQuotaExceeded {
        path: PathBuf,
        table: Option<String>,
        current_bytes: u64,
        added_bytes: u64,
        peak_bytes: u64,
        limit_bytes: u64,
    },
    Catalog(String),
    TransactionClosed {
        transaction_id: String,
        state: &'static str,
    },
    TransactionConflict {
        transaction_id: String,
        message: String,
    },
    NativeStorage {
        path: PathBuf,
        message: String,
    },
    NativeFormatUnsupported {
        path: PathBuf,
        found_version: u32,
        current_version: u32,
        alpha: bool,
    },
    NativeImportConflict {
        import_id: String,
    },
    CommitOutcomeUnknown {
        path: PathBuf,
        transaction_id: String,
        message: String,
    },
    NativeCommitPostCommitFailure {
        path: PathBuf,
        transaction_id: String,
        generation: u64,
        message: String,
    },
    CopyPostCommitFailure {
        path: PathBuf,
        message: String,
    },
    Execution(String),
    Internal(String),
}

impl TaskGroup {
    pub(crate) fn new(control: QueryControl) -> Self {
        let inner = Arc::new(Inner {
            control: control.clone(),
            state: Mutex::new(State {
                accepting: true,
                active: 0,
                first_failure: None,
                reaper_started: false,
                reaper_cleanups: Vec::new(),
            }),
            quiescent: Condvar::new(),
            notified: Notify::new(),
            #[cfg(test)]
            fail_next_reaper_spawn: AtomicBool::new(false),
        });
        let weak = Arc::downgrade(&inner);
        control.register_cleanup(move || {
            if let Some(inner) = weak.upgrade() {
                inner.close();
            }
        });
        Self { inner }
    }

    pub(crate) fn spawn<F>(&self, name: &'static str, future: F) -> Result<()>
    where
        F: Future<Output = Result<()>> + Send + 'static,
    {
        self.spawn_on(&Handle::current(), name, future)
    }

    pub(crate) fn spawn_on<F>(&self, handle: &Handle, name: &'static str, future: F) -> Result<()>
    where
        F: Future<Output = Result<()>> + Send + 'static,
    {
        self.spawn_registered_on(handle, name, future, false)
    }

    /// Registers teardown created while a worker is unwinding. A closed group
    /// accepts this only while another registered task is still active, so
    /// cleanup cannot resurrect a query after it reached quiescence.
    pub(crate) fn spawn_cleanup_on<F>(
        &self,
        handle: &Handle,
        name: &'static str,
        future: F,
    ) -> Result<()>
    where
        F: Future<Output = Result<()>> + Send + 'static,
    {
        self.spawn_registered_on(handle, name, future, true)
    }

    fn spawn_registered_on<F>(
        &self,
        handle: &Handle,
        name: &'static str,
        future: F,
        allow_during_unwind: bool,
    ) -> Result<()>
    where
        F: Future<Output = Result<()>> + Send + 'static,
    {
        self.inner.start_task(allow_during_unwind)?;
        let inner = Arc::clone(&self.inner);
        let active = ActiveTask {
            inner: Arc::clone(&inner),
            name,
            completed: false,
        };
        handle.spawn(async move {
            // Keep the worker future (and therefore any result-channel sender
            // it owns) alive until its failure has been recorded. If the
            // sender were dropped first, consumers could observe a generic
            // closed-channel error before the real task error or panic.
            let mut future = Box::pin(future);
            let outcome = AssertUnwindSafe(future.as_mut()).catch_unwind().await;
            match outcome {
                Ok(Ok(())) => {}
                Ok(Err(error)) => inner.record_failure(&error),
                Err(payload) => {
                    let message = panic_message(payload.as_ref());
                    inner.record_failure(&Error::Internal(format!(
                        "query task '{name}' panicked: {message}"
                    )));
                }
            }
            drop(future);
            active.complete();
        });
        Ok(())
    }

    pub(crate) fn record_failure(&self, error: &Error) {
        self.inner.record_failure(error);
    }

    pub(crate) fn first_failure(&self) -> Option<Error> {
        self.inner
            .state
            .lock()
            .first_failure
            .as_ref()
            .map(TaskFailure::to_error)
    }

    pub(crate) fn active_tasks(&self) -> usize {
        self.inner.state.lock().active
    }

    pub(crate) fn downgrade(&self) -> WeakTaskGroup {
        WeakTaskGroup {
            inner: Arc::downgrade(&self.inner),
        }
    }

    pub(crate) fn close(&self) {
        self.inner.close();
    }

    pub(crate) async fn quiesce(&self) {
        self.close();
        loop {
            let notified = self.inner.notified.notified();
            tokio::pin!(notified);
            // Register this waiter before observing the active count. Without
            // `enable`, a task can finish after the count check but before
            // the future is first polled, and `notify_waiters` would be lost.
            notified.as_mut().enable();
            if self.active_tasks() == 0 {
                return;
            }
            notified.await;
        }
    }

    /// Runs teardown away from `Drop`, after every registered task has
    /// released its retained state. Multiple owners may enqueue cleanup.
    pub(crate) fn reap(&self, cleanup: impl FnOnce() + Send + 'static) {
        self.close();
        self.inner.queue_reaper(Box::new(cleanup));
        self.inner.start_reaper();
    }

    #[cfg(test)]
    fn fail_next_reaper_spawn(&self) {
        self.inner
            .fail_next_reaper_spawn
            .store(true, Ordering::Release);
    }
}

impl WeakTaskGroup {
    pub(crate) fn reap(&self, cleanup: impl FnOnce() + Send + 'static) {
        if let Some(inner) = self.inner.upgrade() {
            TaskGroup { inner }.reap(cleanup);
        } else {
            cleanup();
        }
    }
}

impl Drop for ActiveTask {
    fn drop(&mut self) {
        if !self.completed && !self.inner.control.is_cancelled() {
            self.inner.record_failure(&Error::Internal(format!(
                "query task '{}' was aborted before completion",
                self.name
            )));
        }
        self.inner.finish_task();
    }
}

impl ActiveTask {
    fn complete(mut self) {
        self.completed = true;
    }
}

impl Inner {
    fn start_task(&self, allow_during_unwind: bool) -> Result<()> {
        let mut state = self.state.lock();
        if !state.accepting && !(allow_during_unwind && state.active != 0) {
            return Err(self.failure_locked(&state).unwrap_or(Error::Cancelled));
        }
        state.active = state.active.saturating_add(1);
        Ok(())
    }

    fn finish_task(self: &Arc<Self>) {
        let mut state = self.state.lock();
        debug_assert!(state.active > 0, "task group active count underflow");
        state.active = state.active.saturating_sub(1);
        let became_quiescent = state.active == 0;
        if became_quiescent {
            self.quiescent.notify_all();
            self.notified.notify_waiters();
        }
        drop(state);
        // If an earlier OS thread creation failed while workers were still
        // active, the last worker gets one more chance to launch the reaper.
        if became_quiescent {
            self.start_reaper();
        }
    }

    fn close(&self) {
        let mut state = self.state.lock();
        state.accepting = false;
        if state.active == 0 {
            self.quiescent.notify_all();
            self.notified.notify_waiters();
        }
    }

    fn record_failure(&self, error: &Error) {
        let Some(failure) = TaskFailure::from_error(error) else {
            return;
        };
        let first = {
            let mut state = self.state.lock();
            if state.first_failure.is_some() {
                false
            } else {
                state.first_failure = Some(failure);
                state.accepting = false;
                true
            }
        };
        if first {
            self.control.cancel();
        }
    }

    fn failure_locked(&self, state: &State) -> Option<Error> {
        state.first_failure.as_ref().map(TaskFailure::to_error)
    }

    fn queue_reaper(&self, cleanup: ReaperCleanup) {
        let mut state = self.state.lock();
        state.reaper_cleanups.push(cleanup);
    }

    fn start_reaper(self: &Arc<Self>) {
        {
            let mut state = self.state.lock();
            if state.reaper_started || state.reaper_cleanups.is_empty() {
                return;
            }
            state.reaper_started = true;
        }

        let worker_inner = Arc::clone(self);
        #[cfg(test)]
        let injected_failure = self.fail_next_reaper_spawn.swap(false, Ordering::AcqRel);
        #[cfg(not(test))]
        let injected_failure = false;
        let spawn = if injected_failure {
            Err(std::io::Error::other("injected query reaper spawn failure"))
        } else {
            std::thread::Builder::new()
                .name("rustdb-query-reaper".to_owned())
                .spawn(move || worker_inner.run_reaper())
        };
        if let Err(error) = spawn {
            let cleanups = {
                let mut state = self.state.lock();
                state.reaper_started = false;
                if state.active == 0 {
                    std::mem::take(&mut state.reaper_cleanups)
                } else {
                    Vec::new()
                }
            };
            tracing::error!(%error, "failed to start query cleanup reaper");
            // Thread creation failure is exceptional. If no worker remains,
            // synchronous cleanup is the only leak-free fallback; otherwise
            // the last worker retries from `finish_task`.
            run_cleanups(cleanups);
        }
    }

    fn run_reaper(self: Arc<Self>) {
        let cleanups = {
            let mut state = self.state.lock();
            while state.active != 0 {
                self.quiescent.wait(&mut state);
            }
            state.reaper_started = false;
            std::mem::take(&mut state.reaper_cleanups)
        };
        run_cleanups(cleanups);
        self.start_reaper();
    }
}

fn run_cleanups(cleanups: Vec<ReaperCleanup>) {
    for cleanup in cleanups {
        if std::panic::catch_unwind(AssertUnwindSafe(cleanup)).is_err() {
            tracing::error!("query cleanup callback panicked");
        }
    }
}

impl TaskFailure {
    fn from_error(error: &Error) -> Option<Self> {
        Some(match error {
            Error::Cancelled => return None,
            Error::InvalidArgument(message) => Self::InvalidArgument(message.clone()),
            Error::Unsupported(message) => Self::Unsupported(message.clone()),
            Error::ResourceExhausted(message) => Self::ResourceExhausted(message.clone()),
            Error::NativeDiskQuotaExceeded {
                path,
                table,
                current_bytes,
                added_bytes,
                peak_bytes,
                limit_bytes,
            } => Self::NativeDiskQuotaExceeded {
                path: path.clone(),
                table: table.clone(),
                current_bytes: *current_bytes,
                added_bytes: *added_bytes,
                peak_bytes: *peak_bytes,
                limit_bytes: *limit_bytes,
            },
            Error::Catalog(message) => Self::Catalog(message.clone()),
            Error::TransactionClosed {
                transaction_id,
                state,
            } => Self::TransactionClosed {
                transaction_id: transaction_id.clone(),
                state,
            },
            Error::TransactionConflict {
                transaction_id,
                message,
            } => Self::TransactionConflict {
                transaction_id: transaction_id.clone(),
                message: message.clone(),
            },
            Error::NativeStorage { path, message } => Self::NativeStorage {
                path: path.clone(),
                message: message.clone(),
            },
            Error::NativeFormatUnsupported {
                path,
                found_version,
                current_version,
                alpha,
            } => Self::NativeFormatUnsupported {
                path: path.clone(),
                found_version: *found_version,
                current_version: *current_version,
                alpha: *alpha,
            },
            Error::NativeImportConflict { import_id } => Self::NativeImportConflict {
                import_id: import_id.clone(),
            },
            Error::CommitOutcomeUnknown {
                path,
                transaction_id,
                message,
            } => Self::CommitOutcomeUnknown {
                path: path.clone(),
                transaction_id: transaction_id.clone(),
                message: message.clone(),
            },
            Error::NativeCommitPostCommitFailure {
                path,
                transaction_id,
                generation,
                message,
            } => Self::NativeCommitPostCommitFailure {
                path: path.clone(),
                transaction_id: transaction_id.clone(),
                generation: *generation,
                message: message.clone(),
            },
            Error::CopyPostCommitFailure { path, message } => Self::CopyPostCommitFailure {
                path: path.clone(),
                message: message.clone(),
            },
            Error::Execution(message) => Self::Execution(message.clone()),
            Error::Internal(message) => Self::Internal(message.clone()),
            // External error types are not Clone. Preserve their complete
            // display text as a stable execution failure for sibling tasks and
            // the public stream.
            Error::Io { .. }
            | Error::Arrow(_)
            | Error::Parquet(_)
            | Error::ObjectStore(_)
            | Error::SqlParse(_)
            | Error::NativeRepairRefused { .. } => Self::Execution(error.to_string()),
        })
    }

    fn to_error(&self) -> Error {
        match self {
            Self::InvalidArgument(message) => Error::InvalidArgument(message.clone()),
            Self::Unsupported(message) => Error::Unsupported(message.clone()),
            Self::ResourceExhausted(message) => Error::ResourceExhausted(message.clone()),
            Self::NativeDiskQuotaExceeded {
                path,
                table,
                current_bytes,
                added_bytes,
                peak_bytes,
                limit_bytes,
            } => Error::NativeDiskQuotaExceeded {
                path: path.clone(),
                table: table.clone(),
                current_bytes: *current_bytes,
                added_bytes: *added_bytes,
                peak_bytes: *peak_bytes,
                limit_bytes: *limit_bytes,
            },
            Self::Catalog(message) => Error::Catalog(message.clone()),
            Self::TransactionClosed {
                transaction_id,
                state,
            } => Error::TransactionClosed {
                transaction_id: transaction_id.clone(),
                state,
            },
            Self::TransactionConflict {
                transaction_id,
                message,
            } => Error::TransactionConflict {
                transaction_id: transaction_id.clone(),
                message: message.clone(),
            },
            Self::NativeStorage { path, message } => Error::NativeStorage {
                path: path.clone(),
                message: message.clone(),
            },
            Self::NativeFormatUnsupported {
                path,
                found_version,
                current_version,
                alpha,
            } => Error::NativeFormatUnsupported {
                path: path.clone(),
                found_version: *found_version,
                current_version: *current_version,
                alpha: *alpha,
            },
            Self::NativeImportConflict { import_id } => Error::NativeImportConflict {
                import_id: import_id.clone(),
            },
            Self::CommitOutcomeUnknown {
                path,
                transaction_id,
                message,
            } => Error::CommitOutcomeUnknown {
                path: path.clone(),
                transaction_id: transaction_id.clone(),
                message: message.clone(),
            },
            Self::NativeCommitPostCommitFailure {
                path,
                transaction_id,
                generation,
                message,
            } => Error::NativeCommitPostCommitFailure {
                path: path.clone(),
                transaction_id: transaction_id.clone(),
                generation: *generation,
                message: message.clone(),
            },
            Self::CopyPostCommitFailure { path, message } => Error::CopyPostCommitFailure {
                path: path.clone(),
                message: message.clone(),
            },
            Self::Execution(message) => Error::Execution(message.clone()),
            Self::Internal(message) => Error::Internal(message.clone()),
        }
    }
}

fn panic_message(payload: &(dyn Any + Send)) -> String {
    if let Some(message) = payload.downcast_ref::<&str>() {
        (*message).to_owned()
    } else if let Some(message) = payload.downcast_ref::<String>() {
        message.clone()
    } else {
        "non-string panic payload".to_owned()
    }
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{
            Arc,
            atomic::{AtomicBool, AtomicUsize, Ordering},
        },
        time::{Duration, Instant},
    };

    use tokio::sync::Notify;

    use super::TaskGroup;
    use crate::{Error, runtime::QueryControl};

    #[tokio::test]
    async fn successful_tasks_quiesce() {
        let group = TaskGroup::new(QueryControl::new());
        for _ in 0..8 {
            group.spawn("success", async { Ok(()) }).unwrap();
        }
        group.quiesce().await;
        assert_eq!(group.active_tasks(), 0);
        assert!(group.first_failure().is_none());
    }

    #[tokio::test]
    async fn a_short_lifecycle_leaves_no_active_tasks() {
        let group = TaskGroup::new(QueryControl::new());
        group.spawn("lifecycle", async { Ok(()) }).unwrap();
        group.quiesce().await;
        assert_eq!(group.active_tasks(), 0);
        assert!(group.first_failure().is_none());
    }

    #[tokio::test]
    async fn error_panic_and_cancel_lifecycles_converge_once() {
        let error_group = TaskGroup::new(QueryControl::new());
        error_group
            .spawn("ci-error", async {
                Err(Error::Execution("injected error".to_owned()))
            })
            .unwrap();
        error_group.quiesce().await;
        assert_eq!(error_group.active_tasks(), 0);
        assert!(matches!(
            error_group.first_failure(),
            Some(Error::Execution(message)) if message.contains("injected error")
        ));

        let panic_group = TaskGroup::new(QueryControl::new());
        panic_group
            .spawn("ci-panic", async {
                panic!("injected panic");
                #[allow(unreachable_code)]
                Ok(())
            })
            .unwrap();
        panic_group.quiesce().await;
        assert_eq!(panic_group.active_tasks(), 0);
        assert!(matches!(
            panic_group.first_failure(),
            Some(Error::Internal(_))
        ));

        let control = QueryControl::new();
        let cancel_group = TaskGroup::new(control.clone());
        let worker_control = control.clone();
        cancel_group
            .spawn("ci-cancel", async move {
                worker_control.cancelled().await;
                Ok(())
            })
            .unwrap();
        control.cancel();
        cancel_group.quiesce().await;
        assert_eq!(cancel_group.active_tasks(), 0);
        assert!(cancel_group.first_failure().is_none());
    }

    #[tokio::test]
    async fn concurrent_quiescence_waiters_do_not_miss_the_last_task_exit() {
        let group = TaskGroup::new(QueryControl::new());
        let release = Arc::new(Notify::new());
        let worker_release = Arc::clone(&release);
        group
            .spawn("last-worker", async move {
                worker_release.notified().await;
                Ok(())
            })
            .unwrap();

        let mut waiters = Vec::new();
        for _ in 0..8 {
            let waiting = group.clone();
            waiters.push(tokio::spawn(async move {
                waiting.quiesce().await;
            }));
        }
        tokio::task::yield_now().await;
        release.notify_one();

        tokio::time::timeout(Duration::from_secs(2), async {
            for waiter in waiters {
                waiter.await.unwrap();
            }
        })
        .await
        .expect("every quiescence waiter must observe the final task exit");
        assert_eq!(group.active_tasks(), 0);
    }

    #[test]
    fn runtime_shutdown_releases_active_task_permits() {
        let group = TaskGroup::new(QueryControl::new());
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        group
            .spawn_on(runtime.handle(), "pending", async {
                std::future::pending::<()>().await;
                Ok(())
            })
            .unwrap();
        assert_eq!(group.active_tasks(), 1);
        drop(runtime);
        assert_eq!(group.active_tasks(), 0);
    }

    #[tokio::test]
    #[ignore = "release soak; run explicitly before tagging"]
    async fn one_thousand_mixed_lifecycle_release_soak() {
        for iteration in 0..1_000 {
            let control = QueryControl::new();
            let group = TaskGroup::new(control.clone());
            match iteration % 4 {
                0 => group.spawn("release-success", async { Ok(()) }).unwrap(),
                1 => group
                    .spawn("release-error", async {
                        Err(Error::Execution("release soak error".into()))
                    })
                    .unwrap(),
                2 => group
                    .spawn("release-panic", async {
                        panic!("release soak panic");
                        #[allow(unreachable_code)]
                        Ok(())
                    })
                    .unwrap(),
                _ => {
                    let worker_control = control.clone();
                    group
                        .spawn("release-cancel", async move {
                            worker_control.cancelled().await;
                            Ok(())
                        })
                        .unwrap();
                    control.cancel();
                }
            }
            group.quiesce().await;
            assert_eq!(group.active_tasks(), 0, "iteration {iteration}");
        }
    }

    #[tokio::test]
    async fn first_error_cancels_sibling_and_is_preserved() {
        let control = QueryControl::new();
        let group = TaskGroup::new(control.clone());
        let sibling_stopped = Arc::new(AtomicBool::new(false));
        let stopped = Arc::clone(&sibling_stopped);
        let sibling_control = control.clone();
        group
            .spawn("sibling", async move {
                sibling_control.cancelled().await;
                stopped.store(true, Ordering::Release);
                Ok(())
            })
            .unwrap();
        group
            .spawn("failure", async {
                Err(Error::Execution("first worker failure".to_owned()))
            })
            .unwrap();

        group.quiesce().await;
        assert!(control.is_cancelled());
        assert!(sibling_stopped.load(Ordering::Acquire));
        assert!(matches!(
            group.first_failure(),
            Some(Error::Execution(message)) if message == "first worker failure"
        ));
    }

    #[tokio::test]
    async fn panic_is_captured_and_cancels_siblings() {
        let control = QueryControl::new();
        let group = TaskGroup::new(control.clone());
        group
            .spawn("panic-lane", async {
                panic!("injected task panic");
                #[allow(unreachable_code)]
                Ok(())
            })
            .unwrap();

        group.quiesce().await;
        assert!(control.is_cancelled());
        assert!(matches!(
            group.first_failure(),
            Some(Error::Internal(message))
                if message.contains("panic-lane") && message.contains("injected task panic")
        ));
    }

    #[tokio::test]
    async fn cancel_is_nonblocking_and_reaper_waits_for_worker_exit() {
        let control = QueryControl::new();
        let group = TaskGroup::new(control.clone());
        let release = Arc::new(Notify::new());
        let worker_release = Arc::clone(&release);
        group
            .spawn("held-worker", async move {
                worker_release.notified().await;
                Ok(())
            })
            .unwrap();
        let cleaned = Arc::new(AtomicUsize::new(0));
        let cleanup = Arc::clone(&cleaned);
        group.reap(move || {
            cleanup.fetch_add(1, Ordering::Release);
        });

        let started = Instant::now();
        control.cancel();
        assert!(started.elapsed() < Duration::from_millis(100));
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert_eq!(cleaned.load(Ordering::Acquire), 0);

        release.notify_one();
        tokio::time::timeout(Duration::from_secs(2), async {
            while cleaned.load(Ordering::Acquire) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("reaper must run after the worker exits");
        assert_eq!(group.active_tasks(), 0);
        assert_eq!(cleaned.load(Ordering::Acquire), 1);
    }

    #[tokio::test]
    async fn reaper_spawn_failure_does_not_lose_cleanup() {
        let group = TaskGroup::new(QueryControl::new());
        group.fail_next_reaper_spawn();
        let cleaned = Arc::new(AtomicUsize::new(0));
        let cleanup = Arc::clone(&cleaned);

        group.reap(move || {
            cleanup.fetch_add(1, Ordering::Release);
        });

        assert_eq!(cleaned.load(Ordering::Acquire), 1);
        assert!(!group.inner.state.lock().reaper_started);
        assert!(group.inner.state.lock().reaper_cleanups.is_empty());
    }

    #[tokio::test]
    async fn one_reaper_cleanup_panic_does_not_skip_other_owners() {
        let group = TaskGroup::new(QueryControl::new());
        let release = Arc::new(Notify::new());
        let worker_release = Arc::clone(&release);
        group
            .spawn("cleanup-panic-worker", async move {
                worker_release.notified().await;
                Ok(())
            })
            .unwrap();
        group.reap(|| panic!("injected cleanup panic"));
        let cleaned = Arc::new(AtomicUsize::new(0));
        let observed = Arc::clone(&cleaned);
        group.reap(move || {
            observed.fetch_add(1, Ordering::Release);
        });

        release.notify_one();
        tokio::time::timeout(Duration::from_secs(2), async {
            while cleaned.load(Ordering::Acquire) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("a panicking cleanup must not skip later callbacks");
    }

    #[tokio::test]
    async fn last_worker_retries_a_failed_reaper_spawn() {
        let group = TaskGroup::new(QueryControl::new());
        let release = Arc::new(Notify::new());
        let worker_release = Arc::clone(&release);
        group
            .spawn("reaper-retry-worker", async move {
                worker_release.notified().await;
                Ok(())
            })
            .unwrap();
        group.fail_next_reaper_spawn();
        let cleaned = Arc::new(AtomicUsize::new(0));
        let cleanup = Arc::clone(&cleaned);
        group.reap(move || {
            cleanup.fetch_add(1, Ordering::Release);
        });
        assert_eq!(cleaned.load(Ordering::Acquire), 0);

        release.notify_one();
        tokio::time::timeout(Duration::from_secs(2), async {
            while cleaned.load(Ordering::Acquire) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("last worker must retry the failed reaper spawn");
        assert_eq!(group.active_tasks(), 0);
        assert_eq!(cleaned.load(Ordering::Acquire), 1);
    }
}
