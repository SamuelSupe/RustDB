use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
        mpsc,
    },
    thread::{self, JoinHandle},
};

use tokio::sync::oneshot;

use crate::{Error, Result};

type Operation = Box<dyn FnOnce() + Send + 'static>;

pub(crate) const DEFAULT_SERVICE_IO_THREADS: usize = 2;

/// Fixed-size executor for blocking HTTP service-state and result I/O.
#[derive(Clone)]
pub(crate) struct ServiceIoPool {
    shared: Arc<Shared>,
}

struct Shared {
    sender: Mutex<Option<mpsc::SyncSender<Operation>>>,
    workers: Mutex<Vec<JoinHandle<()>>>,
    pending: AtomicUsize,
    idle: tokio::sync::Notify,
}

struct PendingOperation {
    shared: Arc<Shared>,
}

impl ServiceIoPool {
    pub(crate) fn new(threads: usize) -> Result<Self> {
        if threads == 0 {
            return Err(Error::InvalidArgument(
                "HTTP service I/O thread count must be positive".into(),
            ));
        }
        let (sender, receiver) = mpsc::sync_channel::<Operation>(threads.saturating_mul(64).max(1));
        let receiver = Arc::new(Mutex::new(receiver));
        let mut workers = Vec::with_capacity(threads);
        for index in 0..threads {
            let receiver = Arc::clone(&receiver);
            workers.push(
                thread::Builder::new()
                    .name(format!("rustdb-service-io-{index}"))
                    .spawn(move || {
                        loop {
                            let operation = receiver
                                .lock()
                                .unwrap_or_else(|poison| poison.into_inner())
                                .recv();
                            match operation {
                                Ok(operation) => operation(),
                                Err(_) => return,
                            }
                        }
                    })
                    .map_err(|error| {
                        Error::Internal(format!(
                            "cannot start HTTP service I/O worker {index}: {error}"
                        ))
                    })?,
            );
        }
        Ok(Self {
            shared: Arc::new(Shared {
                sender: Mutex::new(Some(sender)),
                workers: Mutex::new(workers),
                pending: AtomicUsize::new(0),
                idle: tokio::sync::Notify::new(),
            }),
        })
    }

    pub(crate) fn run<T, F>(&self, operation: F) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce() -> Result<T> + Send + 'static,
    {
        let (sender, receiver) = mpsc::sync_channel(1);
        self.schedule_blocking(Box::new(move || {
            let result = catch_worker_panic(operation);
            let _ = sender.send(result);
        }))?;
        receiver
            .recv()
            .map_err(|_| Error::Internal("HTTP service I/O worker stopped".into()))?
    }

    pub(crate) async fn run_async<T, F>(&self, operation: F) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce() -> Result<T> + Send + 'static,
    {
        let (sender, receiver) = oneshot::channel();
        self.schedule_nonblocking(Box::new(move || {
            let result = catch_worker_panic(operation);
            let _ = sender.send(result);
        }))?;
        receiver
            .await
            .map_err(|_| Error::Internal("HTTP service I/O worker stopped".into()))?
    }

    pub(crate) fn run_detached<F>(&self, operation: F) -> Result<()>
    where
        F: FnOnce() -> Result<()> + Send + 'static,
    {
        self.schedule_nonblocking(Box::new(move || {
            if let Err(error) = catch_worker_panic(operation) {
                tracing::error!(%error, "detached HTTP service I/O operation failed");
            }
        }))
    }

    // Some integration harnesses include this module without the Query manager.
    #[allow(dead_code)]
    pub(crate) async fn wait_idle_until(&self, deadline: tokio::time::Instant) -> bool {
        loop {
            if self.shared.pending.load(Ordering::Acquire) == 0 {
                return true;
            }
            let notified = self.shared.idle.notified();
            if tokio::time::timeout_at(deadline, notified).await.is_err() {
                return self.shared.pending.load(Ordering::Acquire) == 0;
            }
        }
    }

    fn sender(&self) -> Result<mpsc::SyncSender<Operation>> {
        self.shared
            .sender
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .as_ref()
            .cloned()
            .ok_or_else(|| Error::Internal("HTTP service I/O pool is shutting down".into()))
    }

    fn schedule_blocking(&self, operation: Operation) -> Result<()> {
        let operation = self.track(operation);
        self.sender()?
            .send(operation)
            .map_err(|_| Error::Internal("HTTP service I/O pool stopped".into()))
    }

    fn schedule_nonblocking(&self, operation: Operation) -> Result<()> {
        let operation = self.track(operation);
        self.sender()?
            .try_send(operation)
            .map_err(|error| match error {
                mpsc::TrySendError::Full(_) => Error::ResourceExhausted(
                    "HTTP service I/O queue is full; retry the request".into(),
                ),
                mpsc::TrySendError::Disconnected(_) => {
                    Error::Internal("HTTP service I/O pool stopped".into())
                }
            })
    }

    fn track(&self, operation: Operation) -> Operation {
        self.shared.pending.fetch_add(1, Ordering::AcqRel);
        let pending = PendingOperation {
            shared: Arc::clone(&self.shared),
        };
        Box::new(move || {
            operation();
            drop(pending);
        })
    }
}

impl Drop for PendingOperation {
    fn drop(&mut self) {
        if self.shared.pending.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.shared.idle.notify_waiters();
            self.shared.idle.notify_one();
        }
    }
}

fn catch_worker_panic<T, F>(operation: F) -> Result<T>
where
    F: FnOnce() -> Result<T>,
{
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(operation)).unwrap_or_else(|_| {
        Err(Error::Internal(
            "HTTP service I/O worker panicked while executing an operation".into(),
        ))
    })
}

impl Drop for Shared {
    fn drop(&mut self) {
        self.sender
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .take();
        let workers = std::mem::take(
            &mut *self
                .workers
                .lock()
                .unwrap_or_else(|poison| poison.into_inner()),
        );
        if self.pending.load(Ordering::Acquire) != 0 {
            // In-flight operations retain the shared state, so this branch is
            // only defensive. Never make service teardown wait on stuck disk
            // I/O if ownership changes in a future implementation.
            return;
        }
        let current = thread::current().id();
        for worker in workers {
            if worker.thread().id() != current {
                let _ = worker.join();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::mpsc, time::Duration};

    use super::ServiceIoPool;

    #[tokio::test]
    async fn runs_sync_and_async_jobs_on_named_workers() {
        let pool = ServiceIoPool::new(1).unwrap();
        let sync_name = pool
            .run(|| Ok(std::thread::current().name().unwrap_or_default().to_owned()))
            .unwrap();
        let async_name = pool
            .run_async(|| Ok(std::thread::current().name().unwrap_or_default().to_owned()))
            .await
            .unwrap();
        assert!(sync_name.starts_with("rustdb-service-io-"));
        assert!(async_name.starts_with("rustdb-service-io-"));
    }

    #[test]
    fn dropping_pool_does_not_join_an_in_flight_operation() {
        let pool = ServiceIoPool::new(1).unwrap();
        let (release_tx, release_rx) = mpsc::channel();
        let (started_tx, started_rx) = mpsc::channel();
        pool.run_detached(move || {
            started_tx.send(()).unwrap();
            release_rx.recv().unwrap();
            Ok(())
        })
        .unwrap();
        started_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        drop(pool);
        release_tx.send(()).unwrap();
    }
}
