use std::{
    sync::{
        Arc, Mutex,
        mpsc::{self, Sender, SyncSender, TrySendError},
    },
    thread::{self, JoinHandle},
    time::Duration,
};

use crate::runtime::QueryControl;
use crate::{Error, Result};

mod job;

use job::Job;

const CANCEL_POLL_INTERVAL: Duration = Duration::from_millis(2);

/// Fixed-size pool used for blocking spill filesystem operations.
///
/// The public spill APIs remain synchronous for now, but the caller only waits
/// for a dedicated I/O worker; compute-runtime threads never execute `File`
/// reads, writes, fsyncs, metadata calls, or removals themselves.
#[derive(Clone)]
pub(crate) struct SpillIoPool {
    shared: Arc<Shared>,
}

struct Shared {
    data_sender: Mutex<Option<SyncSender<Job>>>,
    cleanup_sender: Mutex<Option<Sender<Job>>>,
    workers: Mutex<Vec<JoinHandle<()>>>,
    data_threads: usize,
}

impl SpillIoPool {
    pub(crate) fn new(threads: usize) -> Result<Self> {
        if threads == 0 {
            return Err(Error::InvalidArgument(
                "spill I/O thread count must be greater than zero".to_owned(),
            ));
        }
        let (data_sender, receiver) = mpsc::sync_channel::<Job>(threads.saturating_mul(4).max(1));
        let receiver = Arc::new(Mutex::new(receiver));
        let mut workers = Vec::with_capacity(threads.saturating_add(1));
        for index in 0..threads {
            let receiver = Arc::clone(&receiver);
            let worker = thread::Builder::new()
                .name(format!("rustdb-spill-io-{index}"))
                .spawn(move || {
                    loop {
                        let job = {
                            let receiver =
                                receiver.lock().unwrap_or_else(|poison| poison.into_inner());
                            receiver.recv()
                        };
                        let Ok(job) = job else {
                            return;
                        };
                        job.run();
                    }
                })
                .map_err(|error| {
                    Error::Internal(format!("cannot start spill I/O worker {index}: {error}"))
                })?;
            workers.push(worker);
        }
        let (cleanup_sender, cleanup_receiver) = mpsc::channel::<Job>();
        let cleanup_worker = thread::Builder::new()
            .name("rustdb-spill-cleanup".to_owned())
            .spawn(move || {
                while let Ok(job) = cleanup_receiver.recv() {
                    job.run();
                }
            })
            .map_err(|error| {
                Error::Internal(format!("cannot start spill cleanup worker: {error}"))
            })?;
        workers.push(cleanup_worker);
        Ok(Self {
            shared: Arc::new(Shared {
                data_sender: Mutex::new(Some(data_sender)),
                cleanup_sender: Mutex::new(Some(cleanup_sender)),
                workers: Mutex::new(workers),
                data_threads: threads,
            }),
        })
    }

    pub(crate) fn run<T, F>(&self, operation: F) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce() -> Result<T> + Send + 'static,
    {
        let (job, result_receiver) = Job::new(operation);
        self.data_sender()?
            .send(job)
            .map_err(|_| Error::Internal("spill I/O pool stopped unexpectedly".to_owned()))?;
        result_receiver
            .recv()
            .map_err(|_| Error::Internal("spill I/O worker stopped unexpectedly".to_owned()))?
    }

    /// Runs ordinary data I/O while allowing an abandoned or cancelled query
    /// to remove a job that has not started and stop waiting for an in-flight
    /// operation.
    pub(crate) fn run_cancelable<T, F>(&self, control: &QueryControl, operation: F) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce() -> Result<T> + Send + 'static,
    {
        let execution_control = control.clone();
        let (job, result_receiver) = Job::new(move || {
            execution_control.check_cancelled()?;
            operation()
        });
        let sender = self.data_sender()?;
        loop {
            control.check_cancelled().inspect_err(|_| job.cancel())?;
            match sender.try_send(job.clone()) {
                Ok(()) => break,
                Err(TrySendError::Full(_)) => thread::park_timeout(CANCEL_POLL_INTERVAL),
                Err(TrySendError::Disconnected(_)) => {
                    job.cancel();
                    return Err(Error::Internal(
                        "spill I/O pool stopped unexpectedly".to_owned(),
                    ));
                }
            }
        }
        loop {
            match result_receiver.recv_timeout(CANCEL_POLL_INTERVAL) {
                Ok(result) => return result,
                Err(mpsc::RecvTimeoutError::Timeout) => {
                    control.check_cancelled().inspect_err(|_| job.cancel())?;
                }
                Err(mpsc::RecvTimeoutError::Disconnected) => {
                    return control.check_cancelled().and_then(|()| {
                        Err(Error::Internal(
                            "spill I/O worker stopped unexpectedly".to_owned(),
                        ))
                    });
                }
            }
        }
    }

    /// Runs query-directory cleanup on an independent worker so cancellation
    /// cannot wait behind a saturated data-I/O queue.
    pub(crate) fn run_cleanup<T, F>(&self, operation: F) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce() -> Result<T> + Send + 'static,
    {
        let (job, result_receiver) = Job::new(operation);
        self.shared
            .cleanup_sender
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .as_ref()
            .cloned()
            .ok_or_else(|| Error::Internal("spill cleanup worker is shutting down".to_owned()))?
            .send(job)
            .map_err(|_| Error::Internal("spill cleanup worker stopped unexpectedly".to_owned()))?;
        result_receiver
            .recv()
            .map_err(|_| Error::Internal("spill cleanup worker stopped unexpectedly".to_owned()))?
    }

    fn data_sender(&self) -> Result<SyncSender<Job>> {
        self.shared
            .data_sender
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .as_ref()
            .cloned()
            .ok_or_else(|| Error::Internal("spill I/O pool is shutting down".to_owned()))
    }

    #[cfg(test)]
    pub(crate) fn shutdown_for_test(&self) {
        self.shared
            .data_sender
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .take();
        self.shared
            .cleanup_sender
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .take();
    }
}

impl std::fmt::Debug for SpillIoPool {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SpillIoPool")
            .field("threads", &self.shared.data_threads)
            .finish()
    }
}

impl Drop for Shared {
    fn drop(&mut self) {
        self.data_sender
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .take();
        self.cleanup_sender
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .take();
        let workers = std::mem::take(
            &mut *self
                .workers
                .lock()
                .unwrap_or_else(|poison| poison.into_inner()),
        );
        for worker in workers {
            let _ = worker.join();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        sync::mpsc,
        thread,
        time::{Duration, Instant},
    };

    use super::SpillIoPool;
    use crate::{
        Error,
        runtime::{MemoryPool, QueryControl},
    };

    #[test]
    fn operations_run_on_a_fixed_spill_worker() {
        let pool = SpillIoPool::new(1).unwrap();
        let first = pool
            .run(|| Ok(std::thread::current().name().unwrap_or_default().to_owned()))
            .unwrap();
        let second = pool
            .run(|| Ok(std::thread::current().name().unwrap_or_default().to_owned()))
            .unwrap();
        assert_eq!(first, "rustdb-spill-io-0");
        assert_eq!(second, first);
    }

    #[test]
    fn cancellation_drops_a_queued_job_and_cleanup_bypasses_busy_data_workers() {
        let pool = SpillIoPool::new(1).unwrap();
        let (started_sender, started_receiver) = mpsc::sync_channel(1);
        let (release_sender, release_receiver) = mpsc::sync_channel(1);
        let blocking_pool = pool.clone();
        let blocker = thread::spawn(move || {
            blocking_pool.run(move || {
                started_sender.send(()).unwrap();
                release_receiver.recv().unwrap();
                Ok(())
            })
        });
        started_receiver
            .recv_timeout(Duration::from_secs(1))
            .expect("data worker must enter the blocking job");

        let cleanup_started = Instant::now();
        let cleanup_thread = pool
            .run_cleanup(|| Ok(thread::current().name().unwrap_or_default().to_owned()))
            .expect("cleanup worker must not wait behind data I/O");
        assert_eq!(cleanup_thread, "rustdb-spill-cleanup");
        assert!(cleanup_started.elapsed() < Duration::from_secs(1));

        let control = QueryControl::new();
        let queued_control = control.clone();
        let queued_pool = pool.clone();
        let (result_sender, result_receiver) = mpsc::sync_channel(1);
        let queued = thread::spawn(move || {
            let result = queued_pool.run_cancelable(&queued_control, || Ok::<_, Error>(()));
            result_sender.send(result).unwrap();
        });
        thread::sleep(Duration::from_millis(20));
        control.cancel();
        assert!(matches!(
            result_receiver
                .recv_timeout(Duration::from_secs(1))
                .expect("cancelled queue wait must return promptly"),
            Err(Error::Cancelled)
        ));

        release_sender.send(()).unwrap();
        blocker.join().unwrap().unwrap();
        queued.join().unwrap();
    }

    #[test]
    fn in_flight_job_keeps_its_lease_while_cancel_and_cleanup_return() {
        let pool = SpillIoPool::new(1).unwrap();
        let control = QueryControl::new();
        let memory = MemoryPool::new(1 << 10);
        let reservation = memory.try_reserve(1 << 10).unwrap();
        let (started_sender, started_receiver) = mpsc::sync_channel(1);
        let (release_sender, release_receiver) = mpsc::sync_channel(1);
        let running_pool = pool.clone();
        let running_control = control.clone();
        let (result_sender, result_receiver) = mpsc::sync_channel(1);
        let running = thread::spawn(move || {
            let result = running_pool.run_cancelable(&running_control, move || {
                let _reservation = reservation;
                started_sender.send(()).unwrap();
                release_receiver.recv().unwrap();
                Ok::<_, Error>(())
            });
            result_sender.send(result).unwrap();
        });
        started_receiver
            .recv_timeout(Duration::from_secs(1))
            .expect("I/O job must start");

        let cancel_started = Instant::now();
        control.cancel();
        assert!(matches!(
            result_receiver
                .recv_timeout(Duration::from_secs(1))
                .expect("in-flight caller must stop waiting"),
            Err(Error::Cancelled)
        ));
        assert!(cancel_started.elapsed() < Duration::from_secs(1));
        assert_eq!(memory.used(), 1 << 10);

        let cleanup_thread = pool
            .run_cleanup(|| Ok(thread::current().name().unwrap_or_default().to_owned()))
            .unwrap();
        assert_eq!(cleanup_thread, "rustdb-spill-cleanup");
        assert_eq!(memory.used(), 1 << 10);

        release_sender.send(()).unwrap();
        running.join().unwrap();
        let deadline = Instant::now() + Duration::from_secs(1);
        while memory.used() != 0 && Instant::now() < deadline {
            thread::yield_now();
        }
        assert_eq!(memory.used(), 0);
    }
}
