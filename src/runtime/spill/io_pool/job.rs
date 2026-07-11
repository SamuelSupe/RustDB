use std::sync::{Arc, Mutex, mpsc};

use crate::{Error, Result};

type Operation = Box<dyn FnOnce() + Send + 'static>;

/// A queued job whose not-yet-started operation can be dropped by its caller.
/// This releases owned I/O buffers and their memory reservations immediately
/// when a query is cancelled, even while the queue itself is stalled.
#[derive(Clone)]
pub(super) struct Job {
    operation: Arc<Mutex<Option<Operation>>>,
}

impl Job {
    pub(super) fn new<T, F>(operation: F) -> (Self, mpsc::Receiver<Result<T>>)
    where
        T: Send + 'static,
        F: FnOnce() -> Result<T> + Send + 'static,
    {
        let (result_sender, result_receiver) = mpsc::sync_channel(1);
        let operation = Box::new(move || {
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(operation))
                .unwrap_or_else(|_| {
                    Err(Error::Internal(
                        "spill I/O worker panicked while executing an operation".to_owned(),
                    ))
                });
            let _ = result_sender.send(result);
        });
        (
            Self {
                operation: Arc::new(Mutex::new(Some(operation))),
            },
            result_receiver,
        )
    }

    pub(super) fn run(self) {
        let operation = self
            .operation
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .take();
        if let Some(operation) = operation {
            operation();
        }
    }

    pub(super) fn cancel(&self) {
        self.operation
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .take();
    }
}
