use std::sync::Arc;

use parking_lot::{Condvar, Mutex};

use crate::{Error, Result};

#[derive(Clone, Debug)]
pub(super) struct IoTracker {
    inner: Arc<Inner>,
}

#[derive(Debug)]
struct Inner {
    state: Mutex<State>,
    idle: Condvar,
}

#[derive(Debug)]
struct State {
    accepting: bool,
    active: usize,
}

pub(super) struct IoPermit {
    inner: Arc<Inner>,
}

impl IoTracker {
    pub(super) fn new() -> Self {
        Self {
            inner: Arc::new(Inner {
                state: Mutex::new(State {
                    accepting: true,
                    active: 0,
                }),
                idle: Condvar::new(),
            }),
        }
    }

    pub(super) fn start(&self) -> Result<IoPermit> {
        let mut state = self.inner.state.lock();
        if !state.accepting {
            return Err(Error::Cancelled);
        }
        state.active = state.active.saturating_add(1);
        Ok(IoPermit {
            inner: Arc::clone(&self.inner),
        })
    }

    pub(super) fn close_and_wait(&self) {
        let mut state = self.inner.state.lock();
        state.accepting = false;
        while state.active != 0 {
            self.inner.idle.wait(&mut state);
        }
    }

    #[cfg(test)]
    pub(super) fn active(&self) -> usize {
        self.inner.state.lock().active
    }
}

impl Drop for IoPermit {
    fn drop(&mut self) {
        let mut state = self.inner.state.lock();
        debug_assert!(state.active > 0, "spill I/O tracker underflow");
        state.active = state.active.saturating_sub(1);
        if state.active == 0 {
            self.inner.idle.notify_all();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::mpsc, thread, time::Duration};

    use super::IoTracker;

    #[test]
    fn cleanup_barrier_waits_for_in_flight_operation() {
        let tracker = IoTracker::new();
        let permit = tracker.start().unwrap();
        let waiting = tracker.clone();
        let (finished_sender, finished_receiver) = mpsc::sync_channel(1);
        let waiter = thread::spawn(move || {
            waiting.close_and_wait();
            finished_sender.send(()).unwrap();
        });

        assert!(
            finished_receiver
                .recv_timeout(Duration::from_millis(20))
                .is_err()
        );
        assert_eq!(tracker.active(), 1);
        assert!(tracker.start().is_err());

        drop(permit);
        finished_receiver
            .recv_timeout(Duration::from_secs(1))
            .unwrap();
        waiter.join().unwrap();
        assert_eq!(tracker.active(), 0);
    }
}
