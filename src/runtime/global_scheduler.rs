use std::{
    collections::{HashMap, VecDeque},
    sync::Arc,
    time::{Duration, Instant},
};

use parking_lot::Mutex;
use tokio::sync::oneshot;
use uuid::Uuid;

use super::QueryControl;
use crate::{Error, Result};

/// Engine-wide compute-slot scheduler.
///
/// Waiters are FIFO within a query. Queries with pending work are visited in
/// round-robin order, while an otherwise idle engine can lend every slot to a
/// single query.
#[derive(Clone)]
pub(crate) struct GlobalComputeScheduler {
    inner: Arc<Inner>,
}

struct Inner {
    slots: usize,
    state: Mutex<State>,
}

#[derive(Default)]
struct State {
    available: usize,
    active: usize,
    peak_active: usize,
    next_ticket: u64,
    ready: VecDeque<Uuid>,
    queries: HashMap<Uuid, VecDeque<Waiter>>,
    queued_waiters: usize,
    grants: u64,
    cancelled_waiters: u64,
    total_wait: Duration,
}

struct Waiter {
    ticket: u64,
    queued_at: Instant,
    sender: oneshot::Sender<GlobalComputePermit>,
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct GlobalSchedulerSnapshot {
    pub(crate) slots: usize,
    pub(crate) active_slots: usize,
    pub(crate) peak_active_slots: usize,
    pub(crate) queued_waiters: usize,
    pub(crate) waiting_queries: usize,
    pub(crate) grants: u64,
    pub(crate) cancelled_waiters: u64,
    pub(crate) total_wait: Duration,
}

pub(crate) struct GlobalComputePermit {
    inner: Arc<Inner>,
    #[cfg(test)]
    query_id: Uuid,
    wait_time: Duration,
    released: bool,
}

impl GlobalComputeScheduler {
    pub(crate) fn new(slots: usize) -> Result<Self> {
        if slots == 0 {
            return Err(Error::InvalidArgument(
                "global compute scheduler requires at least one slot".to_string(),
            ));
        }
        Ok(Self {
            inner: Arc::new(Inner {
                slots,
                state: Mutex::new(State {
                    available: slots,
                    ..State::default()
                }),
            }),
        })
    }

    pub(crate) async fn acquire(
        &self,
        query_id: Uuid,
        control: &QueryControl,
    ) -> Result<GlobalComputePermit> {
        control.check_cancelled()?;
        if let Some(permit) = self.inner.try_acquire_immediate(query_id) {
            if let Err(error) = control.check_cancelled() {
                drop(permit);
                return Err(error);
            }
            return Ok(permit);
        }
        let (sender, receiver) = oneshot::channel();
        let ticket = self.inner.enqueue(query_id, sender);
        let mut guard = WaiterGuard::new(Arc::clone(&self.inner), query_id, ticket);

        let permit = tokio::select! {
            biased;
            _ = control.cancelled() => return Err(Error::Cancelled),
            permit = receiver => permit.map_err(|_| {
                Error::Internal("global compute scheduler dropped a waiter".to_string())
            })?,
        };
        guard.disarm();

        if let Err(error) = control.check_cancelled() {
            drop(permit);
            return Err(error);
        }
        Ok(permit)
    }

    #[cfg(test)]
    pub(crate) fn snapshot(&self) -> GlobalSchedulerSnapshot {
        let state = self.inner.state.lock();
        GlobalSchedulerSnapshot {
            slots: self.inner.slots,
            active_slots: state.active,
            peak_active_slots: state.peak_active,
            queued_waiters: state.queued_waiters,
            waiting_queries: state.queries.len(),
            grants: state.grants,
            cancelled_waiters: state.cancelled_waiters,
            total_wait: state.total_wait,
        }
    }
}

impl Inner {
    fn try_acquire_immediate(
        self: &Arc<Self>,
        #[cfg_attr(not(test), allow(unused_variables))] query_id: Uuid,
    ) -> Option<GlobalComputePermit> {
        let mut state = self.state.lock();
        if state.available == 0 || state.queued_waiters != 0 {
            return None;
        }
        state.available -= 1;
        state.active += 1;
        state.peak_active = state.peak_active.max(state.active);
        state.grants = state.grants.saturating_add(1);
        Some(GlobalComputePermit {
            inner: Arc::clone(self),
            #[cfg(test)]
            query_id,
            wait_time: Duration::ZERO,
            released: false,
        })
    }

    fn enqueue(
        self: &Arc<Self>,
        query_id: Uuid,
        sender: oneshot::Sender<GlobalComputePermit>,
    ) -> u64 {
        let ticket = {
            let mut state = self.state.lock();
            let ticket = state.next_ticket;
            state.next_ticket = state.next_ticket.wrapping_add(1);
            let waiter = Waiter {
                ticket,
                queued_at: Instant::now(),
                sender,
            };
            if let Some(queue) = state.queries.get_mut(&query_id) {
                queue.push_back(waiter);
            } else {
                state.ready.push_back(query_id);
                state.queries.insert(query_id, VecDeque::from([waiter]));
            }
            state.queued_waiters += 1;
            ticket
        };
        self.dispatch();
        ticket
    }

    fn dispatch(self: &Arc<Self>) {
        loop {
            let Some((sender, permit)) = self.next_grant() else {
                return;
            };
            if let Err(mut permit) = sender.send(permit) {
                permit.release(false);
            }
        }
    }

    fn next_grant(
        self: &Arc<Self>,
    ) -> Option<(oneshot::Sender<GlobalComputePermit>, GlobalComputePermit)> {
        let mut state = self.state.lock();
        while state.available > 0 {
            let query_id = state.ready.pop_front()?;
            let Some(mut queue) = state.queries.remove(&query_id) else {
                continue;
            };

            let mut selected = None;
            while let Some(waiter) = queue.pop_front() {
                state.queued_waiters -= 1;
                if waiter.sender.is_closed() {
                    state.cancelled_waiters = state.cancelled_waiters.saturating_add(1);
                } else {
                    selected = Some(waiter);
                    break;
                }
            }
            if !queue.is_empty() {
                state.ready.push_back(query_id);
                state.queries.insert(query_id, queue);
            }
            let Some(waiter) = selected else {
                continue;
            };

            let wait_time = waiter.queued_at.elapsed();
            state.available -= 1;
            state.active += 1;
            state.peak_active = state.peak_active.max(state.active);
            state.grants = state.grants.saturating_add(1);
            state.total_wait = state.total_wait.saturating_add(wait_time);
            let permit = GlobalComputePermit {
                inner: Arc::clone(self),
                #[cfg(test)]
                query_id,
                wait_time,
                released: false,
            };
            return Some((waiter.sender, permit));
        }
        None
    }

    fn cancel_waiter(&self, query_id: Uuid, ticket: u64) {
        let mut state = self.state.lock();
        let Some(mut queue) = state.queries.remove(&query_id) else {
            return;
        };
        let Some(index) = queue.iter().position(|waiter| waiter.ticket == ticket) else {
            state.queries.insert(query_id, queue);
            return;
        };
        queue.remove(index);
        state.queued_waiters -= 1;
        state.cancelled_waiters = state.cancelled_waiters.saturating_add(1);
        if queue.is_empty() {
            state.ready.retain(|queued| *queued != query_id);
        } else {
            state.queries.insert(query_id, queue);
        }
    }

    fn release(self: &Arc<Self>, dispatch: bool) {
        {
            let mut state = self.state.lock();
            debug_assert!(state.active > 0, "compute permit released twice");
            state.active -= 1;
            state.available += 1;
            debug_assert!(state.available <= self.slots);
        }
        if dispatch {
            self.dispatch();
        }
    }
}

impl GlobalComputePermit {
    #[cfg(test)]
    pub(crate) fn query_id(&self) -> Uuid {
        self.query_id
    }

    pub(crate) fn wait_time(&self) -> Duration {
        self.wait_time
    }

    fn release(&mut self, dispatch: bool) {
        if !self.released {
            self.released = true;
            self.inner.release(dispatch);
        }
    }
}

impl Drop for GlobalComputePermit {
    fn drop(&mut self) {
        self.release(true);
    }
}

struct WaiterGuard {
    inner: Arc<Inner>,
    query_id: Uuid,
    ticket: u64,
    armed: bool,
}

impl WaiterGuard {
    fn new(inner: Arc<Inner>, query_id: Uuid, ticket: u64) -> Self {
        Self {
            inner,
            query_id,
            ticket,
            armed: true,
        }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for WaiterGuard {
    fn drop(&mut self) {
        if self.armed {
            self.inner.cancel_waiter(self.query_id, self.ticket);
        }
    }
}

#[cfg(test)]
#[path = "global_scheduler/tests.rs"]
mod tests;
