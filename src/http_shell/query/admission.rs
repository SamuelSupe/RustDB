//! Layered logical admission control for HTTP Queries.
//!
//! The scheduler is intentionally independent from Query persistence. A caller
//! enqueues one principal-scoped request, awaits its decision, and retains the
//! returned ticket for exactly as long as the Query owns its logical budgets.

#![allow(dead_code)]

use std::{fmt, sync::Arc};

use parking_lot::Mutex;
use tokio::sync::oneshot;

#[path = "admission/state.rs"]
mod state;
#[path = "admission/types.rs"]
mod types;

use state::State;
#[allow(unused_imports)]
pub use types::{
    AdmissionError, AdmissionLayer, AdmissionLimits, AdmissionPrincipal, AdmissionSnapshot,
    PrincipalAdmissionConfig, PrincipalAdmissionSnapshot, ResourceKind, ResourceRequest,
};

#[derive(Clone)]
pub struct AdmissionController {
    inner: Arc<AdmissionControllerInner>,
}

pub(super) struct AdmissionControllerInner {
    state: Mutex<State>,
}

impl AdmissionController {
    pub fn new(engine_limits: AdmissionLimits) -> Result<Self, AdmissionError> {
        engine_limits.validate()?;
        Ok(Self {
            inner: Arc::new(AdmissionControllerInner {
                state: Mutex::new(State::new(engine_limits)),
            }),
        })
    }

    pub fn register_principal(
        &self,
        principal: AdmissionPrincipal,
        config: PrincipalAdmissionConfig,
    ) -> Result<(), AdmissionError> {
        let mut state = self.inner.state.lock();
        state.register(principal, config)?;
        state.schedule(&self.inner);
        Ok(())
    }

    pub fn update_principal(
        &self,
        principal: &AdmissionPrincipal,
        config: PrincipalAdmissionConfig,
    ) -> Result<(), AdmissionError> {
        let mut state = self.inner.state.lock();
        state.update_principal(principal, config)?;
        state.schedule(&self.inner);
        Ok(())
    }

    /// Rejects queued work and prevents new work. Active tickets remain valid
    /// and remove the retired state when the last one is dropped.
    pub fn remove_principal(&self, principal: &AdmissionPrincipal) -> Result<(), AdmissionError> {
        let mut state = self.inner.state.lock();
        state.remove_principal(principal)?;
        state.schedule(&self.inner);
        Ok(())
    }

    pub fn enqueue(
        &self,
        principal: &AdmissionPrincipal,
        request: ResourceRequest,
    ) -> Result<AdmissionWaiter, AdmissionError> {
        let (sender, receiver) = oneshot::channel();
        let mut state = self.inner.state.lock();
        let id = state.enqueue(principal, request, sender)?;
        state.schedule(&self.inner);
        Ok(AdmissionWaiter {
            inner: Arc::clone(&self.inner),
            principal: principal.clone(),
            id,
            receiver,
            armed: true,
        })
    }

    pub fn snapshot(&self) -> AdmissionSnapshot {
        self.inner.state.lock().snapshot()
    }
}

#[must_use = "await or cancel the queued admission"]
pub struct AdmissionWaiter {
    inner: Arc<AdmissionControllerInner>,
    principal: AdmissionPrincipal,
    id: u64,
    receiver: oneshot::Receiver<Result<AdmissionTicket, AdmissionError>>,
    armed: bool,
}

impl fmt::Debug for AdmissionWaiter {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AdmissionWaiter")
            .field("principal", &self.principal)
            .field("id", &self.id)
            .finish_non_exhaustive()
    }
}

impl AdmissionWaiter {
    pub fn id(&self) -> u64 {
        self.id
    }

    pub fn cancel(&mut self) -> bool {
        if !self.armed {
            return false;
        }
        let mut state = self.inner.state.lock();
        let cancelled = state.cancel(&self.principal, self.id);
        state.schedule(&self.inner);
        self.armed = false;
        cancelled
    }

    pub async fn wait(mut self) -> Result<AdmissionTicket, AdmissionError> {
        let decision = (&mut self.receiver)
            .await
            .map_err(|_| AdmissionError::WaiterClosed)?;
        self.armed = false;
        decision
    }
}

impl Drop for AdmissionWaiter {
    fn drop(&mut self) {
        if self.armed {
            let mut state = self.inner.state.lock();
            state.cancel(&self.principal, self.id);
            state.schedule(&self.inner);
        }
    }
}

#[must_use = "dropping the admission ticket releases its logical resources"]
pub struct AdmissionTicket {
    lease: Option<TicketLease>,
}

impl fmt::Debug for AdmissionTicket {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut output = formatter.debug_struct("AdmissionTicket");
        if let Some(lease) = &self.lease {
            output
                .field("principal", &lease.principal)
                .field("resources", &lease.request);
        }
        output.finish_non_exhaustive()
    }
}

struct TicketLease {
    inner: Arc<AdmissionControllerInner>,
    principal: AdmissionPrincipal,
    request: ResourceRequest,
}

impl AdmissionTicket {
    fn new(
        inner: Arc<AdmissionControllerInner>,
        principal: AdmissionPrincipal,
        request: ResourceRequest,
    ) -> Self {
        Self {
            lease: Some(TicketLease {
                inner,
                principal,
                request,
            }),
        }
    }

    pub fn principal(&self) -> &AdmissionPrincipal {
        &self.lease.as_ref().expect("ticket is active").principal
    }

    pub fn resources(&self) -> ResourceRequest {
        self.lease.as_ref().expect("ticket is active").request
    }

    pub fn release(self) {}

    pub(super) fn disarm(&mut self) {
        self.lease.take();
    }
}

impl Drop for AdmissionTicket {
    fn drop(&mut self) {
        let Some(lease) = self.lease.take() else {
            return;
        };
        let mut state = lease.inner.state.lock();
        state.release(&lease.principal, lease.request);
        state.schedule(&lease.inner);
    }
}
