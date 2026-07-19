use std::{
    collections::{BTreeMap, VecDeque},
    sync::Arc,
    time::{Duration, Instant},
};

use tokio::sync::oneshot;

use super::{
    AdmissionControllerInner, AdmissionError, AdmissionLayer, AdmissionLimits, AdmissionPrincipal,
    AdmissionSnapshot, AdmissionTicket, PrincipalAdmissionConfig, PrincipalAdmissionSnapshot,
    ResourceRequest,
};

pub(super) struct State {
    engine_limits: AdmissionLimits,
    engine_active: usize,
    engine_queued: usize,
    engine_resources: ResourceRequest,
    rejected: u64,
    total_wait: Duration,
    max_wait: Duration,
    next_id: u64,
    principals: BTreeMap<AdmissionPrincipal, PrincipalState>,
    rotation: VecDeque<AdmissionPrincipal>,
}

struct PrincipalState {
    config: PrincipalAdmissionConfig,
    active: usize,
    resources: ResourceRequest,
    queue: VecDeque<QueueEntry>,
    deficit: u32,
    rejected: u64,
    total_wait: Duration,
    max_wait: Duration,
    retired: bool,
}

pub(super) struct QueueEntry {
    pub id: u64,
    pub request: ResourceRequest,
    pub enqueued_at: Instant,
    pub sender: oneshot::Sender<Result<AdmissionTicket, AdmissionError>>,
}

impl State {
    pub(super) fn new(engine_limits: AdmissionLimits) -> Self {
        Self {
            engine_limits,
            engine_active: 0,
            engine_queued: 0,
            engine_resources: ResourceRequest::default(),
            rejected: 0,
            total_wait: Duration::ZERO,
            max_wait: Duration::ZERO,
            next_id: 1,
            principals: BTreeMap::new(),
            rotation: VecDeque::new(),
        }
    }

    pub(super) fn register(
        &mut self,
        principal: AdmissionPrincipal,
        config: PrincipalAdmissionConfig,
    ) -> Result<(), AdmissionError> {
        config.validate()?;
        if let Some(existing) = self.principals.get(&principal) {
            return Err(if existing.retired {
                AdmissionError::PrincipalRetired(principal.to_string())
            } else {
                AdmissionError::PrincipalExists(principal.to_string())
            });
        }
        self.rotation.push_back(principal.clone());
        self.principals.insert(
            principal,
            PrincipalState {
                config,
                active: 0,
                resources: ResourceRequest::default(),
                queue: VecDeque::new(),
                deficit: 0,
                rejected: 0,
                total_wait: Duration::ZERO,
                max_wait: Duration::ZERO,
                retired: false,
            },
        );
        Ok(())
    }

    pub(super) fn enqueue(
        &mut self,
        principal: &AdmissionPrincipal,
        request: ResourceRequest,
        sender: oneshot::Sender<Result<AdmissionTicket, AdmissionError>>,
    ) -> Result<u64, AdmissionError> {
        let Some(principal_state) = self.principals.get(principal) else {
            self.rejected = self.rejected.saturating_add(1);
            return Err(AdmissionError::UnknownPrincipal(principal.to_string()));
        };
        if principal_state.retired {
            self.reject(principal);
            return Err(AdmissionError::PrincipalRetired(principal.to_string()));
        }
        if let Some(resource) = request.first_exceeded(self.engine_limits.resources) {
            self.reject(principal);
            return Err(AdmissionError::RequestExceedsLimit {
                layer: AdmissionLayer::Engine,
                resource,
            });
        }
        if let Some(resource) = request.first_exceeded(principal_state.config.limits.resources) {
            self.reject(principal);
            return Err(AdmissionError::RequestExceedsLimit {
                layer: AdmissionLayer::Principal,
                resource,
            });
        }
        if self.engine_queued >= self.engine_limits.max_queued {
            self.reject(principal);
            return Err(AdmissionError::QueueFull {
                layer: AdmissionLayer::Engine,
            });
        }
        if principal_state.queue.len() >= principal_state.config.limits.max_queued {
            self.reject(principal);
            return Err(AdmissionError::QueueFull {
                layer: AdmissionLayer::Principal,
            });
        }
        let id = self.next_id;
        self.next_id = self.next_id.wrapping_add(1).max(1);
        self.engine_queued = self.engine_queued.saturating_add(1);
        self.principals
            .get_mut(principal)
            .expect("principal checked above")
            .queue
            .push_back(QueueEntry {
                id,
                request,
                enqueued_at: Instant::now(),
                sender,
            });
        Ok(id)
    }

    pub(super) fn cancel(&mut self, principal: &AdmissionPrincipal, id: u64) -> bool {
        let Some(state) = self.principals.get_mut(principal) else {
            return false;
        };
        let Some(position) = state.queue.iter().position(|entry| entry.id == id) else {
            return false;
        };
        let entry = state
            .queue
            .remove(position)
            .expect("position checked above");
        self.engine_queued = self.engine_queued.saturating_sub(1);
        let _ = entry.sender.send(Err(AdmissionError::Cancelled));
        true
    }

    pub(super) fn update_principal(
        &mut self,
        principal: &AdmissionPrincipal,
        config: PrincipalAdmissionConfig,
    ) -> Result<(), AdmissionError> {
        config.validate()?;
        let Some(state) = self.principals.get_mut(principal) else {
            return Err(AdmissionError::UnknownPrincipal(principal.to_string()));
        };
        if state.retired {
            return Err(AdmissionError::PrincipalRetired(principal.to_string()));
        }
        state.config = config;
        state.deficit = state.deficit.min(config.weight);
        let mut retained = VecDeque::new();
        while let Some(entry) = state.queue.pop_front() {
            let fits = entry.request.fits_within(config.limits.resources)
                && retained.len() < config.limits.max_queued;
            if fits {
                retained.push_back(entry);
            } else {
                self.engine_queued = self.engine_queued.saturating_sub(1);
                self.rejected = self.rejected.saturating_add(1);
                state.rejected = state.rejected.saturating_add(1);
                let _ = entry.sender.send(Err(AdmissionError::PrincipalUpdated));
            }
        }
        state.queue = retained;
        Ok(())
    }

    pub(super) fn remove_principal(
        &mut self,
        principal: &AdmissionPrincipal,
    ) -> Result<(), AdmissionError> {
        let Some(state) = self.principals.get_mut(principal) else {
            return Err(AdmissionError::UnknownPrincipal(principal.to_string()));
        };
        state.retired = true;
        self.rotation.retain(|candidate| candidate != principal);
        while let Some(entry) = state.queue.pop_front() {
            self.engine_queued = self.engine_queued.saturating_sub(1);
            let _ = entry.sender.send(Err(AdmissionError::PrincipalRemoved));
        }
        if state.active == 0 {
            self.principals.remove(principal);
        }
        Ok(())
    }

    pub(super) fn release(&mut self, principal: &AdmissionPrincipal, request: ResourceRequest) {
        self.engine_active = self.engine_active.saturating_sub(1);
        self.engine_resources.subtract(request);
        let mut remove = false;
        if let Some(state) = self.principals.get_mut(principal) {
            state.active = state.active.saturating_sub(1);
            state.resources.subtract(request);
            remove = state.retired && state.active == 0;
        }
        if remove {
            self.principals.remove(principal);
        }
    }

    pub(super) fn schedule(&mut self, inner: &Arc<AdmissionControllerInner>) {
        let mut no_progress = 0usize;
        while self.engine_active < self.engine_limits.max_running
            && no_progress < self.rotation.len()
        {
            let Some(principal) = self.rotation.pop_front() else {
                break;
            };
            self.rotation.push_back(principal.clone());
            let weight = self
                .principals
                .get(&principal)
                .map_or(1, |state| state.config.weight);
            if let Some(state) = self.principals.get_mut(&principal) {
                // Do not let an idle or resource-blocked principal bank future
                // priority merely because another enqueue triggered scheduling.
                let cap = weight;
                state.deficit = state.deficit.saturating_add(weight).min(cap);
                if state.queue.is_empty() {
                    state.deficit = 0;
                }
            }

            let mut admitted_here = 0usize;
            loop {
                if self.engine_active >= self.engine_limits.max_running {
                    break;
                }
                let Some(state) = self.principals.get(&principal) else {
                    break;
                };
                let Some(entry) = state.queue.front() else {
                    break;
                };
                if state.deficit == 0
                    || state.active >= state.config.limits.max_running
                    || !state
                        .resources
                        .can_add(entry.request, state.config.limits.resources)
                    || !self
                        .engine_resources
                        .can_add(entry.request, self.engine_limits.resources)
                {
                    break;
                }
                if entry.sender.is_closed() {
                    let state = self
                        .principals
                        .get_mut(&principal)
                        .expect("principal checked above");
                    state.queue.pop_front();
                    self.engine_queued = self.engine_queued.saturating_sub(1);
                    continue;
                }

                let entry = {
                    let state = self
                        .principals
                        .get_mut(&principal)
                        .expect("principal checked above");
                    state.deficit = state.deficit.saturating_sub(1);
                    state.active = state.active.saturating_add(1);
                    let entry = state.queue.pop_front().expect("front checked above");
                    state.resources.add(entry.request);
                    entry
                };
                self.engine_active = self.engine_active.saturating_add(1);
                self.engine_queued = self.engine_queued.saturating_sub(1);
                self.engine_resources.add(entry.request);
                let wait = entry.enqueued_at.elapsed();
                let ticket =
                    AdmissionTicket::new(Arc::clone(inner), principal.clone(), entry.request);
                match entry.sender.send(Ok(ticket)) {
                    Ok(()) => {
                        self.record_wait(&principal, wait);
                        admitted_here = admitted_here.saturating_add(1);
                    }
                    Err(Ok(mut ticket)) => {
                        ticket.disarm();
                        self.release(&principal, entry.request);
                    }
                    Err(Err(_)) => unreachable!("scheduler sends an admission ticket"),
                }
            }
            if admitted_here == 0 {
                no_progress = no_progress.saturating_add(1);
            } else {
                no_progress = 0;
            }
        }
    }

    pub(super) fn snapshot(&self) -> AdmissionSnapshot {
        let now = Instant::now();
        AdmissionSnapshot {
            active: self.engine_active,
            queued: self.engine_queued,
            rejected: self.rejected,
            resources: self.engine_resources,
            total_wait: self.total_wait,
            max_wait: self.max_wait,
            oldest_queued_wait: self
                .principals
                .values()
                .filter_map(|state| state.queue.front())
                .map(|entry| now.saturating_duration_since(entry.enqueued_at))
                .max()
                .unwrap_or(Duration::ZERO),
            principals: self
                .principals
                .iter()
                .map(|(principal, state)| PrincipalAdmissionSnapshot {
                    principal: principal.clone(),
                    active: state.active,
                    queued: state.queue.len(),
                    rejected: state.rejected,
                    weight: state.config.weight,
                    retired: state.retired,
                    resources: state.resources,
                    total_wait: state.total_wait,
                    max_wait: state.max_wait,
                    oldest_queued_wait: state
                        .queue
                        .front()
                        .map(|entry| now.saturating_duration_since(entry.enqueued_at))
                        .unwrap_or(Duration::ZERO),
                })
                .collect(),
        }
    }

    fn reject(&mut self, principal: &AdmissionPrincipal) {
        self.rejected = self.rejected.saturating_add(1);
        if let Some(state) = self.principals.get_mut(principal) {
            state.rejected = state.rejected.saturating_add(1);
        }
    }

    fn record_wait(&mut self, principal: &AdmissionPrincipal, wait: Duration) {
        self.total_wait = self.total_wait.saturating_add(wait);
        self.max_wait = self.max_wait.max(wait);
        if let Some(state) = self.principals.get_mut(principal) {
            state.total_wait = state.total_wait.saturating_add(wait);
            state.max_wait = state.max_wait.max(wait);
        }
    }
}
