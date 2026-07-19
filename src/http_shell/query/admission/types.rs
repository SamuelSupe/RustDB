use std::{fmt, time::Duration};

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ResourceRequest {
    pub memory_bytes: u64,
    pub spill_bytes: u64,
    pub result_bytes: u64,
}

impl ResourceRequest {
    pub const fn new(memory_bytes: u64, spill_bytes: u64, result_bytes: u64) -> Self {
        Self {
            memory_bytes,
            spill_bytes,
            result_bytes,
        }
    }

    pub const fn unlimited() -> Self {
        Self::new(u64::MAX, u64::MAX, u64::MAX)
    }

    pub(crate) fn fits_within(self, limit: Self) -> bool {
        self.memory_bytes <= limit.memory_bytes
            && self.spill_bytes <= limit.spill_bytes
            && self.result_bytes <= limit.result_bytes
    }

    pub(super) fn can_add(self, request: Self, limit: Self) -> bool {
        request.memory_bytes <= limit.memory_bytes.saturating_sub(self.memory_bytes)
            && request.spill_bytes <= limit.spill_bytes.saturating_sub(self.spill_bytes)
            && request.result_bytes <= limit.result_bytes.saturating_sub(self.result_bytes)
    }

    pub(super) fn add(&mut self, request: Self) {
        self.memory_bytes = self.memory_bytes.saturating_add(request.memory_bytes);
        self.spill_bytes = self.spill_bytes.saturating_add(request.spill_bytes);
        self.result_bytes = self.result_bytes.saturating_add(request.result_bytes);
    }

    pub(super) fn subtract(&mut self, request: Self) {
        self.memory_bytes = self.memory_bytes.saturating_sub(request.memory_bytes);
        self.spill_bytes = self.spill_bytes.saturating_sub(request.spill_bytes);
        self.result_bytes = self.result_bytes.saturating_sub(request.result_bytes);
    }

    pub(super) fn first_exceeded(self, limit: Self) -> Option<ResourceKind> {
        if self.memory_bytes > limit.memory_bytes {
            Some(ResourceKind::Memory)
        } else if self.spill_bytes > limit.spill_bytes {
            Some(ResourceKind::Spill)
        } else if self.result_bytes > limit.result_bytes {
            Some(ResourceKind::Result)
        } else {
            None
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AdmissionLimits {
    pub max_running: usize,
    pub max_queued: usize,
    pub resources: ResourceRequest,
}

impl AdmissionLimits {
    pub const fn new(max_running: usize, max_queued: usize, resources: ResourceRequest) -> Self {
        Self {
            max_running,
            max_queued,
            resources,
        }
    }

    pub const fn unbounded_resources(max_running: usize, max_queued: usize) -> Self {
        Self::new(max_running, max_queued, ResourceRequest::unlimited())
    }

    pub(crate) fn validate(self) -> Result<(), AdmissionError> {
        if self.max_running == 0 {
            return Err(AdmissionError::InvalidConfiguration(
                "max_running must be greater than zero".into(),
            ));
        }
        if self.max_queued == 0 {
            return Err(AdmissionError::InvalidConfiguration(
                "max_queued must be greater than zero".into(),
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PrincipalAdmissionConfig {
    pub limits: AdmissionLimits,
    pub weight: u32,
}

impl PrincipalAdmissionConfig {
    pub const fn new(limits: AdmissionLimits) -> Self {
        Self { limits, weight: 1 }
    }

    pub const fn with_weight(mut self, weight: u32) -> Self {
        self.weight = weight;
        self
    }

    pub(crate) fn validate(self) -> Result<(), AdmissionError> {
        self.limits.validate()?;
        if self.weight == 0 || self.weight > 1_024 {
            return Err(AdmissionError::InvalidConfiguration(
                "principal weight must be between 1 and 1024".into(),
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct AdmissionPrincipal(String);

impl AdmissionPrincipal {
    pub fn new(value: impl Into<String>) -> Result<Self, AdmissionError> {
        let value = value.into();
        if value.is_empty() || value.len() > 256 || value.chars().any(char::is_control) {
            return Err(AdmissionError::InvalidConfiguration(
                "admission principal must contain 1 to 256 non-control bytes".into(),
            ));
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for AdmissionPrincipal {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AdmissionLayer {
    Engine,
    Principal,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ResourceKind {
    Memory,
    Spill,
    Result,
}

#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum AdmissionError {
    #[error("invalid admission configuration: {0}")]
    InvalidConfiguration(String),
    #[error("admission principal already exists: {0}")]
    PrincipalExists(String),
    #[error("admission principal is unknown: {0}")]
    UnknownPrincipal(String),
    #[error("admission principal is being removed: {0}")]
    PrincipalRetired(String),
    #[error("{layer:?} admission queue is full")]
    QueueFull { layer: AdmissionLayer },
    #[error("query {resource:?} request exceeds the {layer:?} admission limit")]
    RequestExceedsLimit {
        layer: AdmissionLayer,
        resource: ResourceKind,
    },
    #[error("queued admission was cancelled")]
    Cancelled,
    #[error("queued admission was rejected by a principal limit update")]
    PrincipalUpdated,
    #[error("queued admission was removed with its principal")]
    PrincipalRemoved,
    #[error("admission waiter closed before receiving a decision")]
    WaiterClosed,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdmissionSnapshot {
    pub active: usize,
    pub queued: usize,
    pub rejected: u64,
    pub resources: ResourceRequest,
    pub total_wait: Duration,
    pub max_wait: Duration,
    pub oldest_queued_wait: Duration,
    pub principals: Vec<PrincipalAdmissionSnapshot>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PrincipalAdmissionSnapshot {
    pub principal: AdmissionPrincipal,
    pub active: usize,
    pub queued: usize,
    pub rejected: u64,
    pub weight: u32,
    pub retired: bool,
    pub resources: ResourceRequest,
    pub total_wait: Duration,
    pub max_wait: Duration,
    pub oldest_queued_wait: Duration,
}
