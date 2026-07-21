//! Process RSS pressure detection.
//!
//! The guardian deliberately reports policy decisions without cancelling work
//! itself. Service and embedded callers can therefore apply the same pressure
//! policy without coupling this module to a query manager.

mod probe;

use crate::{Error, Result};

use probe::SystemMemoryProbe;

#[cfg(test)]
mod tests;

/// Watermarks used to translate process RSS into an admission decision.
#[derive(Clone, Copy, Debug, PartialEq)]
#[non_exhaustive]
pub struct RssGuardConfig {
    pub warning_ratio: f64,
    pub high_ratio: f64,
    pub critical_ratio: f64,
}

impl RssGuardConfig {
    pub fn validate(&self) -> Result<()> {
        let valid_ratio = |value: f64| value.is_finite() && (0.0..=1.0).contains(&value);
        if !valid_ratio(self.warning_ratio)
            || !valid_ratio(self.high_ratio)
            || !valid_ratio(self.critical_ratio)
            || self.warning_ratio == 0.0
            || self.warning_ratio >= self.high_ratio
            || self.high_ratio >= self.critical_ratio
        {
            return Err(Error::InvalidArgument(
                "RSS guard ratios must be finite and satisfy 0 < warning < high < critical <= 1"
                    .to_owned(),
            ));
        }
        Ok(())
    }
}

impl Default for RssGuardConfig {
    fn default() -> Self {
        Self {
            warning_ratio: 0.70,
            high_ratio: 0.80,
            critical_ratio: 0.90,
        }
    }
}

/// A policy signal for the caller. The guardian never mutates query state.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum RssGuardDecision {
    Normal,
    Throttle,
    Reject,
    CancelLargest,
}

/// One process-memory observation and its policy decision.
#[derive(Clone, Copy, Debug, PartialEq)]
#[non_exhaustive]
pub struct RssGuardSnapshot {
    pub process_rss_bytes: u64,
    pub physical_memory_bytes: u64,
    pub cgroup_limit_bytes: Option<u64>,
    pub effective_limit_bytes: u64,
    pub pressure_ratio: f64,
    pub decision: RssGuardDecision,
}

/// Samples the current process and classifies its memory pressure.
#[derive(Debug)]
pub struct RssGuardian {
    config: RssGuardConfig,
    probe: SystemMemoryProbe,
}

impl RssGuardian {
    pub fn new(config: RssGuardConfig) -> Result<Self> {
        config.validate()?;
        Ok(Self {
            config,
            probe: SystemMemoryProbe::new(),
        })
    }

    pub fn config(&self) -> RssGuardConfig {
        self.config
    }

    /// Reads process RSS, physical memory and the active Linux cgroup limit.
    /// Missing or malformed cgroup files safely fall back to physical memory.
    pub fn sample(&mut self) -> Result<RssGuardSnapshot> {
        let observation = self.probe.sample()?;
        Ok(self.assess(
            observation.process_rss_bytes,
            observation.physical_memory_bytes,
            observation.cgroup_limit_bytes,
        ))
    }

    /// Pure decision function for callers that already collect process data.
    pub fn assess(
        &self,
        process_rss_bytes: u64,
        physical_memory_bytes: u64,
        cgroup_limit_bytes: Option<u64>,
    ) -> RssGuardSnapshot {
        let effective_limit_bytes = cgroup_limit_bytes
            .map(|limit| limit.min(physical_memory_bytes))
            .unwrap_or(physical_memory_bytes);
        let pressure_ratio = if effective_limit_bytes == 0 {
            1.0
        } else {
            process_rss_bytes as f64 / effective_limit_bytes as f64
        };
        let decision = if pressure_ratio >= self.config.critical_ratio {
            RssGuardDecision::CancelLargest
        } else if pressure_ratio >= self.config.high_ratio {
            RssGuardDecision::Reject
        } else if pressure_ratio >= self.config.warning_ratio {
            RssGuardDecision::Throttle
        } else {
            RssGuardDecision::Normal
        };

        RssGuardSnapshot {
            process_rss_bytes,
            physical_memory_bytes,
            cgroup_limit_bytes,
            effective_limit_bytes,
            pressure_ratio,
            decision,
        }
    }
}

impl Default for RssGuardian {
    fn default() -> Self {
        Self::new(RssGuardConfig::default()).expect("default RSS guard config is valid")
    }
}
