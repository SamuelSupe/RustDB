use crate::{EngineConfig, Error, Result};

use super::admission::{AdmissionLimits, PrincipalAdmissionConfig, ResourceRequest};
use crate::http_shell::result_store::ResultStoreConfig;

const MIB: u64 = 1024 * 1024;
const GIB: u64 = 1024 * MIB;

/// Layered limits used by the background HTTP query scheduler.
///
/// Resource values are logical admission reservations. Memory is also enforced
/// by a per-query engine pool, while Spill and result bytes remain enforced by
/// the engine and result store hard limits.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct QueryManagerConfig {
    pub max_running: usize,
    pub max_queued: usize,
    pub max_query_time: std::time::Duration,
    pub query_memory_limit_bytes: u64,
    pub query_spill_limit_bytes: u64,
    pub query_result_limit_bytes: u64,
    pub principal_max_running: usize,
    pub principal_max_queued: usize,
    pub principal_memory_limit_bytes: u64,
    pub principal_spill_limit_bytes: u64,
    pub principal_result_limit_bytes: u64,
    /// Weighted-fair scheduler share. Roles never change this value.
    pub principal_weight: u32,
}

impl Default for QueryManagerConfig {
    fn default() -> Self {
        Self {
            max_running: 1,
            max_queued: 64,
            max_query_time: std::time::Duration::from_secs(30 * 60),
            query_memory_limit_bytes: 512 * MIB,
            query_spill_limit_bytes: 4 * GIB,
            query_result_limit_bytes: 2 * GIB,
            principal_max_running: 4,
            principal_max_queued: 16,
            principal_memory_limit_bytes: 2 * GIB,
            principal_spill_limit_bytes: 16 * GIB,
            principal_result_limit_bytes: 8 * GIB,
            principal_weight: 1,
        }
    }
}

impl QueryManagerConfig {
    pub(super) fn validate(
        &self,
        engine: &EngineConfig,
        results: &ResultStoreConfig,
    ) -> Result<()> {
        self.admission_limits(engine, results)
            .validate()
            .map_err(admission_config)?;
        self.principal_config()
            .validate()
            .map_err(admission_config)?;
        if self.max_query_time.is_zero() {
            return Err(Error::InvalidArgument(
                "HTTP query timeout must be positive".into(),
            ));
        }
        let request = self.query_resources();
        if [
            request.memory_bytes,
            request.spill_bytes,
            request.result_bytes,
            self.principal_memory_limit_bytes,
            self.principal_spill_limit_bytes,
            self.principal_result_limit_bytes,
        ]
        .contains(&0)
        {
            return Err(Error::InvalidArgument(
                "HTTP query and principal resource limits must be positive".into(),
            ));
        }
        if !request.fits_within(self.principal_resources()) {
            return Err(Error::InvalidArgument(
                "HTTP per-query resource limits must fit within per-principal limits".into(),
            ));
        }
        if self.query_memory_limit_bytes > u64::try_from(engine.memory_limit).unwrap_or(u64::MAX) {
            return Err(Error::InvalidArgument(
                "server.query_memory_limit must not exceed engine.memory_limit".into(),
            ));
        }
        if let Some(limit) = engine.spill.query_limit_bytes
            && self.query_spill_limit_bytes > limit
        {
            return Err(Error::InvalidArgument(
                "server.query_spill_limit must not exceed engine.spill.query_limit_bytes".into(),
            ));
        }
        if let Some(limit) = results.query_limit_bytes
            && self.query_result_limit_bytes > limit
        {
            return Err(Error::InvalidArgument(
                "server.query_result_limit must not exceed server.result_query_limit".into(),
            ));
        }
        if let Some(limit) = results.global_limit_bytes
            && self.query_result_limit_bytes > limit
        {
            return Err(Error::InvalidArgument(
                "server.query_result_limit must not exceed server.result_global_limit".into(),
            ));
        }
        usize::try_from(self.query_memory_limit_bytes).map_err(|_| {
            Error::InvalidArgument("server.query_memory_limit is too large for this host".into())
        })?;
        Ok(())
    }

    pub(super) fn engine_limits(&self) -> AdmissionLimits {
        let running = u64::try_from(self.max_running).unwrap_or(u64::MAX);
        AdmissionLimits::new(
            self.max_running,
            self.max_queued,
            ResourceRequest::new(
                self.query_memory_limit_bytes.saturating_mul(running),
                self.query_spill_limit_bytes.saturating_mul(running),
                self.query_result_limit_bytes.saturating_mul(running),
            ),
        )
    }

    pub(super) fn admission_limits(
        &self,
        engine: &EngineConfig,
        results: &ResultStoreConfig,
    ) -> AdmissionLimits {
        let requested = self.engine_limits();
        AdmissionLimits::new(
            requested.max_running,
            requested.max_queued,
            ResourceRequest::new(
                requested
                    .resources
                    .memory_bytes
                    .min(u64::try_from(engine.memory_limit).unwrap_or(u64::MAX)),
                requested.resources.spill_bytes.min(
                    engine
                        .spill
                        .engine_limit_bytes
                        .unwrap_or(requested.resources.spill_bytes),
                ),
                requested.resources.result_bytes.min(
                    results
                        .global_limit_bytes
                        .unwrap_or(requested.resources.result_bytes),
                ),
            ),
        )
    }

    pub(super) fn principal_config(&self) -> PrincipalAdmissionConfig {
        PrincipalAdmissionConfig::new(AdmissionLimits::new(
            self.principal_max_running,
            self.principal_max_queued,
            self.principal_resources(),
        ))
        .with_weight(self.principal_weight)
    }

    pub(super) const fn query_resources(&self) -> ResourceRequest {
        ResourceRequest::new(
            self.query_memory_limit_bytes,
            self.query_spill_limit_bytes,
            self.query_result_limit_bytes,
        )
    }

    fn principal_resources(&self) -> ResourceRequest {
        ResourceRequest::new(
            self.principal_memory_limit_bytes,
            self.principal_spill_limit_bytes,
            self.principal_result_limit_bytes,
        )
    }
}

fn admission_config(error: super::admission::AdmissionError) -> Error {
    Error::InvalidArgument(error.to_string())
}
