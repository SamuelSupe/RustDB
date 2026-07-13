use std::{sync::Arc, time::Instant};

use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio_util::sync::CancellationToken;

use crate::{Error, Result, runtime::QueryContext};

const PERMIT_BYTES: usize = 64 << 10;
// A Grace worker keeps its build table while it streams probe output into the
// next pipeline. Bound all concurrent builds to half of query memory so the
// downstream operator can always make progress instead of waiting on memory
// retained by its own upstream producer.
const BUILD_BUDGET_DIVISOR: usize = 2;

#[derive(Clone)]
pub(super) struct BuildAdmission {
    semaphore: Arc<Semaphore>,
    budget_bytes: usize,
    permits: u32,
}

impl BuildAdmission {
    pub(super) fn new(query_limit: usize) -> Self {
        let budget_bytes = query_limit
            .checked_div(BUILD_BUDGET_DIVISOR)
            .unwrap_or(0)
            .max(1);
        let permits = budget_bytes.div_ceil(PERMIT_BYTES).min(u32::MAX as usize) as u32;
        Self {
            semaphore: Arc::new(Semaphore::new(permits as usize)),
            budget_bytes,
            permits,
        }
    }

    pub(super) async fn acquire(
        &self,
        estimated_bytes: usize,
        cancellation: &CancellationToken,
        context: &QueryContext,
    ) -> Result<BuildPermit> {
        self.acquire_permits(self.permits_for(estimated_bytes), cancellation, context)
            .await
    }

    pub(super) async fn acquire_exclusive(
        &self,
        cancellation: &CancellationToken,
        context: &QueryContext,
    ) -> Result<BuildPermit> {
        self.acquire_permits(self.permits, cancellation, context)
            .await
    }

    async fn acquire_permits(
        &self,
        permits: u32,
        cancellation: &CancellationToken,
        context: &QueryContext,
    ) -> Result<BuildPermit> {
        let started = Instant::now();
        let acquire = Arc::clone(&self.semaphore).acquire_many_owned(permits);
        let permit = tokio::select! {
            _ = cancellation.cancelled() => return Err(Error::Cancelled),
            _ = context.control.cancelled() => return Err(Error::Cancelled),
            permit = acquire => permit.map_err(|_| Error::Cancelled)?,
        };
        context.scheduler.record_wait(started.elapsed());
        let limit_bytes = if permits == self.permits {
            self.budget_bytes
        } else {
            (permits as usize)
                .saturating_mul(PERMIT_BYTES)
                .min(self.budget_bytes)
        };
        Ok(BuildPermit {
            _permit: permit,
            limit_bytes,
            exclusive: permits == self.permits,
        })
    }

    fn permits_for(&self, estimated_bytes: usize) -> u32 {
        estimated_bytes
            .max(1)
            .div_ceil(PERMIT_BYTES)
            .min(self.permits as usize) as u32
    }
}

pub(super) struct BuildPermit {
    _permit: OwnedSemaphorePermit,
    limit_bytes: usize,
    exclusive: bool,
}

impl BuildPermit {
    pub(super) fn limit_bytes(&self) -> usize {
        self.limit_bytes
    }

    pub(super) fn is_exclusive(&self) -> bool {
        self.exclusive
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tokio_util::sync::CancellationToken;

    use super::BuildAdmission;
    use crate::runtime::{MemoryPool, QueryContext};

    #[tokio::test]
    async fn weighted_permits_bound_builds_and_allow_exclusive_retry() {
        let directory = tempfile::tempdir().unwrap();
        let context =
            Arc::new(QueryContext::new(MemoryPool::new(64 << 20), directory.path()).unwrap());
        let admission = BuildAdmission::new(context.memory.limit());
        let cancellation = CancellationToken::new();

        let first = admission
            .acquire(16 << 20, &cancellation, &context)
            .await
            .unwrap();
        assert_eq!(first.limit_bytes(), 16 << 20);
        assert!(!first.is_exclusive());

        let second = admission
            .acquire(16 << 20, &cancellation, &context)
            .await
            .unwrap();
        assert_eq!(second.limit_bytes(), 16 << 20);
        drop((first, second));

        let exclusive = admission
            .acquire_exclusive(&cancellation, &context)
            .await
            .unwrap();
        assert_eq!(exclusive.limit_bytes(), 32 << 20);
        assert!(exclusive.is_exclusive());
    }
}
