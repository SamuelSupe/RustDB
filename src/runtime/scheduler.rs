use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use super::QueryMetrics;

/// Each active lane may retain one batch at every nested pipeline boundary.
/// Keeping at least 32 MiB per lane bounds that fan-out while leaving the
/// operators' existing spill headroom available for forward progress.
const MIN_MEMORY_PER_LANE: usize = 32 << 20;

/// Query-local lane policy and scheduler instrumentation.
#[derive(Clone, Debug)]
pub(crate) struct QueryScheduler {
    inner: Arc<Inner>,
}

#[derive(Debug)]
struct Inner {
    configured_lanes: AtomicUsize,
    active_lanes: AtomicUsize,
    metrics: QueryMetrics,
}

impl QueryScheduler {
    pub(crate) fn new(metrics: QueryMetrics) -> Self {
        Self {
            inner: Arc::new(Inner {
                configured_lanes: AtomicUsize::new(1),
                active_lanes: AtomicUsize::new(0),
                metrics,
            }),
        }
    }

    pub(crate) fn configure(&self, lanes: usize, memory_limit: usize) {
        self.inner
            .configured_lanes
            .store(memory_bounded_lanes(lanes, memory_limit), Ordering::Release);
    }

    #[cfg(test)]
    pub(crate) fn configure_unbounded(&self, lanes: usize) {
        self.inner
            .configured_lanes
            .store(lanes.max(1), Ordering::Release);
    }

    pub(crate) fn configured_lanes(&self) -> usize {
        self.inner.configured_lanes.load(Ordering::Acquire).max(1)
    }

    pub(crate) fn active_lanes(&self) -> usize {
        self.inner.active_lanes.load(Ordering::Acquire)
    }

    pub(crate) fn partitioning_lanes(&self) -> usize {
        // Partitioning runs synchronously in the current coordinator/worker.
        // When no lane guard is active, count that caller as one lane instead
        // of assuming every configured worker will retain a partition at once.
        self.active_lanes().max(1)
    }

    pub(crate) fn lanes_for(&self, task_count: usize) -> usize {
        self.configured_lanes().min(task_count).max(1)
    }

    pub(crate) fn enter_lane(&self) -> ActiveLane {
        let active = self.inner.active_lanes.fetch_add(1, Ordering::AcqRel) + 1;
        self.inner
            .metrics
            .observe_active_lanes(active.min(self.configured_lanes()));
        ActiveLane {
            scheduler: self.clone(),
        }
    }

    pub(crate) fn record_wait(&self, duration: Duration) {
        self.inner.metrics.record_scheduler_wait(duration);
    }
}

fn memory_bounded_lanes(requested: usize, memory_limit: usize) -> usize {
    let memory_lanes = memory_limit.checked_div(MIN_MEMORY_PER_LANE).unwrap_or(0);
    requested.max(1).min(memory_lanes.max(1))
}

pub(crate) struct ActiveLane {
    scheduler: QueryScheduler,
}

impl Drop for ActiveLane {
    fn drop(&mut self) {
        self.scheduler
            .inner
            .active_lanes
            .fetch_sub(1, Ordering::AcqRel);
    }
}

#[cfg(test)]
mod tests {
    use super::{QueryScheduler, memory_bounded_lanes};
    use crate::runtime::QueryMetrics;

    #[test]
    fn caps_lanes_and_records_peak_activity() {
        let metrics = QueryMetrics::new();
        let scheduler = QueryScheduler::new(metrics.clone());
        scheduler.configure(4, 128 << 20);
        assert_eq!(scheduler.lanes_for(2), 2);
        assert_eq!(scheduler.partitioning_lanes(), 1);

        let first = scheduler.enter_lane();
        assert_eq!(scheduler.partitioning_lanes(), 1);
        let second = scheduler.enter_lane();
        assert_eq!(scheduler.active_lanes(), 2);
        assert_eq!(scheduler.partitioning_lanes(), 2);
        assert_eq!(metrics.snapshot().peak_active_lanes, 2);
        drop((first, second));
        assert_eq!(scheduler.active_lanes(), 0);
        assert_eq!(scheduler.partitioning_lanes(), 1);
    }

    #[test]
    fn compute_lanes_are_bounded_by_query_memory() {
        assert_eq!(memory_bounded_lanes(18, 0), 1);
        assert_eq!(memory_bounded_lanes(18, (32 << 20) - 1), 1);
        assert_eq!(memory_bounded_lanes(18, 64 << 20), 2);
        assert_eq!(memory_bounded_lanes(18, 128 << 20), 4);
        assert_eq!(memory_bounded_lanes(8, 128 << 20), 4);
        assert_eq!(memory_bounded_lanes(3, 128 << 20), 3);
        assert_eq!(memory_bounded_lanes(18, 1 << 30), 18);
        assert_eq!(memory_bounded_lanes(0, 1 << 30), 1);
    }
}
