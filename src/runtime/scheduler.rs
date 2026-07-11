use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use super::QueryMetrics;

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

    pub(crate) fn configure(&self, lanes: usize) {
        self.inner
            .configured_lanes
            .store(lanes.max(1), Ordering::Release);
    }

    pub(crate) fn configured_lanes(&self) -> usize {
        self.inner.configured_lanes.load(Ordering::Acquire).max(1)
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
    use super::QueryScheduler;
    use crate::runtime::QueryMetrics;

    #[test]
    fn caps_lanes_and_records_peak_activity() {
        let metrics = QueryMetrics::new();
        let scheduler = QueryScheduler::new(metrics.clone());
        scheduler.configure(4);
        assert_eq!(scheduler.lanes_for(2), 2);

        let first = scheduler.enter_lane();
        let second = scheduler.enter_lane();
        assert_eq!(metrics.snapshot().peak_active_lanes, 2);
        drop((first, second));
    }
}
