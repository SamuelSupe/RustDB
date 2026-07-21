use std::{
    sync::{
        Arc,
        atomic::{AtomicU8, AtomicU64, Ordering},
    },
    time::Duration,
};

use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use super::{metrics::HttpMetrics, query::QueryManager};
use crate::{Result, RssGuardConfig, RssGuardDecision, RssGuardSnapshot, RssGuardian};

pub(super) struct RssPressure {
    decision: AtomicU8,
    retry_after_secs: AtomicU64,
}

impl RssPressure {
    pub(super) fn observe(&self, snapshot: RssGuardSnapshot) {
        self.decision
            .store(encode(snapshot.decision), Ordering::Release);
    }

    pub(super) fn decision(&self) -> RssGuardDecision {
        decode(self.decision.load(Ordering::Acquire))
    }

    pub(super) fn retry_after_secs(&self) -> u64 {
        self.retry_after_secs.load(Ordering::Acquire)
    }

    fn set_sample_interval(&self, interval: Duration) {
        let seconds = interval
            .as_secs()
            .saturating_add(u64::from(interval.subsec_nanos() != 0))
            .max(1);
        self.retry_after_secs.store(seconds, Ordering::Release);
    }
}

impl Default for RssPressure {
    fn default() -> Self {
        Self {
            decision: AtomicU8::new(encode(RssGuardDecision::Normal)),
            retry_after_secs: AtomicU64::new(1),
        }
    }
}

pub(super) fn spawn(
    config: RssGuardConfig,
    interval: Duration,
    pressure: Arc<RssPressure>,
    queries: QueryManager,
    metrics: Arc<HttpMetrics>,
    stop: CancellationToken,
) -> Result<JoinHandle<()>> {
    let mut guardian = RssGuardian::new(config)?;
    pressure.set_sample_interval(interval);
    Ok(tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                _ = stop.cancelled() => break,
                _ = ticker.tick() => match guardian.sample() {
                    Ok(snapshot) => apply_snapshot(&pressure, &queries, &metrics, snapshot),
                    Err(error) => tracing::error!(%error, "failed to sample process RSS"),
                },
            }
        }
    }))
}

fn apply_snapshot(
    pressure: &RssPressure,
    queries: &QueryManager,
    metrics: &HttpMetrics,
    snapshot: RssGuardSnapshot,
) {
    pressure.observe(snapshot);
    metrics.rss_observation(snapshot);
    if snapshot.decision == RssGuardDecision::CancelLargest
        && let Some(query_id) = queries.cancel_largest_for_rss_pressure()
    {
        metrics.rss_query_cancelled();
        tracing::warn!(
            %query_id,
            pressure_ratio = snapshot.pressure_ratio,
            process_rss_bytes = snapshot.process_rss_bytes,
            effective_limit_bytes = snapshot.effective_limit_bytes,
            "cancelled the largest live query under critical RSS pressure"
        );
    }
}

const fn encode(decision: RssGuardDecision) -> u8 {
    match decision {
        RssGuardDecision::Normal => 0,
        RssGuardDecision::Throttle => 1,
        RssGuardDecision::Reject => 2,
        RssGuardDecision::CancelLargest => 3,
    }
}

const fn decode(value: u8) -> RssGuardDecision {
    match value {
        1 => RssGuardDecision::Throttle,
        2 => RssGuardDecision::Reject,
        3 => RssGuardDecision::CancelLargest,
        _ => RssGuardDecision::Normal,
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::RssPressure;
    use crate::{RssGuardDecision, RssGuardian};

    #[test]
    fn injected_snapshot_updates_decision_without_a_system_probe() {
        let pressure = RssPressure::default();
        pressure.set_sample_interval(Duration::from_millis(1_001));
        pressure.observe(RssGuardian::default().assess(850, 1_000, None));

        assert_eq!(pressure.decision(), RssGuardDecision::Reject);
        assert_eq!(pressure.retry_after_secs(), 2);
    }
}
