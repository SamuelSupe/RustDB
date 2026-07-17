use std::{
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};

use super::QueryMetrics;

#[derive(Debug, Default)]
pub(super) struct PhaseMetrics {
    query_admission_wait_ns: AtomicU64,
    sql_parse_ns: AtomicU64,
    table_function_prepare_ns: AtomicU64,
    bind_ns: AtomicU64,
    provider_prepare_ns: AtomicU64,
    optimize_ns: AtomicU64,
    native_verification_ns: AtomicU64,
    native_full_verification_segments: AtomicU64,
}

pub(super) struct PhaseSnapshot {
    pub(super) query_admission_wait: Duration,
    pub(super) sql_parse_time: Duration,
    pub(super) table_function_prepare_time: Duration,
    pub(super) bind_time: Duration,
    pub(super) provider_prepare_time: Duration,
    pub(super) optimize_time: Duration,
    pub(super) native_verification_time: Duration,
    pub(super) native_full_verification_segments: u64,
}

impl PhaseMetrics {
    pub(super) fn snapshot(&self) -> PhaseSnapshot {
        PhaseSnapshot {
            query_admission_wait: load_duration(&self.query_admission_wait_ns),
            sql_parse_time: load_duration(&self.sql_parse_ns),
            table_function_prepare_time: load_duration(&self.table_function_prepare_ns),
            bind_time: load_duration(&self.bind_ns),
            provider_prepare_time: load_duration(&self.provider_prepare_ns),
            optimize_time: load_duration(&self.optimize_ns),
            native_verification_time: load_duration(&self.native_verification_ns),
            native_full_verification_segments: self
                .native_full_verification_segments
                .load(Ordering::Relaxed),
        }
    }
}

impl QueryMetrics {
    pub(crate) fn record_query_admission_wait(&self, elapsed: Duration) {
        add_duration(&self.inner.phase.query_admission_wait_ns, elapsed);
    }

    pub(crate) fn record_sql_parse_time(&self, elapsed: Duration) {
        add_duration(&self.inner.phase.sql_parse_ns, elapsed);
    }

    pub(crate) fn record_table_function_prepare_time(&self, elapsed: Duration) {
        add_duration(&self.inner.phase.table_function_prepare_ns, elapsed);
    }

    pub(crate) fn record_bind_time(&self, elapsed: Duration) {
        add_duration(&self.inner.phase.bind_ns, elapsed);
    }

    pub(crate) fn record_provider_prepare_time(&self, elapsed: Duration) {
        add_duration(&self.inner.phase.provider_prepare_ns, elapsed);
    }

    pub(crate) fn record_optimize_time(&self, elapsed: Duration) {
        add_duration(&self.inner.phase.optimize_ns, elapsed);
    }

    pub(crate) fn record_native_verification_time(&self, elapsed: Duration) {
        add_duration(&self.inner.phase.native_verification_ns, elapsed);
    }

    pub(crate) fn add_native_full_verification_segments(&self, segments: usize) {
        add(
            &self.inner.phase.native_full_verification_segments,
            u64::try_from(segments).unwrap_or(u64::MAX),
        );
    }
}

fn add_duration(counter: &AtomicU64, elapsed: Duration) {
    let nanos = u64::try_from(elapsed.as_nanos()).unwrap_or(u64::MAX);
    add(counter, nanos);
}

fn add(counter: &AtomicU64, value: u64) {
    if value == 0 {
        return;
    }
    let _ = counter.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
        Some(current.saturating_add(value))
    });
}

fn load_duration(counter: &AtomicU64) -> Duration {
    Duration::from_nanos(counter.load(Ordering::Relaxed))
}
