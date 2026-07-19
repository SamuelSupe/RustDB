use std::sync::atomic::{AtomicU64, Ordering};

use super::query::admission::AdmissionSnapshot;

/// Low-cardinality process metrics for the HTTP service.
#[derive(Default)]
pub(crate) struct HttpMetrics {
    requests: AtomicU64,
    auth_failures: AtomicU64,
    submitted: AtomicU64,
    rejected: AtomicU64,
    running: AtomicU64,
    running_peak: AtomicU64,
    succeeded: AtomicU64,
    failed: AtomicU64,
    cancelled: AtomicU64,
    result_bytes_served: AtomicU64,
    journal_recoveries: AtomicU64,
    scheduler_wait_ms: AtomicU64,
}

impl HttpMetrics {
    pub(crate) fn request(&self) {
        self.requests.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn auth_failure(&self) {
        self.auth_failures.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn query_submitted(&self) {
        self.submitted.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn query_rejected(&self) {
        self.rejected.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn query_started(&self) {
        let running = self.running.fetch_add(1, Ordering::AcqRel) + 1;
        self.running_peak.fetch_max(running, Ordering::AcqRel);
    }

    pub(crate) fn query_finished(&self, outcome: QueryOutcome, was_running: bool) {
        if was_running {
            let _ = self
                .running
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |running| {
                    Some(running.saturating_sub(1))
                });
        }
        match outcome {
            QueryOutcome::Succeeded => &self.succeeded,
            QueryOutcome::Failed => &self.failed,
            QueryOutcome::Cancelled => &self.cancelled,
        }
        .fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn result_bytes(&self, bytes: u64) {
        self.result_bytes_served.fetch_add(bytes, Ordering::Relaxed);
    }

    pub(crate) fn journal_recovered(&self, queries: u64) {
        self.journal_recoveries
            .fetch_add(queries, Ordering::Relaxed);
    }

    pub(crate) fn scheduler_wait(&self, millis: u64) {
        self.scheduler_wait_ms.fetch_add(millis, Ordering::Relaxed);
    }

    pub(crate) fn render_prometheus(&self, admission: &AdmissionSnapshot) -> String {
        let mut output = String::with_capacity(1_024);
        metric(&mut output, "rustdb_http_requests_total", &self.requests);
        metric(
            &mut output,
            "rustdb_http_auth_failures_total",
            &self.auth_failures,
        );
        metric(
            &mut output,
            "rustdb_http_queries_submitted_total",
            &self.submitted,
        );
        metric(
            &mut output,
            "rustdb_http_queries_rejected_total",
            &self.rejected,
        );
        metric(&mut output, "rustdb_http_queries_running", &self.running);
        metric(
            &mut output,
            "rustdb_http_queries_running_peak",
            &self.running_peak,
        );
        metric(
            &mut output,
            "rustdb_http_queries_succeeded_total",
            &self.succeeded,
        );
        metric(
            &mut output,
            "rustdb_http_queries_failed_total",
            &self.failed,
        );
        metric(
            &mut output,
            "rustdb_http_queries_cancelled_total",
            &self.cancelled,
        );
        metric(
            &mut output,
            "rustdb_http_result_bytes_served_total",
            &self.result_bytes_served,
        );
        metric(
            &mut output,
            "rustdb_http_journal_queries_recovered_total",
            &self.journal_recoveries,
        );
        metric(
            &mut output,
            "rustdb_http_scheduler_wait_milliseconds_total",
            &self.scheduler_wait_ms,
        );
        value_metric(
            &mut output,
            "rustdb_http_admission_active",
            admission.active,
        );
        value_metric(
            &mut output,
            "rustdb_http_admission_queued",
            admission.queued,
        );
        value_metric(
            &mut output,
            "rustdb_http_admission_rejected_total",
            admission.rejected,
        );
        value_metric(
            &mut output,
            "rustdb_http_admission_memory_bytes",
            admission.resources.memory_bytes,
        );
        value_metric(
            &mut output,
            "rustdb_http_admission_spill_bytes",
            admission.resources.spill_bytes,
        );
        value_metric(
            &mut output,
            "rustdb_http_admission_result_bytes",
            admission.resources.result_bytes,
        );
        value_metric(
            &mut output,
            "rustdb_http_admission_oldest_wait_milliseconds",
            u64::try_from(admission.oldest_queued_wait.as_millis()).unwrap_or(u64::MAX),
        );
        value_metric(
            &mut output,
            "rustdb_http_admission_max_wait_milliseconds",
            u64::try_from(admission.max_wait.as_millis()).unwrap_or(u64::MAX),
        );
        output
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum QueryOutcome {
    Succeeded,
    Failed,
    Cancelled,
}

fn metric(output: &mut String, name: &str, value: &AtomicU64) {
    value_metric(output, name, value.load(Ordering::Acquire));
}

fn value_metric(output: &mut String, name: &str, value: impl ToString) {
    output.push_str(name);
    output.push(' ');
    output.push_str(&value.to_string());
    output.push('\n');
}

#[cfg(test)]
mod tests {
    use super::{HttpMetrics, QueryOutcome};
    use crate::http_shell::query::admission::{AdmissionSnapshot, ResourceRequest};

    #[test]
    fn renders_stable_low_cardinality_metrics() {
        let metrics = HttpMetrics::default();
        metrics.request();
        metrics.query_submitted();
        metrics.query_started();
        metrics.result_bytes(42);
        metrics.query_finished(QueryOutcome::Succeeded, true);
        let rendered = metrics.render_prometheus(&AdmissionSnapshot {
            active: 0,
            queued: 0,
            rejected: 0,
            resources: ResourceRequest::default(),
            total_wait: std::time::Duration::ZERO,
            max_wait: std::time::Duration::ZERO,
            oldest_queued_wait: std::time::Duration::ZERO,
            principals: Vec::new(),
        });
        assert!(rendered.contains("rustdb_http_requests_total 1\n"));
        assert!(rendered.contains("rustdb_http_queries_running 0\n"));
        assert!(rendered.contains("rustdb_http_queries_succeeded_total 1\n"));
        assert!(rendered.contains("rustdb_http_result_bytes_served_total 42\n"));
        assert!(!rendered.contains("principal"));
        assert!(!rendered.contains("query_id"));
    }
}
