use std::{sync::Arc, time::Duration};

use super::record::QueryRecord;
use crate::http_shell::{
    metrics::{HttpMetrics, QueryOutcome},
    security::{AuditEvent, AuditKind, AuditLog},
    types::QueryState,
};

#[derive(Clone)]
pub(super) struct QueryObserver {
    metrics: Arc<HttpMetrics>,
    audit: AuditLog,
}

impl QueryObserver {
    pub(super) fn new(metrics: Arc<HttpMetrics>, audit: AuditLog) -> Self {
        Self { metrics, audit }
    }

    pub(super) fn recovered(&self, count: usize) {
        self.metrics
            .journal_recovered(u64::try_from(count).unwrap_or(u64::MAX));
    }

    pub(super) fn started(&self, scheduler_wait: Duration) {
        self.metrics.query_started();
        self.metrics
            .scheduler_wait(u64::try_from(scheduler_wait.as_millis()).unwrap_or(u64::MAX));
    }

    pub(super) fn terminal(&self, record: &QueryRecord, was_running: bool) {
        let status = record.status();
        let (outcome, text) = match status.state {
            QueryState::Succeeded => (QueryOutcome::Succeeded, "succeeded"),
            QueryState::Cancelled => (QueryOutcome::Cancelled, "cancelled"),
            QueryState::Failed => (QueryOutcome::Failed, "failed"),
            QueryState::Queued | QueryState::Running => return,
        };
        self.metrics.query_finished(outcome, was_running);
        if let Err(error) = self.audit.record(AuditEvent {
            kind: AuditKind::QueryFinished,
            principal_id: Some(record.owner.audit_id()),
            query_id: Some(&record.id),
            request_id: None,
            sql_fingerprint: None,
            outcome: text,
        }) {
            tracing::error!(%error, query_id = %record.id, "failed to persist terminal query audit event");
        }
    }
}
