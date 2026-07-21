use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use parking_lot::RwLock;
use tokio::sync::{Mutex, MutexGuard};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::{
    journal::PersistedQuery,
    request::{now_ms, terminal_error},
};
use crate::http_shell::{
    error::ErrorBody,
    result_store::{ResultSummary, StoredResult},
    security::QueryOwner,
    types::{HttpQueryMetrics, QueryState, QueryStatusResponse},
};
use crate::{QueryMetrics, RetryClass};

pub(crate) struct QueryRecord {
    pub(super) id: String,
    pub(super) owner: QueryOwner,
    pub(super) request_hash: String,
    pub(super) scoped_idempotency_digest: String,
    pub(super) created_at_ms: u64,
    pub(super) cancel: CancellationToken,
    cancel_requested: AtomicBool,
    interrupted: Arc<AtomicBool>,
    pub(super) state: RwLock<RecordState>,
    live_metrics: RwLock<Option<QueryMetrics>>,
    transition: Mutex<()>,
    deleting: AtomicBool,
}

pub(super) struct RecordState {
    pub(super) phase: QueryState,
    pub(super) started_at_ms: Option<u64>,
    pub(super) finished_at_ms: Option<u64>,
    pub(super) error: Option<ErrorBody>,
    pub(super) metrics: Option<HttpQueryMetrics>,
    pub(super) result: Option<Arc<StoredResult>>,
    pub(super) result_available: bool,
    pub(super) result_summary: Option<ResultSummary>,
}

pub(super) struct LiveMetricsRegistration<'a> {
    record: &'a QueryRecord,
}

impl Drop for LiveMetricsRegistration<'_> {
    fn drop(&mut self) {
        *self.record.live_metrics.write() = None;
    }
}

impl QueryRecord {
    pub(super) fn new(
        owner: QueryOwner,
        request_hash: String,
        scoped_idempotency_digest: String,
    ) -> Self {
        Self {
            id: Uuid::new_v4().simple().to_string(),
            owner,
            request_hash,
            scoped_idempotency_digest,
            created_at_ms: now_ms(),
            cancel: CancellationToken::new(),
            cancel_requested: AtomicBool::new(false),
            interrupted: Arc::new(AtomicBool::new(false)),
            state: RwLock::new(RecordState {
                phase: QueryState::Queued,
                started_at_ms: None,
                finished_at_ms: None,
                error: None,
                metrics: None,
                result: None,
                result_available: false,
                result_summary: None,
            }),
            live_metrics: RwLock::new(None),
            transition: Mutex::new(()),
            deleting: AtomicBool::new(false),
        }
    }

    pub(super) fn from_persisted(
        query: PersistedQuery,
        result: Option<Arc<StoredResult>>,
        result_summary: Option<ResultSummary>,
    ) -> Self {
        Self {
            id: query.query_id,
            owner: query.owner,
            request_hash: query.request_hash,
            scoped_idempotency_digest: query.scoped_idempotency_digest,
            created_at_ms: query.created_at_ms,
            cancel: CancellationToken::new(),
            cancel_requested: AtomicBool::new(!matches!(
                query.state,
                QueryState::Queued | QueryState::Running
            )),
            interrupted: Arc::new(AtomicBool::new(query.state == QueryState::Interrupted)),
            state: RwLock::new(RecordState {
                phase: query.state,
                started_at_ms: query.started_at_ms,
                finished_at_ms: query.finished_at_ms,
                error: query.error,
                metrics: query.metrics,
                result,
                result_available: query.result_available,
                result_summary,
            }),
            live_metrics: RwLock::new(None),
            transition: Mutex::new(()),
            deleting: AtomicBool::new(false),
        }
    }

    pub(super) fn persisted(&self) -> PersistedQuery {
        let state = self.state.read();
        PersistedQuery {
            query_id: self.id.clone(),
            owner: self.owner.clone(),
            request_hash: self.request_hash.clone(),
            scoped_idempotency_digest: self.scoped_idempotency_digest.clone(),
            state: state.phase,
            created_at_ms: self.created_at_ms,
            started_at_ms: state.started_at_ms,
            finished_at_ms: state.finished_at_ms,
            error: state.error.clone(),
            metrics: state.metrics.clone(),
            result_available: state.result_available,
        }
    }

    pub(super) fn register_live_metrics(
        &self,
        metrics: QueryMetrics,
    ) -> LiveMetricsRegistration<'_> {
        let previous = self.live_metrics.write().replace(metrics);
        debug_assert!(
            previous.is_none(),
            "query metrics registered more than once"
        );
        LiveMetricsRegistration { record: self }
    }

    pub(super) fn current_memory_bytes(&self) -> Option<u64> {
        self.live_metrics
            .read()
            .as_ref()
            .map(|metrics| metrics.snapshot().current_memory_bytes)
    }

    pub(super) fn running_and_cancellable(&self) -> bool {
        !self.cancel_requested.load(Ordering::Acquire)
            && self.state.read().phase == QueryState::Running
    }

    pub(super) fn request_pressure_cancel(&self) -> bool {
        if self.state.read().phase != QueryState::Running
            || self
                .cancel_requested
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                .is_err()
        {
            return false;
        }
        self.cancel.cancel();
        true
    }

    /// Builds the durable success record without making success observable to
    /// status/result readers. The caller must persist it before publishing.
    pub(super) fn pending_success(
        &self,
        finished_at_ms: u64,
        metrics: HttpQueryMetrics,
    ) -> PersistedQuery {
        let mut value = self.persisted();
        value.state = QueryState::Succeeded;
        value.finished_at_ms = Some(finished_at_ms);
        value.error = None;
        value.metrics = Some(metrics);
        value.result_available = true;
        value
    }

    pub(super) fn publish_success(
        &self,
        finished_at_ms: u64,
        metrics: HttpQueryMetrics,
        result: StoredResult,
    ) {
        let mut state = self.state.write();
        debug_assert_eq!(state.phase, QueryState::Running);
        state.phase = QueryState::Succeeded;
        state.finished_at_ms = Some(finished_at_ms);
        state.error = None;
        state.metrics = Some(metrics);
        state.result_summary = Some(result.summary());
        state.result_available = true;
        state.result = Some(Arc::new(result));
    }

    pub(super) fn publish_interrupted_result(&self, result: StoredResult) -> bool {
        let mut state = self.state.write();
        if state.phase != QueryState::Interrupted {
            return false;
        }
        state.result_summary = Some(result.summary());
        state.result_available = true;
        state.result = Some(Arc::new(result));
        true
    }

    pub(super) fn status(&self) -> QueryStatusResponse {
        self.status_with_ttl_option(None)
    }

    pub(super) fn status_with_ttl(&self, ttl: Duration) -> QueryStatusResponse {
        self.status_with_ttl_option(Some(ttl))
    }

    fn status_with_ttl_option(&self, ttl: Option<Duration>) -> QueryStatusResponse {
        let state = self.state.read();
        let summary = state
            .result_summary
            .or_else(|| state.result.as_ref().map(|result| result.summary()));
        QueryStatusResponse {
            query_id: self.id.clone(),
            state: state.phase,
            created_at_ms: self.created_at_ms,
            started_at_ms: state.started_at_ms,
            finished_at_ms: state.finished_at_ms,
            error: state.error.clone(),
            metrics: state.metrics.clone(),
            result_available: state.result_available,
            result_expires_at_ms: ttl.and_then(|ttl| {
                summary.and_then(|summary| {
                    summary
                        .updated_at_ms
                        .checked_add(u64::try_from(ttl.as_millis()).unwrap_or(u64::MAX))
                })
            }),
            result_rows: summary.map(|value| value.rows),
            result_bytes: summary.map(|value| value.bytes),
            result_batches: summary.map(|value| value.batches),
        }
    }

    pub(super) async fn lock_transition(&self) -> MutexGuard<'_, ()> {
        self.transition.lock().await
    }

    #[cfg(test)]
    pub(super) fn blocking_lock_transition(&self) -> MutexGuard<'_, ()> {
        self.transition.blocking_lock()
    }

    pub(super) fn cancel(&self) {
        self.cancel_requested.store(true, Ordering::Release);
        self.cancel.cancel();
        let mut state = self.state.write();
        if state.phase == QueryState::Queued {
            state.phase = QueryState::Cancelled;
            state.finished_at_ms = Some(now_ms());
            state.error = Some(terminal_error(
                "query.cancelled",
                "query was cancelled",
                &self.id,
            ));
            state.result_available = false;
            state.result_summary = None;
        }
    }

    pub(super) fn interrupt(&self) -> bool {
        self.signal_interrupt();
        let mut state = self.state.write();
        if !matches!(state.phase, QueryState::Queued | QueryState::Running) {
            return false;
        }
        state.phase = QueryState::Interrupted;
        state.finished_at_ms = Some(now_ms());
        state.error = Some(terminal_error(
            "query.interrupted",
            "query was interrupted while the server was shutting down",
            &self.id,
        ));
        true
    }

    pub(super) fn interrupted_flag(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.interrupted)
    }

    pub(super) fn interruption_requested(&self) -> bool {
        self.interrupted.load(Ordering::Acquire)
    }

    pub(super) fn signal_interrupt(&self) {
        self.cancel_requested.store(true, Ordering::Release);
        self.interrupted.store(true, Ordering::Release);
        self.cancel.cancel();
    }

    pub(super) fn fail_stably(&self, code: &str, message: &str, retry: RetryClass) {
        self.cancel_requested.store(true, Ordering::Release);
        self.cancel.cancel();
        let mut error = terminal_error(code, message, &self.id);
        error.retry = retry;
        let mut state = self.state.write();
        state.phase = QueryState::Failed;
        state.finished_at_ms = Some(now_ms());
        state.error = Some(error);
        state.result = None;
        state.result_available = false;
        state.result_summary = None;
    }

    pub(super) fn begin_delete(&self) -> bool {
        self.deleting
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }

    pub(super) fn restore_access(&self) {
        self.deleting.store(false, Ordering::Release);
    }

    pub(super) fn deleting(&self) -> bool {
        self.deleting.load(Ordering::Acquire)
    }

    pub(super) fn terminal(&self) -> bool {
        matches!(
            self.state.read().phase,
            QueryState::Succeeded
                | QueryState::Failed
                | QueryState::Cancelled
                | QueryState::Interrupted
        )
    }

    pub(super) fn owner(&self) -> &QueryOwner {
        &self.owner
    }
}
