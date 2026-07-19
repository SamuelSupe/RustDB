use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use parking_lot::{Mutex, MutexGuard, RwLock};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::{
    journal::PersistedQuery,
    request::{now_ms, terminal_error},
};
use crate::RetryClass;
use crate::http_shell::{
    error::ErrorBody,
    result_store::StoredResult,
    security::QueryOwner,
    types::{HttpQueryMetrics, QueryState, QueryStatusResponse},
};

pub(crate) struct QueryRecord {
    pub(super) id: String,
    pub(super) owner: QueryOwner,
    pub(super) request_hash: String,
    pub(super) scoped_idempotency_digest: String,
    pub(super) created_at_ms: u64,
    pub(super) cancel: CancellationToken,
    pub(super) state: RwLock<RecordState>,
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
            state: RwLock::new(RecordState {
                phase: QueryState::Queued,
                started_at_ms: None,
                finished_at_ms: None,
                error: None,
                metrics: None,
                result: None,
            }),
            transition: Mutex::new(()),
            deleting: AtomicBool::new(false),
        }
    }

    pub(super) fn from_persisted(query: PersistedQuery, result: Option<Arc<StoredResult>>) -> Self {
        Self {
            id: query.query_id,
            owner: query.owner,
            request_hash: query.request_hash,
            scoped_idempotency_digest: query.scoped_idempotency_digest,
            created_at_ms: query.created_at_ms,
            cancel: CancellationToken::new(),
            state: RwLock::new(RecordState {
                phase: query.state,
                started_at_ms: query.started_at_ms,
                finished_at_ms: query.finished_at_ms,
                error: query.error,
                metrics: query.metrics,
                result,
            }),
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
            result_available: state.result.is_some(),
        }
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
        state.result = Some(Arc::new(result));
    }

    pub(super) fn status(&self) -> QueryStatusResponse {
        let state = self.state.read();
        QueryStatusResponse {
            query_id: self.id.clone(),
            state: state.phase,
            created_at_ms: self.created_at_ms,
            started_at_ms: state.started_at_ms,
            finished_at_ms: state.finished_at_ms,
            error: state.error.clone(),
            metrics: state.metrics.clone(),
        }
    }

    pub(super) fn lock_transition(&self) -> MutexGuard<'_, ()> {
        self.transition.lock()
    }

    pub(super) fn cancel(&self) {
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
        }
    }

    pub(super) fn fail_stably(&self, code: &str, message: &str, retry: RetryClass) {
        self.cancel.cancel();
        let mut error = terminal_error(code, message, &self.id);
        error.retry = retry;
        let mut state = self.state.write();
        state.phase = QueryState::Failed;
        state.finished_at_ms = Some(now_ms());
        state.error = Some(error);
        state.result = None;
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
            QueryState::Succeeded | QueryState::Failed | QueryState::Cancelled
        )
    }

    pub(super) fn owner(&self) -> &QueryOwner {
        &self.owner
    }
}
