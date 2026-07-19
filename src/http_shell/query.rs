use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Instant,
};

use axum::http::StatusCode;
use parking_lot::{Mutex, RwLock};
use tokio::sync::{Notify, mpsc};
use tokio_util::sync::CancellationToken;

use crate::{Engine, Error, HttpReadOnlyPolicy, QueryMetricsSnapshot, Result};

use super::{
    error::HttpError,
    metrics::HttpMetrics,
    result_store::{ResultSnapshot, ResultStore, ResultStoreConfig, StoredResult},
    security::{AuditLog, AuthenticatedActor},
    types::{HttpQueryMetrics, QueryRequest, QueryState, QueryStatusResponse, SubmitResponse},
};

pub(super) mod admission;
mod config;
mod dispatch;
mod execution;
mod expiration;
mod journal;
mod lifecycle;
mod observer;
mod record;
mod recovery;
mod request;

pub use config::QueryManagerConfig;

use admission::{AdmissionController, AdmissionError, AdmissionPrincipal, AdmissionWaiter};
use journal::{DeleteReason, QueryJournal, QueryJournalConfig, scoped_idempotency_digest};
use lifecycle::{
    cancel_and_persist, delete_terminal_record, fail_record, log_terminal, persist_or_log,
    persist_terminal,
};
use observer::QueryObserver;
use record::QueryRecord;
use recovery::recover_records;
use request::{now_ms, request_hash, submit_response, terminal_error, validate_idempotency_key};

#[derive(Clone)]
pub(crate) struct QueryManager {
    inner: Arc<ManagerInner>,
    owner: Arc<()>,
}

struct ManagerInner {
    records: RwLock<HashMap<String, Arc<QueryRecord>>>,
    idempotency: Mutex<HashMap<String, IdempotencyRecord>>,
    sender: mpsc::Sender<Job>,
    shutdown: CancellationToken,
    store: Arc<ResultStore>,
    journal: Arc<QueryJournal>,
    admission: AdmissionController,
    observer: Option<QueryObserver>,
    config: QueryManagerConfig,
    active_tasks: AtomicUsize,
    tasks_idle: Notify,
    accepting: Mutex<bool>,
}

struct Job {
    record: Arc<QueryRecord>,
    request: QueryRequest,
    admission: AdmissionWaiter,
    queued_at: Instant,
}

struct IdempotencyRecord {
    request_hash: String,
    query_id: String,
}

struct ActiveTask {
    inner: Arc<ManagerInner>,
}

impl ActiveTask {
    fn start(inner: &Arc<ManagerInner>) -> Self {
        inner.active_tasks.fetch_add(1, Ordering::AcqRel);
        Self {
            inner: Arc::clone(inner),
        }
    }
}

impl Drop for ActiveTask {
    fn drop(&mut self) {
        if self.inner.active_tasks.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.inner.tasks_idle.notify_waiters();
        }
    }
}

impl QueryManager {
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn new(
        engine: Engine,
        config: QueryManagerConfig,
        result_config: ResultStoreConfig,
    ) -> Result<Self> {
        Self::build(engine, config, result_config, None)
    }

    pub(crate) fn new_observed(
        engine: Engine,
        config: QueryManagerConfig,
        result_config: ResultStoreConfig,
        metrics: Arc<HttpMetrics>,
        audit: AuditLog,
    ) -> Result<Self> {
        Self::build(
            engine,
            config,
            result_config,
            Some(QueryObserver::new(metrics, audit)),
        )
    }

    fn build(
        engine: Engine,
        config: QueryManagerConfig,
        mut result_config: ResultStoreConfig,
        observer: Option<QueryObserver>,
    ) -> Result<Self> {
        config.validate(engine.config(), &result_config)?;
        let admission =
            AdmissionController::new(config.admission_limits(engine.config(), &result_config))
                .map_err(|error| Error::InvalidArgument(error.to_string()))?;
        result_config.query_limit_bytes = Some(config.query_result_limit_bytes);
        result_config.global_limit_bytes =
            Some(result_config.global_limit_bytes.unwrap_or_else(|| {
                config
                    .query_result_limit_bytes
                    .saturating_mul(u64::try_from(config.max_running).unwrap_or(u64::MAX))
            }));
        let journal_directory = result_config.directory.join("query-journal");
        let store = Arc::new(ResultStore::open(result_config)?);
        let journal = QueryJournal::open(QueryJournalConfig::new(journal_directory))?;
        let (records, idempotency) = recover_records(&store, &journal);
        if let Some(observer) = &observer {
            observer.recovered(records.len());
        }
        let transport_capacity = config.max_queued.saturating_add(config.max_running);
        let (sender, receiver) = mpsc::channel(transport_capacity);
        let inner = Arc::new(ManagerInner {
            records: RwLock::new(records),
            idempotency: Mutex::new(idempotency),
            sender,
            shutdown: CancellationToken::new(),
            store,
            journal,
            admission,
            observer,
            config: config.clone(),
            active_tasks: AtomicUsize::new(0),
            tasks_idle: Notify::new(),
            accepting: Mutex::new(true),
        });
        dispatch::spawn(Arc::clone(&inner), engine, receiver);
        expiration::spawn(Arc::clone(&inner));
        Ok(Self {
            inner,
            owner: Arc::new(()),
        })
    }

    pub(crate) fn submit(
        &self,
        actor: &AuthenticatedActor,
        key: &str,
        request: QueryRequest,
        request_id: &str,
    ) -> std::result::Result<SubmitResponse, HttpError> {
        let accepting = self.inner.accepting.lock();
        if !*accepting {
            return Err(HttpError::new(
                StatusCode::SERVICE_UNAVAILABLE,
                "server.shutting_down",
                "the server is shutting down",
                request_id.to_owned(),
            ));
        }
        validate_idempotency_key(key, request_id)?;
        if request.sql.len() > 1024 * 1024 {
            return Err(HttpError::new(
                StatusCode::PAYLOAD_TOO_LARGE,
                "request.too_large",
                "SQL text exceeds 1 MiB",
                request_id.to_owned(),
            ));
        }
        request
            .timeout(self.inner.config.max_query_time)
            .map_err(|error| HttpError::from_engine(&error, request_id.to_owned()))?;
        request
            .parameter_values()
            .map_err(|error| HttpError::from_engine(&error, request_id.to_owned()))?;
        HttpReadOnlyPolicy::validate(&request.sql)
            .map_err(|error| HttpError::from_engine(&error, request_id.to_owned()))?;
        let request_hash = request_hash(&request)
            .map_err(|error| HttpError::from_engine(&error, request_id.to_owned()))?;
        let owner = actor.query_owner();
        let scoped_digest = scoped_idempotency_digest(&owner, key)
            .map_err(|error| HttpError::from_engine(&error, request_id.to_owned()))?;
        let mut idempotency = self.inner.idempotency.lock();
        if let Some(existing) = idempotency.get(&scoped_digest) {
            if existing.request_hash != request_hash {
                return Err(HttpError::new(
                    StatusCode::CONFLICT,
                    "idempotency.key_conflict",
                    "Idempotency-Key was already used for another request",
                    request_id.to_owned(),
                ));
            }
            if let Some(record) = self.inner.records.read().get(&existing.query_id) {
                return Ok(submit_response(record, true));
            }
            return Err(HttpError::new(
                StatusCode::CONFLICT,
                "query.state_conflict",
                "the previous query for this idempotency key is being deleted",
                request_id.to_owned(),
            )
            .retry_after(1));
        }
        let permit = self.inner.sender.try_reserve().map_err(|_| {
            HttpError::new(
                StatusCode::TOO_MANY_REQUESTS,
                crate::ErrorCode::AdmissionQueueFull.as_str(),
                "the query admission queue is full",
                request_id.to_owned(),
            )
            .retry_after(1)
        })?;
        let admission_principal = AdmissionPrincipal::new(owner.audit_id())
            .map_err(|error| admission_http_error(error, request_id))?;
        match self.inner.admission.register_principal(
            admission_principal.clone(),
            self.inner.config.principal_config(),
        ) {
            Ok(()) | Err(AdmissionError::PrincipalExists(_)) => {}
            Err(error) => return Err(admission_http_error(error, request_id)),
        }
        let admission = self
            .inner
            .admission
            .enqueue(&admission_principal, self.inner.config.query_resources())
            .map_err(|error| admission_http_error(error, request_id))?;
        let record = Arc::new(QueryRecord::new(
            owner,
            request_hash.clone(),
            scoped_digest.clone(),
        ));
        self.inner
            .journal
            .upsert(record.persisted())
            .map_err(|error| HttpError::from_engine(&error, request_id.to_owned()))?;
        let job = Job {
            record: Arc::clone(&record),
            request,
            admission,
            queued_at: Instant::now(),
        };
        idempotency.insert(
            scoped_digest,
            IdempotencyRecord {
                request_hash,
                query_id: record.id.clone(),
            },
        );
        self.inner
            .records
            .write()
            .insert(record.id.clone(), Arc::clone(&record));
        permit.send(job);
        drop(accepting);
        Ok(submit_response(&record, false))
    }

    pub(crate) fn status(
        &self,
        actor: &AuthenticatedActor,
        query_id: &str,
        request_id: &str,
    ) -> std::result::Result<QueryStatusResponse, HttpError> {
        let record = self.record(actor, query_id, request_id)?;
        Ok(record.status())
    }

    pub(crate) fn completed_result(
        &self,
        actor: &AuthenticatedActor,
        query_id: &str,
        request_id: &str,
    ) -> std::result::Result<Arc<StoredResult>, HttpError> {
        let record = self.record(actor, query_id, request_id)?;
        let state = record.state.read();
        match state.phase {
            QueryState::Succeeded => state.result.clone().ok_or_else(|| {
                HttpError::new(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "server.internal",
                    "completed query result is unavailable",
                    request_id.to_owned(),
                )
                .query(query_id)
            }),
            QueryState::Queued | QueryState::Running => Err(HttpError::new(
                StatusCode::CONFLICT,
                "query.not_complete",
                "query results are available only after successful completion",
                request_id.to_owned(),
            )
            .query(query_id)),
            QueryState::Failed | QueryState::Cancelled => Err(HttpError::new(
                StatusCode::CONFLICT,
                "query.no_result",
                "the query did not produce a result",
                request_id.to_owned(),
            )
            .query(query_id)),
        }
    }

    #[allow(dead_code)]
    pub(crate) fn result_snapshot(
        &self,
        actor: &AuthenticatedActor,
        query_id: &str,
        request_id: &str,
    ) -> std::result::Result<Option<ResultSnapshot>, HttpError> {
        let record = self.record(actor, query_id, request_id)?;
        let (phase, result) = {
            let state = record.state.read();
            if matches!(state.phase, QueryState::Failed | QueryState::Cancelled) {
                return Err(HttpError::new(
                    StatusCode::CONFLICT,
                    "query.no_result",
                    "the query did not produce a result",
                    request_id.to_owned(),
                )
                .query(query_id));
            }
            (state.phase, state.result.clone())
        };
        if let Some(result) = result {
            return result.snapshot().map(Some).map_err(|error| {
                HttpError::from_engine(&error, request_id.to_owned()).query(query_id)
            });
        }
        let mut snapshot = self.inner.store.snapshot(query_id).map_err(|error| {
            HttpError::from_engine(&error, request_id.to_owned()).query(query_id)
        })?;
        if phase != QueryState::Succeeded
            && let Some(snapshot) = &mut snapshot
        {
            snapshot.hide_completion();
        }
        Ok(snapshot)
    }

    pub(crate) fn cancel(
        &self,
        actor: &AuthenticatedActor,
        query_id: &str,
        request_id: &str,
    ) -> std::result::Result<QueryStatusResponse, HttpError> {
        let record = self.record(actor, query_id, request_id)?;
        cancel_and_persist(&record, &self.inner.journal).map_err(|error| {
            HttpError::from_engine(&error, request_id.to_owned()).query(query_id)
        })?;
        Ok(record.status())
    }

    pub(crate) fn delete(
        &self,
        actor: &AuthenticatedActor,
        query_id: &str,
        request_id: &str,
    ) -> std::result::Result<(), HttpError> {
        let Some(record) = self.inner.records.read().get(query_id).cloned() else {
            return Ok(());
        };
        if !actor.can_access_query(record.owner()) {
            return Ok(());
        }
        if matches!(
            record.state.read().phase,
            QueryState::Queued | QueryState::Running
        ) {
            return Err(HttpError::new(
                StatusCode::CONFLICT,
                "query.active",
                "cancel an active query before deleting it",
                request_id.to_owned(),
            )
            .query(query_id));
        }
        delete_terminal_record(&self.inner, query_id, &record, DeleteReason::Explicit)
            .map_err(|error| HttpError::from_engine(&error, request_id.to_owned()).query(query_id))
    }

    pub(crate) async fn shutdown(&self) -> Result<()> {
        self.begin_shutdown();
        let records = self
            .inner
            .records
            .read()
            .values()
            .cloned()
            .collect::<Vec<_>>();
        let mut failures = Vec::new();
        for record in &records {
            if !matches!(
                record.state.read().phase,
                QueryState::Queued | QueryState::Running
            ) {
                continue;
            }
            if let Err(error) = cancel_and_persist(record, &self.inner.journal) {
                failures.push(format!("{}: {error}", record.id));
            }
        }
        loop {
            let idle = self.inner.tasks_idle.notified();
            if self.inner.active_tasks.load(Ordering::Acquire) == 0 {
                break;
            }
            idle.await;
        }
        if failures.is_empty() {
            Ok(())
        } else {
            Err(Error::Execution(format!(
                "failed to persist cancelled HTTP queries during shutdown: {}",
                failures.join("; ")
            )))
        }
    }

    pub(crate) fn begin_shutdown(&self) {
        *self.inner.accepting.lock() = false;
        self.inner.shutdown.cancel();
    }

    pub(crate) fn admission_snapshot(&self) -> admission::AdmissionSnapshot {
        let mut snapshot = self.inner.admission.snapshot();
        snapshot.principals.clear();
        snapshot
    }

    fn record(
        &self,
        actor: &AuthenticatedActor,
        query_id: &str,
        request_id: &str,
    ) -> std::result::Result<Arc<QueryRecord>, HttpError> {
        let record = self
            .inner
            .records
            .read()
            .get(query_id)
            .cloned()
            .ok_or_else(|| query_not_found(query_id, request_id))?;
        if record.deleting() || !actor.can_access_query(record.owner()) {
            return Err(query_not_found(query_id, request_id));
        }
        Ok(record)
    }
}

impl Drop for QueryManager {
    fn drop(&mut self) {
        if Arc::strong_count(&self.owner) == 1 {
            for record in self.inner.records.read().values() {
                if !record.terminal() {
                    record.cancel();
                }
            }
            self.inner.shutdown.cancel();
        }
    }
}

fn admission_http_error(error: AdmissionError, request_id: &str) -> HttpError {
    match error {
        AdmissionError::QueueFull { .. } => HttpError::new(
            StatusCode::TOO_MANY_REQUESTS,
            crate::ErrorCode::AdmissionQueueFull.as_str(),
            "the query admission queue is full",
            request_id.to_owned(),
        )
        .retry_after(1),
        AdmissionError::RequestExceedsLimit { .. } => HttpError::new(
            StatusCode::TOO_MANY_REQUESTS,
            crate::ErrorCode::AdmissionResourceLimit.as_str(),
            "the query exceeds an admission resource limit",
            request_id.to_owned(),
        ),
        AdmissionError::Cancelled => HttpError::new(
            StatusCode::CONFLICT,
            "query.cancelled",
            "query admission was cancelled",
            request_id.to_owned(),
        ),
        other => HttpError::new(
            StatusCode::SERVICE_UNAVAILABLE,
            crate::ErrorCode::AdmissionUnavailable.as_str(),
            format!("query admission is unavailable: {other}"),
            request_id.to_owned(),
        )
        .retry_after(1),
    }
}

fn fail_admission(record: &QueryRecord, error: AdmissionError) {
    match error {
        AdmissionError::Cancelled => record.cancel(),
        AdmissionError::RequestExceedsLimit { .. } => record.fail_stably(
            crate::ErrorCode::AdmissionResourceLimit.as_str(),
            "the query exceeds an admission resource limit",
            crate::RetryClass::Never,
        ),
        AdmissionError::QueueFull { .. }
        | AdmissionError::PrincipalUpdated
        | AdmissionError::PrincipalRemoved
        | AdmissionError::PrincipalRetired(_)
        | AdmissionError::UnknownPrincipal(_)
        | AdmissionError::WaiterClosed
        | AdmissionError::PrincipalExists(_)
        | AdmissionError::InvalidConfiguration(_) => record.fail_stably(
            crate::ErrorCode::AdmissionUnavailable.as_str(),
            "query admission became unavailable",
            crate::RetryClass::Safe,
        ),
    }
}

fn query_not_found(query_id: &str, request_id: &str) -> HttpError {
    HttpError::new(
        StatusCode::NOT_FOUND,
        "query.not_found",
        "query was not found or has expired",
        request_id.to_owned(),
    )
    .query(query_id)
}

fn metrics(value: QueryMetricsSnapshot) -> HttpQueryMetrics {
    HttpQueryMetrics {
        elapsed_ms: u64::try_from(value.elapsed.as_millis()).unwrap_or(u64::MAX),
        rows_returned: value.rows_returned,
        rows_scanned: value.rows_scanned,
        bytes_scanned: value.bytes_scanned,
        peak_memory_bytes: value.peak_memory_bytes,
        spill_read_bytes: value.spill_read_bytes,
        spill_write_bytes: value.spill_write_bytes,
    }
}

#[cfg(test)]
mod tests;
