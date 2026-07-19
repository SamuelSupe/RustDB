use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use axum::http::StatusCode;
use futures::{FutureExt, StreamExt};
use parking_lot::{Mutex, RwLock};
use sha2::{Digest, Sha256};
use tokio::sync::{Notify, Semaphore, mpsc};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::{Engine, Error, HttpReadOnlyPolicy, QueryMetricsSnapshot, Result};

use super::{
    error::{ErrorBody, HttpError},
    result_store::{ResultStore, ResultStoreConfig, StoredResult},
    types::{HttpQueryMetrics, QueryRequest, QueryState, QueryStatusResponse, SubmitResponse},
};

#[derive(Clone, Debug)]
pub struct QueryManagerConfig {
    pub max_running: usize,
    pub max_queued: usize,
    pub max_query_time: Duration,
}

impl Default for QueryManagerConfig {
    fn default() -> Self {
        Self {
            max_running: 1,
            max_queued: 64,
            max_query_time: Duration::from_secs(30 * 60),
        }
    }
}

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
    config: QueryManagerConfig,
    active_tasks: AtomicUsize,
    tasks_idle: Notify,
    accepting: Mutex<bool>,
}

struct Job {
    record: Arc<QueryRecord>,
    request: QueryRequest,
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

pub(crate) struct QueryRecord {
    id: String,
    created_at_ms: u64,
    cancel: CancellationToken,
    state: RwLock<RecordState>,
}

struct RecordState {
    phase: QueryState,
    started_at_ms: Option<u64>,
    finished_at_ms: Option<u64>,
    error: Option<ErrorBody>,
    metrics: Option<HttpQueryMetrics>,
    result: Option<Arc<StoredResult>>,
}

impl QueryManager {
    pub(crate) fn new(
        engine: Engine,
        config: QueryManagerConfig,
        result_config: ResultStoreConfig,
    ) -> Result<Self> {
        if config.max_running == 0 || config.max_queued == 0 || config.max_query_time.is_zero() {
            return Err(Error::InvalidArgument(
                "HTTP query limits and timeout must be positive".into(),
            ));
        }
        let store = Arc::new(ResultStore::open(result_config)?);
        let (sender, receiver) = mpsc::channel(config.max_queued);
        let inner = Arc::new(ManagerInner {
            records: RwLock::new(HashMap::new()),
            idempotency: Mutex::new(HashMap::new()),
            sender,
            shutdown: CancellationToken::new(),
            store,
            config: config.clone(),
            active_tasks: AtomicUsize::new(0),
            tasks_idle: Notify::new(),
            accepting: Mutex::new(true),
        });
        spawn_dispatcher(Arc::clone(&inner), engine, receiver);
        spawn_expiration(Arc::clone(&inner));
        Ok(Self {
            inner,
            owner: Arc::new(()),
        })
    }

    pub(crate) fn submit(
        &self,
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
        let mut idempotency = self.inner.idempotency.lock();
        if let Some(existing) = idempotency.get(key) {
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
        }
        let record = Arc::new(QueryRecord::new());
        let job = Job {
            record: Arc::clone(&record),
            request,
        };
        if self.inner.sender.try_send(job).is_err() {
            return Err(HttpError::new(
                StatusCode::TOO_MANY_REQUESTS,
                "query.queue_full",
                "the query admission queue is full",
                request_id.to_owned(),
            )
            .retry_after(1));
        }
        idempotency.insert(
            key.to_owned(),
            IdempotencyRecord {
                request_hash,
                query_id: record.id.clone(),
            },
        );
        self.inner
            .records
            .write()
            .insert(record.id.clone(), Arc::clone(&record));
        drop(accepting);
        Ok(submit_response(&record, false))
    }

    pub(crate) fn status(
        &self,
        query_id: &str,
        request_id: &str,
    ) -> std::result::Result<QueryStatusResponse, HttpError> {
        let record = self.record(query_id, request_id)?;
        Ok(record.status())
    }

    pub(crate) fn completed_result(
        &self,
        query_id: &str,
        request_id: &str,
    ) -> std::result::Result<Arc<StoredResult>, HttpError> {
        let record = self.record(query_id, request_id)?;
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

    pub(crate) fn cancel(
        &self,
        query_id: &str,
        request_id: &str,
    ) -> std::result::Result<QueryStatusResponse, HttpError> {
        let record = self.record(query_id, request_id)?;
        record.cancel();
        Ok(record.status())
    }

    pub(crate) fn delete(
        &self,
        query_id: &str,
        request_id: &str,
    ) -> std::result::Result<(), HttpError> {
        let Some(record) = self.inner.records.read().get(query_id).cloned() else {
            return Ok(());
        };
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
        cleanup_record_result(&record).map_err(|error| {
            HttpError::from_engine(&error, request_id.to_owned()).query(query_id)
        })?;
        self.inner.records.write().remove(query_id);
        self.inner
            .idempotency
            .lock()
            .retain(|_, value| value.query_id != query_id);
        Ok(())
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
        for record in &records {
            record.cancel();
        }
        loop {
            let idle = self.inner.tasks_idle.notified();
            if self.inner.active_tasks.load(Ordering::Acquire) == 0 {
                break;
            }
            idle.await;
        }
        let records = self
            .inner
            .records
            .read()
            .iter()
            .map(|(id, record)| (id.clone(), Arc::clone(record)))
            .collect::<Vec<_>>();
        let mut removed = Vec::new();
        let mut failures = Vec::new();
        for (id, record) in records {
            match cleanup_record_result(&record) {
                Ok(()) => removed.push((id, record)),
                Err(error) => failures.push((id, error)),
            }
        }
        remove_records(&self.inner, &removed);
        if failures.is_empty() {
            Ok(())
        } else {
            let failures = failures
                .into_iter()
                .map(|(id, error)| format!("{id}: {error}"))
                .collect::<Vec<_>>()
                .join("; ");
            Err(Error::Execution(format!(
                "failed to clean HTTP query results during shutdown: {failures}"
            )))
        }
    }

    pub(crate) fn begin_shutdown(&self) {
        *self.inner.accepting.lock() = false;
        self.inner.shutdown.cancel();
    }

    fn record(
        &self,
        query_id: &str,
        request_id: &str,
    ) -> std::result::Result<Arc<QueryRecord>, HttpError> {
        self.inner
            .records
            .read()
            .get(query_id)
            .cloned()
            .ok_or_else(|| {
                HttpError::new(
                    StatusCode::NOT_FOUND,
                    "query.not_found",
                    "query was not found or has expired",
                    request_id.to_owned(),
                )
                .query(query_id)
            })
    }
}

impl Drop for QueryManager {
    fn drop(&mut self) {
        if Arc::strong_count(&self.owner) == 1 {
            self.inner.shutdown.cancel();
        }
    }
}

impl QueryRecord {
    fn new() -> Self {
        Self {
            id: Uuid::new_v4().simple().to_string(),
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
        }
    }

    fn status(&self) -> QueryStatusResponse {
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

    fn cancel(&self) {
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

    fn terminal(&self) -> bool {
        matches!(
            self.state.read().phase,
            QueryState::Succeeded | QueryState::Failed | QueryState::Cancelled
        )
    }
}

fn spawn_dispatcher(inner: Arc<ManagerInner>, engine: Engine, mut receiver: mpsc::Receiver<Job>) {
    let active = ActiveTask::start(&inner);
    tokio::spawn(async move {
        let _active = active;
        let permits = Arc::new(Semaphore::new(inner.config.max_running));
        loop {
            let job = tokio::select! {
                _ = inner.shutdown.cancelled() => break,
                job = receiver.recv() => match job { Some(job) => job, None => break },
            };
            let permit = tokio::select! {
                _ = inner.shutdown.cancelled() => break,
                permit = Arc::clone(&permits).acquire_owned() => match permit { Ok(value) => value, Err(_) => break },
            };
            if job.record.terminal() {
                continue;
            }
            let engine = engine.clone();
            let store = Arc::clone(&inner.store);
            let maximum = inner.config.max_query_time;
            let active = ActiveTask::start(&inner);
            let record = Arc::clone(&job.record);
            tokio::spawn(async move {
                let _active = active;
                let _permit = permit;
                if std::panic::AssertUnwindSafe(run_job(engine, store, job, maximum))
                    .catch_unwind()
                    .await
                    .is_err()
                {
                    fail_record(
                        &record,
                        Error::Internal("HTTP query task panicked".to_owned()),
                    );
                    log_terminal(&record);
                }
            });
        }
    });
}

fn spawn_expiration(inner: Arc<ManagerInner>) {
    let active = ActiveTask::start(&inner);
    tokio::spawn(async move {
        let _active = active;
        let interval = inner
            .store
            .ttl()
            .min(Duration::from_secs(60))
            .max(Duration::from_secs(1));
        loop {
            tokio::select! {
                _ = inner.shutdown.cancelled() => break,
                _ = tokio::time::sleep(interval) => expire_records(&inner),
            }
        }
    });
}

fn expire_records(inner: &ManagerInner) {
    let now = SystemTime::now();
    let ttl = inner.store.ttl();
    let expired = inner
        .records
        .read()
        .iter()
        .filter_map(|(id, record)| {
            let state = record.state.read();
            if !matches!(
                state.phase,
                QueryState::Succeeded | QueryState::Failed | QueryState::Cancelled
            ) {
                return None;
            }
            let result_expired = state
                .result
                .as_ref()
                .is_some_and(|result| result.expired(ttl, now));
            let terminal_expired = state.finished_at_ms.is_some_and(|finished| {
                now_ms().saturating_sub(finished)
                    >= u64::try_from(ttl.as_millis()).unwrap_or(u64::MAX)
            });
            (result_expired || terminal_expired).then(|| (id.clone(), Arc::clone(record)))
        })
        .collect::<Vec<_>>();
    if expired.is_empty() {
        return;
    }
    let mut removed = Vec::new();
    for (id, record) in expired {
        match cleanup_record_result(&record) {
            Ok(()) => removed.push((id, record)),
            Err(error) => {
                tracing::error!(%error, query_id = %id, "failed to expire HTTP query result");
            }
        }
    }
    remove_records(inner, &removed);
}

fn cleanup_record_result(record: &QueryRecord) -> Result<()> {
    let mut state = record.state.write();
    if let Some(result) = state.result.as_ref() {
        result.delete()?;
    }
    state.result.take();
    Ok(())
}

fn remove_records(inner: &ManagerInner, removed: &[(String, Arc<QueryRecord>)]) {
    if removed.is_empty() {
        return;
    }
    let mut records = inner.records.write();
    for (id, expected) in removed {
        if records
            .get(id)
            .is_some_and(|current| Arc::ptr_eq(current, expected))
        {
            records.remove(id);
        }
    }
    drop(records);
    inner
        .idempotency
        .lock()
        .retain(|_, value| !removed.iter().any(|(id, _)| id == &value.query_id));
}

async fn run_job(engine: Engine, store: Arc<ResultStore>, job: Job, maximum: Duration) {
    {
        let mut state = job.record.state.write();
        if state.phase != QueryState::Queued {
            return;
        }
        state.phase = QueryState::Running;
        state.started_at_ms = Some(now_ms());
    }
    let timeout = match job.request.timeout(maximum) {
        Ok(timeout) => timeout,
        Err(error) => {
            fail_record(&job.record, error);
            return;
        }
    };
    match execute_and_store(&engine, &store, &job.record, &job.request, timeout).await {
        Ok((result, metrics)) => {
            let mut state = job.record.state.write();
            state.phase = QueryState::Succeeded;
            state.finished_at_ms = Some(now_ms());
            state.metrics = Some(metrics);
            state.result = Some(Arc::new(result));
        }
        Err(JobFailure::Cancelled) => {
            let mut state = job.record.state.write();
            state.phase = QueryState::Cancelled;
            state.finished_at_ms = Some(now_ms());
            state.error = Some(terminal_error(
                "query.cancelled",
                "query was cancelled",
                &job.record.id,
            ));
        }
        Err(JobFailure::Timeout) => {
            let mut state = job.record.state.write();
            state.phase = QueryState::Failed;
            state.finished_at_ms = Some(now_ms());
            state.error = Some(terminal_error(
                "query.timeout",
                "query exceeded its time limit",
                &job.record.id,
            ));
        }
        Err(JobFailure::Engine(error)) => fail_record(&job.record, error),
    }
    log_terminal(&job.record);
}

async fn execute_and_store(
    engine: &Engine,
    store: &ResultStore,
    record: &QueryRecord,
    request: &QueryRequest,
    timeout: Duration,
) -> std::result::Result<(StoredResult, HttpQueryMetrics), JobFailure> {
    let deadline = tokio::time::Instant::now() + timeout;
    let session = engine.session();
    let parameters = request.parameter_values().map_err(JobFailure::Engine)?;
    let execute = async {
        if parameters.is_empty() {
            session.execute_http_read_only(&request.sql).await
        } else {
            HttpReadOnlyPolicy::validate(&request.sql)?;
            session
                .prepare(&request.sql)?
                .execute_http_read_only(&parameters)
                .await
        }
    };
    tokio::pin!(execute);
    let mut result = tokio::select! {
        _ = record.cancel.cancelled() => return Err(JobFailure::Cancelled),
        _ = tokio::time::sleep_until(deadline) => return Err(JobFailure::Timeout),
        result = &mut execute => result.map_err(JobFailure::Engine)?,
    };
    let cancellation = result.cancellation_handle();
    let writer = store
        .writer(&record.id, result.schema())
        .map_err(JobFailure::Engine)?;
    let outcome = loop {
        let next = tokio::select! {
            _ = record.cancel.cancelled() => {
                cancellation.cancel();
                break Err(JobFailure::Cancelled);
            }
            _ = tokio::time::sleep_until(deadline) => {
                cancellation.cancel();
                break Err(JobFailure::Timeout);
            }
            next = result.stream().next() => next,
        };
        let Some(batch) = next else {
            break Ok(());
        };
        let batch = match batch {
            Ok(batch) => batch,
            Err(error) => break Err(JobFailure::Engine(error)),
        };
        let write = writer.write(batch);
        tokio::pin!(write);
        let write_result = tokio::select! {
            _ = record.cancel.cancelled() => {
                cancellation.cancel();
                break Err(JobFailure::Cancelled);
            }
            _ = tokio::time::sleep_until(deadline) => {
                cancellation.cancel();
                break Err(JobFailure::Timeout);
            }
            result = &mut write => result,
        };
        if let Err(error) = write_result {
            break Err(JobFailure::Engine(error));
        }
    };
    let cleanup_error = if outcome.is_err() {
        result.cancel_and_quiesce().await.err()
    } else {
        None
    };
    let metrics = metrics(result.metrics().snapshot());
    drop(result);
    match outcome {
        Ok(()) => {
            let finish = writer.finish();
            tokio::pin!(finish);
            tokio::select! {
                _ = record.cancel.cancelled() => {
                    cancellation.cancel();
                    Err(JobFailure::Cancelled)
                }
                _ = tokio::time::sleep_until(deadline) => {
                    cancellation.cancel();
                    Err(JobFailure::Timeout)
                }
                result = &mut finish => result
                    .map(|result| (result, metrics))
                    .map_err(JobFailure::Engine),
            }
        }
        Err(error) => {
            if let Err(cleanup) = writer.abort().await {
                tracing::error!(%cleanup, query_id = %record.id, "failed to clean partial HTTP result");
            }
            match cleanup_error {
                Some(cleanup) => Err(JobFailure::Engine(Error::Execution(format!(
                    "{}; additionally failed to quiesce query resources: {cleanup}",
                    failure_message(&error)
                )))),
                None => Err(error),
            }
        }
    }
}

enum JobFailure {
    Cancelled,
    Timeout,
    Engine(Error),
}

fn failure_message(failure: &JobFailure) -> String {
    match failure {
        JobFailure::Cancelled => "query was cancelled".to_owned(),
        JobFailure::Timeout => "query exceeded its time limit".to_owned(),
        JobFailure::Engine(error) => error.to_string(),
    }
}

fn fail_record(record: &QueryRecord, error: Error) {
    let mapped = HttpError::from_engine(&error, String::new());
    let mut body = *mapped.body;
    body.request_id = None;
    body.query_id = Some(record.id.clone());
    let mut state = record.state.write();
    state.phase = if matches!(error, Error::Cancelled) {
        QueryState::Cancelled
    } else {
        QueryState::Failed
    };
    state.finished_at_ms = Some(now_ms());
    state.error = Some(body);
    tracing::warn!(
        query_id = %record.id,
        error_code = %state.error.as_ref().map_or("server.internal", |error| error.error.as_str()),
        "HTTP query failed"
    );
}

fn log_terminal(record: &QueryRecord) {
    let status = record.status();
    let metrics = status.metrics.unwrap_or_default();
    let duration_ms = status
        .started_at_ms
        .zip(status.finished_at_ms)
        .map(|(started, finished)| finished.saturating_sub(started))
        .unwrap_or(0);
    tracing::info!(
        query_id = %status.query_id,
        state = ?status.state,
        duration_ms,
        rows_returned = metrics.rows_returned,
        rows_scanned = metrics.rows_scanned,
        bytes_scanned = metrics.bytes_scanned,
        peak_memory_bytes = metrics.peak_memory_bytes,
        spill_read_bytes = metrics.spill_read_bytes,
        spill_write_bytes = metrics.spill_write_bytes,
        "HTTP query finished"
    );
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

fn validate_idempotency_key(key: &str, request_id: &str) -> std::result::Result<(), HttpError> {
    if !(16..=128).contains(&key.len()) || !key.bytes().all(|byte| byte.is_ascii_graphic()) {
        return Err(HttpError::new(
            StatusCode::BAD_REQUEST,
            "idempotency.invalid_key",
            "Idempotency-Key must contain 16-128 printable ASCII characters",
            request_id.to_owned(),
        ));
    }
    Ok(())
}

fn request_hash(request: &QueryRequest) -> Result<String> {
    let bytes = serde_json::to_vec(request)
        .map_err(|error| Error::Internal(format!("failed to hash query request: {error}")))?;
    let digest = Sha256::digest(bytes);
    Ok(digest.iter().map(|byte| format!("{byte:02x}")).collect())
}

fn submit_response(record: &QueryRecord, replayed: bool) -> SubmitResponse {
    SubmitResponse {
        query_id: record.id.clone(),
        state: record.state.read().phase,
        replayed,
        status_url: format!("/v1/queries/{}", record.id),
        results_url: format!("/v1/queries/{}/results", record.id),
    }
}

fn terminal_error(code: &str, message: &str, query_id: &str) -> ErrorBody {
    ErrorBody {
        error: code.into(),
        message: message.into(),
        request_id: None,
        query_id: Some(query_id.into()),
        details: None,
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| u64::try_from(duration.as_millis()).unwrap_or(u64::MAX))
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::validate_idempotency_key;

    #[test]
    fn rejects_short_and_non_ascii_idempotency_keys() {
        assert!(validate_idempotency_key("short", "request").is_err());
        assert!(validate_idempotency_key("这是一个不安全的key", "request").is_err());
        assert!(validate_idempotency_key("0123456789abcdef", "request").is_ok());
        let _ = Duration::ZERO;
    }
}
