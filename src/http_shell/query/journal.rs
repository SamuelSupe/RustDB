use std::{
    collections::BTreeMap,
    fs::File,
    path::PathBuf,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

#[cfg(test)]
use std::sync::atomic::{AtomicBool, Ordering};

use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::http_shell::{ErrorBody, HttpQueryMetrics, QueryState, security::QueryOwner};
use crate::{Error, Result, RetryClass};

#[path = "journal/codec.rs"]
mod codec;
#[path = "journal/disk.rs"]
mod disk;
#[path = "journal/recovery.rs"]
mod recovery;

const PRODUCER_VERSION: &str = env!("CARGO_PKG_VERSION");
const DEFAULT_COMPACT_AFTER_EVENTS: u64 = 256;

#[derive(Clone, Debug)]
pub(crate) struct QueryJournalConfig {
    pub(crate) directory: PathBuf,
    pub(crate) compact_after_events: u64,
}

impl QueryJournalConfig {
    pub(crate) fn new(directory: impl Into<PathBuf>) -> Self {
        Self {
            directory: directory.into(),
            compact_after_events: DEFAULT_COMPACT_AFTER_EVENTS,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PersistedQuery {
    pub(crate) query_id: String,
    pub(crate) owner: QueryOwner,
    pub(crate) request_hash: String,
    pub(crate) scoped_idempotency_digest: String,
    pub(crate) state: QueryState,
    pub(crate) created_at_ms: u64,
    pub(crate) started_at_ms: Option<u64>,
    pub(crate) finished_at_ms: Option<u64>,
    pub(crate) error: Option<ErrorBody>,
    pub(crate) metrics: Option<HttpQueryMetrics>,
    pub(crate) result_available: bool,
}

impl PersistedQuery {
    fn prepare(mut self) -> Result<Self> {
        if let Some(error) = self.error.as_mut() {
            error.message = stable_error_message(&error.error).to_owned();
            error.request_id = None;
            error.query_id = Some(self.query_id.clone());
            error.details = None;
        }
        self.validate()?;
        Ok(self)
    }

    fn validate(&self) -> Result<()> {
        if !valid_query_id(&self.query_id) {
            return Err(Error::InvalidArgument(
                "persisted HTTP query has an invalid query ID".into(),
            ));
        }
        if !valid_digest(&self.request_hash) || !valid_digest(&self.scoped_idempotency_digest) {
            return Err(Error::InvalidArgument(
                "persisted HTTP query hashes must be lowercase SHA-256 digests".into(),
            ));
        }
        if self.created_at_ms == 0
            || self
                .started_at_ms
                .is_some_and(|value| value < self.created_at_ms)
            || self
                .finished_at_ms
                .is_some_and(|value| value < self.started_at_ms.unwrap_or(self.created_at_ms))
        {
            return Err(Error::InvalidArgument(
                "persisted HTTP query has invalid timestamps".into(),
            ));
        }
        #[allow(unreachable_patterns)]
        match self.state {
            QueryState::Queued => {
                if self.started_at_ms.is_some()
                    || self.finished_at_ms.is_some()
                    || self.error.is_some()
                    || self.result_available
                {
                    return Err(invalid_state());
                }
            }
            QueryState::Running => {
                if self.started_at_ms.is_none()
                    || self.finished_at_ms.is_some()
                    || self.error.is_some()
                    || self.result_available
                {
                    return Err(invalid_state());
                }
            }
            QueryState::Succeeded => {
                if self.started_at_ms.is_none()
                    || self.finished_at_ms.is_none()
                    || self.error.is_some()
                {
                    return Err(invalid_state());
                }
            }
            QueryState::Failed | QueryState::Cancelled => {
                if self.finished_at_ms.is_none() || self.error.is_none() || self.result_available {
                    return Err(invalid_state());
                }
            }
            _ => return Err(invalid_state()),
        }
        if let Some(error) = &self.error
            && (!valid_error_code(&error.error)
                || error.error.len() > 128
                || error.message.len() > 256
                || error.request_id.is_some()
                || error.query_id.as_deref() != Some(&self.query_id)
                || error.details.is_some())
        {
            return Err(Error::InvalidArgument(
                "persisted HTTP query has an unsafe error body".into(),
            ));
        }
        Ok(())
    }

    fn fail_for_restart(&self) -> Self {
        let mut value = self.clone();
        value.state = QueryState::Failed;
        value.finished_at_ms = Some(now_ms());
        value.error = Some(stable_error(
            "query.server_restarted",
            RetryClass::Safe,
            &value.query_id,
        ));
        value.result_available = false;
        value
    }

    fn invalidate_result(&self) -> Self {
        let mut value = self.clone();
        value.state = QueryState::Failed;
        value.finished_at_ms = Some(now_ms());
        value.error = Some(stable_error(
            "query.result_invalidated",
            RetryClass::Never,
            &value.query_id,
        ));
        value.result_available = false;
        value
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum DeleteReason {
    Explicit,
    Ttl,
}

pub(crate) struct QueryJournal {
    directory: PathBuf,
    producer_version: String,
    compact_after_events: u64,
    inner: Mutex<JournalState>,
    #[cfg(test)]
    fail_next_success: AtomicBool,
    _lock: File,
}

struct JournalState {
    queries: BTreeMap<String, PersistedQuery>,
    last_seq: u64,
    events_since_snapshot: u64,
    journal: File,
}

impl QueryJournal {
    pub(crate) fn open(config: QueryJournalConfig) -> Result<Arc<Self>> {
        Self::open_with_producer(config, PRODUCER_VERSION)
    }

    #[allow(dead_code)]
    pub(crate) fn open_with_producer(
        config: QueryJournalConfig,
        producer_version: &str,
    ) -> Result<Arc<Self>> {
        if config.compact_after_events == 0 {
            return Err(Error::InvalidArgument(
                "query journal compaction threshold must be positive".into(),
            ));
        }
        if producer_version.is_empty() || producer_version.len() > 128 {
            return Err(Error::InvalidArgument(
                "query journal producer version is invalid".into(),
            ));
        }
        let root_lock = disk::open_root(&config.directory)?;
        let recovered = recovery::load(&config.directory, producer_version)?;
        let journal = disk::open_journal(&config.directory)?;
        let value = Arc::new(Self {
            directory: config.directory,
            producer_version: producer_version.to_owned(),
            compact_after_events: config.compact_after_events,
            inner: Mutex::new(JournalState {
                queries: recovered.queries,
                last_seq: recovered.last_seq,
                events_since_snapshot: recovered.events_since_snapshot,
                journal,
            }),
            #[cfg(test)]
            fail_next_success: AtomicBool::new(false),
            _lock: root_lock,
        });
        if !recovered.normalized.is_empty() {
            let mut inner = value.inner.lock();
            for query in recovered.normalized {
                value.append_upsert(&mut inner, query)?;
            }
            value.maybe_compact(&mut inner);
        }
        Ok(value)
    }

    pub(crate) fn load(&self) -> Vec<PersistedQuery> {
        self.inner.lock().queries.values().cloned().collect()
    }

    pub(crate) fn upsert(&self, query: PersistedQuery) -> Result<u64> {
        #[cfg(test)]
        if query.state == QueryState::Succeeded
            && self.fail_next_success.swap(false, Ordering::AcqRel)
        {
            return Err(Error::Execution(
                "injected successful Query journal failure".into(),
            ));
        }
        let query = query.prepare()?;
        let mut inner = self.inner.lock();
        let seq = self.append_upsert(&mut inner, query)?;
        self.maybe_compact(&mut inner);
        Ok(seq)
    }

    #[cfg(test)]
    #[allow(dead_code)]
    pub(crate) fn fail_next_success(&self) {
        self.fail_next_success.store(true, Ordering::Release);
    }

    pub(crate) fn delete(&self, query_id: &str, reason: DeleteReason) -> Result<bool> {
        if !valid_query_id(query_id) {
            return Err(Error::InvalidArgument("invalid HTTP query ID".into()));
        }
        let mut inner = self.inner.lock();
        if !inner.queries.contains_key(query_id) {
            return Ok(false);
        }
        let seq = next_seq(inner.last_seq)?;
        let event = codec::Event::delete(&self.producer_version, seq, query_id, reason, now_ms());
        disk::append_event(&mut inner.journal, &codec::encode_event(&event)?)?;
        inner.last_seq = seq;
        inner.events_since_snapshot = inner.events_since_snapshot.saturating_add(1);
        inner.queries.remove(query_id);
        self.maybe_compact(&mut inner);
        Ok(true)
    }

    #[allow(dead_code)]
    pub(crate) fn compact(&self) -> Result<()> {
        let mut inner = self.inner.lock();
        self.compact_locked(&mut inner)
    }

    fn append_upsert(&self, inner: &mut JournalState, query: PersistedQuery) -> Result<u64> {
        query.validate()?;
        if inner
            .queries
            .get(&query.query_id)
            .is_some_and(|current| terminal_state(current.state) && !terminal_state(query.state))
        {
            return Err(Error::Execution(format!(
                "query journal rejected terminal state regression for {}",
                query.query_id
            )));
        }
        let seq = next_seq(inner.last_seq)?;
        let event = codec::Event::upsert(&self.producer_version, seq, query.clone());
        disk::append_event(&mut inner.journal, &codec::encode_event(&event)?)?;
        inner.last_seq = seq;
        inner.events_since_snapshot = inner.events_since_snapshot.saturating_add(1);
        inner.queries.insert(query.query_id.clone(), query);
        Ok(seq)
    }

    fn maybe_compact(&self, inner: &mut JournalState) {
        if inner.events_since_snapshot >= self.compact_after_events
            && let Err(error) = self.compact_locked(inner)
        {
            tracing::warn!(%error, path = %self.directory.display(), "query journal compaction failed");
        }
    }

    fn compact_locked(&self, inner: &mut JournalState) -> Result<()> {
        let snapshot = codec::Snapshot::new(
            &self.producer_version,
            inner.last_seq,
            inner.queries.values().cloned().collect(),
        );
        disk::replace_snapshot(&self.directory, &codec::encode_snapshot(&snapshot)?)?;
        let (journal, sync_error) = disk::replace_journal(&self.directory)?;
        inner.journal = journal;
        if let Some(error) = sync_error {
            return Err(error);
        }
        inner.events_since_snapshot = 0;
        Ok(())
    }
}

pub(super) const fn terminal_state(state: QueryState) -> bool {
    matches!(
        state,
        QueryState::Succeeded | QueryState::Failed | QueryState::Cancelled
    )
}

pub(crate) fn scoped_idempotency_digest(owner: &QueryOwner, key: &str) -> Result<String> {
    if key.is_empty() || key.len() > 1024 {
        return Err(Error::InvalidArgument(
            "idempotency key cannot be hashed safely".into(),
        ));
    }
    let owner = serde_json::to_vec(owner)
        .map_err(|error| Error::Internal(format!("failed to hash query owner: {error}")))?;
    let mut digest = Sha256::new();
    digest.update(owner);
    digest.update([0]);
    digest.update(key.as_bytes());
    Ok(format!("{:x}", digest.finalize()))
}

fn next_seq(current: u64) -> Result<u64> {
    current
        .checked_add(1)
        .ok_or_else(|| Error::ResourceExhausted("query journal sequence overflowed".into()))
}

fn stable_error(code: &str, retry: RetryClass, query_id: &str) -> ErrorBody {
    let mut error = ErrorBody::new(code, stable_error_message(code), retry);
    error.query_id = Some(query_id.to_owned());
    error
}

fn stable_error_message(code: &str) -> &'static str {
    match code {
        "query.cancelled" => "query was cancelled",
        "query.timeout" => "query exceeded its time limit",
        "query.server_restarted" => "query was interrupted by a server restart",
        "query.result_invalidated" => "query result was invalidated by a server upgrade",
        _ => "query failed",
    }
}

fn valid_query_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
}

fn valid_digest(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn valid_error_code(value: &str) -> bool {
    !value.is_empty()
        && value.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'.' | b'_' | b'-')
        })
}

fn invalid_state() -> Error {
    Error::InvalidArgument("persisted HTTP query fields do not match its state".into())
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|value| u64::try_from(value.as_millis()).unwrap_or(u64::MAX))
        .unwrap_or(1)
        .max(1)
}
