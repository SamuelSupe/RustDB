use std::{
    collections::HashMap,
    fs::File,
    path::PathBuf,
    sync::{Arc, Weak},
    time::{Duration, SystemTime},
};

use arrow::{datatypes::SchemaRef, record_batch::RecordBatch};
use parking_lot::Mutex;

use crate::{Error, Result};

use super::result_read::{ResultAccess, ResultReadGuard};

mod layout;
mod manifest;
mod quota;
mod reader;
mod recovery;
mod writer;

use layout::{
    QUERY_MARKER, acquire_root_lock, establish_root_marker, remove_owned_query, secure_directory,
    sync_directory, write_private,
};
use manifest::{BatchEntry, MANIFEST_FILE, Manifest, ManifestState, PRODUCER_VERSION};
use quota::{QuotaLease, QuotaPool};
pub(crate) use writer::ResultWriter;

#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct ResultStoreConfig {
    pub directory: PathBuf,
    pub ttl: Duration,
    pub global_limit_bytes: Option<u64>,
    pub query_limit_bytes: Option<u64>,
}

impl ResultStoreConfig {
    pub fn new(directory: impl Into<PathBuf>) -> Self {
        Self {
            directory: directory.into(),
            ttl: Duration::from_secs(60 * 60),
            global_limit_bytes: None,
            query_limit_bytes: None,
        }
    }
}

pub(crate) struct ResultStore {
    root: PathBuf,
    ttl: Duration,
    quota: Arc<QuotaPool>,
    accesses: AccessRegistry,
    recovered: Mutex<Vec<RecoveredResult>>,
    _lock: File,
}

pub(super) type AccessRegistry = Mutex<HashMap<String, Weak<ResultAccess>>>;

impl ResultStore {
    pub(crate) fn open(config: ResultStoreConfig) -> Result<Self> {
        if config.ttl.is_zero() {
            return Err(Error::InvalidArgument(
                "HTTP result TTL must be positive".into(),
            ));
        }
        secure_directory(&config.directory)?;
        establish_root_marker(&config.directory)?;
        let root_lock = acquire_root_lock(&config.directory)?;
        let quota = Arc::new(QuotaPool::configured(
            &config.directory,
            config.global_limit_bytes,
            config.query_limit_bytes,
        )?);
        let accesses = Mutex::new(HashMap::new());
        let recovered = recovery::recover(&config.directory, config.ttl, &quota, &accesses)?;
        Ok(Self {
            root: config.directory,
            ttl: config.ttl,
            quota,
            accesses,
            recovered: Mutex::new(recovered),
            _lock: root_lock,
        })
    }

    pub(crate) fn ttl(&self) -> Duration {
        self.ttl
    }

    pub(crate) fn writer(&self, query_id: &str, schema: SchemaRef) -> Result<ResultWriter> {
        if !valid_query_id(query_id) {
            return Err(Error::InvalidArgument(
                "HTTP query ID contains unsafe characters".into(),
            ));
        }
        self.quota.ensure_free_space(0).map_err(|error| {
            Error::ResourceExhausted(format!(
                "HTTP result filesystem has insufficient space: {error}"
            ))
        })?;
        let directory = self.root.join(format!("q-{query_id}"));
        if directory.exists() {
            return Err(Error::Internal(format!(
                "HTTP result directory for query {query_id} already exists"
            )));
        }
        secure_directory(&directory)?;
        let access = access_for(&self.accesses, query_id);
        let initialized = (|| -> Result<Manifest> {
            write_private(&directory.join("OWNER"), QUERY_MARKER)?;
            secure_directory(&directory.join("batches"))?;
            let manifest = Manifest::new(query_id, &schema)?;
            manifest::persist(&directory, &manifest)?;
            sync_directory(&self.root)?;
            Ok(manifest)
        })();
        let manifest = match initialized {
            Ok(value) => value,
            Err(error) => {
                return match access.delete(|| remove_owned_query(&directory)) {
                    Ok(()) => {
                        self.accesses.lock().remove(query_id);
                        Err(error)
                    }
                    Err(cleanup) => Err(Error::Execution(format!(
                        "{error}; additionally failed to clean HTTP result directory: {cleanup}"
                    ))),
                };
            }
        };
        let lease = Arc::new(QuotaLease::new(Arc::clone(&self.quota)));
        Ok(ResultWriter::start(
            directory, schema, lease, access, manifest,
        ))
    }

    pub(crate) fn delete_query_artifacts(&self, query_id: &str) -> Result<()> {
        if !valid_query_id(query_id) {
            return Err(Error::InvalidArgument("invalid HTTP query ID".into()));
        }
        let directory = self.root.join(format!("q-{query_id}"));
        let result = match directory.symlink_metadata() {
            Ok(_) => access_for(&self.accesses, query_id).delete(|| remove_owned_query(&directory)),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(Error::io(Some(directory), error)),
        };
        if result.is_ok() {
            self.accesses.lock().remove(query_id);
        }
        result
    }

    #[allow(dead_code)]
    pub(crate) fn take_recovered(&self) -> Vec<RecoveredResult> {
        std::mem::take(&mut *self.recovered.lock())
    }

    #[allow(dead_code)]
    pub(crate) fn snapshot(&self, query_id: &str) -> Result<Option<ResultSnapshot>> {
        if !valid_query_id(query_id) {
            return Err(Error::InvalidArgument("invalid HTTP query ID".into()));
        }
        let directory = self.root.join(format!("q-{query_id}"));
        let path = directory.join(MANIFEST_FILE);
        match path.symlink_metadata() {
            Ok(metadata)
                if metadata.file_type().is_file() && !metadata.file_type().is_symlink() =>
            {
                load_snapshot(directory, access_for(&self.accesses, query_id)).map(Some)
            }
            Ok(_) => Err(Error::InvalidArgument(format!(
                "HTTP result manifest is not a regular file: {}",
                path.display()
            ))),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(Error::io(Some(path), error)),
        }
    }
}

pub(super) fn access_for(registry: &AccessRegistry, query_id: &str) -> Arc<ResultAccess> {
    let mut registry = registry.lock();
    if let Some(access) = registry.get(query_id).and_then(Weak::upgrade) {
        return access;
    }
    let access = ResultAccess::new();
    registry.insert(query_id.to_owned(), Arc::downgrade(&access));
    access
}

fn load_snapshot(directory: PathBuf, access: Arc<ResultAccess>) -> Result<ResultSnapshot> {
    let query_id = directory
        .file_name()
        .and_then(|value| value.to_str())
        .and_then(|value| value.strip_prefix("q-"))
        .ok_or_else(|| Error::InvalidArgument("invalid HTTP result directory".into()))?;
    let path = directory.join(MANIFEST_FILE);
    let value = manifest::load(&path)?;
    value.validate(query_id)?;
    if value.producer_version != PRODUCER_VERSION {
        return Err(Error::Execution(
            "HTTP result was produced by an incompatible server version".into(),
        ));
    }
    let schema = value.schema()?;
    Ok(ResultSnapshot {
        directory,
        schema,
        manifest: value,
        access,
    })
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum StoredResultState {
    Running,
    Completed,
    Failed,
    Invalidated,
}

impl From<ManifestState> for StoredResultState {
    fn from(value: ManifestState) -> Self {
        match value {
            ManifestState::Running => Self::Running,
            ManifestState::Completed => Self::Completed,
            ManifestState::Failed => Self::Failed,
            ManifestState::Invalidated => Self::Invalidated,
        }
    }
}

#[derive(Clone)]
#[allow(dead_code)]
pub(crate) struct RecoveredResult {
    query_id: String,
    state: StoredResultState,
    result: Option<Arc<StoredResult>>,
    error: Option<String>,
    _access: Arc<ResultAccess>,
}

#[allow(dead_code)]
impl RecoveredResult {
    fn completed(query_id: String, result: Arc<StoredResult>, access: Arc<ResultAccess>) -> Self {
        Self {
            query_id,
            state: StoredResultState::Completed,
            result: Some(result),
            error: None,
            _access: access,
        }
    }

    fn terminal(
        query_id: String,
        state: ManifestState,
        error: Option<String>,
        access: Arc<ResultAccess>,
    ) -> Self {
        Self {
            query_id,
            state: state.into(),
            result: None,
            error,
            _access: access,
        }
    }

    pub(crate) fn query_id(&self) -> &str {
        &self.query_id
    }

    pub(crate) fn state(&self) -> StoredResultState {
        self.state
    }

    pub(crate) fn result(&self) -> Option<Arc<StoredResult>> {
        self.result.clone()
    }

    pub(crate) fn error(&self) -> Option<&str> {
        self.error.as_deref()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[allow(dead_code)]
pub(crate) struct ResultChunk {
    pub(crate) seq: u64,
    pub(crate) rows: u64,
    pub(crate) bytes: u64,
}

#[allow(dead_code)]
pub(crate) struct ResultSnapshot {
    directory: PathBuf,
    schema: SchemaRef,
    manifest: Manifest,
    access: Arc<ResultAccess>,
}

#[allow(dead_code)]
impl ResultSnapshot {
    pub(crate) fn state(&self) -> StoredResultState {
        self.manifest.state.into()
    }

    /// A completed result is not publicly terminal until its Query journal
    /// success record is durable. Committed batches remain readable.
    pub(crate) fn hide_completion(&mut self) {
        if self.manifest.state == ManifestState::Completed {
            self.manifest.state = ManifestState::Running;
        }
    }

    pub(crate) fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    pub(crate) fn next_batch_seq(&self) -> u64 {
        self.manifest.next_batch_seq
    }

    pub(crate) fn rows(&self) -> u64 {
        self.manifest.rows
    }

    pub(crate) fn bytes(&self) -> u64 {
        self.manifest.bytes
    }

    pub(crate) fn error(&self) -> Option<&str> {
        self.manifest.error.as_deref()
    }

    pub(crate) fn chunks_from(&self, seq: u64) -> Result<Vec<ResultChunk>> {
        chunks_from(&self.manifest.batches, seq)
    }

    pub(crate) async fn read_chunk_bytes(
        &self,
        seq: u64,
        guard: ResultReadGuard,
    ) -> Result<Vec<u8>> {
        let entry = batch_entry(&self.manifest.batches, seq)?.clone();
        let directory = self.directory.clone();
        let access = self.access.read().await?;
        tokio::task::spawn_blocking(move || {
            let _guard = guard;
            let _access = access;
            reader::read_chunk_bytes(&directory, &entry)
        })
        .await
        .map_err(|error| Error::Internal(format!("HTTP result reader panicked: {error}")))?
    }
}

pub(crate) struct StoredResult {
    directory: PathBuf,
    schema: SchemaRef,
    rows: u64,
    batches: Vec<BatchEntry>,
    completed_at: SystemTime,
    #[allow(dead_code)]
    lease: Arc<QuotaLease>,
    access: Arc<ResultAccess>,
}

impl StoredResult {
    fn completed(
        directory: PathBuf,
        schema: SchemaRef,
        manifest: &Manifest,
        lease: Arc<QuotaLease>,
        access: Arc<ResultAccess>,
    ) -> Self {
        Self {
            directory,
            schema,
            rows: manifest.rows,
            batches: manifest.batches.clone(),
            completed_at: manifest::updated_at(manifest),
            lease,
            access,
        }
    }

    pub(crate) fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    pub(crate) fn rows(&self) -> u64 {
        self.rows
    }

    pub(crate) fn expired(&self, ttl: Duration, now: SystemTime) -> bool {
        now.duration_since(self.completed_at)
            .is_ok_and(|age| age >= ttl)
    }

    pub(crate) fn snapshot(&self) -> Result<ResultSnapshot> {
        load_snapshot(self.directory.clone(), Arc::clone(&self.access))
    }

    pub(crate) async fn read(
        &self,
        offset: u64,
        limit: usize,
        guard: ResultReadGuard,
    ) -> Result<Vec<RecordBatch>> {
        let directory = self.directory.clone();
        let schema = Arc::clone(&self.schema);
        let batches = self.batches.clone();
        let rows = self.rows;
        let access = self.access.read().await?;
        tokio::task::spawn_blocking(move || {
            let _guard = guard;
            let _access = access;
            reader::read_pages(&directory, &schema, &batches, rows, offset, limit)
        })
        .await
        .map_err(|error| Error::Internal(format!("HTTP result reader panicked: {error}")))?
    }

    #[allow(dead_code)]
    pub(crate) fn chunks_from(&self, seq: u64) -> Result<Vec<ResultChunk>> {
        chunks_from(&self.batches, seq)
    }

    #[allow(dead_code)]
    pub(crate) async fn read_chunk_bytes(
        &self,
        seq: u64,
        guard: ResultReadGuard,
    ) -> Result<Vec<u8>> {
        let entry = batch_entry(&self.batches, seq)?.clone();
        let directory = self.directory.clone();
        let access = self.access.read().await?;
        tokio::task::spawn_blocking(move || {
            let _guard = guard;
            let _access = access;
            reader::read_chunk_bytes(&directory, &entry)
        })
        .await
        .map_err(|error| Error::Internal(format!("HTTP result reader panicked: {error}")))?
    }

    pub(crate) fn delete(&self) -> Result<()> {
        self.access.delete(|| remove_owned_query(&self.directory))
    }
}

fn chunks_from(entries: &[BatchEntry], seq: u64) -> Result<Vec<ResultChunk>> {
    if seq > u64::try_from(entries.len()).unwrap_or(u64::MAX) {
        return Err(Error::InvalidArgument(format!(
            "result batch sequence {seq} is beyond the committed result"
        )));
    }
    let start = usize::try_from(seq)
        .map_err(|_| Error::InvalidArgument("result batch sequence is too large".into()))?;
    Ok(entries[start..]
        .iter()
        .map(|entry| ResultChunk {
            seq: entry.seq,
            rows: entry.rows,
            bytes: entry.bytes,
        })
        .collect())
}

fn batch_entry(entries: &[BatchEntry], seq: u64) -> Result<&BatchEntry> {
    let index = usize::try_from(seq)
        .map_err(|_| Error::InvalidArgument("result batch sequence is too large".into()))?;
    entries
        .get(index)
        .filter(|entry| entry.seq == seq)
        .ok_or_else(|| {
            Error::InvalidArgument(format!("result batch sequence {seq} is not committed"))
        })
}

fn valid_query_id(query_id: &str) -> bool {
    !query_id.is_empty()
        && query_id.len() <= 128
        && query_id
            .bytes()
            .all(|value| value.is_ascii_alphanumeric() || matches!(value, b'-' | b'_'))
}

#[cfg(test)]
mod tests;
