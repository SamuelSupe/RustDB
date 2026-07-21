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

use super::{
    result_read::{ResultAccess, ResultReadGuard},
    service_io::{DEFAULT_SERVICE_IO_THREADS, ServiceIoPool},
};

mod layout;
mod manifest;
mod quota;
mod reader;
mod recovery;
mod writer;

use layout::{
    QUERY_MARKER, acquire_existing_root_lock, acquire_root_lock, establish_root_marker,
    remove_owned_query, secure_directory, sync_directory, write_private,
};
use manifest::{BatchEntry, MANIFEST_FILE, Manifest, ManifestState};
use quota::{QuotaLease, QuotaPool};
pub(crate) use writer::ResultWriter;

#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct ResultStoreConfig {
    pub directory: PathBuf,
    pub ttl: Duration,
    pub global_limit_bytes: Option<u64>,
    pub query_limit_bytes: Option<u64>,
    pub service_io_threads: usize,
}

impl ResultStoreConfig {
    pub fn new(directory: impl Into<PathBuf>) -> Self {
        Self {
            directory: directory.into(),
            ttl: Duration::from_secs(60 * 60),
            global_limit_bytes: None,
            query_limit_bytes: None,
            service_io_threads: DEFAULT_SERVICE_IO_THREADS,
        }
    }
}

pub(crate) struct ResultStore {
    root: PathBuf,
    ttl: Duration,
    quota: Arc<QuotaPool>,
    accesses: AccessRegistry,
    recovered: Mutex<Vec<RecoveredResult>>,
    io: ServiceIoPool,
    _lock: File,
}

pub(super) type AccessRegistry = Arc<Mutex<HashMap<String, Weak<ResultAccess>>>>;

impl ResultStore {
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn open(config: ResultStoreConfig) -> Result<Self> {
        Self::open_with_io(config, ServiceIoPool::new(DEFAULT_SERVICE_IO_THREADS)?)
    }

    pub(crate) fn open_with_io(config: ResultStoreConfig, io: ServiceIoPool) -> Result<Self> {
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
        let accesses = Arc::new(Mutex::new(HashMap::new()));
        let recovered = recovery::recover(&config.directory, config.ttl, &quota, &accesses, &io)?;
        Ok(Self {
            root: config.directory,
            ttl: config.ttl,
            quota,
            accesses,
            recovered: Mutex::new(recovered),
            io,
            _lock: root_lock,
        })
    }

    pub(crate) fn ttl(&self) -> Duration {
        self.ttl
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn writer(&self, query_id: &str, schema: SchemaRef) -> Result<ResultWriter> {
        validate_query_id(query_id)?;
        let initialized = self.io.run(writer_initialization(
            self.root.clone(),
            Arc::clone(&self.quota),
            Arc::clone(&self.accesses),
            query_id.to_owned(),
            Arc::clone(&schema),
        ))?;
        Ok(self.result_writer(initialized, schema))
    }

    pub(crate) async fn writer_async(
        &self,
        query_id: &str,
        schema: SchemaRef,
    ) -> Result<ResultWriter> {
        validate_query_id(query_id)?;
        let initialized = self
            .io
            .run_async(writer_initialization(
                self.root.clone(),
                Arc::clone(&self.quota),
                Arc::clone(&self.accesses),
                query_id.to_owned(),
                Arc::clone(&schema),
            ))
            .await?;
        Ok(self.result_writer(initialized, schema))
    }

    pub(crate) async fn seal_interrupted_prefix(
        &self,
        query_id: &str,
        message: &str,
    ) -> Result<bool> {
        validate_query_id(query_id)?;
        let directory = self.root.join(format!("q-{query_id}"));
        let message = message.to_owned();
        self.io
            .run_async(move || {
                let path = directory.join(MANIFEST_FILE);
                match path.symlink_metadata() {
                    Ok(metadata)
                        if metadata.file_type().is_file() && !metadata.file_type().is_symlink() =>
                    {
                        mark_result_interrupted(&directory, &message)?;
                        Ok(true)
                    }
                    Ok(_) => Err(Error::InvalidArgument(format!(
                        "HTTP result manifest is not a regular file: {}",
                        path.display()
                    ))),
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
                    Err(error) => Err(Error::io(Some(path), error)),
                }
            })
            .await
    }

    fn result_writer(
        &self,
        initialized: (PathBuf, Arc<ResultAccess>, Manifest),
        schema: SchemaRef,
    ) -> ResultWriter {
        let (directory, access, manifest) = initialized;
        let lease = Arc::new(QuotaLease::new(Arc::clone(&self.quota)));
        ResultWriter::start(directory, schema, lease, access, manifest, self.io.clone())
    }

    pub(crate) fn delete_query_artifacts(&self, query_id: &str) -> Result<()> {
        if !valid_query_id(query_id) {
            return Err(Error::InvalidArgument("invalid HTTP query ID".into()));
        }
        let directory = self.root.join(format!("q-{query_id}"));
        let accesses = Arc::clone(&self.accesses);
        let query_id = query_id.to_owned();
        self.io.run(move || {
            let result = match directory.symlink_metadata() {
                Ok(_) => access_for(&accesses, &query_id).delete(|| remove_owned_query(&directory)),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
                Err(error) => Err(Error::io(Some(directory), error)),
            };
            if result.is_ok() {
                accesses.lock().remove(&query_id);
            }
            result
        })
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
        let accesses = Arc::clone(&self.accesses);
        let query_id = query_id.to_owned();
        let io = self.io.clone();
        self.io.run(move || {
            let path = directory.join(MANIFEST_FILE);
            match path.symlink_metadata() {
                Ok(metadata)
                    if metadata.file_type().is_file() && !metadata.file_type().is_symlink() =>
                {
                    load_snapshot(directory, access_for(&accesses, &query_id), io).map(Some)
                }
                Ok(_) => Err(Error::InvalidArgument(format!(
                    "HTTP result manifest is not a regular file: {}",
                    path.display()
                ))),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
                Err(error) => Err(Error::io(Some(path), error)),
            }
        })
    }
}

pub(super) fn check_state(root: &std::path::Path) -> Result<usize> {
    recovery::check(root)
}

pub(super) fn repair_state(root: &std::path::Path) -> Result<usize> {
    recovery::repair(root)
}

pub(super) fn lock_state(root: &std::path::Path) -> Result<File> {
    acquire_existing_root_lock(root)
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

fn load_snapshot(
    directory: PathBuf,
    access: Arc<ResultAccess>,
    io: ServiceIoPool,
) -> Result<ResultSnapshot> {
    let query_id = directory
        .file_name()
        .and_then(|value| value.to_str())
        .and_then(|value| value.strip_prefix("q-"))
        .ok_or_else(|| Error::InvalidArgument("invalid HTTP result directory".into()))?;
    let path = directory.join(MANIFEST_FILE);
    let value = manifest::load(&path)?;
    value.validate(query_id)?;
    let schema = value.schema()?;
    Ok(ResultSnapshot {
        directory,
        schema,
        manifest: value,
        access,
        io,
    })
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum StoredResultState {
    Running,
    Completed,
    Interrupted,
    Failed,
    Invalidated,
}

impl From<ManifestState> for StoredResultState {
    fn from(value: ManifestState) -> Self {
        match value {
            ManifestState::Running => Self::Running,
            ManifestState::Completed => Self::Completed,
            ManifestState::Interrupted => Self::Interrupted,
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
    summary: ResultSummary,
    error: Option<String>,
    _access: Arc<ResultAccess>,
}

#[allow(dead_code)]
impl RecoveredResult {
    fn completed(query_id: String, result: Arc<StoredResult>, access: Arc<ResultAccess>) -> Self {
        let summary = result.summary();
        Self {
            query_id,
            state: StoredResultState::Completed,
            result: Some(result),
            summary,
            error: None,
            _access: access,
        }
    }

    fn interrupted(
        query_id: String,
        result: Arc<StoredResult>,
        error: Option<String>,
        access: Arc<ResultAccess>,
    ) -> Self {
        let summary = result.summary();
        Self {
            query_id,
            state: StoredResultState::Interrupted,
            result: Some(result),
            summary,
            error,
            _access: access,
        }
    }

    fn terminal(query_id: String, manifest: &Manifest, access: Arc<ResultAccess>) -> Self {
        Self {
            query_id,
            state: manifest.state.into(),
            result: None,
            summary: ResultSummary::from_manifest(manifest),
            error: manifest.error.clone(),
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

    pub(crate) fn summary(&self) -> ResultSummary {
        self.summary
    }

    pub(crate) fn error(&self) -> Option<&str> {
        self.error.as_deref()
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct ResultSummary {
    pub(crate) rows: u64,
    pub(crate) bytes: u64,
    pub(crate) batches: u64,
    pub(crate) updated_at_ms: u64,
}

impl ResultSummary {
    fn from_manifest(manifest: &Manifest) -> Self {
        Self {
            rows: manifest.rows,
            bytes: manifest.bytes,
            batches: manifest.next_batch_seq,
            updated_at_ms: manifest.updated_at_ms,
        }
    }

    pub(crate) fn available(self) -> bool {
        self.batches > 0
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
    io: ServiceIoPool,
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
        self.io
            .run_async(move || {
                let _guard = guard;
                let _access = access;
                reader::read_chunk_bytes(&directory, &entry)
            })
            .await
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
    io: ServiceIoPool,
}

impl StoredResult {
    fn completed(
        directory: PathBuf,
        schema: SchemaRef,
        manifest: &Manifest,
        lease: Arc<QuotaLease>,
        access: Arc<ResultAccess>,
        io: ServiceIoPool,
    ) -> Self {
        Self {
            directory,
            schema,
            rows: manifest.rows,
            batches: manifest.batches.clone(),
            completed_at: manifest::updated_at(manifest),
            lease,
            access,
            io,
        }
    }

    pub(crate) fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    pub(crate) fn rows(&self) -> u64 {
        self.rows
    }

    pub(crate) fn bytes(&self) -> u64 {
        self.batches.iter().map(|entry| entry.bytes).sum()
    }

    pub(crate) fn batch_count(&self) -> u64 {
        u64::try_from(self.batches.len()).unwrap_or(u64::MAX)
    }

    pub(crate) fn summary(&self) -> ResultSummary {
        ResultSummary {
            rows: self.rows(),
            bytes: self.bytes(),
            batches: self.batch_count(),
            updated_at_ms: self
                .completed_at
                .duration_since(SystemTime::UNIX_EPOCH)
                .ok()
                .map(|value| u64::try_from(value.as_millis()).unwrap_or(u64::MAX))
                .unwrap_or(0),
        }
    }

    pub(crate) fn expired(&self, ttl: Duration, now: SystemTime) -> bool {
        now.duration_since(self.completed_at)
            .is_ok_and(|age| age >= ttl)
    }

    pub(crate) fn snapshot(&self) -> Result<ResultSnapshot> {
        let directory = self.directory.clone();
        let access = Arc::clone(&self.access);
        let io = self.io.clone();
        self.io.run(move || load_snapshot(directory, access, io))
    }

    #[cfg(test)]
    pub(crate) fn mark_interrupted(&self, message: &str) -> Result<()> {
        let directory = self.directory.clone();
        let message = message.to_owned();
        self.io
            .run(move || mark_result_interrupted(&directory, &message))
    }

    pub(crate) async fn mark_interrupted_async(&self, message: &str) -> Result<()> {
        let directory = self.directory.clone();
        let message = message.to_owned();
        self.io
            .run_async(move || mark_result_interrupted(&directory, &message))
            .await
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
        self.io
            .run_async(move || {
                let _guard = guard;
                let _access = access;
                reader::read_pages(&directory, &schema, &batches, rows, offset, limit)
            })
            .await
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
        self.io
            .run_async(move || {
                let _guard = guard;
                let _access = access;
                reader::read_chunk_bytes(&directory, &entry)
            })
            .await
    }

    #[cfg(test)]
    pub(crate) fn delete(&self) -> Result<()> {
        let directory = self.directory.clone();
        let access = Arc::clone(&self.access);
        let result = self
            .io
            .run(move || access.delete(|| remove_owned_query(&directory)));
        if result.is_ok() {
            self.lease.release_all();
        } else {
            self.lease.retain_on_drop();
        }
        result
    }

    pub(crate) async fn delete_async(&self) -> Result<()> {
        let directory = self.directory.clone();
        let access = Arc::clone(&self.access);
        let result = self
            .io
            .run_async(move || access.delete(|| remove_owned_query(&directory)))
            .await;
        if result.is_ok() {
            self.lease.release_all();
        } else {
            self.lease.retain_on_drop();
        }
        result
    }
}

type WriterInitialization =
    Box<dyn FnOnce() -> Result<(PathBuf, Arc<ResultAccess>, Manifest)> + Send + 'static>;

fn writer_initialization(
    root: PathBuf,
    quota: Arc<QuotaPool>,
    accesses: AccessRegistry,
    query_id: String,
    schema: SchemaRef,
) -> WriterInitialization {
    Box::new(move || {
        quota.ensure_free_space(0).map_err(|error| {
            Error::ResourceExhausted(format!(
                "HTTP result filesystem has insufficient space: {error}"
            ))
        })?;
        let directory = root.join(format!("q-{query_id}"));
        if directory.exists() {
            return Err(Error::Internal(format!(
                "HTTP result directory for query {query_id} already exists"
            )));
        }
        secure_directory(&directory)?;
        let access = access_for(&accesses, &query_id);
        let initialized = (|| -> Result<Manifest> {
            write_private(&directory.join("OWNER"), QUERY_MARKER)?;
            secure_directory(&directory.join("batches"))?;
            let manifest = Manifest::new(&query_id, &schema)?;
            manifest::persist(&directory, &manifest)?;
            sync_directory(&root)?;
            Ok(manifest)
        })();
        match initialized {
            Ok(manifest) => Ok((directory, access, manifest)),
            Err(error) => match access.delete(|| remove_owned_query(&directory)) {
                Ok(()) => {
                    accesses.lock().remove(&query_id);
                    Err(error)
                }
                Err(cleanup) => Err(Error::Execution(format!(
                    "{error}; additionally failed to clean HTTP result directory: {cleanup}"
                ))),
            },
        }
    })
}

fn validate_query_id(query_id: &str) -> Result<()> {
    if valid_query_id(query_id) {
        Ok(())
    } else {
        Err(Error::InvalidArgument(
            "HTTP query ID contains unsafe characters".into(),
        ))
    }
}

fn mark_result_interrupted(directory: &std::path::Path, message: &str) -> Result<()> {
    let path = directory.join(MANIFEST_FILE);
    let mut value = manifest::load(&path)?;
    value.validate(
        directory
            .file_name()
            .and_then(|name| name.to_str())
            .and_then(|name| name.strip_prefix("q-"))
            .ok_or_else(|| Error::InvalidArgument("invalid HTTP result directory".into()))?,
    )?;
    if matches!(
        value.state,
        ManifestState::Running | ManifestState::Completed
    ) {
        value.interrupt(message);
        manifest::persist(directory, &value)?;
    }
    Ok(())
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
