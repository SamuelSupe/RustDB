use std::{
    fs::{self, File, OpenOptions},
    io::{self, Write},
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::{Duration, SystemTime},
};

use arrow::{
    array::UInt32Array,
    compute::take_record_batch,
    datatypes::SchemaRef,
    ipc::{reader::FileReader, writer::FileWriter},
    record_batch::RecordBatch,
};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use sysinfo::Disks;
use tokio::sync::{mpsc, oneshot};

use crate::{Error, Result};

use super::result_read::{ResultAccess, ResultReadGuard};

const ROOT_MARKER: &[u8] = b"rustdb-http-results-v1\n";
const QUERY_MARKER: &[u8] = b"rustdb-http-query-result-v1\n";
const ROOT_LOCK_FILE: &str = ".server.lock";
const ROOT_LOCK_MARKER: &[u8] = b"rustdb-http-result-lock-v1\n";
const DEFAULT_GLOBAL_LIMIT: u64 = 10 * 1024 * 1024 * 1024;
const MIN_FREE_BYTES: u64 = 1024 * 1024 * 1024;
const SPACE_CHECK_INTERVAL: u64 = 64 * 1024 * 1024;
const MAX_STORED_BATCH_BYTES: usize = 8 * 1024 * 1024;
const MAX_SOURCE_BATCH_BYTES: usize = 256 * 1024 * 1024;
const MAX_READ_MEMORY_BYTES: usize = 32 * 1024 * 1024;

#[derive(Clone, Debug)]
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
    _lock: File,
}

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
        scavenge_owned_queries(&config.directory)?;
        let space = filesystem_space(&config.directory);
        let capacity_limit = space
            .as_ref()
            .map(|space| space.total / 10)
            .unwrap_or(DEFAULT_GLOBAL_LIMIT);
        let free_reserve = space
            .as_ref()
            .map(|space| MIN_FREE_BYTES.max(space.total / 10))
            .unwrap_or(0);
        let writable = space
            .as_ref()
            .map(|space| space.available.saturating_sub(free_reserve))
            .unwrap_or(DEFAULT_GLOBAL_LIMIT);
        let global = config
            .global_limit_bytes
            .unwrap_or(DEFAULT_GLOBAL_LIMIT.min(capacity_limit))
            .min(writable);
        if global == 0 {
            return Err(Error::ResourceExhausted(
                "HTTP result store cannot preserve its filesystem free-space reserve".into(),
            ));
        }
        let per_query = config.query_limit_bytes.unwrap_or(global / 4).max(1);
        if per_query > global {
            return Err(Error::InvalidArgument(
                "HTTP per-query result limit cannot exceed the global limit".into(),
            ));
        }
        Ok(Self {
            root: config.directory.clone(),
            ttl: config.ttl,
            quota: Arc::new(QuotaPool::new(
                global,
                per_query,
                config.directory,
                free_reserve,
            )),
            _lock: root_lock,
        })
    }

    pub(crate) fn ttl(&self) -> Duration {
        self.ttl
    }

    pub(crate) fn writer(&self, query_id: &str, schema: SchemaRef) -> Result<ResultWriter> {
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
        write_private(&directory.join("OWNER"), QUERY_MARKER)?;
        let partial = directory.join("result.arrow.partial");
        let completed = directory.join("result.arrow");
        let lease = Arc::new(QuotaLease::new(Arc::clone(&self.quota)));
        let rejected = Arc::new(AtomicBool::new(false));
        let terminal = Arc::new(Mutex::new(None));
        let (sender, receiver) = mpsc::channel(1);
        let writer_lease = Arc::clone(&lease);
        let writer_rejected = Arc::clone(&rejected);
        let writer_schema = Arc::clone(&schema);
        let writer_partial = partial.clone();
        let writer_terminal = Arc::clone(&terminal);
        let worker = tokio::task::spawn_blocking(move || {
            write_batches(
                &writer_partial,
                writer_schema,
                receiver,
                writer_lease,
                writer_rejected,
                writer_terminal,
            )
        });
        Ok(ResultWriter {
            directory,
            partial,
            completed,
            schema,
            sender: Some(sender),
            worker: Some(worker),
            lease,
            rejected,
            terminal,
            owns_directory: true,
        })
    }
}

pub(crate) struct ResultWriter {
    directory: PathBuf,
    partial: PathBuf,
    completed: PathBuf,
    schema: SchemaRef,
    sender: Option<mpsc::Sender<WriteRequest>>,
    worker: Option<tokio::task::JoinHandle<Result<WriteSummary>>>,
    lease: Arc<QuotaLease>,
    rejected: Arc<AtomicBool>,
    terminal: Arc<Mutex<Option<WriteFailure>>>,
    owns_directory: bool,
}

struct WriteRequest {
    batch: RecordBatch,
    completed: oneshot::Sender<std::result::Result<(), WriteFailure>>,
}

#[derive(Clone)]
enum WriteFailure {
    ResourceExhausted(String),
    Execution(String),
}

impl WriteFailure {
    fn into_error(self) -> Error {
        match self {
            Self::ResourceExhausted(message) => Error::ResourceExhausted(message),
            Self::Execution(message) => Error::Execution(message),
        }
    }
}

impl ResultWriter {
    pub(crate) async fn write(&self, batch: RecordBatch) -> Result<()> {
        let (completed, completion) = oneshot::channel();
        self.sender
            .as_ref()
            .ok_or_else(|| Error::Internal("HTTP result writer is closed".into()))?
            .send(WriteRequest { batch, completed })
            .await
            .map_err(|_| self.terminal_error())?;
        completion
            .await
            .map_err(|_| self.terminal_error())?
            .map_err(WriteFailure::into_error)
    }

    fn terminal_error(&self) -> Error {
        self.terminal
            .lock()
            .clone()
            .map(WriteFailure::into_error)
            .unwrap_or_else(|| Error::Internal("HTTP result writer stopped unexpectedly".into()))
    }

    pub(crate) async fn finish(mut self) -> Result<StoredResult> {
        self.sender.take();
        let summary = match self
            .worker
            .take()
            .ok_or_else(|| Error::Internal("HTTP result writer worker is missing".into()))?
            .await
            .map_err(|error| Error::Internal(format!("HTTP result writer panicked: {error}")))
            .and_then(|result| result)
        {
            Ok(summary) => summary,
            Err(error) => return cleanup_failed_finish(&self.directory, error),
        };
        if let Err(error) = fs::rename(&self.partial, &self.completed)
            .map_err(|error| Error::io(Some(self.completed.clone()), error))
        {
            return cleanup_failed_finish(&self.directory, error);
        }
        let index = StoredIndex {
            rows: summary.rows,
            batch_starts: summary.batch_starts.clone(),
        };
        if let Err(error) = write_private_json(&self.directory.join("index.json"), &index) {
            return cleanup_failed_finish(&self.directory, error);
        }
        let result = StoredResult {
            directory: self.directory.clone(),
            path: self.completed.clone(),
            schema: Arc::clone(&self.schema),
            rows: summary.rows,
            batch_starts: summary.batch_starts,
            created_at: SystemTime::now(),
            lease: Arc::clone(&self.lease),
            access: ResultAccess::new(),
        };
        self.owns_directory = false;
        Ok(result)
    }

    pub(crate) async fn abort(mut self) -> Result<()> {
        self.sender.take();
        if let Some(worker) = self.worker.take() {
            let _ = worker.await;
        }
        let result = remove_owned_query(&self.directory);
        if result.is_ok() {
            self.owns_directory = false;
        }
        result
    }
}

fn cleanup_failed_finish<T>(directory: &Path, error: Error) -> Result<T> {
    match remove_owned_query(directory) {
        Ok(()) => Err(error),
        Err(cleanup) => Err(Error::Execution(format!(
            "{error}; additionally failed to clean HTTP result: {cleanup}"
        ))),
    }
}

impl Drop for ResultWriter {
    fn drop(&mut self) {
        self.sender.take();
        if let Some(worker) = self.worker.take() {
            match futures::executor::block_on(worker) {
                Ok(Ok(_)) => {}
                Ok(Err(error)) => tracing::error!(
                    %error,
                    path = %self.directory.display(),
                    "HTTP result writer failed"
                ),
                Err(error) => tracing::error!(
                    %error,
                    path = %self.directory.display(),
                    "HTTP result writer panicked"
                ),
            }
        }
        if self.owns_directory
            && self.directory.exists()
            && let Err(error) = remove_owned_query(&self.directory)
        {
            tracing::error!(%error, path = %self.directory.display(), "failed to clean abandoned HTTP result");
        }
        if self.rejected.load(Ordering::Acquire) {
            tracing::warn!(path = %self.directory.display(), "HTTP result quota rejected a write");
        }
    }
}

pub(crate) struct StoredResult {
    directory: PathBuf,
    path: PathBuf,
    schema: SchemaRef,
    rows: u64,
    batch_starts: Vec<u64>,
    created_at: SystemTime,
    #[allow(dead_code)]
    lease: Arc<QuotaLease>,
    access: Arc<ResultAccess>,
}

impl StoredResult {
    pub(crate) fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    pub(crate) fn rows(&self) -> u64 {
        self.rows
    }

    pub(crate) fn expired(&self, ttl: Duration, now: SystemTime) -> bool {
        now.duration_since(self.created_at)
            .is_ok_and(|age| age >= ttl)
    }

    pub(crate) async fn read(
        &self,
        offset: u64,
        limit: usize,
        guard: ResultReadGuard,
    ) -> Result<Vec<RecordBatch>> {
        if offset > self.rows {
            return Err(Error::InvalidArgument(format!(
                "result offset {offset} exceeds row count {}",
                self.rows
            )));
        }
        let path = self.path.clone();
        let starts = self.batch_starts.clone();
        let access = self.access.read().await?;
        tokio::task::spawn_blocking(move || {
            let _guard = guard;
            let _access = access;
            read_batches(&path, &starts, offset, limit)
        })
        .await
        .map_err(|error| Error::Internal(format!("HTTP result reader panicked: {error}")))?
    }

    pub(crate) fn delete(&self) -> Result<()> {
        self.access.delete(|| remove_owned_query(&self.directory))
    }
}

impl Drop for StoredResult {
    fn drop(&mut self) {
        if self.directory.exists()
            && let Err(error) = remove_owned_query(&self.directory)
        {
            tracing::error!(%error, path = %self.directory.display(), "failed to remove HTTP result");
        }
    }
}

struct WriteSummary {
    rows: u64,
    batch_starts: Vec<u64>,
}

#[derive(Deserialize, Serialize)]
struct StoredIndex {
    rows: u64,
    batch_starts: Vec<u64>,
}

fn write_batches(
    path: &Path,
    schema: SchemaRef,
    mut receiver: mpsc::Receiver<WriteRequest>,
    lease: Arc<QuotaLease>,
    rejected: Arc<AtomicBool>,
    terminal: Arc<Mutex<Option<WriteFailure>>>,
) -> Result<WriteSummary> {
    let file = private_file(path)?;
    let quota_writer = QuotaWriter {
        file,
        lease,
        rejected: Arc::clone(&rejected),
    };
    let mut rows = 0_u64;
    let mut starts = Vec::new();
    let outcome = (|| -> Result<()> {
        let mut writer = FileWriter::try_new(quota_writer, &schema)?;
        while let Some(request) = receiver.blocking_recv() {
            if let Err(error) =
                write_batch_chunks(&mut writer, &request.batch, &mut rows, &mut starts)
            {
                let failure = write_failure(&error, rejected.load(Ordering::Acquire));
                *terminal.lock() = Some(failure.clone());
                let _ = request.completed.send(Err(failure));
                return Err(error);
            }
            let _ = request.completed.send(Ok(()));
        }
        writer.finish()?;
        Ok(())
    })();
    if rejected.load(Ordering::Acquire) {
        let failure = WriteFailure::ResourceExhausted(
            "HTTP query result exceeded its disk quota or free-space reserve".into(),
        );
        *terminal.lock() = Some(failure.clone());
        return Err(failure.into_error());
    }
    if let Err(error) = outcome {
        let failure = write_failure(&error, false);
        *terminal.lock() = Some(failure);
        return Err(error);
    }
    Ok(WriteSummary {
        rows,
        batch_starts: starts,
    })
}

fn write_batch_chunks(
    writer: &mut FileWriter<QuotaWriter>,
    batch: &RecordBatch,
    rows: &mut u64,
    starts: &mut Vec<u64>,
) -> Result<()> {
    if batch.num_rows() == 0 {
        return Ok(());
    }
    let source_bytes = batch.get_array_memory_size();
    if source_bytes > MAX_SOURCE_BATCH_BYTES {
        return Err(Error::ResourceExhausted(format!(
            "one HTTP result batch uses {source_bytes} bytes; the limit is {MAX_SOURCE_BATCH_BYTES}"
        )));
    }
    let estimated = batch
        .num_rows()
        .saturating_mul(MAX_STORED_BATCH_BYTES)
        .checked_div(source_bytes.max(1))
        .unwrap_or(1)
        .max(1);
    let mut offset = 0usize;
    while offset < batch.num_rows() {
        let mut take = estimated.min(batch.num_rows() - offset).max(1);
        let compact = loop {
            let end = offset.saturating_add(take);
            let indices = (offset..end)
                .map(u32::try_from)
                .collect::<std::result::Result<Vec<_>, _>>()
                .map_err(|_| Error::ResourceExhausted("HTTP result row index overflowed".into()))?;
            let compact = take_record_batch(batch, &UInt32Array::from(indices))?;
            if compact.get_array_memory_size() <= MAX_STORED_BATCH_BYTES {
                break compact;
            }
            if take == 1 {
                return Err(Error::ResourceExhausted(
                    "one result row exceeds the HTTP result memory limit".into(),
                ));
            }
            take = (take / 2).max(1);
        };
        writer.write(&compact)?;
        starts.push(*rows);
        *rows = rows
            .checked_add(u64::try_from(compact.num_rows()).unwrap_or(u64::MAX))
            .ok_or_else(|| Error::ResourceExhausted("result row count overflowed".into()))?;
        offset = offset.saturating_add(take);
    }
    Ok(())
}

fn write_failure(error: &Error, exhausted: bool) -> WriteFailure {
    if exhausted
        || matches!(
            error,
            Error::ResourceExhausted(_) | Error::NativeDiskQuotaExceeded { .. }
        )
        || matches!(error, Error::Io { source, .. } if source.kind() == io::ErrorKind::StorageFull)
    {
        WriteFailure::ResourceExhausted(
            "HTTP query result exceeded its disk quota or free-space reserve".into(),
        )
    } else {
        WriteFailure::Execution(format!("failed to persist HTTP query result: {error}"))
    }
}

fn read_batches(
    path: &Path,
    starts: &[u64],
    offset: u64,
    limit: usize,
) -> Result<Vec<RecordBatch>> {
    if limit == 0 {
        return Ok(Vec::new());
    }
    let file = File::open(path).map_err(|error| Error::io(Some(path.to_owned()), error))?;
    let mut reader = FileReader::try_new(file, None)?;
    let first = starts
        .partition_point(|start| *start <= offset)
        .saturating_sub(1);
    if !starts.is_empty() {
        reader.set_index(first)?;
    }
    let mut current = starts.get(first).copied().unwrap_or(0);
    let mut remaining = limit;
    let mut output = Vec::new();
    let mut output_bytes = 0usize;
    for batch in &mut reader {
        let batch = batch?;
        let batch_end = current.saturating_add(u64::try_from(batch.num_rows()).unwrap_or(u64::MAX));
        if batch_end <= offset {
            current = batch_end;
            continue;
        }
        let local = usize::try_from(offset.saturating_sub(current)).unwrap_or(usize::MAX);
        if local >= batch.num_rows() {
            current = batch_end;
            continue;
        }
        let take = remaining.min(batch.num_rows() - local);
        let sliced = batch.slice(local, take);
        let batch_bytes = sliced.get_array_memory_size();
        if !output.is_empty() && output_bytes.saturating_add(batch_bytes) > MAX_READ_MEMORY_BYTES {
            break;
        }
        if batch_bytes > MAX_READ_MEMORY_BYTES {
            return Err(Error::ResourceExhausted(
                "one stored result batch exceeds the HTTP read memory limit".into(),
            ));
        }
        output_bytes = output_bytes.saturating_add(batch_bytes);
        output.push(sliced);
        remaining -= take;
        if remaining == 0 {
            break;
        }
        current = batch_end;
    }
    Ok(output)
}

struct QuotaPool {
    used: Mutex<u64>,
    global_limit: u64,
    query_limit: u64,
    directory: PathBuf,
    free_reserve: u64,
}

impl QuotaPool {
    fn new(global_limit: u64, query_limit: u64, directory: PathBuf, free_reserve: u64) -> Self {
        Self {
            used: Mutex::new(0),
            global_limit,
            query_limit,
            directory,
            free_reserve,
        }
    }

    fn ensure_free_space(&self, additional: u64) -> io::Result<()> {
        let Some(space) = filesystem_space(&self.directory) else {
            return Ok(());
        };
        if space.available.saturating_sub(additional) < self.free_reserve {
            Err(storage_full())
        } else {
            Ok(())
        }
    }
}

struct QuotaLease {
    pool: Arc<QuotaPool>,
    bytes: AtomicU64,
    next_space_check: AtomicU64,
}

impl QuotaLease {
    fn new(pool: Arc<QuotaPool>) -> Self {
        Self {
            pool,
            bytes: AtomicU64::new(0),
            next_space_check: AtomicU64::new(0),
        }
    }

    fn add(&self, bytes: u64) -> io::Result<()> {
        let current = self.bytes.load(Ordering::Acquire);
        let query_next = current.checked_add(bytes).ok_or_else(storage_full)?;
        let mut global = self.pool.used.lock();
        let global_next = global.checked_add(bytes).ok_or_else(storage_full)?;
        if query_next > self.pool.query_limit || global_next > self.pool.global_limit {
            return Err(storage_full());
        }
        if current >= self.next_space_check.load(Ordering::Acquire) {
            self.pool.ensure_free_space(bytes)?;
            self.next_space_check.store(
                query_next.saturating_add(SPACE_CHECK_INTERVAL),
                Ordering::Release,
            );
        }
        *global = global_next;
        self.bytes.store(query_next, Ordering::Release);
        Ok(())
    }

    fn rollback(&self, bytes: u64) {
        self.bytes.fetch_sub(bytes, Ordering::AcqRel);
        let mut global = self.pool.used.lock();
        *global = global.saturating_sub(bytes);
    }
}

impl Drop for QuotaLease {
    fn drop(&mut self) {
        let bytes = self.bytes.swap(0, Ordering::AcqRel);
        let mut global = self.pool.used.lock();
        *global = global.saturating_sub(bytes);
    }
}

struct QuotaWriter {
    file: File,
    lease: Arc<QuotaLease>,
    rejected: Arc<AtomicBool>,
}

impl Write for QuotaWriter {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        let bytes = u64::try_from(buffer.len()).unwrap_or(u64::MAX);
        if let Err(error) = self.lease.add(bytes) {
            self.rejected.store(true, Ordering::Release);
            return Err(error);
        }
        match self.file.write(buffer) {
            Ok(written) => {
                let unwritten = buffer.len() - written;
                if unwritten > 0 {
                    self.lease
                        .rollback(u64::try_from(unwritten).unwrap_or(u64::MAX));
                }
                Ok(written)
            }
            Err(error) => {
                if error.kind() == io::ErrorKind::StorageFull {
                    self.rejected.store(true, Ordering::Release);
                }
                self.lease.rollback(bytes);
                Err(error)
            }
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        self.file.flush().inspect_err(|error| {
            if error.kind() == io::ErrorKind::StorageFull {
                self.rejected.store(true, Ordering::Release);
            }
        })
    }
}

fn storage_full() -> io::Error {
    io::Error::new(io::ErrorKind::StorageFull, "HTTP result quota exceeded")
}

fn establish_root_marker(root: &Path) -> Result<()> {
    let marker = root.join("OWNER");
    if marker.exists() {
        let contents = fs::read(&marker).map_err(|error| Error::io(Some(marker.clone()), error))?;
        if contents != ROOT_MARKER {
            return Err(Error::InvalidArgument(format!(
                "HTTP result directory has an unknown owner marker: {}",
                root.display()
            )));
        }
        return Ok(());
    }
    if fs::read_dir(root)
        .map_err(|error| Error::io(Some(root.to_owned()), error))?
        .next()
        .is_some()
    {
        return Err(Error::InvalidArgument(format!(
            "refusing to claim non-empty HTTP result directory {}",
            root.display()
        )));
    }
    write_private(&marker, ROOT_MARKER)
}

fn acquire_root_lock(root: &Path) -> Result<File> {
    use std::io::{Read, Seek};

    let path = root.join(ROOT_LOCK_FILE);
    let mut options = OpenOptions::new();
    options.read(true).write(true).create(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options
        .open(&path)
        .map_err(|error| Error::io(Some(path.clone()), error))?;
    let metadata =
        fs::symlink_metadata(&path).map_err(|error| Error::io(Some(path.clone()), error))?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(Error::InvalidArgument(format!(
            "HTTP result lock is not a regular file: {}",
            path.display()
        )));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if metadata.permissions().mode() & 0o077 != 0 {
            return Err(Error::InvalidArgument(format!(
                "HTTP result lock {} must be private",
                path.display()
            )));
        }
    }
    file.try_lock().map_err(|error| {
        Error::InvalidArgument(format!(
            "HTTP result directory {} is already in use: {error}",
            root.display()
        ))
    })?;
    let mut marker = Vec::new();
    file.rewind()
        .and_then(|_| file.read_to_end(&mut marker))
        .map_err(|error| Error::io(Some(path.clone()), error))?;
    if marker.is_empty() {
        file.rewind()
            .and_then(|_| file.write_all(ROOT_LOCK_MARKER))
            .and_then(|_| file.set_len(ROOT_LOCK_MARKER.len() as u64))
            .and_then(|_| file.sync_all())
            .map_err(|error| Error::io(Some(path.clone()), error))?;
    } else if marker != ROOT_LOCK_MARKER {
        return Err(Error::InvalidArgument(format!(
            "invalid HTTP result lock marker at {}",
            path.display()
        )));
    }
    Ok(file)
}

fn scavenge_owned_queries(root: &Path) -> Result<()> {
    for entry in fs::read_dir(root).map_err(|error| Error::io(Some(root.to_owned()), error))? {
        let entry = entry.map_err(|error| Error::io(Some(root.to_owned()), error))?;
        if entry.file_name() == "OWNER"
            || !entry
                .file_type()
                .map_err(|error| Error::io(None, error))?
                .is_dir()
        {
            continue;
        }
        let path = entry.path();
        let marker = path.join("OWNER");
        if fs::read(&marker).ok().as_deref() == Some(QUERY_MARKER) {
            remove_owned_query(&path)?;
        }
    }
    Ok(())
}

fn remove_owned_query(path: &Path) -> Result<()> {
    let marker = path.join("OWNER");
    if fs::read(&marker).ok().as_deref() != Some(QUERY_MARKER) {
        return Err(Error::InvalidArgument(format!(
            "refusing to remove unowned HTTP result directory {}",
            path.display()
        )));
    }
    fs::remove_dir_all(path).map_err(|error| Error::io(Some(path.to_owned()), error))
}

struct FilesystemSpace {
    total: u64,
    available: u64,
}

fn filesystem_space(path: &Path) -> Option<FilesystemSpace> {
    let canonical = path.canonicalize().ok()?;
    Disks::new_with_refreshed_list()
        .list()
        .iter()
        .filter(|disk| canonical.starts_with(disk.mount_point()))
        .max_by_key(|disk| disk.mount_point().components().count())
        .map(|disk| FilesystemSpace {
            total: disk.total_space(),
            available: disk.available_space(),
        })
}

fn secure_directory(path: &Path) -> Result<()> {
    let created = match fs::symlink_metadata(path) {
        Ok(_) => false,
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            fs::create_dir_all(path).map_err(|error| Error::io(Some(path.to_owned()), error))?;
            true
        }
        Err(error) => return Err(Error::io(Some(path.to_owned()), error)),
    };
    let metadata =
        fs::symlink_metadata(path).map_err(|error| Error::io(Some(path.to_owned()), error))?;
    if !metadata.file_type().is_dir() || metadata.file_type().is_symlink() {
        return Err(Error::InvalidArgument(format!(
            "HTTP state path is not a real directory: {}",
            path.display()
        )));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if created {
            fs::set_permissions(path, fs::Permissions::from_mode(0o700))
                .map_err(|error| Error::io(Some(path.to_owned()), error))?;
        } else if metadata.permissions().mode() & 0o077 != 0 {
            return Err(Error::InvalidArgument(format!(
                "HTTP state directory {} must be private",
                path.display()
            )));
        }
    }
    Ok(())
}

fn private_file(path: &Path) -> Result<File> {
    let mut options = OpenOptions::new();
    options.create_new(true).write(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    options
        .open(path)
        .map_err(|error| Error::io(Some(path.to_owned()), error))
}

fn write_private(path: &Path, bytes: &[u8]) -> Result<()> {
    let mut file = private_file(path)?;
    file.write_all(bytes)
        .and_then(|_| file.sync_all())
        .map_err(|error| Error::io(Some(path.to_owned()), error))
}

fn write_private_json(path: &Path, value: &impl Serialize) -> Result<()> {
    let bytes = serde_json::to_vec(value)
        .map_err(|error| Error::Internal(format!("failed to encode HTTP result index: {error}")))?;
    write_private(path, &bytes)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::{
        array::Int64Array,
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    };

    use super::{ResultStore, ResultStoreConfig};
    use crate::http_shell::result_read::ResultReadTracker;

    #[tokio::test]
    async fn persists_and_pages_an_indexed_result() {
        let directory = tempfile::tempdir().unwrap();
        let store =
            ResultStore::open(ResultStoreConfig::new(directory.path().join("results"))).unwrap();
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
        let writer = store.writer("test", Arc::clone(&schema)).unwrap();
        writer
            .write(
                RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1, 2, 3]))])
                    .unwrap(),
            )
            .await
            .unwrap();
        let result = writer.finish().await.unwrap();
        let reads = ResultReadTracker::new();
        let batches = result.read(1, 1, reads.start().unwrap()).await.unwrap();
        assert_eq!(batches[0].num_rows(), 1);
        assert_eq!(result.rows(), 3);
        result.delete().unwrap();
    }

    #[test]
    fn result_root_has_one_process_owner() {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path().join("results");
        let first = ResultStore::open(ResultStoreConfig::new(&root)).unwrap();
        assert!(ResultStore::open(ResultStoreConfig::new(&root)).is_err());
        drop(first);
        ResultStore::open(ResultStoreConfig::new(&root)).unwrap();
    }
}
