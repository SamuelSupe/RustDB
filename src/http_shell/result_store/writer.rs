use std::{
    fs, io,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use arrow::{
    array::UInt32Array, compute::take_record_batch, datatypes::SchemaRef, ipc::writer::FileWriter,
    record_batch::RecordBatch,
};
use parking_lot::Mutex;
use tokio::sync::{mpsc, oneshot};

use crate::{Error, Result};

use super::{
    StoredResult,
    layout::{sync_directory, write_private},
    manifest::{self, BatchEntry, Manifest},
    quota::QuotaLease,
    reader::{chunk_path, schema_matches},
};
use crate::http_shell::result_read::ResultAccess;

const MAX_STORED_BATCH_BYTES: usize = 8 * 1024 * 1024;
const MAX_SOURCE_BATCH_BYTES: usize = 256 * 1024 * 1024;

pub(crate) struct ResultWriter {
    directory: PathBuf,
    schema: SchemaRef,
    sender: Option<mpsc::Sender<WriteRequest>>,
    worker: Option<tokio::task::JoinHandle<Result<()>>>,
    lease: Arc<QuotaLease>,
    access: Arc<ResultAccess>,
    rejected: Arc<AtomicBool>,
    terminal: Arc<Mutex<Option<WriteFailure>>>,
    manifest: Arc<Mutex<Manifest>>,
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
    pub(super) fn start(
        directory: PathBuf,
        schema: SchemaRef,
        lease: Arc<QuotaLease>,
        access: Arc<ResultAccess>,
        manifest: Manifest,
    ) -> Self {
        let rejected = Arc::new(AtomicBool::new(false));
        let terminal = Arc::new(Mutex::new(None));
        let manifest = Arc::new(Mutex::new(manifest));
        let (sender, receiver) = mpsc::channel(1);
        let worker_directory = directory.clone();
        let worker_schema = Arc::clone(&schema);
        let worker_lease = Arc::clone(&lease);
        let worker_rejected = Arc::clone(&rejected);
        let worker_terminal = Arc::clone(&terminal);
        let worker_manifest = Arc::clone(&manifest);
        let worker = tokio::task::spawn_blocking(move || {
            write_batches(
                &worker_directory,
                worker_schema,
                receiver,
                worker_lease,
                worker_rejected,
                worker_terminal,
                worker_manifest,
            )
        });
        Self {
            directory,
            schema,
            sender: Some(sender),
            worker: Some(worker),
            lease,
            access,
            rejected,
            terminal,
            manifest,
            owns_directory: true,
        }
    }

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

    pub(crate) async fn finish(mut self) -> Result<StoredResult> {
        self.sender.take();
        if let Err(error) = self.join_worker().await {
            return self.fail_finish(error);
        }
        let completed = {
            let mut current = self.manifest.lock();
            current.complete();
            current.clone()
        };
        if let Err(error) = manifest::persist(&self.directory, &completed) {
            return self.fail_finish(error);
        }
        let result = StoredResult::completed(
            self.directory.clone(),
            Arc::clone(&self.schema),
            &completed,
            Arc::clone(&self.lease),
            Arc::clone(&self.access),
        );
        self.owns_directory = false;
        Ok(result)
    }

    pub(crate) async fn abort(mut self) -> Result<()> {
        self.sender.take();
        if let Some(worker) = self.worker.take() {
            let _ = worker.await;
        }
        let result = mark_failed(
            &self.directory,
            &self.manifest,
            &self.lease,
            "query producer aborted before completion",
        );
        if result.is_ok() {
            self.owns_directory = false;
        }
        result
    }

    async fn join_worker(&mut self) -> Result<()> {
        self.worker
            .take()
            .ok_or_else(|| Error::Internal("HTTP result writer worker is missing".into()))?
            .await
            .map_err(|error| Error::Internal(format!("HTTP result writer panicked: {error}")))?
    }

    fn fail_finish<T>(&mut self, error: Error) -> Result<T> {
        match mark_failed(
            &self.directory,
            &self.manifest,
            &self.lease,
            &format!("result producer failed: {error}"),
        ) {
            Ok(()) => {
                self.owns_directory = false;
                Err(error)
            }
            Err(cleanup) => Err(Error::Execution(format!(
                "{error}; additionally failed to persist failed HTTP result state: {cleanup}"
            ))),
        }
    }

    fn terminal_error(&self) -> Error {
        self.terminal
            .lock()
            .clone()
            .map(WriteFailure::into_error)
            .unwrap_or_else(|| Error::Internal("HTTP result writer stopped unexpectedly".into()))
    }
}

impl Drop for ResultWriter {
    fn drop(&mut self) {
        self.sender.take();
        if let Some(worker) = self.worker.take() {
            match futures::executor::block_on(worker) {
                Ok(Ok(())) => {}
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
            && let Err(error) = mark_failed(
                &self.directory,
                &self.manifest,
                &self.lease,
                "result writer was abandoned",
            )
        {
            tracing::error!(%error, path = %self.directory.display(), "failed to persist abandoned HTTP result");
        }
        if self.rejected.load(Ordering::Acquire) {
            tracing::warn!(path = %self.directory.display(), "HTTP result quota rejected a write");
        }
    }
}

fn write_batches(
    directory: &Path,
    schema: SchemaRef,
    mut receiver: mpsc::Receiver<WriteRequest>,
    lease: Arc<QuotaLease>,
    rejected: Arc<AtomicBool>,
    terminal: Arc<Mutex<Option<WriteFailure>>>,
    manifest: Arc<Mutex<Manifest>>,
) -> Result<()> {
    while let Some(request) = receiver.blocking_recv() {
        let outcome = write_request(
            directory,
            &schema,
            &request.batch,
            &lease,
            &rejected,
            &manifest,
        );
        if let Err(error) = outcome {
            let failure = write_failure(&error, rejected.load(Ordering::Acquire));
            *terminal.lock() = Some(failure.clone());
            let _ = request.completed.send(Err(failure));
            return Err(error);
        }
        let _ = request.completed.send(Ok(()));
    }
    Ok(())
}

fn write_request(
    directory: &Path,
    schema: &SchemaRef,
    batch: &RecordBatch,
    lease: &QuotaLease,
    rejected: &AtomicBool,
    manifest: &Mutex<Manifest>,
) -> Result<()> {
    if batch.num_rows() == 0 {
        return Ok(());
    }
    if !schema_matches(batch, schema) {
        return Err(Error::Execution(
            "HTTP result batch schema changed while writing".into(),
        ));
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
    let first_seq = manifest.lock().next_batch_seq;
    let mut entries = Vec::new();
    let mut offset = 0usize;
    while offset < batch.num_rows() {
        let (compact, taken) = compact_batch(batch, offset, estimated)?;
        let seq = first_seq
            .checked_add(u64::try_from(entries.len()).unwrap_or(u64::MAX))
            .ok_or_else(|| Error::ResourceExhausted("result batch sequence overflowed".into()))?;
        let (bytes, sha256) = write_chunk(directory, schema, &compact, seq, lease, rejected)?;
        entries.push(BatchEntry {
            seq,
            rows: u64::try_from(compact.num_rows()).unwrap_or(u64::MAX),
            bytes,
            sha256,
        });
        offset = offset.saturating_add(taken);
    }
    let snapshot = {
        let mut current = manifest.lock();
        current.append(entries)?;
        current.clone()
    };
    manifest::persist(directory, &snapshot)
}

fn compact_batch(
    batch: &RecordBatch,
    offset: usize,
    estimated: usize,
) -> Result<(RecordBatch, usize)> {
    let mut take = estimated.min(batch.num_rows() - offset).max(1);
    loop {
        let end = offset.saturating_add(take);
        let indices = (offset..end)
            .map(u32::try_from)
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|_| Error::ResourceExhausted("HTTP result row index overflowed".into()))?;
        let compact = take_record_batch(batch, &UInt32Array::from(indices))?;
        if compact.get_array_memory_size() <= MAX_STORED_BATCH_BYTES {
            return Ok((compact, take));
        }
        if take == 1 {
            return Err(Error::ResourceExhausted(
                "one result row exceeds the HTTP result memory limit".into(),
            ));
        }
        take = (take / 2).max(1);
    }
}

fn write_chunk(
    directory: &Path,
    schema: &SchemaRef,
    batch: &RecordBatch,
    seq: u64,
    lease: &QuotaLease,
    rejected: &AtomicBool,
) -> Result<(u64, String)> {
    let mut encoded = Vec::new();
    {
        let mut writer = FileWriter::try_new(&mut encoded, schema)?;
        writer.write(batch)?;
        writer.finish()?;
    }
    let bytes = u64::try_from(encoded.len()).unwrap_or(u64::MAX);
    let sha256 = manifest::sha256(&encoded);
    if let Err(error) = lease.reserve(bytes) {
        rejected.store(true, Ordering::Release);
        return Err(Error::io(None, error));
    }
    let completed = chunk_path(directory, seq);
    let partial = completed.with_extension("arrow.partial");
    let outcome = write_private(&partial, &encoded)
        .and_then(|_| {
            fs::rename(&partial, &completed)
                .map_err(|error| Error::io(Some(completed.clone()), error))
        })
        .and_then(|_| sync_directory(&directory.join("batches")));
    if let Err(error) = outcome {
        let _ = fs::remove_file(&partial);
        let _ = fs::remove_file(&completed);
        lease.release(bytes);
        return Err(error);
    }
    Ok((bytes, sha256))
}

pub(super) fn clear_batch_files(directory: &Path, lease: Option<&QuotaLease>) -> Result<u64> {
    let batches = directory.join("batches");
    let mut removed = 0_u64;
    if !batches.exists() {
        return Ok(0);
    }
    for entry in fs::read_dir(&batches).map_err(|error| Error::io(Some(batches.clone()), error))? {
        let entry = entry.map_err(|error| Error::io(Some(batches.clone()), error))?;
        let metadata = entry
            .path()
            .symlink_metadata()
            .map_err(|error| Error::io(Some(entry.path()), error))?;
        if !metadata.file_type().is_file() || metadata.file_type().is_symlink() {
            return Err(Error::InvalidArgument(format!(
                "unexpected entry in HTTP result batch directory: {}",
                entry.path().display()
            )));
        }
        fs::remove_file(entry.path()).map_err(|error| Error::io(Some(entry.path()), error))?;
        removed = removed.saturating_add(metadata.len());
    }
    sync_directory(&batches)?;
    if let Some(lease) = lease {
        lease.release(removed);
    }
    Ok(removed)
}

fn mark_failed(
    directory: &Path,
    manifest: &Mutex<Manifest>,
    lease: &QuotaLease,
    message: &str,
) -> Result<()> {
    clear_batch_files(directory, Some(lease))?;
    let failed = {
        let mut current = manifest.lock();
        current.fail(message);
        current.clone()
    };
    manifest::persist(directory, &failed)
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
