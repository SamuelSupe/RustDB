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

use crate::{Error, Result};

use super::{
    StoredResult,
    layout::{sync_directory, write_private},
    manifest::{self, BatchEntry, Manifest},
    quota::QuotaLease,
    reader::{chunk_path, schema_matches},
};
use crate::http_shell::result_read::ResultAccess;
use crate::http_shell::service_io::ServiceIoPool;

const MAX_STORED_BATCH_BYTES: usize = 8 * 1024 * 1024;
const MAX_SOURCE_BATCH_BYTES: usize = 256 * 1024 * 1024;

pub(crate) struct ResultWriter {
    directory: PathBuf,
    schema: SchemaRef,
    lease: Arc<QuotaLease>,
    access: Arc<ResultAccess>,
    rejected: Arc<AtomicBool>,
    manifest: Arc<Mutex<Manifest>>,
    io: ServiceIoPool,
    owns_directory: bool,
    preserve_on_drop: Option<Arc<AtomicBool>>,
}

impl ResultWriter {
    pub(super) fn start(
        directory: PathBuf,
        schema: SchemaRef,
        lease: Arc<QuotaLease>,
        access: Arc<ResultAccess>,
        manifest: Manifest,
        io: ServiceIoPool,
    ) -> Self {
        let rejected = Arc::new(AtomicBool::new(false));
        let manifest = Arc::new(Mutex::new(manifest));
        Self {
            directory,
            schema,
            lease,
            access,
            rejected,
            manifest,
            io,
            owns_directory: true,
            preserve_on_drop: None,
        }
    }

    pub(crate) fn preserve_on_drop_when(mut self, interrupted: Arc<AtomicBool>) -> Self {
        self.preserve_on_drop = Some(interrupted);
        self
    }

    pub(crate) async fn write(&self, batch: RecordBatch) -> Result<()> {
        let directory = self.directory.clone();
        let schema = Arc::clone(&self.schema);
        let lease = Arc::clone(&self.lease);
        let rejected = Arc::clone(&self.rejected);
        let rejection_state = Arc::clone(&rejected);
        let manifest = Arc::clone(&self.manifest);
        self.io
            .run_async(move || {
                write_request(&directory, &schema, &batch, &lease, &rejected, &manifest)
            })
            .await
            .map_err(|error| normalize_write_error(error, rejection_state.load(Ordering::Acquire)))
    }

    pub(crate) async fn finish(mut self) -> Result<StoredResult> {
        let completed = {
            let mut current = self.manifest.lock();
            current.complete();
            current.clone()
        };
        let directory = self.directory.clone();
        let persisted = completed.clone();
        if let Err(error) = self
            .io
            .run_async(move || manifest::persist(&directory, &persisted))
            .await
        {
            return self.fail_finish(error).await;
        }
        let result = StoredResult::completed(
            self.directory.clone(),
            Arc::clone(&self.schema),
            &completed,
            Arc::clone(&self.lease),
            Arc::clone(&self.access),
            self.io.clone(),
        );
        self.owns_directory = false;
        Ok(result)
    }

    pub(crate) async fn abort(mut self) -> Result<()> {
        let directory = self.directory.clone();
        let manifest = Arc::clone(&self.manifest);
        let lease = Arc::clone(&self.lease);
        let result = self
            .io
            .run_async(move || {
                mark_failed(
                    &directory,
                    &manifest,
                    &lease,
                    "query producer aborted before completion",
                )
            })
            .await;
        if result.is_ok() {
            self.owns_directory = false;
        }
        result
    }

    pub(crate) async fn interrupt(mut self, message: &str) -> Result<StoredResult> {
        let interrupted = {
            let mut current = self.manifest.lock();
            current.interrupt(message);
            current.clone()
        };
        let directory = self.directory.clone();
        let persisted = interrupted.clone();
        if let Err(error) = self
            .io
            .run_async(move || manifest::persist(&directory, &persisted))
            .await
        {
            return self.fail_finish(error).await;
        }
        let result = StoredResult::completed(
            self.directory.clone(),
            Arc::clone(&self.schema),
            &interrupted,
            Arc::clone(&self.lease),
            Arc::clone(&self.access),
            self.io.clone(),
        );
        self.owns_directory = false;
        Ok(result)
    }

    async fn fail_finish<T>(&mut self, error: Error) -> Result<T> {
        let directory = self.directory.clone();
        let manifest = Arc::clone(&self.manifest);
        let lease = Arc::clone(&self.lease);
        let message = format!("result producer failed: {error}");
        match self
            .io
            .run_async(move || mark_failed(&directory, &manifest, &lease, &message))
            .await
        {
            Ok(()) => {
                self.owns_directory = false;
                Err(error)
            }
            Err(cleanup) => Err(Error::Execution(format!(
                "{error}; additionally failed to persist failed HTTP result state: {cleanup}"
            ))),
        }
    }
}

impl Drop for ResultWriter {
    fn drop(&mut self) {
        if self.owns_directory {
            // A dropped async task must never block a runtime worker on the
            // service I/O pool. Keep quota accounting conservative until the
            // shutdown sealing, detached cleanup, or restart recovery sees
            // the files.
            self.lease.retain_on_drop();
            let preserve = self
                .preserve_on_drop
                .as_ref()
                .is_some_and(|flag| flag.load(Ordering::Acquire));
            if !preserve {
                let directory = self.directory.clone();
                let manifest = Arc::clone(&self.manifest);
                let lease = Arc::clone(&self.lease);
                let operation = move || {
                    mark_failed(&directory, &manifest, &lease, "result writer was abandoned")
                };
                if let Err(error) = self.io.run_detached(operation) {
                    tracing::error!(%error, path = %self.directory.display(), "failed to schedule abandoned HTTP result cleanup");
                }
            }
        }
        if self.rejected.load(Ordering::Acquire) {
            tracing::warn!(path = %self.directory.display(), "HTTP result quota rejected a write");
        }
    }
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

fn normalize_write_error(error: Error, quota_rejected: bool) -> Error {
    if quota_rejected
        || matches!(&error, Error::NativeDiskQuotaExceeded { .. })
        || matches!(&error, Error::Io { source, .. } if source.kind() == io::ErrorKind::StorageFull)
    {
        Error::ResourceExhausted(
            "HTTP query result exceeded its disk quota or free-space reserve".into(),
        )
    } else {
        error
    }
}
