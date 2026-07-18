use std::{
    fs::{File, OpenOptions},
    io::{self, Write},
    path::{Path, PathBuf},
    sync::Arc,
};

use arrow::{csv::WriterBuilder, datatypes::SchemaRef, record_batch::RecordBatch};
use futures::StreamExt;
use object_store::{
    ObjectStore, ObjectStoreExt, PutMode, PutOptions, PutPayload, path::Path as ObjectPath,
};
use parquet::{arrow::ArrowWriter, basic::Compression, file::properties::WriterProperties};
use uuid::Uuid;

use crate::{
    Error, Result,
    command::{CopyCsvOptions, CopyFormat},
    runtime::{
        AsyncCleanupGuard, MemoryPool, MemoryReservation, QueryContext, SpillIoPool, TaskGroup,
    },
    storage::{CopyManifestEntry, WriteDestination},
};

use self::multipart::SharedMultipart;

#[path = "copy_sink/multipart.rs"]
mod multipart;
#[path = "copy_sink/remote_cleanup.rs"]
mod remote_cleanup;

pub(super) enum CopySink {
    Local {
        writer: Option<LocalWriter>,
        io: SpillIoPool,
    },
    Remote(RemoteWriter),
}

impl CopySink {
    pub(super) async fn create(
        destination: WriteDestination,
        format: CopyFormat,
        csv: CopyCsvOptions,
        schema: SchemaRef,
        context: &QueryContext,
        io: SpillIoPool,
    ) -> Result<Self> {
        match destination {
            WriteDestination::Local(path) => {
                let memory = context.memory.clone();
                let batch_size = context.batch_size;
                let writer = io.run(move || {
                    LocalWriter::create(path, format, csv, schema, memory, batch_size)
                })?;
                Ok(Self::Local {
                    writer: Some(writer),
                    io,
                })
            }
            WriteDestination::S3 {
                uri,
                store,
                location,
            } => RemoteWriter::create(uri, store, location, format, csv, schema, context)
                .await
                .map(Self::Remote),
        }
    }

    pub(super) async fn write(&mut self, batch: RecordBatch, context: &QueryContext) -> Result<()> {
        match self {
            Self::Local { writer, io } => {
                let mut owned = writer.take().ok_or_else(|| {
                    Error::Internal("local COPY writer is already finished".to_owned())
                })?;
                let (owned, result) = io.run(move || {
                    let result = owned.write(&batch);
                    Ok((owned, result))
                })?;
                *writer = Some(owned);
                result
            }
            Self::Remote(writer) => writer.write(batch, context).await,
        }
    }

    pub(super) async fn finish(mut self, context: &QueryContext) -> Result<u64> {
        match &mut self {
            Self::Local { writer, io } => {
                let writer = writer.take().ok_or_else(|| {
                    Error::Internal("local COPY writer is already finished".to_owned())
                })?;
                io.run(move || writer.finish())
            }
            Self::Remote(writer) => writer.finish(context).await,
        }
    }

    pub(super) async fn abort(mut self, error: Error) -> Error {
        match &mut self {
            Self::Local { writer, io } => {
                let Some(mut writer) = writer.take() else {
                    return error;
                };
                match io.run(move || writer.abort::<()>(error)) {
                    Ok(()) => Error::Internal(
                        "local COPY cleanup unexpectedly returned success".to_owned(),
                    ),
                    Err(error) => error,
                }
            }
            Self::Remote(writer) => match writer.abort_upload::<()>(error).await {
                Ok(()) => {
                    Error::Internal("remote COPY cleanup unexpectedly returned success".to_owned())
                }
                Err(error) => error,
            },
        }
    }
}

enum Encoder<W: Write + Send> {
    Csv {
        writer: arrow::csv::Writer<W>,
        wrote_batch: bool,
        schema: SchemaRef,
        memory: MemoryReservation,
    },
    Parquet {
        writer: ArrowWriter<W>,
        memory: MemoryReservation,
    },
}

impl<W: Write + Send> Encoder<W> {
    fn create(
        output: W,
        format: CopyFormat,
        csv: &CopyCsvOptions,
        schema: SchemaRef,
        memory: MemoryPool,
        batch_size: usize,
    ) -> Result<Self> {
        match format {
            CopyFormat::Csv => {
                let mut builder = WriterBuilder::new()
                    .with_header(csv.header.unwrap_or(true))
                    .with_delimiter(csv.delimiter)
                    .with_quote(csv.quote);
                if let Some(escape) = csv.escape {
                    builder = builder.with_escape(escape).with_double_quote(false);
                }
                if let Some(null) = &csv.null {
                    builder = builder.with_null(null.clone());
                }
                Ok(Self::Csv {
                    writer: builder.build(output),
                    wrote_batch: false,
                    schema,
                    memory: memory.reservation(),
                })
            }
            CopyFormat::Parquet => {
                let properties = WriterProperties::builder()
                    .set_compression(Compression::ZSTD(Default::default()))
                    .set_max_row_group_row_count(Some(batch_size.max(1)))
                    .set_max_row_group_bytes(Some(parquet_row_group_bytes(&memory)))
                    .build();
                let writer = ArrowWriter::try_new(output, schema, Some(properties))?;
                let retained = writer.memory_size();
                let memory = memory.try_reserve(retained).map_err(|error| {
                    copy_memory_error("Parquet encoder initialization", retained, error)
                })?;
                Ok(Self::Parquet { writer, memory })
            }
        }
    }

    fn write(&mut self, batch: &RecordBatch) -> Result<()> {
        match self {
            Self::Csv {
                writer,
                wrote_batch,
                memory,
                ..
            } => {
                // Text formatting can expand compact Arrow values (notably
                // decimals, timestamps, quoting and escape sequences).
                let workspace = batch
                    .get_array_memory_size()
                    .saturating_mul(CSV_WORKSPACE_MULTIPLIER);
                memory.try_resize(workspace).map_err(|error| {
                    copy_memory_error("CSV encoder workspace", workspace, error)
                })?;
                let result = writer.write(batch).map_err(Error::from);
                memory.shrink(memory.size());
                result?;
                *wrote_batch = true;
                Ok(())
            }
            Self::Parquet { writer, memory } => {
                let workspace = batch.get_array_memory_size();
                memory.try_grow(workspace).map_err(|error| {
                    copy_memory_error("Parquet encoder workspace", workspace, error)
                })?;
                writer.write(batch)?;
                let retained = writer.memory_size();
                memory.try_resize(retained).map_err(|error| {
                    copy_memory_error("Parquet encoder retained state", retained, error)
                })
            }
        }
    }

    fn finish(mut self) -> Result<W> {
        match &mut self {
            Self::Csv {
                writer,
                wrote_batch,
                schema,
                ..
            } if !*wrote_batch => writer.write(&RecordBatch::new_empty(Arc::clone(schema)))?,
            _ => {}
        }
        match self {
            Self::Csv { writer, .. } => Ok(writer.into_inner()),
            Self::Parquet { writer, mut memory } => {
                let workspace = writer.in_progress_size().max(writer.memory_size());
                memory.try_resize(workspace).map_err(|error| {
                    copy_memory_error("Parquet encoder finalization", workspace, error)
                })?;
                writer.into_inner().map_err(Error::from)
            }
        }
    }
}

const MAX_PARQUET_ROW_GROUP_BYTES: usize = 8 << 20;
const CSV_WORKSPACE_MULTIPLIER: usize = 4;

fn parquet_row_group_bytes(memory: &MemoryPool) -> usize {
    memory
        .operation_limit()
        .saturating_div(4)
        .clamp(1, MAX_PARQUET_ROW_GROUP_BYTES)
}

fn copy_memory_error(owner: &str, bytes: usize, error: Error) -> Error {
    match error {
        Error::ResourceExhausted(message) => {
            Error::ResourceExhausted(format!("COPY {owner} requires {bytes} bytes: {message}"))
        }
        error => error,
    }
}

pub(super) struct LocalWriter {
    destination: PathBuf,
    staging: PathBuf,
    encoder: Option<Encoder<File>>,
    active: bool,
}

impl LocalWriter {
    fn create(
        destination: PathBuf,
        format: CopyFormat,
        csv: CopyCsvOptions,
        schema: SchemaRef,
        memory: MemoryPool,
        batch_size: usize,
    ) -> Result<Self> {
        if destination.exists() {
            return Err(Error::InvalidArgument(format!(
                "COPY destination already exists: {}",
                destination.display()
            )));
        }
        ensure_no_existing_local_staging(&destination, None)?;
        let staging = staging_path(&destination)?;
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let file = options
            .open(&staging)
            .map_err(|error| Error::io(Some(staging.clone()), error))?;
        if let Err(error) = ensure_no_existing_local_staging(&destination, Some(&staging)) {
            drop(file);
            return match std::fs::remove_file(&staging) {
                Ok(()) => Err(error),
                Err(cleanup) => Err(Error::Execution(format!(
                    "{error}; COPY staging cleanup failed for '{}': {cleanup}",
                    staging.display()
                ))),
            };
        }
        let encoder = match Encoder::create(file, format, &csv, schema, memory, batch_size) {
            Ok(encoder) => encoder,
            Err(error) => {
                return match std::fs::remove_file(&staging) {
                    Ok(()) => Err(error),
                    Err(cleanup) => Err(Error::Execution(format!(
                        "{error}; COPY staging cleanup failed for '{}': {cleanup}",
                        staging.display()
                    ))),
                };
            }
        };
        Ok(Self {
            destination,
            staging,
            encoder: Some(encoder),
            active: true,
        })
    }

    fn write(&mut self, batch: &RecordBatch) -> Result<()> {
        self.encoder
            .as_mut()
            .ok_or_else(|| Error::Internal("local COPY encoder is already finished".to_owned()))?
            .write(batch)
    }

    fn finish(mut self) -> Result<u64> {
        let encoder = self
            .encoder
            .take()
            .ok_or_else(|| Error::Internal("local COPY encoder is already finished".to_owned()))?;
        let file = match encoder.finish() {
            Ok(file) => file,
            Err(error) => return self.abort(error),
        };
        if let Err(error) = file.sync_all() {
            return self.abort(Error::io(Some(self.staging.clone()), error));
        }
        let bytes = match file.metadata() {
            Ok(metadata) => metadata.len(),
            Err(error) => return self.abort(Error::io(Some(self.staging.clone()), error)),
        };
        drop(file);
        if let Err(error) = rename_no_replace(&self.staging, &self.destination) {
            return self.abort(error);
        }
        self.active = false;
        sync_parent(&self.destination)
            .map_err(|error| local_durability_error(&self.destination, error))?;
        Ok(bytes)
    }

    fn abort<T>(&mut self, error: Error) -> Result<T> {
        self.active = false;
        match std::fs::remove_file(&self.staging) {
            Ok(()) => Err(error),
            Err(cleanup) if cleanup.kind() == io::ErrorKind::NotFound => Err(error),
            Err(cleanup) => Err(Error::Execution(format!(
                "{error}; COPY staging cleanup failed for '{}': {cleanup}",
                self.staging.display()
            ))),
        }
    }
}

impl Drop for LocalWriter {
    fn drop(&mut self) {
        if self.active
            && let Err(error) = std::fs::remove_file(&self.staging)
            && error.kind() != io::ErrorKind::NotFound
        {
            tracing::error!(%error, path = %self.staging.display(), "failed to remove abandoned COPY staging file");
        }
    }
}

pub(super) struct RemoteWriter {
    uri: String,
    store: Arc<dyn ObjectStore>,
    manifest: ObjectPath,
    data: ObjectPath,
    format: CopyFormat,
    encoder: Option<Encoder<SharedMultipart>>,
    upload: SharedMultipart,
    published: bool,
    tasks: TaskGroup,
    cleanup_guard: Option<AsyncCleanupGuard>,
}

impl RemoteWriter {
    async fn create(
        uri: String,
        store: Arc<dyn ObjectStore>,
        destination: ObjectPath,
        format: CopyFormat,
        csv: CopyCsvOptions,
        schema: SchemaRef,
        context: &QueryContext,
    ) -> Result<Self> {
        let manifest = ObjectPath::parse(format!(
            "{}/{}",
            destination.as_ref(),
            crate::storage::COPY_MANIFEST_FILE
        ))
        .map_err(|error| Error::InvalidArgument(format!("invalid COPY manifest key: {error}")))?;
        context.check_cancelled()?;
        context.metrics.add_s3_requests(3);
        ensure_remote_destination_available(&store, &destination, &manifest).await?;
        let suffix = match format {
            CopyFormat::Csv => "csv",
            CopyFormat::Parquet => "parquet",
        };
        let data = ObjectPath::parse(format!(
            "{}/part-{}.{}",
            destination.as_ref(),
            Uuid::new_v4(),
            suffix
        ))
        .map_err(|error| Error::InvalidArgument(format!("invalid COPY data key: {error}")))?;
        context.check_cancelled()?;
        context.metrics.add_s3_requests(1);
        let upload = tokio::select! {
            _ = context.control.cancelled() => return Err(Error::Cancelled),
            result = store.put_multipart(&data) => result?,
        };
        let cleanup_guard = context.protect_async_cleanup();
        let upload =
            SharedMultipart::create(upload, &context.tasks, context.memory.clone()).await?;
        let encoder = match Encoder::create(
            upload.clone(),
            format,
            &csv,
            schema,
            context.memory.clone(),
            context.batch_size,
        ) {
            Ok(encoder) => encoder,
            Err(error) => {
                return match upload.abort().await {
                    Ok(()) => Err(error),
                    Err(cleanup) => Err(Error::Execution(format!(
                        "{error}; remote COPY multipart cleanup failed: {cleanup}"
                    ))),
                };
            }
        };
        Ok(Self {
            uri,
            store,
            manifest,
            data,
            format,
            encoder: Some(encoder),
            upload,
            published: false,
            tasks: context.tasks.clone(),
            cleanup_guard: Some(cleanup_guard),
        })
    }

    async fn write(&mut self, batch: RecordBatch, context: &QueryContext) -> Result<()> {
        let write = self
            .encoder
            .as_mut()
            .ok_or_else(|| Error::Internal("remote COPY encoder is already finished".to_owned()))?
            .write(&batch);
        if let Err(error) = write {
            let error = self.upload.take_resource_error().unwrap_or(error);
            return self.abort_upload(error).await;
        }
        let wait = tokio::select! {
            _ = context.control.cancelled() => Err(Error::Cancelled),
            result = self.upload.wait_for_capacity(2) => result,
        };
        if let Err(error) = wait {
            return self.abort_upload(error).await;
        }
        Ok(())
    }

    async fn finish(&mut self, context: &QueryContext) -> Result<u64> {
        let encoder = self
            .encoder
            .take()
            .ok_or_else(|| Error::Internal("remote COPY encoder is already finished".to_owned()))?;
        if let Err(error) = encoder.finish() {
            let error = self.upload.take_resource_error().unwrap_or(error);
            return self.abort_upload(error).await;
        }
        let bytes = self.upload.bytes();
        let sha256 = self.upload.sha256();
        if let Err(error) = context.check_cancelled() {
            return self.abort_upload(error).await;
        }
        let mut identity = match self.upload.finish().await {
            Ok(identity) => identity,
            Err(error) => {
                if let Some(resource_error) = self.upload.take_resource_error() {
                    return self.abort_upload(resource_error).await;
                }
                return self.remove_data(error).await;
            }
        };
        if identity.e_tag.is_none() && identity.version.is_none() {
            if let Err(error) = context.check_cancelled() {
                return self.remove_data(error).await;
            }
            context.metrics.add_s3_requests(1);
            let meta = match tokio::select! {
                _ = context.control.cancelled() => Err(Error::Cancelled),
                result = self.store.head(&self.data) => result.map_err(Error::from),
            } {
                Ok(meta) => meta,
                Err(error) => return self.remove_data(error).await,
            };
            if meta.size != bytes {
                return self
                    .remove_data(Error::Execution(format!(
                        "remote COPY object has size {}, expected {bytes}",
                        meta.size
                    )))
                    .await;
            }
            identity.e_tag = meta.e_tag;
            identity.version = meta.version;
            if identity.e_tag.is_none() && identity.version.is_none() {
                return self
                    .remove_data(Error::Execution(
                        "remote COPY object store did not return a stable object identity"
                            .to_owned(),
                    ))
                    .await;
            }
        }
        if let Err(error) = context.check_cancelled() {
            return self.remove_data(error).await;
        }
        context.metrics.add_s3_requests(1);
        let entry = CopyManifestEntry {
            format: match self.format {
                CopyFormat::Csv => "csv",
                CopyFormat::Parquet => "parquet",
            }
            .to_owned(),
            object: self.data.as_ref().to_owned(),
            bytes,
            sha256,
            e_tag: identity.e_tag,
            version: identity.version,
        };
        let payload = match crate::storage::encode_copy_manifest(&entry) {
            Ok(payload) => payload,
            Err(error) => return self.remove_data(error).await,
        };
        let options = PutOptions {
            mode: PutMode::Create,
            ..PutOptions::default()
        };
        if let Err(error) = context.check_cancelled() {
            return self.remove_data(error).await;
        }
        match self
            .store
            .put_opts(&self.manifest, PutPayload::from(payload.clone()), options)
            .await
        {
            Ok(_) => self.published = true,
            Err(error) => {
                context.metrics.add_s3_requests(1);
                let state =
                    crate::storage::inspect_remote_manifest(&self.store, &self.manifest, &payload)
                        .await;
                match state {
                    crate::storage::PublicationState::Matches => self.published = true,
                    _ if crate::storage::is_definitive_remote_rejection(&error) => {
                        return self.remove_data(error.into()).await;
                    }
                    state => {
                        // The manifest may already refer to this object. Retain the data
                        // until an operator can reconcile the outcome; deleting it here
                        // could turn a successful COPY into silent data loss.
                        self.published = true;
                        return Err(Error::commit_outcome_unknown(
                            std::path::PathBuf::from(&self.uri),
                            format!("remote-copy-{}", self.data),
                            format!(
                                "COPY manifest publication failed and could not be reconciled ({state:?}): {error}"
                            ),
                        ));
                    }
                }
            }
        }
        Ok(bytes)
    }

    async fn abort_upload<T>(&mut self, error: Error) -> Result<T> {
        match self.upload.abort().await {
            Ok(()) => {
                self.published = true;
                Err(error)
            }
            Err(cleanup) => Err(Error::Execution(format!(
                "{error}; remote COPY multipart cleanup failed: {cleanup}"
            ))),
        }
    }

    async fn remove_data<T>(&mut self, error: Error) -> Result<T> {
        match self.store.delete(&self.data).await {
            Ok(()) | Err(object_store::Error::NotFound { .. }) => {
                self.published = true;
                Err(error)
            }
            Err(cleanup) => Err(Error::Execution(format!(
                "{error}; remote COPY data cleanup failed for '{}': {cleanup}",
                self.data
            ))),
        }
    }
}

async fn ensure_remote_destination_available(
    store: &Arc<dyn ObjectStore>,
    destination: &ObjectPath,
    manifest: &ObjectPath,
) -> Result<()> {
    let (exact, child_manifest) = tokio::join!(store.head(destination), store.head(manifest));
    for result in [exact, child_manifest] {
        match result {
            Ok(_) => {
                return Err(Error::InvalidArgument(
                    "COPY destination already exists".to_owned(),
                ));
            }
            Err(object_store::Error::NotFound { .. }) => {}
            Err(error) => return Err(error.into()),
        }
    }
    let prefix = format!("{}/", destination.as_ref().trim_end_matches('/'));
    let mut children = store.list(Some(destination));
    while let Some(result) = children.next().await {
        let meta = result?;
        if meta.location.as_ref() == destination.as_ref()
            || meta.location.as_ref().starts_with(&prefix)
        {
            return Err(Error::InvalidArgument(format!(
                "COPY destination contains an existing object '{}'; inspect or remove the incomplete output before retrying",
                meta.location
            )));
        }
    }
    Ok(())
}

impl Drop for RemoteWriter {
    fn drop(&mut self) {
        self.encoder.take();
        let Some(guard) = self.cleanup_guard.take() else {
            return;
        };
        if self.published {
            drop(guard);
            return;
        }
        let cleanup = remote_cleanup::PendingRemoteCleanup::new(
            self.uri.clone(),
            Arc::clone(&self.store),
            self.data.clone(),
            self.upload.clone(),
            guard,
        );
        remote_cleanup::schedule(&self.tasks, cleanup);
    }
}

fn staging_path(destination: &Path) -> Result<PathBuf> {
    let parent = destination.parent().ok_or_else(|| {
        Error::InvalidArgument(format!(
            "COPY destination has no parent: {}",
            destination.display()
        ))
    })?;
    let name = destination.file_name().ok_or_else(|| {
        Error::InvalidArgument(format!(
            "COPY destination has no file name: {}",
            destination.display()
        ))
    })?;
    Ok(parent.join(format!(
        ".{}.rustdb-copy-{}.tmp",
        name.to_string_lossy(),
        Uuid::new_v4()
    )))
}

fn ensure_no_existing_local_staging(destination: &Path, ignore: Option<&Path>) -> Result<()> {
    let parent = destination.parent().ok_or_else(|| {
        Error::InvalidArgument(format!(
            "COPY destination has no parent: {}",
            destination.display()
        ))
    })?;
    let name = destination.file_name().ok_or_else(|| {
        Error::InvalidArgument(format!(
            "COPY destination has no file name: {}",
            destination.display()
        ))
    })?;
    let prefix = format!(".{}.rustdb-copy-", name.to_string_lossy());
    let entries =
        std::fs::read_dir(parent).map_err(|error| Error::io(Some(parent.to_path_buf()), error))?;
    for entry in entries {
        let entry = entry.map_err(|error| Error::io(Some(parent.to_path_buf()), error))?;
        let path = entry.path();
        if ignore.is_some_and(|ignore| ignore == path.as_path()) {
            continue;
        }
        let file_name = entry.file_name();
        let file_name = file_name.to_string_lossy();
        let Some(uuid) = file_name
            .strip_prefix(&prefix)
            .and_then(|value| value.strip_suffix(".tmp"))
        else {
            continue;
        };
        if Uuid::parse_str(uuid).is_ok() {
            return Err(Error::InvalidArgument(format!(
                "COPY staging file already exists at '{}'; inspect or remove the incomplete output before retrying",
                path.display()
            )));
        }
    }
    Ok(())
}

fn rename_no_replace(source: &Path, destination: &Path) -> Result<()> {
    #[cfg(any(target_os = "linux", target_vendor = "apple"))]
    {
        rustix::fs::renameat_with(
            rustix::fs::CWD,
            source,
            rustix::fs::CWD,
            destination,
            rustix::fs::RenameFlags::NOREPLACE,
        )
        .map_err(|error| {
            let error = io::Error::from_raw_os_error(error.raw_os_error());
            if error.kind() == io::ErrorKind::AlreadyExists {
                Error::InvalidArgument(format!(
                    "COPY destination already exists: {}",
                    destination.display()
                ))
            } else {
                Error::io(Some(destination.to_path_buf()), error)
            }
        })
    }
    #[cfg(not(any(target_os = "linux", target_vendor = "apple")))]
    {
        let _ = source;
        Err(Error::Unsupported(format!(
            "atomic COPY publication is unsupported on this platform: {}",
            destination.display()
        )))
    }
}

fn sync_parent(path: &Path) -> Result<()> {
    let parent = path.parent().ok_or_else(|| {
        Error::InvalidArgument(format!("COPY output has no parent: {}", path.display()))
    })?;
    File::open(parent)
        .and_then(|directory| directory.sync_all())
        .map_err(|error| Error::io(Some(parent.to_path_buf()), error))
}

fn local_durability_error(destination: &Path, error: Error) -> Error {
    Error::commit_outcome_unknown(
        destination,
        "local-copy",
        format!(
            "COPY output was renamed into place but parent-directory durability could not be confirmed: {error}"
        ),
    )
}

#[cfg(test)]
#[path = "copy_sink/tests.rs"]
mod tests;
