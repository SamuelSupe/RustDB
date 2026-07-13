use std::{
    fs::{File, OpenOptions},
    io::{self, BufReader, BufWriter, Read, Write},
    path::Path,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use arrow::{
    datatypes::{DataType, Field, Schema, SchemaRef},
    ipc::{
        CompressionType,
        reader::StreamReader,
        writer::{IpcWriteOptions, StreamWriter},
    },
    record_batch::RecordBatch,
};

use crate::{Error, Result};

use super::{SpillFile, State};
use crate::runtime::{MemoryPool, MemoryReservation};

const SPILL_WRITER_BUFFER_BYTES: usize = 8 * 1024;
const SPILL_WRITER_BASE_MEMORY_BYTES: usize = 16 * 1024;
const FIELD_WRITER_MEMORY_BYTES: usize = 1024;
const IO_COPY_CHUNK_BYTES: usize = 256 * 1024;
const MIN_IO_COPY_BYTES: usize = 512;

pub(crate) struct SpillWriter {
    state: Arc<State>,
    spill_file: Option<SpillFile>,
    writer: Option<StreamWriter<BufWriter<SpillOutput>>>,
    memory: Option<MemoryReservation>,
    copy_memory: Arc<Mutex<Option<MemoryReservation>>>,
    io_error: Arc<IoErrorState>,
}

pub(super) struct WriterMemory {
    writer: MemoryReservation,
}

pub(super) struct SpillReader {
    state: Arc<State>,
    reader: StreamReader<BufReader<SpillInput>>,
    io_error: Arc<IoErrorState>,
    finished: bool,
}

struct SpillOutput {
    state: Arc<State>,
    spill_file: SpillFile,
    file: Arc<Mutex<File>>,
    copy_memory: Arc<Mutex<Option<MemoryReservation>>>,
    io_error: Arc<IoErrorState>,
}

struct SpillInput {
    state: Arc<State>,
    path: std::path::PathBuf,
    file: Arc<Mutex<File>>,
    io_error: Arc<IoErrorState>,
}

struct IoErrorState {
    error: Mutex<Option<Error>>,
    failed: AtomicBool,
}

impl IoErrorState {
    fn new() -> Self {
        Self {
            error: Mutex::new(None),
            failed: AtomicBool::new(false),
        }
    }
}

impl SpillWriter {
    pub(super) fn create(
        state: Arc<State>,
        spill_file: SpillFile,
        schema: SchemaRef,
        memory: WriterMemory,
    ) -> Result<Self> {
        let WriterMemory { writer: memory } = memory;
        if schema
            .fields()
            .iter()
            .any(|field| contains_dictionary(field.data_type()))
        {
            let error = Error::InvalidArgument(
                "spill does not retain dictionary-encoded arrays; decode dictionaries before spilling"
                    .into(),
            );
            return Err(discard_file(&state, &spill_file, error));
        }
        let options = IpcWriteOptions::default()
            .try_with_compression(Some(CompressionType::LZ4_FRAME))
            .map_err(|error| discard_file(&state, &spill_file, error.into()))?;
        let path = spill_file.path().to_path_buf();
        let create_path = path.clone();
        let quota = state.quota.clone();
        let metrics = state.metrics.clone();
        let file = match state.run_io(move || {
            quota.check_available().inspect_err(|_| {
                if let Some(metrics) = &metrics {
                    metrics.add_spill_quota_rejection();
                }
            })?;
            secure_create(&create_path)
        }) {
            Ok(file) => file,
            Err(error) => {
                return Err(discard_file(&state, &spill_file, error));
            }
        };
        state.record_spill_file();
        let file = Arc::new(Mutex::new(file));
        let io_error = Arc::new(IoErrorState::new());
        let copy_memory = match reserve_copy_memory(&state, copy_memory_bytes(state.memory.limit()))
        {
            Ok(memory) => Arc::new(Mutex::new(Some(memory))),
            Err(error) => return Err(discard_file(&state, &spill_file, error)),
        };
        let output = SpillOutput {
            state: Arc::clone(&state),
            spill_file: spill_file.clone(),
            file,
            copy_memory: Arc::clone(&copy_memory),
            io_error: Arc::clone(&io_error),
        };
        let writer = match StreamWriter::try_new_with_options(
            BufWriter::with_capacity(SPILL_WRITER_BUFFER_BYTES, output),
            schema.as_ref(),
            options,
        ) {
            Ok(writer) => writer,
            Err(error) => {
                let error = take_io_error(&io_error).unwrap_or_else(|| error.into());
                return Err(discard_file(&state, &spill_file, error));
            }
        };
        take_copy_memory(&copy_memory);
        Ok(Self {
            state,
            spill_file: Some(spill_file),
            writer: Some(writer),
            memory: Some(memory),
            copy_memory,
            io_error,
        })
    }

    pub(crate) fn write_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        self.state.ensure_active()?;
        self.install_copy_memory()?;
        let result = match self.writer.as_mut() {
            Some(writer) => writer.write(batch),
            None => {
                take_copy_memory(&self.copy_memory);
                return Err(Error::Internal(
                    "spill writer was already finished".to_owned(),
                ));
            }
        };
        take_copy_memory(&self.copy_memory);
        match result {
            Ok(()) => Ok(()),
            Err(error) => Err(take_io_error(&self.io_error).unwrap_or_else(|| error.into())),
        }
    }

    /// Bytes serialized into the buffered IPC stream but not yet charged to
    /// Spill metrics by the I/O layer.
    pub(crate) fn pending_write_bytes(&self) -> u64 {
        self.writer
            .as_ref()
            .map(|writer| u64::try_from(writer.get_ref().buffer().len()).unwrap_or(u64::MAX))
            .unwrap_or(0)
    }

    pub(crate) fn finish(mut self, partitions: u64) -> Result<SpillFile> {
        self.state.ensure_active()?;
        self.install_copy_memory()?;
        let mut writer = match self.writer.take() {
            Some(writer) => writer,
            None => {
                take_copy_memory(&self.copy_memory);
                return Err(Error::Internal(
                    "spill writer was already finished".to_owned(),
                ));
            }
        };
        if let Err(error) = writer.finish() {
            take_copy_memory(&self.copy_memory);
            return Err(take_io_error(&self.io_error).unwrap_or_else(|| error.into()));
        }
        drop(writer);
        take_copy_memory(&self.copy_memory);
        self.memory.take();

        let spill_file = self
            .spill_file
            .as_ref()
            .ok_or_else(|| Error::Internal("spill writer has no file".to_owned()))?;
        let path = spill_file.path().to_path_buf();
        let metadata_path = path.clone();
        let bytes = self.state.run_io(move || {
            std::fs::metadata(&metadata_path)
                .map(|metadata| metadata.len())
                .map_err(|error| Error::io(Some(metadata_path), error))
        })?;
        if let Some(metrics) = &self.state.metrics {
            metrics.record_spill(bytes, partitions);
        }
        Ok(self.spill_file.take().expect("spill file checked above"))
    }

    fn install_copy_memory(&self) -> Result<()> {
        let memory =
            reserve_copy_memory(&self.state, copy_memory_bytes(self.state.memory.limit()))?;
        let mut slot = self
            .copy_memory
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        if slot.is_some() {
            return Err(Error::Internal(
                "spill I/O copy reservation is already in use".to_owned(),
            ));
        }
        *slot = Some(memory);
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn path(&self) -> &Path {
        self.spill_file
            .as_ref()
            .expect("unfinished spill writer has a file")
            .path()
    }
}

impl SpillReader {
    pub(super) fn open(state: Arc<State>, spill_file: &SpillFile) -> Result<Self> {
        let path = spill_file.path().to_path_buf();
        let open_path = path.clone();
        let file = state.run_io(move || {
            File::open(&open_path).map_err(|error| Error::io(Some(open_path), error))
        })?;
        let io_error = Arc::new(IoErrorState::new());
        let input = SpillInput {
            state: Arc::clone(&state),
            path,
            file: Arc::new(Mutex::new(file)),
            io_error: Arc::clone(&io_error),
        };
        let reader = StreamReader::try_new_buffered(input, None)
            .map_err(|error| take_io_error(&io_error).unwrap_or_else(|| error.into()))?;
        Ok(Self {
            state,
            reader,
            io_error,
            finished: false,
        })
    }
}

impl Iterator for SpillReader {
    type Item = Result<RecordBatch>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.finished {
            return None;
        }
        if let Err(error) = self.state.ensure_active() {
            self.finished = true;
            return Some(Err(error));
        }
        match self.reader.next() {
            Some(Ok(batch)) => Some(Ok(batch)),
            Some(Err(error)) => {
                self.finished = true;
                Some(Err(
                    take_io_error(&self.io_error).unwrap_or_else(|| error.into())
                ))
            }
            None => {
                self.finished = true;
                None
            }
        }
    }
}

impl Drop for SpillWriter {
    fn drop(&mut self) {
        // Close the file before unlinking it so this is also valid on
        // platforms that do not allow deleting an open file.
        if self.writer.is_some() {
            let _ = self.install_copy_memory();
        }
        self.writer.take();
        take_copy_memory(&self.copy_memory);
        self.memory.take();
        if let Some(spill_file) = self.spill_file.take() {
            // Query-level cleanup owns the directory after cancellation. A
            // per-writer unlink would only return Cancelled and can emit tens
            // of thousands of duplicate errors for a partitioned spill.
            if self.state.control.is_cancelled()
                || self
                    .state
                    .cleaned
                    .load(std::sync::atomic::Ordering::Acquire)
            {
                return;
            }
            if let Err(error) = self.state.remove_file(&spill_file)
                && !matches!(error, Error::Cancelled)
            {
                tracing::error!(%error, path = %spill_file.path().display(), "failed to remove unfinished spill file");
            }
        }
    }
}

impl Write for SpillOutput {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        if let Some(error) = existing_io_error(&self.io_error) {
            return Err(error);
        }
        if buffer.is_empty() {
            return Ok(0);
        }
        if let Err(error) = self.state.ensure_active() {
            return Err(store_io_error(&self.io_error, error));
        }

        let copy_memory = self
            .copy_memory
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .take()
            .ok_or_else(|| {
                store_io_error(
                    &self.io_error,
                    Error::Internal("spill I/O copy reservation is unavailable".to_owned()),
                )
            })?;
        let copy_bytes = buffer.len().min(copy_memory.size());
        let chunk = &buffer[..copy_bytes];
        let copy_bytes_u64 = u64::try_from(copy_bytes).unwrap_or(u64::MAX);
        debug_assert!(copy_memory.size() >= copy_bytes);
        // The memory reservation must be installed before this owned queue
        // copy is allocated. Both are captured by the cancelable Job and
        // released together if the job is removed before it starts.
        let data = chunk.to_vec();
        let file = Arc::clone(&self.file);
        let path = self.spill_file.path().to_path_buf();
        let spill_file = self.spill_file.clone();
        let files = Arc::clone(&self.state.files);
        let metrics = self.state.metrics.clone();
        let quota = self.state.quota.clone();
        let outcome = self.state.run_io(move || {
            let reserved = quota.try_reserve(copy_bytes_u64).inspect_err(|_| {
                if let Some(metrics) = &metrics {
                    metrics.add_spill_quota_rejection();
                }
            })?;
            let mut file = file.lock().unwrap_or_else(|poison| poison.into_inner());
            let before = file.metadata().ok().map(|metadata| metadata.len());
            let result = file.write_all(&data);
            let actual = if result.is_ok() {
                u64::try_from(data.len()).unwrap_or(u64::MAX)
            } else {
                before
                    .and_then(|before| {
                        file.metadata()
                            .ok()
                            .map(|metadata| metadata.len().saturating_sub(before))
                    })
                    // If metadata also fails, retain the complete reserved
                    // amount rather than under-accounting a partial write.
                    .unwrap_or_else(|| u64::try_from(data.len()).unwrap_or(u64::MAX))
            };
            drop(file);
            let charge = reserved.commit(actual)?;
            files.add_charge(spill_file.path(), charge)?;
            if let Some(metrics) = metrics {
                metrics.add_spill_write_bytes(actual);
            }
            Ok((result, path, copy_memory))
        });
        let (result, path, copy_memory) =
            outcome.map_err(|error| store_io_error(&self.io_error, error))?;
        *self
            .copy_memory
            .lock()
            .unwrap_or_else(|poison| poison.into_inner()) = Some(copy_memory);
        result.map_err(|error| store_io_error(&self.io_error, Error::io(Some(path), error)))?;
        Ok(copy_bytes)
    }

    fn flush(&mut self) -> io::Result<()> {
        if let Some(error) = existing_io_error(&self.io_error) {
            return Err(error);
        }
        if let Err(error) = self.state.ensure_active() {
            return Err(store_io_error(&self.io_error, error));
        }
        let file = Arc::clone(&self.file);
        let path = self.spill_file.path().to_path_buf();
        self.state
            .run_io(move || {
                file.lock()
                    .unwrap_or_else(|poison| poison.into_inner())
                    .flush()
                    .map_err(|error| Error::io(Some(path), error))
            })
            .map_err(|error| store_io_error(&self.io_error, error))
    }
}

impl Read for SpillInput {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        if let Some(error) = existing_io_error(&self.io_error) {
            return Err(error);
        }
        if buffer.is_empty() {
            return Ok(0);
        }
        if let Err(error) = self.state.ensure_active() {
            return Err(store_io_error(&self.io_error, error));
        }
        let file = Arc::clone(&self.file);
        let path = self.path.clone();
        let capacity = buffer.len();
        let read_memory = reserve_copy_memory(&self.state, capacity)
            .map_err(|error| store_io_error(&self.io_error, error))?;
        let (data, _read_memory) = self
            .state
            .run_io(move || {
                let mut data = vec![0_u8; capacity];
                let read = file
                    .lock()
                    .unwrap_or_else(|poison| poison.into_inner())
                    .read(&mut data)
                    .map_err(|error| Error::io(Some(path), error))?;
                data.truncate(read);
                Ok((data, read_memory))
            })
            .map_err(|error| store_io_error(&self.io_error, error))?;
        buffer[..data.len()].copy_from_slice(&data);
        self.state
            .record_spill_read(u64::try_from(data.len()).unwrap_or(u64::MAX));
        Ok(data.len())
    }
}

fn take_io_error(slot: &Arc<IoErrorState>) -> Option<Error> {
    slot.error
        .lock()
        .unwrap_or_else(|poison| poison.into_inner())
        .take()
}

fn take_copy_memory(slot: &Arc<Mutex<Option<MemoryReservation>>>) {
    slot.lock()
        .unwrap_or_else(|poison| poison.into_inner())
        .take();
}

fn existing_io_error(slot: &Arc<IoErrorState>) -> Option<io::Error> {
    if !slot.failed.load(Ordering::Acquire) {
        return None;
    }
    let message = slot
        .error
        .lock()
        .unwrap_or_else(|poison| poison.into_inner())
        .as_ref()
        .map(ToString::to_string)
        .unwrap_or_else(|| "previous spill I/O operation failed".to_owned());
    Some(io::Error::other(message))
}

fn store_io_error(slot: &Arc<IoErrorState>, error: Error) -> io::Error {
    let message = error.to_string();
    let mut stored = slot
        .error
        .lock()
        .unwrap_or_else(|poison| poison.into_inner());
    if stored.is_none() {
        *stored = Some(error);
    }
    slot.failed.store(true, Ordering::Release);
    io::Error::other(message)
}

fn discard_file(state: &State, spill_file: &SpillFile, error: Error) -> Error {
    match state.remove_file(spill_file) {
        Ok(()) => error,
        Err(cleanup) => Error::Execution(format!(
            "{error}; additionally failed to remove spill file '{}': {cleanup}",
            spill_file.path().display()
        )),
    }
}

pub(super) fn reserve_writer_memory(memory: &MemoryPool, schema: &Schema) -> Result<WriterMemory> {
    let writer_bytes = writer_state_memory_bytes(schema);
    let writer = memory.try_reserve(writer_bytes).map_err(|_| {
        Error::ResourceExhausted(format!(
            "spill writer requires {writer_bytes} bytes for its buffered streaming IPC/LZ4 \
                 state (query limit {} bytes, currently available {} bytes); finish an active \
                 spill writer or increase the memory limit",
            memory.limit(),
            memory.available()
        ))
    })?;
    Ok(WriterMemory { writer })
}

fn reserve_copy_memory(state: &State, bytes: usize) -> Result<MemoryReservation> {
    let reservation = loop {
        match state.memory.try_reserve_emergency(bytes) {
            Ok(reservation) => break reservation,
            Err(_) if state.memory.emergency_headroom() >= bytes => {
                state.control.check_cancelled()?;
                // Another spill reader/writer is using the protected slot.
                // It runs on the bounded I/O pool and therefore must release
                // the slot; wait briefly without allocating another copy.
                std::thread::park_timeout(Duration::from_millis(2));
            }
            Err(error) => {
                return Err(Error::ResourceExhausted(format!(
                    "spill I/O queue copy requires {bytes} bytes before allocation (query limit {} bytes, currently available {} bytes): {error}",
                    state.memory.limit(),
                    state.memory.available()
                )));
            }
        }
    };
    if let Some(metrics) = &state.metrics {
        metrics.observe_memory(state.memory.used());
    }
    Ok(reservation)
}

pub(super) fn writer_memory_bytes(schema: &Schema) -> usize {
    writer_state_memory_bytes(schema)
}

pub(super) fn copy_memory_bytes(memory_limit: usize) -> usize {
    memory_limit
        .checked_div(256)
        .unwrap_or(0)
        .clamp(MIN_IO_COPY_BYTES, IO_COPY_CHUNK_BYTES)
}

fn writer_state_memory_bytes(schema: &Schema) -> usize {
    schema.fields().iter().fold(
        SPILL_WRITER_BASE_MEMORY_BYTES.saturating_add(metadata_writer_memory(schema.metadata())),
        |bytes, field| bytes.saturating_add(field_writer_memory(field)),
    )
}

fn field_writer_memory(field: &Field) -> usize {
    let nested = match field.data_type() {
        DataType::List(child)
        | DataType::ListView(child)
        | DataType::FixedSizeList(child, _)
        | DataType::LargeList(child)
        | DataType::LargeListView(child)
        | DataType::Map(child, _) => field_writer_memory(child),
        DataType::Struct(fields) => fields.iter().map(|field| field_writer_memory(field)).sum(),
        DataType::Union(fields, _) => fields
            .iter()
            .map(|(_, field)| field_writer_memory(field))
            .sum(),
        DataType::RunEndEncoded(run_ends, values) => {
            field_writer_memory(run_ends).saturating_add(field_writer_memory(values))
        }
        _ => 0,
    };
    FIELD_WRITER_MEMORY_BYTES
        .saturating_add(field.name().len().saturating_mul(2))
        .saturating_add(metadata_writer_memory(field.metadata()))
        .saturating_add(nested)
}

fn metadata_writer_memory(metadata: &std::collections::HashMap<String, String>) -> usize {
    metadata.iter().fold(0usize, |bytes, (key, value)| {
        bytes
            .saturating_add(128)
            .saturating_add(key.len().saturating_mul(2))
            .saturating_add(value.len().saturating_mul(2))
    })
}

fn contains_dictionary(data_type: &DataType) -> bool {
    match data_type {
        DataType::Dictionary(_, _) => true,
        DataType::List(field)
        | DataType::ListView(field)
        | DataType::FixedSizeList(field, _)
        | DataType::LargeList(field)
        | DataType::LargeListView(field)
        | DataType::Map(field, _) => contains_dictionary(field.data_type()),
        DataType::Struct(fields) => fields
            .iter()
            .any(|field| contains_dictionary(field.data_type())),
        DataType::Union(fields, _) => fields
            .iter()
            .any(|(_, field)| contains_dictionary(field.data_type())),
        DataType::RunEndEncoded(run_ends, values) => {
            contains_dictionary(run_ends.data_type()) || contains_dictionary(values.data_type())
        }
        _ => false,
    }
}

pub(super) fn safe_label(label: &str) -> String {
    let label: String = label
        .chars()
        .filter(|character| character.is_ascii_alphanumeric() || matches!(character, '-' | '_'))
        .take(48)
        .collect();
    if label.is_empty() {
        "spill".to_owned()
    } else {
        label
    }
}

fn secure_create(path: &Path) -> Result<File> {
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    options
        .open(path)
        .map_err(|error| Error::io(Some(path.to_path_buf()), error))
}

pub(super) fn create_query_directory(path: &Path) -> Result<()> {
    let mut builder = std::fs::DirBuilder::new();
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        builder.mode(0o700);
    }
    builder
        .create(path)
        .map_err(|error| Error::io(Some(path.to_path_buf()), error))
}

pub(super) fn sync_parent_directory(path: &Path) -> Result<()> {
    #[cfg(unix)]
    if let Some(parent) = path.parent() {
        let directory =
            File::open(parent).map_err(|error| Error::io(Some(parent.to_path_buf()), error))?;
        directory
            .sync_all()
            .map_err(|error| Error::io(Some(parent.to_path_buf()), error))?;
    }
    #[cfg(not(unix))]
    let _ = path;
    Ok(())
}

pub(super) fn set_directory_permissions(path: &Path) -> Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let permissions = std::fs::Permissions::from_mode(0o700);
        std::fs::set_permissions(path, permissions)
            .map_err(|error| Error::io(Some(path.to_path_buf()), error))?;
    }
    Ok(())
}
