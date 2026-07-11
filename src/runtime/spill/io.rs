use std::{
    fs::{File, OpenOptions},
    io::{BufReader, BufWriter},
    path::Path,
    sync::Arc,
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

pub(crate) struct SpillWriter {
    state: Arc<State>,
    spill_file: Option<SpillFile>,
    writer: Option<StreamWriter<BufWriter<File>>>,
    memory: Option<MemoryReservation>,
}

pub(super) struct SpillReader {
    state: Arc<State>,
    reader: StreamReader<BufReader<File>>,
    finished: bool,
}

impl SpillWriter {
    pub(super) fn create(
        state: Arc<State>,
        spill_file: SpillFile,
        schema: SchemaRef,
        memory: MemoryReservation,
    ) -> Result<Self> {
        if schema
            .fields()
            .iter()
            .any(|field| contains_dictionary(field.data_type()))
        {
            state.remove_file(&spill_file);
            return Err(Error::InvalidArgument(
                "spill does not retain dictionary-encoded arrays; decode dictionaries before spilling"
                    .into(),
            ));
        }
        let file = match secure_create(spill_file.path()) {
            Ok(file) => file,
            Err(error) => {
                state.remove_file(&spill_file);
                return Err(error);
            }
        };
        let options =
            IpcWriteOptions::default().try_with_compression(Some(CompressionType::LZ4_FRAME))?;
        let writer = match StreamWriter::try_new_with_options(
            BufWriter::with_capacity(SPILL_WRITER_BUFFER_BYTES, file),
            schema.as_ref(),
            options,
        ) {
            Ok(writer) => writer,
            Err(error) => {
                state.remove_file(&spill_file);
                return Err(error.into());
            }
        };
        Ok(Self {
            state,
            spill_file: Some(spill_file),
            writer: Some(writer),
            memory: Some(memory),
        })
    }

    pub(crate) fn write_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        self.state.ensure_active()?;
        self.writer
            .as_mut()
            .ok_or_else(|| Error::Internal("spill writer was already finished".to_owned()))?
            .write(batch)?;
        Ok(())
    }

    pub(crate) fn finish(mut self, partitions: u64) -> Result<SpillFile> {
        self.state.ensure_active()?;
        let mut writer = self
            .writer
            .take()
            .ok_or_else(|| Error::Internal("spill writer was already finished".to_owned()))?;
        writer.finish()?;
        drop(writer);
        self.memory.take();

        let spill_file = self
            .spill_file
            .as_ref()
            .ok_or_else(|| Error::Internal("spill writer has no file".to_owned()))?;
        let bytes = std::fs::metadata(spill_file.path())
            .map_err(|error| Error::io(Some(spill_file.path().to_path_buf()), error))?
            .len();
        if let Some(metrics) = &self.state.metrics {
            metrics.record_spill(bytes, partitions);
        }
        Ok(self.spill_file.take().expect("spill file checked above"))
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
        let file = File::open(spill_file.path())
            .map_err(|error| Error::io(Some(spill_file.path().to_path_buf()), error))?;
        let reader = StreamReader::try_new_buffered(file, None)?;
        Ok(Self {
            state,
            reader,
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
                Some(Err(error.into()))
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
        self.writer.take();
        self.memory.take();
        if let Some(spill_file) = self.spill_file.take() {
            self.state.remove_file(&spill_file);
        }
    }
}

pub(super) fn reserve_writer_memory(
    memory: &MemoryPool,
    schema: &Schema,
) -> Result<MemoryReservation> {
    let bytes = writer_memory_bytes(schema);
    memory.try_reserve(bytes).map_err(|_| {
        Error::ResourceExhausted(format!(
            "spill writer requires {bytes} bytes for its buffered streaming IPC/LZ4 \
                 state (query limit {} bytes, currently available {} bytes); finish an active \
                 spill writer or increase the memory limit",
            memory.limit(),
            memory.available()
        ))
    })
}

pub(super) fn writer_memory_bytes(schema: &Schema) -> usize {
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
