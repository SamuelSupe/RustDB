use std::{io::Cursor, path::Path, sync::Arc};

use arrow::{datatypes::SchemaRef, ipc::reader::FileReader, record_batch::RecordBatch};

use crate::{Error, Result};

use super::manifest::{self, BatchEntry};

const MAX_READ_MEMORY_BYTES: usize = 32 * 1024 * 1024;

pub(super) fn chunk_path(directory: &Path, seq: u64) -> std::path::PathBuf {
    directory
        .join("batches")
        .join(format!("batch-{seq:020}.arrow"))
}

pub(super) fn read_chunk(
    directory: &Path,
    schema: &SchemaRef,
    entry: &BatchEntry,
) -> Result<RecordBatch> {
    let bytes = read_chunk_bytes(directory, entry)?;
    let mut reader = FileReader::try_new(Cursor::new(bytes), None)?;
    if reader.schema().as_ref() != schema.as_ref() {
        return Err(Error::Execution(format!(
            "HTTP result batch {} has an unexpected schema",
            entry.seq
        )));
    }
    let batch = reader
        .next()
        .transpose()?
        .ok_or_else(|| Error::Execution("HTTP result batch is empty".into()))?;
    if reader.next().transpose()?.is_some()
        || u64::try_from(batch.num_rows()).unwrap_or(u64::MAX) != entry.rows
    {
        return Err(Error::Execution(format!(
            "HTTP result batch {} has invalid row metadata",
            entry.seq
        )));
    }
    Ok(batch)
}

pub(super) fn read_chunk_bytes(directory: &Path, entry: &BatchEntry) -> Result<Vec<u8>> {
    let path = chunk_path(directory, entry.seq);
    let length = usize::try_from(entry.bytes).map_err(|_| {
        Error::ResourceExhausted("stored HTTP result batch is too large to read".into())
    })?;
    if length > MAX_READ_MEMORY_BYTES {
        return Err(Error::ResourceExhausted(
            "one stored result batch exceeds the HTTP read memory limit".into(),
        ));
    }
    let metadata = path
        .symlink_metadata()
        .map_err(|error| Error::io(Some(path.clone()), error))?;
    if !metadata.file_type().is_file()
        || metadata.file_type().is_symlink()
        || metadata.len() != entry.bytes
    {
        return Err(Error::Execution(format!(
            "HTTP result batch {} does not match its manifest",
            entry.seq
        )));
    }
    let bytes = std::fs::read(&path).map_err(|error| Error::io(Some(path.clone()), error))?;
    if bytes.len() != length {
        return Err(Error::Execution(format!(
            "HTTP result batch {} does not match its manifest",
            entry.seq
        )));
    }
    if manifest::sha256(&bytes) != entry.sha256 {
        return Err(Error::Execution(format!(
            "HTTP result batch {} failed its SHA-256 integrity check",
            entry.seq
        )));
    }
    Ok(bytes)
}

pub(super) fn read_pages(
    directory: &Path,
    schema: &SchemaRef,
    batches: &[BatchEntry],
    total_rows: u64,
    offset: u64,
    limit: usize,
) -> Result<Vec<RecordBatch>> {
    if offset > total_rows {
        return Err(Error::InvalidArgument(format!(
            "result offset {offset} exceeds row count {total_rows}"
        )));
    }
    if limit == 0 || offset == total_rows {
        return Ok(Vec::new());
    }
    let mut start = 0_u64;
    let mut remaining = limit;
    let mut output = Vec::new();
    let mut output_bytes = 0usize;
    for entry in batches {
        let end = start.saturating_add(entry.rows);
        if end <= offset {
            start = end;
            continue;
        }
        let batch = read_chunk(directory, schema, entry)?;
        let local = usize::try_from(offset.saturating_sub(start)).unwrap_or(usize::MAX);
        if local >= batch.num_rows() {
            start = end;
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
        start = end;
    }
    Ok(output)
}

pub(super) fn schema_matches(batch: &RecordBatch, schema: &SchemaRef) -> bool {
    Arc::ptr_eq(&batch.schema(), schema) || batch.schema().as_ref() == schema.as_ref()
}
