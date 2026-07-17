use std::{
    fs::{File, Metadata},
    ops::Range,
    path::{Path, PathBuf},
    sync::Arc,
    time::{Instant, SystemTime},
};

use bytes::Bytes;

use crate::{
    Error, Result,
    runtime::{QueryControl, QueryLocalFileHandle, QueryMetrics},
    storage::{LocalFileIdentity, ObjectSnapshot},
};

const READ_CHUNK_BYTES: usize = 4 * 1024 * 1024;
const MAX_COALESCED_SPAN_BYTES: u64 = 16 * 1024 * 1024;

pub(super) async fn read_ranges(
    handle: Arc<QueryLocalFileHandle>,
    path: PathBuf,
    uri: String,
    snapshot: ObjectSnapshot,
    control: Option<QueryControl>,
    metrics: Option<QueryMetrics>,
    ranges: Vec<Range<u64>>,
) -> Result<Vec<Bytes>> {
    let started = Instant::now();
    check_cancelled(control.as_ref())?;
    // Validate logical ranges before opening the path. Besides avoiding an
    // unnecessary descriptor, this preserves the more useful range error when
    // both the request and the path are invalid.
    let plan = plan_ranges(&uri, &ranges, snapshot.size)?;
    let physical_bytes = plan.physical_bytes;
    let file = handle
        .open(
            path.clone(),
            uri.clone(),
            snapshot.clone(),
            control.clone(),
            metrics.clone(),
        )
        .await?;
    let blocking_control = control.clone();
    let blocking_uri = uri.clone();
    let read = tokio::task::spawn_blocking(move || {
        read_plan_blocking(
            file.as_ref(),
            &path,
            &blocking_uri,
            &snapshot,
            blocking_control.as_ref(),
            plan,
        )
    });

    // Await the blocking job even after cancellation. The job observes the
    // same control between bounded reads; detaching it would let query cleanup
    // race an untracked file operation.
    let output = read.await.map_err(|error| {
        Error::Execution(format!(
            "local Parquet range reader failed for {uri}: {error}"
        ))
    })??;
    check_cancelled(control.as_ref())?;
    if let Some(metrics) = metrics {
        metrics.record_parquet_range_read(physical_bytes, started.elapsed());
    }
    Ok(output)
}

impl QueryLocalFileHandle {
    async fn open(
        &self,
        path: PathBuf,
        uri: String,
        snapshot: ObjectSnapshot,
        control: Option<QueryControl>,
        metrics: Option<QueryMetrics>,
    ) -> Result<Arc<File>> {
        let file = self
            .file
            .get_or_try_init(|| async {
                check_cancelled(control.as_ref())?;
                let blocking_path = path.clone();
                let blocking_uri = uri.clone();
                let blocking_snapshot = snapshot.clone();
                let open = tokio::task::spawn_blocking(move || {
                    open_validated_file(&blocking_path, &blocking_uri, &blocking_snapshot)
                });
                let file = open.await.map_err(|error| {
                    Error::Execution(format!("local Parquet file open failed for {uri}: {error}"))
                })??;
                check_cancelled(control.as_ref())?;
                if let Some(metrics) = &metrics {
                    metrics.add_parquet_local_file_open();
                }
                Ok::<Arc<File>, Error>(Arc::new(file))
            })
            .await?;
        Ok(Arc::clone(file))
    }
}

#[cfg(test)]
fn read_ranges_blocking(
    path: &Path,
    uri: &str,
    snapshot: &ObjectSnapshot,
    control: Option<&QueryControl>,
    ranges: &[Range<u64>],
) -> Result<Vec<Bytes>> {
    check_cancelled(control)?;
    let plan = plan_ranges(uri, ranges, snapshot.size)?;
    check_cancelled(control)?;
    let file = open_validated_file(path, uri, snapshot)?;
    read_plan_blocking(&file, path, uri, snapshot, control, plan)
}

fn read_plan_blocking(
    file: &File,
    path: &Path,
    uri: &str,
    snapshot: &ObjectSnapshot,
    control: Option<&QueryControl>,
    plan: ReadPlan,
) -> Result<Vec<Bytes>> {
    check_cancelled(control)?;
    tracing::trace!(
        uri = %uri,
        logical_ranges = plan.slices.len(),
        physical_spans = plan.spans.len(),
        physical_bytes = plan.physical_bytes,
        "planned local Parquet bulk read"
    );
    validate_path_identity(path, uri, snapshot)?;
    validate_file_identity(file, uri, snapshot)?;

    let mut buffers = Vec::new();
    buffers
        .try_reserve_exact(plan.spans.len())
        .map_err(|error| {
            Error::ResourceExhausted(format!(
                "local Parquet span list for {uri} cannot be allocated: {error}"
            ))
        })?;

    for span in &plan.spans {
        check_cancelled(control)?;
        let mut bytes = Vec::new();
        bytes.try_reserve_exact(span.length).map_err(|error| {
            Error::ResourceExhausted(format!(
                "local Parquet byte span for {uri} cannot allocate {} bytes: {error}",
                span.length
            ))
        })?;
        while bytes.len() < span.length {
            check_cancelled(control)?;
            let offset = bytes.len();
            let end = offset.saturating_add(READ_CHUNK_BYTES).min(span.length);
            bytes.resize(end, 0);
            let physical_offset = u64::try_from(offset)
                .ok()
                .and_then(|offset| span.range.start.checked_add(offset))
                .ok_or_else(|| {
                    Error::ResourceExhausted(format!(
                        "local Parquet read offset overflow for {uri}"
                    ))
                })?;
            if let Err(error) = read_exact_at(file, &mut bytes[offset..end], physical_offset) {
                return Err(map_read_error(file, path, uri, snapshot, error));
            }
        }
        buffers.push(Bytes::from(bytes));
    }

    check_cancelled(control)?;
    validate_file_identity(file, uri, snapshot)?;
    validate_path_identity(path, uri, snapshot)?;

    let mut output = Vec::new();
    output
        .try_reserve_exact(plan.slices.len())
        .map_err(|error| {
            Error::ResourceExhausted(format!(
                "local Parquet range result for {uri} cannot be allocated: {error}"
            ))
        })?;
    for slice in plan.slices {
        check_cancelled(control)?;
        match slice {
            Some(slice) => {
                output.push(buffers[slice.span].slice(slice.offset..slice.offset + slice.length))
            }
            None => output.push(Bytes::new()),
        }
    }
    Ok(output)
}

fn open_validated_file(path: &Path, uri: &str, snapshot: &ObjectSnapshot) -> Result<File> {
    let file = File::open(path).map_err(|error| local_io_error(path, uri, error))?;
    validate_file_identity(&file, uri, snapshot)?;
    validate_path_identity(path, uri, snapshot)?;
    Ok(file)
}

fn map_read_error(
    file: &File,
    path: &Path,
    uri: &str,
    snapshot: &ObjectSnapshot,
    error: std::io::Error,
) -> Error {
    if let Err(changed) = validate_file_identity(file, uri, snapshot) {
        return changed;
    }
    if let Err(changed) = validate_path_identity(path, uri, snapshot) {
        return changed;
    }
    local_io_error(path, uri, error)
}

struct ReadPlan {
    spans: Vec<ReadSpan>,
    slices: Vec<Option<SpanSlice>>,
    physical_bytes: u64,
}

struct ReadSpan {
    range: Range<u64>,
    length: usize,
}

#[derive(Clone, Copy)]
struct SpanSlice {
    span: usize,
    offset: usize,
    length: usize,
}

fn plan_ranges(uri: &str, ranges: &[Range<u64>], size: u64) -> Result<ReadPlan> {
    let lengths = ranges
        .iter()
        .map(|range| validate_range(uri, range, size))
        .collect::<Result<Vec<_>>>()?;
    let mut sorted = ranges
        .iter()
        .enumerate()
        .filter(|(index, _)| lengths[*index] != 0)
        .collect::<Vec<_>>();
    sorted.sort_unstable_by(|(left_index, left), (right_index, right)| {
        (left.start, left.end, *left_index).cmp(&(right.start, right.end, *right_index))
    });

    let mut spans = Vec::<ReadSpan>::new();
    let mut span_for_range = vec![None; ranges.len()];
    for (index, range) in sorted {
        let span = match spans.last_mut() {
            Some(span) if should_merge(span, range) => {
                span.range.end = span.range.end.max(range.end);
                spans.len() - 1
            }
            _ => {
                spans.push(ReadSpan {
                    range: range.clone(),
                    length: 0,
                });
                spans.len() - 1
            }
        };
        span_for_range[index] = Some(span);
    }

    let mut physical_bytes = 0_u64;
    for span in &mut spans {
        span.length = validate_range(uri, &span.range, size)?;
        physical_bytes = physical_bytes
            .checked_add(span.range.end - span.range.start)
            .ok_or_else(|| {
                Error::ResourceExhausted(format!(
                    "local Parquet physical byte count overflow for {uri}"
                ))
            })?;
    }

    let slices = ranges
        .iter()
        .enumerate()
        .map(|(index, range)| {
            span_for_range[index]
                .map(|span| {
                    let offset =
                        usize::try_from(range.start - spans[span].range.start).map_err(|_| {
                            Error::ResourceExhausted(format!(
                                "local Parquet range offset for {uri} is too large: {range:?}"
                            ))
                        })?;
                    Ok(SpanSlice {
                        span,
                        offset,
                        length: lengths[index],
                    })
                })
                .transpose()
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(ReadPlan {
        spans,
        slices,
        physical_bytes,
    })
}

fn should_merge(span: &ReadSpan, range: &Range<u64>) -> bool {
    if range.start < span.range.end {
        // Overlapping ranges must share a span to avoid duplicate physical I/O.
        return true;
    }
    range.start == span.range.end && range.end - span.range.start <= MAX_COALESCED_SPAN_BYTES
}

fn validate_range(uri: &str, range: &Range<u64>, size: u64) -> Result<usize> {
    if range.start > range.end || range.end > size {
        return Err(Error::Execution(format!(
            "invalid byte range for {uri}: {range:?} (object size {size})"
        )));
    }
    usize::try_from(range.end - range.start).map_err(|_| {
        Error::ResourceExhausted(format!(
            "byte range for {uri} is too large for this platform: {range:?}"
        ))
    })
}

fn validate_file_identity(file: &File, uri: &str, expected: &ObjectSnapshot) -> Result<()> {
    let metadata = file
        .metadata()
        .map_err(|error| Error::Execution(format!("object read failed for {uri}: {error}")))?;
    let actual = ObjectSnapshot {
        size: metadata.len(),
        e_tag: Some(local_etag(&metadata)),
        version: None,
        local_identity: LocalFileIdentity::from_metadata(&metadata),
    };
    expected.validate_snapshot(uri, &actual)
}

fn validate_path_identity(path: &Path, uri: &str, expected: &ObjectSnapshot) -> Result<()> {
    let metadata = std::fs::metadata(path).map_err(|error| local_io_error(path, uri, error))?;
    let actual = ObjectSnapshot {
        size: metadata.len(),
        e_tag: Some(local_etag(&metadata)),
        version: None,
        local_identity: LocalFileIdentity::from_metadata(&metadata),
    };
    expected.validate_snapshot(uri, &actual)
}

#[cfg(unix)]
fn read_exact_at(file: &File, buffer: &mut [u8], offset: u64) -> std::io::Result<()> {
    use std::os::unix::fs::FileExt;

    file.read_exact_at(buffer, offset)
}

#[cfg(windows)]
fn read_exact_at(file: &File, mut buffer: &mut [u8], mut offset: u64) -> std::io::Result<()> {
    use std::{io, os::windows::fs::FileExt};

    while !buffer.is_empty() {
        let read = file.seek_read(buffer, offset)?;
        if read == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "failed to fill whole buffer",
            ));
        }
        offset = offset.saturating_add(read as u64);
        buffer = &mut buffer[read..];
    }
    Ok(())
}

#[cfg(not(any(unix, windows)))]
fn read_exact_at(file: &File, buffer: &mut [u8], offset: u64) -> std::io::Result<()> {
    use std::io::{Read, Seek, SeekFrom};

    let mut file = file.try_clone()?;
    file.seek(SeekFrom::Start(offset))?;
    file.read_exact(buffer)
}

fn local_etag(metadata: &Metadata) -> String {
    let inode = inode(metadata);
    let size = metadata.len();
    let mtime = metadata
        .modified()
        .ok()
        .and_then(|mtime| mtime.duration_since(SystemTime::UNIX_EPOCH).ok())
        .unwrap_or_default()
        .as_micros();
    format!("{inode:x}-{mtime:x}-{size:x}")
}

#[cfg(unix)]
fn inode(metadata: &Metadata) -> u64 {
    use std::os::unix::fs::MetadataExt;
    metadata.ino()
}

#[cfg(not(unix))]
fn inode(_metadata: &Metadata) -> u64 {
    0
}

fn check_cancelled(control: Option<&QueryControl>) -> Result<()> {
    control.map_or(Ok(()), QueryControl::check_cancelled)
}

fn local_io_error(path: &Path, uri: &str, error: std::io::Error) -> Error {
    if error.kind() == std::io::ErrorKind::NotFound {
        Error::Execution(format!("object changed during query: {uri}: {error}"))
    } else {
        Error::io(Some(path.to_path_buf()), error)
    }
}

#[cfg(test)]
#[path = "local_tests.rs"]
mod tests;
