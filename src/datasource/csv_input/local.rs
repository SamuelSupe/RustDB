use std::{
    fs::{File, Metadata},
    io,
    pin::Pin,
    task::{Context, Poll},
    time::{Instant, SystemTime},
};

use tokio::io::{AsyncRead, AsyncReadExt, AsyncSeekExt, ReadBuf, SeekFrom, Take};

use super::{CsvInput, QueryIo, input_error};
use crate::{
    Error, Result,
    storage::{LocalFileIdentity, ObjectSnapshot},
};

pub(super) async fn open(
    file: std::fs::File,
    start: u64,
    end: u64,
    uri: &str,
    snapshot: &ObjectSnapshot,
    query: Option<QueryIo>,
) -> Result<CsvInput> {
    validate_file_identity(&file, uri, snapshot)?;
    let verifier = file.try_clone().map_err(|error| input_error(uri, error))?;
    let mut file = tokio::fs::File::from_std(file);
    if start != 0 {
        file.seek(SeekFrom::Start(start))
            .await
            .map_err(|error| input_error(uri, error))?;
    }
    let remaining = end.saturating_sub(start);
    Ok(Box::pin(MeteredLocalReader {
        inner: file.take(remaining),
        verifier,
        uri: uri.to_owned(),
        snapshot: snapshot.clone(),
        eof_validated: false,
        query,
        pending_since: None,
    }))
}

struct MeteredLocalReader {
    inner: Take<tokio::fs::File>,
    verifier: File,
    uri: String,
    snapshot: ObjectSnapshot,
    eof_validated: bool,
    query: Option<QueryIo>,
    pending_since: Option<Instant>,
}

impl AsyncRead for MeteredLocalReader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.as_mut().get_mut();
        if this
            .query
            .as_ref()
            .is_some_and(|query| query.control.is_cancelled())
        {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::Interrupted,
                Error::Cancelled.to_string(),
            )));
        }
        if this.inner.limit() == 0 {
            if !this.eof_validated {
                let started = this.query.as_ref().map(|_| Instant::now());
                let validation = validate_file_identity(&this.verifier, &this.uri, &this.snapshot);
                if let (Some(query), Some(started)) = (&this.query, started) {
                    query.metrics.record_csv_source_io_time(started.elapsed());
                }
                validation.map_err(identity_io_error)?;
                this.eof_validated = true;
            }
            return Poll::Ready(Ok(()));
        }
        if this.query.is_some() && this.pending_since.is_none() {
            this.pending_since = Some(Instant::now());
        }
        let before = buffer.filled().len();
        let result = Pin::new(&mut this.inner).poll_read(context, buffer);
        if let Poll::Ready(outcome) = &result
            && let Some(query) = &this.query
        {
            if let Some(started) = this.pending_since.take() {
                query.metrics.record_csv_source_io_time(started.elapsed());
            }
            if outcome.is_ok() {
                let read = buffer.filled().len().saturating_sub(before);
                query
                    .metrics
                    .add_csv_source_bytes(u64::try_from(read).unwrap_or(u64::MAX));
            }
        }
        result
    }
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

fn identity_io_error(error: Error) -> io::Error {
    io::Error::other(error.to_string())
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
