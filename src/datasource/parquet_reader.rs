use std::{ops::Range, sync::Arc};

use bytes::{Bytes, BytesMut};
use futures::{FutureExt, future::BoxFuture};
use object_store::{GetOptions, GetRange, ObjectStore, path::Path};
use parquet::{
    arrow::{arrow_reader::ArrowReaderOptions, async_reader::AsyncFileReader},
    errors::{ParquetError, Result as ParquetResult},
    file::metadata::{ParquetMetaData, ParquetMetaDataReader},
};

use crate::{
    Error,
    runtime::{QueryControl, QueryMetrics},
    storage::{ObjectSnapshot, ObjectSource},
};

/// Query-scoped Parquet reader. Every byte request is conditional on the
/// identity captured by the query's HEAD request.
#[derive(Clone)]
pub(super) struct SnapshotParquetReader {
    uri: String,
    store: Arc<dyn ObjectStore>,
    location: Path,
    snapshot: ObjectSnapshot,
    query: Option<QueryIo>,
    s3: bool,
}

#[derive(Clone)]
pub(super) struct QueryIo {
    control: QueryControl,
    metrics: QueryMetrics,
    purpose: IoPurpose,
}

#[derive(Clone, Copy)]
enum IoPurpose {
    General,
    PageIndex,
    BloomFilter,
}

impl QueryIo {
    pub(super) fn new(control: QueryControl, metrics: QueryMetrics) -> Self {
        Self {
            control,
            metrics,
            purpose: IoPurpose::General,
        }
    }

    pub(super) fn for_page_index(control: QueryControl, metrics: QueryMetrics) -> Self {
        Self {
            control,
            metrics,
            purpose: IoPurpose::PageIndex,
        }
    }

    pub(super) fn for_bloom_filter(control: QueryControl, metrics: QueryMetrics) -> Self {
        Self {
            control,
            metrics,
            purpose: IoPurpose::BloomFilter,
        }
    }

    fn record_bytes(&self, bytes: usize) {
        let bytes = u64::try_from(bytes).unwrap_or(u64::MAX);
        match self.purpose {
            IoPurpose::General => {}
            IoPurpose::PageIndex => self.metrics.add_parquet_page_index_bytes_read(bytes),
            IoPurpose::BloomFilter => self.metrics.add_parquet_bloom_filter_bytes_read(bytes),
        }
    }
}

impl SnapshotParquetReader {
    pub(super) fn new(
        source: &ObjectSource,
        snapshot: ObjectSnapshot,
        query: Option<QueryIo>,
    ) -> Self {
        Self {
            uri: source.uri().to_owned(),
            store: Arc::clone(source.store()),
            location: source.location().clone(),
            snapshot,
            query,
            s3: source.is_s3(),
        }
    }

    async fn read_range(&self, range: Range<u64>) -> ParquetResult<Bytes> {
        const MAX_RANGE_BYTES: u64 = 4 * 1024 * 1024;
        if range.start > range.end {
            return Err(external_error(Error::Execution(format!(
                "invalid byte range for {}: {range:?}",
                self.uri
            ))));
        }
        if range.start == range.end {
            return Ok(Bytes::new());
        }
        if range.end - range.start <= MAX_RANGE_BYTES {
            return self.read_range_once(range).await;
        }

        let capacity = usize::try_from(range.end - range.start).map_err(|_| {
            external_error(Error::ResourceExhausted(format!(
                "byte range for {} is too large for this platform: {range:?}",
                self.uri
            )))
        })?;
        let mut output = BytesMut::with_capacity(capacity);
        let mut start = range.start;
        while start < range.end {
            let end = start.saturating_add(MAX_RANGE_BYTES).min(range.end);
            output.extend_from_slice(&self.read_range_once(start..end).await?);
            start = end;
        }
        Ok(output.freeze())
    }

    async fn read_range_once(&self, range: Range<u64>) -> ParquetResult<Bytes> {
        if let Some(query) = &self.query {
            query.control.check_cancelled().map_err(external_error)?;
            if self.s3 {
                query.metrics.add_s3_requests(1);
            }
        }

        let options = GetOptions {
            if_match: self.snapshot.e_tag.clone(),
            version: self.snapshot.version.clone(),
            range: Some(GetRange::Bounded(range)),
            ..GetOptions::default()
        };
        let response = if let Some(query) = &self.query {
            tokio::select! {
                _ = query.control.cancelled() => return Err(external_error(Error::Cancelled)),
                response = self.store.get_opts(&self.location, options) => response,
            }
        } else {
            self.store.get_opts(&self.location, options).await
        }
        .map_err(|error| object_error(&self.uri, error))?;
        self.snapshot
            .validate_get_response(&self.uri, &response.meta)
            .map_err(external_error)?;

        let bytes = if let Some(query) = &self.query {
            tokio::select! {
                _ = query.control.cancelled() => return Err(external_error(Error::Cancelled)),
                bytes = response.bytes() => bytes,
            }
        } else {
            response.bytes().await
        }
        .map_err(|error| object_error(&self.uri, error))?;

        if let Some(query) = &self.query {
            query.record_bytes(bytes.len());
        }

        if self.s3
            && let Some(query) = &self.query
        {
            query
                .metrics
                .add_s3_bytes_transferred(u64::try_from(bytes.len()).unwrap_or(u64::MAX));
        }
        Ok(bytes)
    }

    /// Reads only the fixed Parquet trailer and returns the encoded metadata
    /// length. Callers can reserve a conservative amount before decoding the
    /// footer into Arrow/Parquet objects.
    pub(super) async fn footer_metadata_len(&self) -> ParquetResult<usize> {
        const TRAILER_LEN: u64 = 8;
        if self.snapshot.size < TRAILER_LEN {
            return Err(external_error(Error::Execution(format!(
                "invalid Parquet file {}: file is shorter than the 8-byte trailer",
                self.uri
            ))));
        }

        let start = self.snapshot.size - TRAILER_LEN;
        let trailer = self.read_range(start..self.snapshot.size).await?;
        if trailer.len() != 8 || &trailer[4..] != b"PAR1" {
            return Err(external_error(Error::Execution(format!(
                "invalid Parquet footer for {}: missing PAR1 trailer magic",
                self.uri
            ))));
        }

        let encoded =
            u32::from_le_bytes(trailer[..4].try_into().map_err(|_| {
                external_error(Error::Internal("invalid trailer slice".to_owned()))
            })?);
        let metadata_len = usize::try_from(encoded).map_err(|_| {
            external_error(Error::ResourceExhausted(format!(
                "Parquet footer for {} is too large for this platform: {encoded} bytes",
                self.uri
            )))
        })?;
        let encoded_len = u64::from(encoded);
        if encoded_len > self.snapshot.size - TRAILER_LEN {
            return Err(external_error(Error::Execution(format!(
                "invalid Parquet footer for {}: footer length {encoded_len} exceeds file size {}",
                self.uri, self.snapshot.size
            ))));
        }
        Ok(metadata_len)
    }
}

impl AsyncFileReader for SnapshotParquetReader {
    fn get_bytes(&mut self, range: Range<u64>) -> BoxFuture<'_, ParquetResult<Bytes>> {
        self.read_range(range).boxed()
    }

    fn get_metadata<'a>(
        &'a mut self,
        options: Option<&'a ArrowReaderOptions>,
    ) -> BoxFuture<'a, ParquetResult<Arc<ParquetMetaData>>> {
        let file_size = self.snapshot.size;
        async move {
            let metadata_options = options.map(|options| options.metadata_options().clone());
            let metadata = ParquetMetaDataReader::new()
                .with_metadata_options(metadata_options)
                .load_and_finish(self, file_size)
                .await?;
            Ok(Arc::new(metadata))
        }
        .boxed()
    }
}

fn object_error(uri: &str, error: object_store::Error) -> ParquetError {
    let message = if matches!(
        &error,
        object_store::Error::Precondition { .. } | object_store::Error::NotFound { .. }
    ) {
        format!("object changed during query: {uri}: {error}")
    } else {
        format!("object read failed for {uri}: {error}")
    };
    external_error(Error::Execution(message))
}

fn external_error(error: Error) -> ParquetError {
    ParquetError::External(Box::new(error))
}

pub(super) fn into_query_error(error: ParquetError) -> Error {
    match error {
        ParquetError::External(source) => match source.downcast::<Error>() {
            Ok(error) => *error,
            Err(source) => Error::Parquet(ParquetError::External(source)),
        },
        error => Error::Parquet(error),
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use parquet::arrow::async_reader::AsyncFileReader;
    use tempfile::tempdir;

    use super::{QueryIo, SnapshotParquetReader, external_error, into_query_error};
    use crate::{
        Error, S3Config,
        runtime::{QueryControl, QueryMetrics},
        storage::LocationResolver,
    };

    #[test]
    fn query_errors_survive_the_parquet_async_reader_boundary() {
        assert!(matches!(
            into_query_error(external_error(Error::Cancelled)),
            Error::Cancelled
        ));
        assert!(matches!(
            into_query_error(external_error(Error::ResourceExhausted("query budget".into()))),
            Error::ResourceExhausted(message) if message == "query budget"
        ));
    }

    #[tokio::test]
    async fn conditional_range_rejects_a_changed_object() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("data.bin");
        fs::write(&path, b"before").unwrap();
        let source = resolve(&path).await;
        let snapshot = source.head_snapshot().await.unwrap();
        let mut reader = SnapshotParquetReader::new(&source, snapshot, None);

        fs::write(&path, b"after-is-different").unwrap();
        let error = reader.get_bytes(0..1).await.unwrap_err();

        assert!(error.to_string().contains("object changed during query"));
        assert!(error.to_string().contains(source.uri()));
    }

    #[tokio::test]
    async fn range_rejects_size_change_without_an_identity_token() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("no-token.bin");
        fs::write(&path, b"before").unwrap();
        let source = resolve(&path).await;
        let mut snapshot = source.head_snapshot().await.unwrap();
        snapshot.e_tag = None;
        snapshot.version = None;
        let mut reader = SnapshotParquetReader::new(&source, snapshot, None);

        fs::write(&path, b"after-is-a-different-size").unwrap();
        let error = reader.get_bytes(0..1).await.unwrap_err().to_string();

        assert!(error.contains("object changed during query"), "{error}");
        assert!(error.contains(source.uri()), "{error}");
        assert!(error.contains("expected size"), "{error}");
    }

    #[tokio::test]
    async fn counts_successful_range_bytes_and_honours_cancellation() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("data.bin");
        fs::write(&path, b"0123456789").unwrap();
        let source = resolve(&path).await;
        let snapshot = source.head_snapshot().await.unwrap();
        let control = QueryControl::new();
        let metrics = QueryMetrics::new();
        let query = QueryIo::new(control.clone(), metrics.clone());
        let mut reader = SnapshotParquetReader::new(&source, snapshot, Some(query));
        // Exercise the S3 accounting path with a deterministic local store.
        reader.s3 = true;

        assert_eq!(reader.get_bytes(2..7).await.unwrap().as_ref(), b"23456");
        assert_eq!(metrics.snapshot().s3_requests, 1);
        assert_eq!(metrics.snapshot().s3_bytes_transferred, 5);

        control.cancel();
        let error = reader.get_bytes(0..1).await.unwrap_err();
        assert!(error.to_string().contains("query cancelled"));
        assert_eq!(metrics.snapshot().s3_requests, 1);
    }

    #[tokio::test]
    async fn splits_large_ranges_into_four_mib_requests() {
        const FOUR_MIB: usize = 4 * 1024 * 1024;
        let directory = tempdir().unwrap();
        let path = directory.path().join("large.bin");
        fs::write(&path, vec![7_u8; FOUR_MIB + 1]).unwrap();
        let source = resolve(&path).await;
        let snapshot = source.head_snapshot().await.unwrap();
        let metrics = QueryMetrics::new();
        let query = QueryIo::new(QueryControl::new(), metrics.clone());
        let mut reader = SnapshotParquetReader::new(&source, snapshot, Some(query));
        reader.s3 = true;

        let bytes = reader
            .get_bytes(0..u64::try_from(FOUR_MIB + 1).unwrap())
            .await
            .unwrap();

        assert_eq!(bytes.len(), FOUR_MIB + 1);
        assert_eq!(metrics.snapshot().s3_requests, 2);
        assert_eq!(
            metrics.snapshot().s3_bytes_transferred,
            (FOUR_MIB + 1) as u64
        );
    }

    async fn resolve(path: &std::path::Path) -> crate::storage::ObjectSource {
        LocationResolver::new(S3Config::default())
            .resolve(&[path.display().to_string()])
            .await
            .unwrap()
            .pop()
            .unwrap()
    }
}
