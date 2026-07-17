use std::{ops::Range, path::PathBuf, sync::Arc, time::Instant};

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
    runtime::{QueryControl, QueryLocalFileHandle, QueryMetrics},
    storage::{ObjectSnapshot, ObjectSource},
};

const MAX_RANGE_BYTES: u64 = 4 * 1024 * 1024;

mod local;

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
    local_path: Option<PathBuf>,
    local_file: Option<Arc<QueryLocalFileHandle>>,
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
    NativePredicateSidecar,
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

    pub(in crate::datasource) fn for_native_predicate_sidecar(
        control: QueryControl,
        metrics: QueryMetrics,
    ) -> Self {
        Self {
            control,
            metrics,
            purpose: IoPurpose::NativePredicateSidecar,
        }
    }

    fn record_bytes(&self, bytes: usize) {
        let bytes = u64::try_from(bytes).unwrap_or(u64::MAX);
        match self.purpose {
            IoPurpose::General => {}
            IoPurpose::PageIndex => self.metrics.add_parquet_page_index_bytes_read(bytes),
            IoPurpose::BloomFilter => self.metrics.add_parquet_bloom_filter_bytes_read(bytes),
            IoPurpose::NativePredicateSidecar => {
                self.metrics.record_native_predicate_sidecar_read(bytes)
            }
        }
    }

    fn records_parquet_range(&self) -> bool {
        !matches!(self.purpose, IoPurpose::NativePredicateSidecar)
    }
}

impl SnapshotParquetReader {
    pub(super) fn new(
        source: &ObjectSource,
        snapshot: ObjectSnapshot,
        query: Option<QueryIo>,
    ) -> Self {
        let local_path = source.local_path().map(PathBuf::from);
        let local_file = local_path.as_ref().map(|_| {
            query
                .as_ref()
                .map(|query| query.control.local_file_handle(source.uri()))
                .unwrap_or_default()
        });
        Self {
            uri: source.uri().to_owned(),
            store: Arc::clone(source.store()),
            location: source.location().clone(),
            snapshot,
            query,
            s3: source.is_s3(),
            local_file,
            local_path,
        }
    }

    async fn read_range(&self, range: Range<u64>) -> ParquetResult<Bytes> {
        if range.start > range.end {
            return Err(external_error(Error::Execution(format!(
                "invalid byte range for {}: {range:?}",
                self.uri
            ))));
        }
        if range.start == range.end {
            return Ok(Bytes::new());
        }
        if !self.s3 && self.local_path.is_some() {
            let mut output = self.read_local_ranges(vec![range]).await?;
            return Ok(output.pop().expect("one local range returns one buffer"));
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
        let started = Instant::now();
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
            if query.records_parquet_range() {
                query.metrics.record_parquet_range_read(
                    u64::try_from(bytes.len()).unwrap_or(u64::MAX),
                    started.elapsed(),
                );
            }
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

    async fn read_ranges(&self, ranges: Vec<Range<u64>>) -> ParquetResult<Vec<Bytes>> {
        if self.s3 || self.local_path.is_none() || ranges.is_empty() {
            let mut bytes = Vec::with_capacity(ranges.len());
            for range in ranges {
                bytes.push(self.read_range(range).await?);
            }
            return Ok(bytes);
        }

        self.read_local_ranges(ranges).await
    }

    async fn read_local_ranges(&self, ranges: Vec<Range<u64>>) -> ParquetResult<Vec<Bytes>> {
        let bytes = local::read_ranges(
            Arc::clone(
                self.local_file
                    .as_ref()
                    .expect("local read routing requires a shared file handle"),
            ),
            self.local_path
                .clone()
                .expect("local read routing requires a canonical path"),
            self.uri.clone(),
            self.snapshot.clone(),
            self.query.as_ref().map(|query| query.control.clone()),
            self.query
                .as_ref()
                .filter(|query| query.records_parquet_range())
                .map(|query| query.metrics.clone()),
            ranges,
        )
        .await
        .map_err(external_error)?;
        if let Some(query) = &self.query {
            query.record_bytes(
                bytes
                    .iter()
                    .map(Bytes::len)
                    .fold(0usize, usize::saturating_add),
            );
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

    /// Crate datasource range reads reuse the same conditional object request,
    /// local identity checks, cancellation, and query-local descriptor pool as
    /// Parquet metadata/data reads.
    pub(in crate::datasource) async fn query_range(
        &self,
        range: Range<u64>,
    ) -> crate::Result<Bytes> {
        self.read_range(range).await.map_err(into_query_error)
    }

    pub(in crate::datasource) async fn query_ranges(
        &self,
        ranges: Vec<Range<u64>>,
    ) -> crate::Result<Vec<Bytes>> {
        self.read_ranges(ranges).await.map_err(into_query_error)
    }
}

impl AsyncFileReader for SnapshotParquetReader {
    fn get_bytes(&mut self, range: Range<u64>) -> BoxFuture<'_, ParquetResult<Bytes>> {
        self.read_range(range).boxed()
    }

    fn get_byte_ranges(
        &mut self,
        ranges: Vec<Range<u64>>,
    ) -> BoxFuture<'_, ParquetResult<Vec<Bytes>>> {
        self.read_ranges(ranges).boxed()
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

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    use std::{fs::FileTimes, time::Duration};

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

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[tokio::test]
    async fn local_range_rejects_same_size_mutation_with_restored_mtime() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("strong-identity.bin");
        fs::write(&path, b"before").unwrap();
        let source = resolve(&path).await;
        let snapshot = source.head_snapshot().await.unwrap();
        let modified = fs::metadata(&path).unwrap().modified().unwrap();
        let mut reader = SnapshotParquetReader::new(&source, snapshot, None);
        assert_eq!(reader.get_bytes(0..1).await.unwrap().as_ref(), b"b");

        std::thread::sleep(Duration::from_millis(2));
        fs::write(&path, b"after!").unwrap();
        fs::File::options()
            .write(true)
            .open(&path)
            .unwrap()
            .set_times(FileTimes::new().set_modified(modified))
            .unwrap();

        let error = reader.get_bytes(0..1).await.unwrap_err().to_string();
        assert!(error.contains("object changed during query"), "{error}");
        assert!(error.contains(source.uri()), "{error}");
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
        assert_eq!(metrics.snapshot().parquet_local_file_opens, 0);

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

    #[tokio::test]
    async fn conditional_multi_range_reads_keep_per_range_preconditions() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("conditional-ranges.bin");
        fs::write(&path, b"0123456789").unwrap();
        let source = resolve(&path).await;
        let snapshot = source.head_snapshot().await.unwrap();
        let metrics = QueryMetrics::new();
        let query = QueryIo::new(QueryControl::new(), metrics.clone());
        let mut reader = SnapshotParquetReader::new(&source, snapshot, Some(query));
        reader.s3 = true;

        let bytes = reader.get_byte_ranges(vec![1..3, 7..10]).await.unwrap();

        assert_eq!(bytes[0].as_ref(), b"12");
        assert_eq!(bytes[1].as_ref(), b"789");
        assert_eq!(metrics.snapshot().s3_requests, 2);
        assert_eq!(metrics.snapshot().s3_bytes_transferred, 5);
    }

    #[tokio::test]
    async fn local_multi_range_reads_preserve_order_and_snapshot_identity() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("ranges.bin");
        fs::write(&path, b"0123456789").unwrap();
        let source = resolve(&path).await;
        let snapshot = source.head_snapshot().await.unwrap();
        let mut reader = SnapshotParquetReader::new(&source, snapshot, None);

        let bytes = reader
            .get_byte_ranges(vec![6..10, 1..4, 4..4])
            .await
            .unwrap();
        assert_eq!(bytes[0].as_ref(), b"6789");
        assert_eq!(bytes[1].as_ref(), b"123");
        assert!(bytes[2].is_empty());

        fs::write(&path, b"changed-size").unwrap();
        let error = reader
            .get_byte_ranges(vec![0..1, 1..2])
            .await
            .unwrap_err()
            .to_string();
        assert!(error.contains("object changed during query"), "{error}");
    }

    #[tokio::test]
    async fn query_local_readers_share_one_descriptor_and_read_concurrently() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("shared.bin");
        fs::write(&path, b"0123456789abcdef").unwrap();
        let source = resolve(&path).await;
        let snapshot = source.head_snapshot().await.unwrap();
        let metrics = QueryMetrics::new();
        let query = QueryIo::new(QueryControl::new(), metrics.clone());
        let mut left = SnapshotParquetReader::new(&source, snapshot.clone(), Some(query.clone()));
        let mut right = SnapshotParquetReader::new(&source, snapshot, Some(query));

        let (left_bytes, right_bytes) = tokio::join!(
            left.get_byte_ranges(vec![0..4, 8..12]),
            right.get_byte_ranges(vec![4..8, 12..16]),
        );

        let left_bytes = left_bytes.unwrap();
        let right_bytes = right_bytes.unwrap();
        assert_eq!(left_bytes[0].as_ref(), b"0123");
        assert_eq!(left_bytes[1].as_ref(), b"89ab");
        assert_eq!(right_bytes[0].as_ref(), b"4567");
        assert_eq!(right_bytes[1].as_ref(), b"cdef");
        let metrics = metrics.snapshot();
        assert_eq!(metrics.parquet_local_file_opens, 1);
        assert_eq!(metrics.parquet_range_bytes_read, 16);
        assert!(!metrics.parquet_range_read_time.is_zero());
    }

    #[tokio::test]
    async fn local_bulk_rejects_an_invalid_multi_range_before_reading() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("invalid-ranges.bin");
        fs::write(&path, b"0123456789").unwrap();
        let source = resolve(&path).await;
        let snapshot = source.head_snapshot().await.unwrap();
        let mut reader = SnapshotParquetReader::new(&source, snapshot, None);

        let error = reader
            .get_byte_ranges(vec![0..1, std::ops::Range { start: 7, end: 3 }])
            .await
            .unwrap_err()
            .to_string();

        assert!(error.contains("invalid byte range"), "{error}");
        assert!(error.contains(source.uri()), "{error}");
    }

    #[tokio::test]
    async fn local_bulk_rejects_a_same_size_path_replacement() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("identity.bin");
        let retired = directory.path().join("retired.bin");
        let replacement = directory.path().join("replacement.bin");
        fs::write(&path, b"original").unwrap();
        fs::write(&replacement, b"replaced").unwrap();
        let source = resolve(&path).await;
        let snapshot = source.head_snapshot().await.unwrap();
        let first_query = QueryIo::new(QueryControl::new(), QueryMetrics::new());
        let mut reader = SnapshotParquetReader::new(&source, snapshot, Some(first_query));
        assert_eq!(reader.get_bytes(0..1).await.unwrap().as_ref(), b"o");
        fs::rename(&path, retired).unwrap();
        fs::rename(replacement, &path).unwrap();

        let error = reader
            .get_byte_ranges(vec![0..4, 4..8])
            .await
            .unwrap_err()
            .to_string();

        assert!(error.contains("object changed during query"), "{error}");
        assert!(error.contains(source.uri()), "{error}");

        let replacement_snapshot = source.head_snapshot().await.unwrap();
        let next_query = QueryIo::new(QueryControl::new(), QueryMetrics::new());
        let mut replacement_reader =
            SnapshotParquetReader::new(&source, replacement_snapshot, Some(next_query));
        assert_eq!(
            replacement_reader.get_bytes(0..1).await.unwrap().as_ref(),
            b"r"
        );
    }

    #[tokio::test]
    async fn reused_local_descriptor_rejects_path_deletion() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("deleted.bin");
        fs::write(&path, b"original").unwrap();
        let source = resolve(&path).await;
        let snapshot = source.head_snapshot().await.unwrap();
        let mut reader = SnapshotParquetReader::new(&source, snapshot, None);
        assert_eq!(reader.get_bytes(0..1).await.unwrap().as_ref(), b"o");
        fs::remove_file(&path).unwrap();

        let error = reader.get_bytes(1..2).await.unwrap_err().to_string();

        assert!(error.contains("object changed during query"), "{error}");
        assert!(error.contains(source.uri()), "{error}");
    }

    #[tokio::test]
    async fn local_bulk_honours_preexisting_cancellation() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("cancelled.bin");
        fs::write(&path, b"0123456789").unwrap();
        let source = resolve(&path).await;
        let snapshot = source.head_snapshot().await.unwrap();
        let control = QueryControl::new();
        control.cancel();
        let query = QueryIo::new(control, QueryMetrics::new());
        let mut reader = SnapshotParquetReader::new(&source, snapshot, Some(query));

        let error = reader.get_byte_ranges(vec![0..4, 4..8]).await.unwrap_err();

        assert!(matches!(into_query_error(error), Error::Cancelled));
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
