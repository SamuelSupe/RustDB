use std::{ops::Range, sync::Arc};

use bytes::Bytes;
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
}

impl QueryIo {
    pub(super) fn new(control: QueryControl, metrics: QueryMetrics) -> Self {
        Self { control, metrics }
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

        let bytes = if let Some(query) = &self.query {
            tokio::select! {
                _ = query.control.cancelled() => return Err(external_error(Error::Cancelled)),
                bytes = response.bytes() => bytes,
            }
        } else {
            response.bytes().await
        }
        .map_err(|error| object_error(&self.uri, error))?;

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

#[cfg(test)]
mod tests {
    use std::fs;

    use parquet::arrow::async_reader::AsyncFileReader;
    use tempfile::tempdir;

    use super::{QueryIo, SnapshotParquetReader};
    use crate::{
        S3Config,
        runtime::{QueryControl, QueryMetrics},
        storage::LocationResolver,
    };

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

    async fn resolve(path: &std::path::Path) -> crate::storage::ObjectSource {
        LocationResolver::new(S3Config::default())
            .resolve(&[path.display().to_string()])
            .await
            .unwrap()
            .pop()
            .unwrap()
    }
}
