use std::sync::Arc;

use arrow::{csv::ReaderBuilder, datatypes::SchemaRef};
use async_stream::try_stream;
use async_trait::async_trait;
use bytes::{Buf, Bytes};
use futures::{StreamExt, stream};

use super::{
    ScanRequest, TableProvider, TableStatistics,
    csv_infer::{format, infer_table_schema},
    provider::prepare_object_sources,
};
use crate::{
    CsvOptions, EngineConfig, Error, Result,
    runtime::{QueryContext, RecordBatchStream, boxed_record_batch_stream},
    storage::{LocationResolver, ObjectSource},
};

#[derive(Clone, Debug)]
pub struct CsvTable {
    files: Arc<[ObjectSource]>,
    schema: SchemaRef,
    options: CsvOptions,
    has_header: bool,
    io_concurrency: usize,
    statistics: TableStatistics,
}

impl CsvTable {
    pub async fn try_new(
        locations: Vec<String>,
        options: CsvOptions,
        config: &EngineConfig,
    ) -> Result<Self> {
        Self::try_new_for_query(locations, options, config, None).await
    }

    pub(crate) async fn try_new_for_query(
        locations: Vec<String>,
        options: CsvOptions,
        config: &EngineConfig,
        context: Option<Arc<QueryContext>>,
    ) -> Result<Self> {
        let resolver = LocationResolver::with_memory_limit(config.s3.clone(), config.memory_limit);
        let files = match context.as_deref() {
            Some(context) => resolver.resolve_for_query(&locations, context).await?,
            None => resolver.resolve(&locations).await?,
        };
        Self::from_files(files, options, config.io_concurrency, context).await
    }

    pub async fn from_files(
        files: Vec<ObjectSource>,
        options: CsvOptions,
        io_concurrency: usize,
        context: Option<Arc<QueryContext>>,
    ) -> Result<Self> {
        if io_concurrency == 0 {
            return Err(Error::InvalidArgument(
                "I/O concurrency must be greater than zero".to_owned(),
            ));
        }
        for file in &files {
            let uri = file.uri().to_ascii_lowercase();
            if uri.ends_with(".gz") || uri.ends_with(".bz2") || uri.ends_with(".zst") {
                return Err(Error::Unsupported(format!(
                    "compressed CSV is not supported in v0.1: {}",
                    file.uri()
                )));
            }
        }

        let (schema, has_header) = infer_table_schema(&files, &options, context.as_deref()).await?;
        let total_byte_size = files.iter().fold(0_u64, |total, file| {
            total.saturating_add(file.snapshot().size)
        });
        let statistics = TableStatistics {
            row_count: None,
            total_byte_size: Some(total_byte_size),
            file_count: files.len(),
        };
        Ok(Self {
            files: files.into(),
            schema,
            options,
            has_header,
            io_concurrency,
            statistics,
        })
    }
}

#[async_trait]
impl TableProvider for CsvTable {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn statistics(&self) -> TableStatistics {
        self.statistics.clone()
    }

    async fn prepare(&self, context: Arc<QueryContext>) -> Result<()> {
        prepare_object_sources(&self.files, self.io_concurrency, context).await
    }

    async fn scan(
        &self,
        request: ScanRequest,
        context: Arc<QueryContext>,
    ) -> Result<RecordBatchStream> {
        if request.batch_size == 0 {
            return Err(Error::InvalidArgument(
                "scan batch_size must be greater than zero".to_owned(),
            ));
        }
        let output_schema = request.projected_schema(&self.schema)?;
        let projection = request.projection.clone();
        let has_header = self.has_header;
        let batch_size = request.batch_size;
        let io_concurrency = self.io_concurrency;
        let files = Arc::clone(&self.files);
        let file_scan = CsvFileScan {
            schema: Arc::clone(&self.schema),
            options: self.options.clone(),
            has_header,
            projection,
            batch_size,
        };
        let streams = stream::iter(0..files.len()).map({
            let context = Arc::clone(&context);
            let file_scan = file_scan.clone();
            move |index| {
                csv_file_stream(
                    Arc::clone(&files),
                    index,
                    file_scan.clone(),
                    Arc::clone(&context),
                )
            }
        });
        let mut merged = streams.flatten_unordered(io_concurrency);

        let stream = try_stream! {
            let mut remaining = request.limit.unwrap_or(usize::MAX);
            while remaining > 0 {
                let Some(batch) = merged.next().await else {
                    break;
                };
                context.check_cancelled()?;
                let batch = batch?;
                let batch = if batch.num_rows() > remaining {
                    batch.slice(0, remaining)
                } else {
                    batch
                };
                remaining = remaining.saturating_sub(batch.num_rows());
                debug_assert_eq!(batch.schema(), output_schema);
                yield batch;
            }
        };
        Ok(boxed_record_batch_stream(stream))
    }
}

#[derive(Clone)]
struct CsvFileScan {
    schema: SchemaRef,
    options: CsvOptions,
    has_header: bool,
    projection: Option<Vec<usize>>,
    batch_size: usize,
}

fn csv_file_stream(
    files: Arc<[ObjectSource]>,
    file_index: usize,
    scan: CsvFileScan,
    context: Arc<QueryContext>,
) -> RecordBatchStream {
    let stream = try_stream! {
        context.check_cancelled()?;
        let file = files[file_index].clone();
        let snapshot = context.object_snapshot(file.uri())?;
        if file.is_s3() {
            context.metrics.add_s3_requests(1);
        }
        let get = tokio::select! {
            _ = context.control.cancelled() => Err(Error::Cancelled),
            get = file.store().get_opts(file.location(), file.get_options_for(&snapshot)) => {
                get.map_err(|error| csv_object_error(file.uri(), error))
            },
        }?;
        let mut input = get.into_stream();
        let mut builder = ReaderBuilder::new(scan.schema)
            .with_format(format(&scan.options, scan.has_header))
            .with_batch_size(scan.batch_size)
            .with_truncated_rows(false);
        if let Some(projection) = scan.projection {
            builder = builder.with_projection(projection);
        }
        let mut decoder = builder.build_decoder();
        let mut buffered = Bytes::new();
        let mut input_finished = false;
        let mut bytes_since_batch = 0_u64;

        loop {
            context.check_cancelled()?;
            loop {
                // An empty slice is Arrow CSV's EOF marker. Keep fetching while
                // the object stream is live so records can span input chunks.
                while buffered.is_empty() && !input_finished {
                    let next = tokio::select! {
                        _ = context.control.cancelled() => Err(Error::Cancelled),
                        next = input.next() => Ok(next),
                    }?;
                    match next {
                        Some(bytes) => {
                            buffered = bytes.map_err(|error| csv_object_error(file.uri(), error))?;
                            if file.is_s3() {
                                context.metrics.add_s3_bytes_transferred(
                                    u64::try_from(buffered.len()).unwrap_or(u64::MAX),
                                );
                            }
                            bytes_since_batch = bytes_since_batch.saturating_add(
                                u64::try_from(buffered.len()).unwrap_or(u64::MAX),
                            );
                        }
                        None => input_finished = true,
                    }
                }

                let decoded = decoder
                    .decode(buffered.as_ref())
                    .map_err(|error| csv_decode_error(file.uri(), error))?;
                if decoded == 0 {
                    break;
                }
                buffered.advance(decoded);
            }

            if let Some(batch) = decoder
                .flush()
                .map_err(|error| csv_decode_error(file.uri(), error))?
            {
                context.metrics.record_scan(
                    u64::try_from(batch.num_rows()).unwrap_or(u64::MAX),
                    1,
                    bytes_since_batch,
                );
                bytes_since_batch = 0;
                yield batch;
            } else if input_finished {
                if bytes_since_batch != 0 {
                    context.metrics.record_scan(0, 0, bytes_since_batch);
                }
                break;
            }
        }
    };
    boxed_record_batch_stream(stream)
}

fn csv_object_error(uri: &str, error: object_store::Error) -> Error {
    if matches!(
        error,
        object_store::Error::Precondition { .. } | object_store::Error::NotFound { .. }
    ) {
        Error::Execution(format!("object changed during query: {uri}: {error}"))
    } else {
        Error::Execution(format!("object read failed for {uri}: {error}"))
    }
}

fn csv_decode_error(uri: &str, error: arrow::error::ArrowError) -> Error {
    Error::Execution(format!("CSV decode failed for {uri}: {error}"))
}

#[cfg(test)]
mod tests {
    use std::{fs, sync::Arc};

    use arrow::array::{Array, StringArray};
    use futures::TryStreamExt;
    use tempfile::tempdir;

    use super::CsvTable;
    use crate::{
        CsvOptions, EngineConfig,
        datasource::{ScanRequest, TableProvider},
        runtime::{MemoryPool, QueryContext},
    };

    #[tokio::test]
    async fn streams_quoted_records_across_files_with_projection() {
        let directory = tempdir().unwrap();
        fs::write(
            directory.path().join("a.csv"),
            b"id,note\n1,\"first\nsecond\"\n",
        )
        .unwrap();
        fs::write(directory.path().join("b.csv"), b"id,note\n2,last\n").unwrap();
        let config = EngineConfig {
            io_concurrency: 2,
            ..EngineConfig::default()
        };
        let table = CsvTable::try_new(
            vec![format!("{}/*.csv", directory.path().display())],
            CsvOptions::default(),
            &config,
        )
        .await
        .unwrap();
        let context = Arc::new(
            QueryContext::new(MemoryPool::new(16 * 1024 * 1024), directory.path()).unwrap(),
        );
        let mut request = ScanRequest::new(1);
        request.projection = Some(vec![1]);
        table.prepare(Arc::clone(&context)).await.unwrap();
        context.seal_object_snapshots();

        let batches = table
            .scan(request, context)
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        assert_eq!(
            batches.iter().map(|batch| batch.num_rows()).sum::<usize>(),
            2
        );
        assert!(batches.iter().all(|batch| batch.num_columns() == 1));
        assert_eq!(batches[0].schema().field(0).name(), "note");
    }

    #[tokio::test]
    async fn keeps_quoted_record_open_across_object_stream_chunks() {
        const OBJECT_STREAM_CHUNK: usize = 8 * 1024;

        let directory = tempdir().unwrap();
        let path = directory.path().join("large.csv");
        let mut contents = String::from("id,note\n1,\"");
        contents.push_str(&"x".repeat(OBJECT_STREAM_CHUNK - "id,note\n1,\"".len() - 1));
        contents.push('\n');
        contents.push_str("continued\"\n");
        for id in 2..=3_500 {
            contents.push_str(&format!("{id},plain-{id}\n"));
        }
        assert_eq!(contents.as_bytes()[OBJECT_STREAM_CHUNK - 1], b'\n');
        assert!(contents.len() > OBJECT_STREAM_CHUNK * 4);
        fs::write(&path, contents.as_bytes()).unwrap();

        let table = CsvTable::try_new(
            vec![path.to_string_lossy().into_owned()],
            CsvOptions::default(),
            &EngineConfig::default(),
        )
        .await
        .unwrap();
        let context = Arc::new(
            QueryContext::new(MemoryPool::new(16 * 1024 * 1024), directory.path()).unwrap(),
        );
        let mut request = ScanRequest::new(257);
        request.projection = Some(vec![1]);
        table.prepare(Arc::clone(&context)).await.unwrap();
        context.seal_object_snapshots();

        let batches = table
            .scan(request, Arc::clone(&context))
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        assert_eq!(
            batches.iter().map(|batch| batch.num_rows()).sum::<usize>(),
            3_500
        );
        let first = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert!(!first.is_null(0));
        assert!(first.value(0).contains("\ncontinued"));
        assert_eq!(
            context.metrics.snapshot().bytes_scanned,
            u64::try_from(contents.len()).unwrap()
        );
    }

    #[tokio::test]
    async fn refreshes_the_object_snapshot_for_each_scan() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("changing.csv");
        fs::write(&path, b"value\nold\n").unwrap();
        let table = CsvTable::try_new(
            vec![path.to_string_lossy().into_owned()],
            CsvOptions::default(),
            &EngineConfig::default(),
        )
        .await
        .unwrap();

        fs::write(&path, b"value\nnew\n").unwrap();
        let context = Arc::new(
            QueryContext::new(MemoryPool::new(16 * 1024 * 1024), directory.path()).unwrap(),
        );
        table.prepare(Arc::clone(&context)).await.unwrap();
        context.seal_object_snapshots();
        let batches = table
            .scan(ScanRequest::new(8), context)
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        let values = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(values.value(0), "new");
    }

    #[tokio::test]
    async fn resolves_file_snapshots_lazily_without_a_scan_wide_collect() {
        let directory = tempdir().unwrap();
        fs::write(directory.path().join("a.csv"), b"value\nfirst\n").unwrap();
        fs::write(directory.path().join("b.csv"), b"value\nsecond\n").unwrap();
        let config = EngineConfig {
            io_concurrency: 1,
            ..EngineConfig::default()
        };
        let table = CsvTable::try_new(
            vec![format!("{}/*.csv", directory.path().display())],
            CsvOptions::default(),
            &config,
        )
        .await
        .unwrap();
        let context = Arc::new(
            QueryContext::new(MemoryPool::new(16 * 1024 * 1024), directory.path()).unwrap(),
        );
        let first = &table.files[0];
        context
            .register_object_snapshot(first.uri(), first.head_snapshot().await.unwrap())
            .unwrap();
        context.seal_object_snapshots();

        let mut stream = table
            .scan(ScanRequest::new(8), Arc::clone(&context))
            .await
            .expect("scan construction must not resolve every snapshot");
        let first_batch = stream.try_next().await.unwrap().unwrap();
        let values = first_batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(values.value(0), "first");

        let error = stream.try_collect::<Vec<_>>().await.unwrap_err();
        assert!(error.to_string().contains("not present"));
    }
}
