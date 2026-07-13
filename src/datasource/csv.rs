use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use arrow::{csv::ReaderBuilder, datatypes::SchemaRef};
use async_stream::try_stream;
use async_trait::async_trait;
use futures::{StreamExt, stream};
use tokio::io::AsyncReadExt;

use super::{
    ScanRequest, ScanTask, TableProvider, TableSourceIdentity, TableStatistics,
    csv_infer::{format, infer_table_schema, infer_table_schema_against, sample_byte_cap},
    csv_input::{input_error, open_csv_input},
    csv_parallel,
    parquet_metadata::schema_memory_size,
    provider::prepare_object_sources,
};
use crate::{
    CsvOptions, CsvScanConfig, EngineConfig, Error, Result,
    runtime::{
        MemoryReservation, QueryContext, RecordBatchStream, boxed_record_batch_stream,
        estimate_schema_batch_bytes,
    },
    storage::{LocationResolver, ObjectSource},
};

#[derive(Clone, Debug)]
pub struct CsvTable {
    files: Arc<[ObjectSource]>,
    schema: SchemaRef,
    options: CsvOptions,
    has_header: bool,
    io_concurrency: usize,
    scan_config: CsvScanConfig,
    statistics: TableStatistics,
    _schema_reservation: Option<Arc<MemoryReservation>>,
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
        Self::from_files_checked(
            files,
            options,
            None,
            config.io_concurrency,
            sample_byte_cap(config.memory_limit),
            config.csv_scan.clone(),
            context,
        )
        .await
    }

    pub(crate) async fn try_new_for_query_with_registered_schema(
        locations: Vec<String>,
        inference_options: CsvOptions,
        registered_schema: SchemaRef,
        config: &EngineConfig,
        context: Option<Arc<QueryContext>>,
    ) -> Result<Self> {
        if inference_options.schema.is_some() {
            return Err(Error::Internal(
                "registered CSV inference options unexpectedly contain a schema".to_owned(),
            ));
        }
        let resolver = LocationResolver::with_memory_limit(config.s3.clone(), config.memory_limit);
        let files = match context.as_deref() {
            Some(context) => resolver.resolve_for_query(&locations, context).await?,
            None => resolver.resolve(&locations).await?,
        };
        Self::from_files_checked(
            files,
            inference_options,
            Some(registered_schema),
            config.io_concurrency,
            sample_byte_cap(config.memory_limit),
            config.csv_scan.clone(),
            context,
        )
        .await
    }

    async fn from_files_checked(
        files: Vec<ObjectSource>,
        options: CsvOptions,
        registered_schema: Option<SchemaRef>,
        io_concurrency: usize,
        sample_byte_cap: usize,
        scan_config: CsvScanConfig,
        context: Option<Arc<QueryContext>>,
    ) -> Result<Self> {
        if io_concurrency == 0 {
            return Err(Error::InvalidArgument(
                "I/O concurrency must be greater than zero".to_owned(),
            ));
        }
        if sample_byte_cap == 0 {
            return Err(Error::InvalidArgument(
                "CSV sample byte limit must be greater than zero".to_owned(),
            ));
        }
        scan_config.validate()?;
        let (schema, has_header) = if let Some(registered_schema) = registered_schema {
            infer_table_schema_against(
                &files,
                &options,
                &registered_schema,
                sample_byte_cap,
                context.as_deref(),
            )
            .await?
        } else {
            infer_table_schema(&files, &options, sample_byte_cap, context.as_deref()).await?
        };
        let schema_reservation = context
            .as_ref()
            .map(|context| {
                let bytes = schema_memory_size(&schema);
                context.memory.try_reserve(bytes).map(Arc::new).map_err(|_| {
                    Error::ResourceExhausted(format!(
                        "CSV schema requires {bytes} bytes, but the query memory pool has {} bytes available (limit {} bytes)",
                        context.memory.available(),
                        context.memory.limit(),
                    ))
                })
            })
            .transpose()?;
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
            scan_config,
            statistics,
            _schema_reservation: schema_reservation,
        })
    }

    pub(crate) fn has_header(&self) -> bool {
        self.has_header
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

    fn source_identity(&self) -> Option<TableSourceIdentity> {
        Some(TableSourceIdentity::from_objects(
            "csv",
            &self.files,
            format!(
                "schema={:?};options={:?};header={}",
                self.schema, self.options, self.has_header
            ),
        ))
    }

    fn explain_scan(&self) -> Option<String> {
        Some(format!(
            "format=csv codec={:?} record_morsel_target={} parser_lanes={}",
            self.options.compression,
            self.scan_config.target_morsel_bytes,
            if self.scan_config.parallel_single_file {
                "runtime"
            } else {
                "1"
            }
        ))
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

    async fn scan_tasks(
        &self,
        request: ScanRequest,
        context: Arc<QueryContext>,
        target_tasks: usize,
    ) -> Result<Vec<ScanTask>> {
        if request.batch_size == 0 {
            return Err(Error::InvalidArgument(
                "scan batch_size must be greater than zero".to_owned(),
            ));
        }
        let task_count = target_tasks.max(1).min(self.io_concurrency);
        if self.scan_config.parallel_single_file && self.files.len() == 1 && task_count > 1 {
            return csv_parallel::scan_tasks(
                csv_parallel::ParallelCsvScan {
                    file: self.files[0].clone(),
                    schema: Arc::clone(&self.schema),
                    options: self.options.clone(),
                    has_header: self.has_header,
                    request,
                    task_count,
                    target_morsel_bytes: self.scan_config.target_morsel_bytes,
                },
                context,
            );
        }
        let output_schema = request.projected_schema(&self.schema)?;
        let preclaim = estimate_schema_batch_bytes(output_schema.as_ref(), request.batch_size);
        let file_scan = CsvFileScan {
            schema: Arc::clone(&self.schema),
            options: self.options.clone(),
            has_header: self.has_header,
            projection: request.projection,
            batch_size: request.batch_size,
        };
        let remaining = request.limit.map(|limit| Arc::new(AtomicUsize::new(limit)));
        let next_file = Arc::new(AtomicUsize::new(0));
        let task_count = task_count.min(self.files.len());
        Ok((0..task_count)
            .map(|task| {
                let files = Arc::clone(&self.files);
                let file_scan = file_scan.clone();
                let task_context = Arc::clone(&context);
                let stream_context = Arc::clone(&task_context);
                let output_schema = Arc::clone(&output_schema);
                let remaining = remaining.clone();
                let next_file = Arc::clone(&next_file);
                ScanTask::from_public(
                    task,
                    boxed_record_batch_stream(try_stream! {
                        loop {
                            if remaining.as_ref().is_some_and(|remaining| remaining.load(Ordering::Acquire) == 0) {
                                break;
                            }
                            let index = next_file.fetch_add(1, Ordering::AcqRel);
                            if index >= files.len() {
                                break;
                            }
                            let mut input = csv_file_stream(
                                Arc::clone(&files),
                                index,
                                file_scan.clone(),
                                Arc::clone(&stream_context),
                            );
                            while let Some(batch) = input.next().await {
                                stream_context.check_cancelled()?;
                                let batch = batch?;
                                let claimed = remaining.as_ref().map_or(batch.num_rows(), |remaining| {
                                    claim_rows(remaining, batch.num_rows())
                                });
                                if claimed == 0 {
                                    break;
                                }
                                let batch = if batch.num_rows() > claimed {
                                    batch.slice(0, claimed)
                                } else {
                                    batch
                                };
                                debug_assert_eq!(batch.schema(), output_schema);
                                yield batch;
                            }
                        }
                    }),
                    task_context,
                    preclaim,
                    "CSV scan task",
                )
            })
            .collect())
    }
}

fn claim_rows(remaining: &AtomicUsize, available: usize) -> usize {
    let mut current = remaining.load(Ordering::Acquire);
    loop {
        let claimed = current.min(available);
        if claimed == 0 {
            return 0;
        }
        match remaining.compare_exchange_weak(
            current,
            current - claimed,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => return claimed,
            Err(updated) => current = updated,
        }
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
        let mut input = open_csv_input(
            &file,
            &snapshot,
            scan.options.compression,
            Some((&context.control, &context.metrics)),
        ).await?;
        let mut builder = ReaderBuilder::new(scan.schema)
            .with_format(format(&scan.options, scan.has_header))
            .with_batch_size(scan.batch_size)
            .with_truncated_rows(false);
        if let Some(projection) = scan.projection {
            builder = builder.with_projection(projection);
        }
        let mut decoder = builder.build_decoder();
        let _input_memory = context.memory.try_reserve(64 * 1024).map_err(|error| {
            Error::ResourceExhausted(format!(
                "CSV input buffer requires 65536 bytes (query limit {}, available {}): {error}",
                context.memory.limit(),
                context.memory.available(),
            ))
        })?;
        let mut buffered = vec![0_u8; 64 * 1024];
        let mut buffered_len = 0;
        let mut buffered_offset = 0;
        let mut input_finished = false;
        let mut bytes_since_batch = 0_u64;

        loop {
            context.check_cancelled()?;
            loop {
                while buffered_offset == buffered_len && !input_finished {
                    let read = tokio::select! {
                        _ = context.control.cancelled() => Err(Error::Cancelled),
                        result = input.read(&mut buffered) => {
                            result.map_err(|error| input_error(file.uri(), error))
                        },
                    }?;
                    if read == 0 {
                        input_finished = true;
                    } else {
                        buffered_offset = 0;
                        buffered_len = read;
                        context.metrics.add_csv_decompressed_bytes(
                            u64::try_from(read).unwrap_or(u64::MAX),
                        );
                        bytes_since_batch = bytes_since_batch
                            .saturating_add(u64::try_from(read).unwrap_or(u64::MAX));
                    }
                }

                let decoded = {
                    let _parser_lane = context.metrics.enter_csv_parser_lane();
                    decoder
                        .decode(&buffered[buffered_offset..buffered_len])
                        .map_err(|error| csv_decode_error(file.uri(), error))?
                };
                if decoded == 0 {
                    break;
                }
                buffered_offset += decoded;
            }

            let batch = {
                let _parser_lane = context.metrics.enter_csv_parser_lane();
                decoder
                    .flush()
                    .map_err(|error| csv_decode_error(file.uri(), error))?
            };
            if let Some(batch) = batch {
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

fn csv_decode_error(uri: &str, error: arrow::error::ArrowError) -> Error {
    Error::Execution(format!("CSV decode failed for {uri}: {error}"))
}

#[cfg(test)]
#[path = "csv_tests.rs"]
mod tests;
