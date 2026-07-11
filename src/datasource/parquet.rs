use std::{mem::size_of, sync::Arc};

use arrow::datatypes::{Schema, SchemaRef};
use async_stream::try_stream;
use async_trait::async_trait;
use futures::StreamExt;
use tokio::sync::Semaphore;

use super::{
    MetadataCache, ScanRequest, ScanTask, TableProvider, TableStatistics,
    hive::HivePartitions,
    parquet_metadata::{
        load_parquet_metadata, registration_metadata_limit, resize_schema_budget,
        schema_memory_size,
    },
    parquet_pruning::can_prune_row_group,
    parquet_scan::{ParquetMorsel, ParquetMorselStream, morsel_stream, scan_morsels},
    provider::prepare_object_sources,
    schema_evolution::{ParquetSchemaMode, SchemaSource, merge_file_schemas},
};
use crate::{
    EngineConfig, Error, ParquetOptions, Result,
    runtime::{MemoryReservation, QueryContext, RecordBatchStream, estimate_schema_batch_bytes},
    storage::{LocationResolver, ObjectSource},
};

#[derive(Clone, Debug)]
pub struct ParquetTable {
    files: Arc<[ObjectSource]>,
    schema: SchemaRef,
    physical_schema: SchemaRef,
    statistics: TableStatistics,
    hive: Option<Arc<HivePartitions>>,
    schema_mode: ParquetSchemaMode,
    file_schemas: Arc<[FileSchema]>,
    metadata_cache: MetadataCache,
    io_concurrency: usize,
    _schema_reservation: Option<Arc<MemoryReservation>>,
}

#[derive(Clone, Debug)]
struct FileSchema {
    uri: String,
    schema: SchemaRef,
}

impl ParquetTable {
    pub(crate) async fn try_new_with_cache(
        locations: Vec<String>,
        options: ParquetOptions,
        config: &EngineConfig,
        metadata_cache: MetadataCache,
    ) -> Result<Self> {
        Self::try_new_with_cache_for_query(locations, options, config, metadata_cache, None).await
    }

    pub(crate) async fn try_new_with_cache_for_query(
        locations: Vec<String>,
        options: ParquetOptions,
        config: &EngineConfig,
        metadata_cache: MetadataCache,
        context: Option<Arc<QueryContext>>,
    ) -> Result<Self> {
        let resolver = LocationResolver::with_memory_limit(config.s3.clone(), config.memory_limit);
        let files = match context.as_deref() {
            Some(context) => resolver.resolve_for_query(&locations, context).await?,
            None => resolver.resolve(&locations).await?,
        };
        Self::from_files(
            files,
            options,
            config.io_concurrency,
            metadata_cache,
            context,
            registration_metadata_limit(config),
        )
        .await
    }

    async fn from_files(
        files: Vec<ObjectSource>,
        options: ParquetOptions,
        io_concurrency: usize,
        metadata_cache: MetadataCache,
        context: Option<Arc<QueryContext>>,
        registration_limit: usize,
    ) -> Result<Self> {
        if files.is_empty() {
            return Err(Error::InvalidArgument(
                "Parquet table requires at least one file".to_owned(),
            ));
        }
        if io_concurrency == 0 {
            return Err(Error::InvalidArgument(
                "I/O concurrency must be greater than zero".to_owned(),
            ));
        }

        let schema_mode = options.effective_schema_mode()?;
        let ParquetOptions {
            schema: explicit_schema,
            union_by_name: _,
            schema_mode: _,
            hive_partitioning,
        } = options;
        let has_explicit_schema = explicit_schema.is_some();
        let mut schema_reservation = context.as_ref().map(|context| context.memory.reservation());
        if let Some(schema) = &explicit_schema {
            resize_schema_budget(
                schema_memory_size(schema),
                context.as_deref(),
                registration_limit,
                schema_reservation.as_mut(),
            )?;
        }
        let mut rows = 0_u64;
        let mut bytes = 0_u64;
        let mut file_schemas = Vec::with_capacity(files.len());
        for file in &files {
            let metadata = load_parquet_metadata(
                file,
                file.snapshot().clone(),
                context.as_deref(),
                &metadata_cache,
                registration_limit,
            )
            .await?;
            let reader_metadata = metadata.reader_metadata();
            let incoming = reader_metadata.schema();
            file_schemas.push(FileSchema {
                uri: file.uri().to_owned(),
                schema: Arc::clone(incoming),
            });
            rows = rows.saturating_add(
                u64::try_from(reader_metadata.metadata().file_metadata().num_rows())
                    .unwrap_or(u64::MAX),
            );
            bytes = bytes.saturating_add(file.snapshot().size);
        }

        let mut physical_schema = match explicit_schema {
            Some(schema) => {
                for file in &file_schemas {
                    validate_file_schema(&file.schema, &schema, schema_mode).map_err(|error| {
                        Error::InvalidArgument(format!(
                            "incompatible Parquet schema for {}: {error}",
                            file.uri
                        ))
                    })?;
                }
                schema
            }
            None => {
                let sources = file_schemas
                    .iter()
                    .map(|file| SchemaSource {
                        uri: &file.uri,
                        schema: &file.schema,
                    })
                    .collect::<Vec<_>>();
                merge_file_schemas(&sources, schema_mode, None)?
            }
        };
        if !has_explicit_schema && schema_mode == ParquetSchemaMode::UnionByName {
            physical_schema = nullable_schema(&physical_schema);
        }
        let file_schema_bytes = file_schemas_memory_size(&file_schemas);
        resize_schema_budget(
            file_schema_bytes.saturating_add(schema_memory_size(&physical_schema)),
            context.as_deref(),
            registration_limit,
            schema_reservation.as_mut(),
        )?;
        let hive = if hive_partitioning {
            HivePartitions::discover(&files, &physical_schema)?.map(Arc::new)
        } else {
            None
        };
        let schema = hive.as_ref().map_or_else(
            || Arc::clone(&physical_schema),
            |hive| hive.append_schema(&physical_schema),
        );
        let retained_schema_bytes = file_schema_bytes
            .saturating_add(schema_memory_size(&schema))
            .saturating_add(if Arc::ptr_eq(&schema, &physical_schema) {
                0
            } else {
                schema_memory_size(&physical_schema)
            })
            .saturating_add(hive.as_deref().map_or(0, HivePartitions::memory_size));
        resize_schema_budget(
            retained_schema_bytes,
            context.as_deref(),
            registration_limit,
            schema_reservation.as_mut(),
        )?;
        let statistics = TableStatistics {
            row_count: Some(rows),
            total_byte_size: Some(bytes),
            file_count: files.len(),
        };
        Ok(Self {
            files: files.into(),
            schema,
            physical_schema,
            statistics,
            hive,
            schema_mode,
            file_schemas: file_schemas.into(),
            metadata_cache,
            io_concurrency,
            _schema_reservation: schema_reservation.map(Arc::new),
        })
    }

    pub(crate) fn physical_schema(&self) -> SchemaRef {
        Arc::clone(&self.physical_schema)
    }

    pub(crate) fn validate_compatible_with(
        &self,
        expected: &Schema,
        mode: ParquetSchemaMode,
    ) -> Result<()> {
        for file in self.file_schemas.iter() {
            validate_file_schema(&file.schema, expected, mode).map_err(|error| {
                Error::Execution(format!(
                    "Parquet schema for {} is incompatible with the registered schema: {error}",
                    file.uri
                ))
            })?;
        }
        Ok(())
    }
}

fn file_schemas_memory_size(file_schemas: &[FileSchema]) -> usize {
    file_schemas.iter().fold(
        size_of::<FileSchema>().saturating_mul(file_schemas.len()),
        |bytes, file| {
            bytes
                .saturating_add(file.uri.capacity())
                .saturating_add(schema_memory_size(&file.schema))
        },
    )
}

#[async_trait]
impl TableProvider for ParquetTable {
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
        let files = Arc::clone(&self.files);
        let table_schema = Arc::clone(&self.schema);
        let hive = self.hive.clone();
        let batch_size = request.batch_size;
        let limit = request.limit;
        let morsels = plan_morsels(ScanPlanning {
            files,
            output_schema: Arc::clone(&output_schema),
            table_schema,
            physical_schema: Arc::clone(&self.physical_schema),
            hive: hive.clone(),
            schema_mode: self.schema_mode,
            request,
            context: Arc::clone(&context),
            metadata_cache: self.metadata_cache.clone(),
        });
        Ok(scan_morsels(
            morsels,
            output_schema,
            hive,
            context,
            self.io_concurrency,
            batch_size,
            limit,
        ))
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
        let output_schema = request.projected_schema(&self.schema)?;
        let batch_size = request.batch_size;
        let preclaim = estimate_schema_batch_bytes(output_schema.as_ref(), batch_size);
        let hive = self.hive.clone();
        let mut morsels = plan_morsels(ScanPlanning {
            files: Arc::clone(&self.files),
            output_schema: Arc::clone(&output_schema),
            table_schema: Arc::clone(&self.schema),
            physical_schema: Arc::clone(&self.physical_schema),
            hive: hive.clone(),
            schema_mode: self.schema_mode,
            request,
            context: Arc::clone(&context),
            metadata_cache: self.metadata_cache.clone(),
        });

        // Prefetch at most one first morsel per configured lane. This reveals
        // the exact task count when the source has fewer row groups than
        // workers, while keeping planning metadata bounded for files with very
        // many row groups. Remaining morsels stay lazy behind the shared
        // planning stream.
        let target_tasks = target_tasks.max(1);
        let mut first_morsels = Vec::with_capacity(target_tasks);
        while first_morsels.len() < target_tasks {
            context.check_cancelled()?;
            let Some(morsel) = morsels.next().await else {
                break;
            };
            first_morsels.push(morsel?);
        }
        if first_morsels.is_empty() {
            return Ok(Vec::new());
        }

        // ScanTask is the schedulable lane input. Do not manufacture empty
        // workers up to the configured thread count: a one-row-group file has
        // one unit of work and therefore one active lane. Each task owns one
        // guaranteed first morsel, then pulls remaining work one morsel at a
        // time without holding the planning lock during decode or output.
        let morsels = Arc::new(tokio::sync::Mutex::new(morsels));
        let io_permits = Arc::new(Semaphore::new(self.io_concurrency));
        Ok(first_morsels
            .into_iter()
            .enumerate()
            .map(|(id, first_morsel)| {
                let morsels = Arc::clone(&morsels);
                let io_permits = Arc::clone(&io_permits);
                let output_schema = Arc::clone(&output_schema);
                let hive = hive.clone();
                let task_context = Arc::clone(&context);
                let stream_context = Arc::clone(&task_context);
                ScanTask::from_public(
                    id,
                    crate::runtime::boxed_record_batch_stream(try_stream! {
                        let mut next = Some(first_morsel);
                        loop {
                            let morsel = match next.take() {
                                Some(morsel) => morsel,
                                None => {
                                    let mut remaining = morsels.lock().await;
                                    let Some(morsel) = remaining.next().await else {
                                        break;
                                    };
                                    morsel?
                                }
                            };
                            stream_context.check_cancelled()?;
                            // Compute lanes may outnumber the configured object
                            // I/O concurrency. Keep independently schedulable
                            // row groups, but admit only the configured number
                            // of active Parquet readers at once.
                            let _io_permit = Arc::clone(&io_permits)
                                .acquire_owned()
                                .await
                                .map_err(|_| Error::Internal(
                                    "Parquet I/O concurrency limiter closed unexpectedly".into(),
                                ))?;
                            let mut input = morsel_stream(
                                morsel,
                                Arc::clone(&output_schema),
                                hive.clone(),
                                Arc::clone(&stream_context),
                                batch_size,
                            );
                            while let Some(batch) = input.next().await {
                                yield batch?;
                            }
                        }
                    }),
                    task_context,
                    preclaim,
                    "Parquet scan task",
                )
            })
            .collect())
    }
}

struct ScanPlanning {
    files: Arc<[ObjectSource]>,
    output_schema: SchemaRef,
    table_schema: SchemaRef,
    physical_schema: SchemaRef,
    hive: Option<Arc<HivePartitions>>,
    schema_mode: ParquetSchemaMode,
    request: ScanRequest,
    context: Arc<QueryContext>,
    metadata_cache: MetadataCache,
}

fn plan_morsels(plan: ScanPlanning) -> ParquetMorselStream {
    Box::pin(try_stream! {
        if plan.request.limit == Some(0) {
            return;
        }
        let mut pushdown_remaining = plan.request.limit.unwrap_or(usize::MAX);
        for file_index in 0..plan.files.len() {
            if plan
                .hive
                .as_ref()
                .is_some_and(|hive| hive.can_prune(file_index, plan.request.predicate.as_ref()))
            {
                plan.context.metrics.add_files_pruned(1);
                continue;
            }
            if pushdown_remaining == 0 {
                break;
            }
            plan.context.check_cancelled()?;
            let file = &plan.files[file_index];
            let snapshot = plan.context.object_snapshot(file.uri())?;
            let metadata = load_parquet_metadata(
                file,
                snapshot.clone(),
                Some(&plan.context),
                &plan.metadata_cache,
                usize::MAX,
            ).await?;
            let reader_metadata = metadata.reader_metadata();
            let file_schema = Arc::clone(reader_metadata.schema());
            validate_scan_schema(
                file,
                &file_schema,
                &plan.physical_schema,
                plan.schema_mode,
            )?;
            if let Some(hive) = &plan.hive {
                hive.validate_physical_schema(&file_schema)?;
            }
            let projection = file_projection(&file_schema, &plan.output_schema);
            let mut has_unpruned_group = false;
            for row_group in 0..reader_metadata.metadata().num_row_groups() {
                if can_prune_row_group(
                    reader_metadata.metadata(),
                    reader_metadata.parquet_schema(),
                    &file_schema,
                    &plan.table_schema,
                    row_group,
                    plan.request.predicate.as_ref(),
                ) {
                    plan.context.metrics.add_row_groups_pruned(1);
                    continue;
                }
                has_unpruned_group = true;
                let row_count = usize::try_from(
                    reader_metadata.metadata().row_group(row_group).num_rows(),
                ).map_err(|_| Error::Execution(format!(
                    "Parquet row group {row_group} in {} has an invalid row count",
                    file.uri(),
                )))?;
                if row_count == 0 {
                    continue;
                }
                let row_limit = plan.request.limit.map(|_| {
                    let limit = row_count.min(pushdown_remaining);
                    pushdown_remaining = pushdown_remaining.saturating_sub(limit);
                    limit
                });
                yield ParquetMorsel {
                    file_index,
                    file: file.clone(),
                    snapshot: snapshot.clone(),
                    metadata: metadata.clone(),
                    projection: projection.clone(),
                    row_group,
                    row_limit,
                };
                if pushdown_remaining == 0 {
                    break;
                }
            }
            if !has_unpruned_group {
                plan.context.metrics.add_files_pruned(1);
            }
        }
    })
}

fn validate_file_schema(actual: &Schema, expected: &Schema, mode: ParquetSchemaMode) -> Result<()> {
    for field in expected.fields() {
        match actual.field_with_name(field.name()) {
            Ok(actual_field) if actual_field.data_type() == field.data_type() => {}
            Ok(actual_field) if mode == ParquetSchemaMode::SafeWidening => {
                let actual_schema = Schema::new(vec![actual_field.clone()]);
                let expected_schema = Schema::new(vec![field.clone()]);
                let sources = [
                    SchemaSource {
                        uri: "actual",
                        schema: &actual_schema,
                    },
                    SchemaSource {
                        uri: "registered",
                        schema: &expected_schema,
                    },
                ];
                let merged = merge_file_schemas(&sources, mode, Some(&expected_schema))?;
                if merged.field(0).data_type() != field.data_type() {
                    return Err(Error::InvalidArgument(format!(
                        "column {} requires widening from {:?} to {:?}; run REFRESH TABLE",
                        field.name(),
                        field.data_type(),
                        merged.field(0).data_type(),
                    )));
                }
            }
            Ok(actual_field) => {
                return Err(Error::InvalidArgument(format!(
                    "incompatible Parquet column {}: expected {:?}, found {:?}",
                    field.name(),
                    field.data_type(),
                    actual_field.data_type()
                )));
            }
            Err(_) if mode == ParquetSchemaMode::UnionByName => {}
            Err(_) => {
                return Err(Error::InvalidArgument(format!(
                    "Parquet column {} is missing",
                    field.name()
                )));
            }
        }
    }
    Ok(())
}

fn validate_scan_schema(
    file: &ObjectSource,
    actual: &Schema,
    expected: &Schema,
    schema_mode: ParquetSchemaMode,
) -> Result<()> {
    validate_file_schema(actual, expected, schema_mode).map_err(|error| {
        Error::Execution(format!(
            "Parquet schema changed for {}: {error}",
            file.uri()
        ))
    })
}

fn nullable_schema(schema: &Schema) -> SchemaRef {
    Arc::new(Schema::new_with_metadata(
        schema
            .fields()
            .iter()
            .map(|field| Arc::new(field.as_ref().clone().with_nullable(true)))
            .collect::<Vec<_>>(),
        schema.metadata().clone(),
    ))
}

fn file_projection(file_schema: &Schema, output_schema: &Schema) -> Vec<usize> {
    output_schema
        .fields()
        .iter()
        .filter_map(|field| file_schema.index_of(field.name()).ok())
        .collect()
}

#[cfg(test)]
#[path = "parquet_tests.rs"]
mod tests;
