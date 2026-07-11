use std::sync::Arc;

use arrow::datatypes::{Schema, SchemaRef};
use async_stream::try_stream;
use async_trait::async_trait;

use super::{
    MetadataCache, ScanRequest, TableProvider, TableStatistics,
    hive::HivePartitions,
    parquet_metadata::{
        load_parquet_metadata, registration_metadata_limit, resize_schema_budget,
        schema_memory_size,
    },
    parquet_pruning::can_prune_row_group,
    parquet_scan::{ParquetMorsel, ParquetMorselStream, scan_morsels},
    provider::prepare_object_sources,
};
use crate::{
    EngineConfig, Error, ParquetOptions, Result,
    runtime::{MemoryReservation, QueryContext, RecordBatchStream},
    storage::{LocationResolver, ObjectSource},
};

#[derive(Clone, Debug)]
pub struct ParquetTable {
    files: Arc<[ObjectSource]>,
    schema: SchemaRef,
    physical_schema: SchemaRef,
    statistics: TableStatistics,
    hive: Option<Arc<HivePartitions>>,
    union_by_name: bool,
    metadata_cache: MetadataCache,
    io_concurrency: usize,
    _schema_reservation: Option<Arc<MemoryReservation>>,
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

        let ParquetOptions {
            schema: explicit_schema,
            union_by_name,
            hive_partitioning,
        } = options;
        let has_explicit_schema = explicit_schema.is_some();
        let mut physical_schema = explicit_schema;
        let mut schema_reservation = context.as_ref().map(|context| context.memory.reservation());
        if let Some(schema) = &physical_schema {
            resize_schema_budget(
                schema_memory_size(schema),
                context.as_deref(),
                registration_limit,
                schema_reservation.as_mut(),
            )?;
        }

        let mut rows = 0_u64;
        let mut bytes = 0_u64;
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
            if has_explicit_schema {
                let expected = physical_schema
                    .as_deref()
                    .ok_or_else(|| Error::Internal("missing explicit Parquet schema".to_owned()))?;
                validate_file_schema(incoming, expected, union_by_name)?;
            } else if union_by_name {
                physical_schema = Some(match physical_schema.take() {
                    Some(current) => {
                        resize_schema_budget(
                            schema_memory_size(&current)
                                .saturating_add(schema_memory_size(incoming)),
                            context.as_deref(),
                            registration_limit,
                            schema_reservation.as_mut(),
                        )?;
                        let merged = merge_union_schema(&current, incoming)?;
                        drop(current);
                        resize_schema_budget(
                            schema_memory_size(&merged),
                            context.as_deref(),
                            registration_limit,
                            schema_reservation.as_mut(),
                        )?;
                        merged
                    }
                    None => {
                        let schema = Arc::new(Schema::new(incoming.fields().clone()));
                        resize_schema_budget(
                            schema_memory_size(&schema),
                            context.as_deref(),
                            registration_limit,
                            schema_reservation.as_mut(),
                        )?;
                        schema
                    }
                });
            } else if let Some(expected) = &physical_schema {
                validate_file_schema(incoming, expected, false)?;
            } else {
                resize_schema_budget(
                    schema_memory_size(incoming),
                    context.as_deref(),
                    registration_limit,
                    schema_reservation.as_mut(),
                )?;
                physical_schema = Some(Arc::clone(incoming));
            }
            rows = rows.saturating_add(
                u64::try_from(reader_metadata.metadata().file_metadata().num_rows())
                    .unwrap_or(u64::MAX),
            );
            bytes = bytes.saturating_add(file.snapshot().size);
        }

        let mut physical_schema =
            physical_schema.ok_or_else(|| Error::Internal("missing Parquet schema".to_owned()))?;
        if union_by_name && !has_explicit_schema {
            resize_schema_budget(
                schema_memory_size(&physical_schema).saturating_mul(2),
                context.as_deref(),
                registration_limit,
                schema_reservation.as_mut(),
            )?;
            let fields: Vec<_> = physical_schema
                .fields()
                .iter()
                .map(|field| Arc::new(field.as_ref().clone().with_nullable(true)))
                .collect();
            physical_schema = Arc::new(Schema::new(fields));
            resize_schema_budget(
                schema_memory_size(&physical_schema),
                context.as_deref(),
                registration_limit,
                schema_reservation.as_mut(),
            )?;
        }
        let hive = if hive_partitioning {
            HivePartitions::discover(&files, &physical_schema)?.map(Arc::new)
        } else {
            None
        };
        if hive.is_some() {
            resize_schema_budget(
                schema_memory_size(&physical_schema).saturating_mul(2),
                context.as_deref(),
                registration_limit,
                schema_reservation.as_mut(),
            )?;
        }
        let schema = hive.as_ref().map_or_else(
            || Arc::clone(&physical_schema),
            |hive| hive.append_schema(&physical_schema),
        );
        let retained_schema_bytes = if Arc::ptr_eq(&schema, &physical_schema) {
            schema_memory_size(&schema)
        } else {
            schema_memory_size(&schema).saturating_add(schema_memory_size(&physical_schema))
        };
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
            union_by_name,
            metadata_cache,
            io_concurrency,
            _schema_reservation: schema_reservation.map(Arc::new),
        })
    }
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
            union_by_name: self.union_by_name,
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
}

struct ScanPlanning {
    files: Arc<[ObjectSource]>,
    output_schema: SchemaRef,
    table_schema: SchemaRef,
    physical_schema: SchemaRef,
    hive: Option<Arc<HivePartitions>>,
    union_by_name: bool,
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
                plan.union_by_name,
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

fn merge_union_schema(current: &Schema, incoming: &Schema) -> Result<SchemaRef> {
    let schemas = [
        Schema::new(current.fields().clone()),
        Schema::new(incoming.fields().clone()),
    ];
    Ok(Arc::new(Schema::try_merge(schemas)?))
}

fn validate_file_schema(actual: &Schema, expected: &Schema, allow_missing: bool) -> Result<()> {
    if !allow_missing && actual.fields().len() != expected.fields().len() {
        return Err(Error::InvalidArgument(format!(
            "incompatible Parquet schemas: expected {} columns, found {}",
            expected.fields().len(),
            actual.fields().len()
        )));
    }
    for field in expected.fields() {
        match actual.field_with_name(field.name()) {
            Ok(actual_field) if actual_field.data_type() == field.data_type() => {}
            Ok(actual_field) => {
                return Err(Error::InvalidArgument(format!(
                    "incompatible Parquet column {}: expected {:?}, found {:?}",
                    field.name(),
                    field.data_type(),
                    actual_field.data_type()
                )));
            }
            Err(_) if allow_missing => {}
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
    union_by_name: bool,
) -> Result<()> {
    validate_file_schema(actual, expected, union_by_name).map_err(|error| {
        Error::Execution(format!(
            "Parquet schema changed for {}: {error}",
            file.uri()
        ))
    })
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
