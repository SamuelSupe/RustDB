use std::sync::Arc;

use arrow::datatypes::{Schema, SchemaRef};
use async_stream::try_stream;
use async_trait::async_trait;
use parquet::arrow::arrow_reader::ArrowReaderMetadata;

use super::{
    MetadataCache, ScanRequest, TableProvider, TableStatistics,
    hive::HivePartitions,
    parquet_pruning::row_groups_for_predicate,
    parquet_reader::{QueryIo, SnapshotParquetReader},
    parquet_scan::{ParquetMorsel, ParquetMorselStream, scan_morsels},
    provider::prepare_object_sources,
};
use crate::{
    EngineConfig, Error, ParquetOptions, Result,
    runtime::{QueryContext, RecordBatchStream},
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
        let resolver = LocationResolver::new(config.s3.clone());
        let files = match context.as_deref() {
            Some(context) => resolver.resolve_for_query(&locations, context).await?,
            None => resolver.resolve(&locations).await?,
        };
        let query =
            context.map(|context| QueryIo::new(context.control.clone(), context.metrics.clone()));
        Self::from_files(files, options, config.io_concurrency, metadata_cache, query).await
    }

    async fn from_files(
        files: Vec<ObjectSource>,
        options: ParquetOptions,
        io_concurrency: usize,
        metadata_cache: MetadataCache,
        query: Option<QueryIo>,
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

        let mut schemas = Vec::with_capacity(files.len());
        let mut rows = 0_u64;
        let mut bytes = 0_u64;
        for file in &files {
            let metadata = parquet_metadata(
                file,
                file.snapshot().clone(),
                query.clone(),
                &metadata_cache,
            )
            .await?;
            schemas.push(Arc::clone(metadata.schema()));
            rows = rows.saturating_add(
                u64::try_from(metadata.metadata().file_metadata().num_rows()).unwrap_or(u64::MAX),
            );
            bytes = bytes.saturating_add(file.snapshot().size);
        }

        let union_by_name = options.union_by_name;
        let physical_schema = resolve_schema(schemas, options.schema, union_by_name)?;
        let hive = if options.hive_partitioning {
            HivePartitions::discover(&files, &physical_schema)?.map(Arc::new)
        } else {
            None
        };
        let schema = hive.as_ref().map_or_else(
            || Arc::clone(&physical_schema),
            |hive| hive.append_schema(&physical_schema),
        );
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
        let mut candidates = Vec::with_capacity(plan.files.len());
        for file_index in 0..plan.files.len() {
            if plan
                .hive
                .as_ref()
                .is_some_and(|hive| hive.can_prune(file_index, plan.request.predicate.as_ref()))
            {
                plan.context.metrics.add_files_pruned(1);
            } else {
                candidates.push(file_index);
            }
        }
        let snapshots = candidates
            .into_iter()
            .map(|file_index| {
                let file = &plan.files[file_index];
                let snapshot = plan.context.object_snapshot(file.uri())?;
                Ok((file_index, snapshot))
            })
            .collect::<Result<Vec<_>>>()?;

        let mut pushdown_remaining = plan.request.limit.unwrap_or(usize::MAX);
        for (file_index, snapshot) in snapshots {
            if pushdown_remaining == 0 {
                break;
            }
            plan.context.check_cancelled()?;
            let file = &plan.files[file_index];
            let io = QueryIo::new(
                plan.context.control.clone(),
                plan.context.metrics.clone(),
            );
            let metadata = parquet_metadata(
                file,
                snapshot.clone(),
                Some(io),
                &plan.metadata_cache,
            ).await?;
            let file_schema = Arc::clone(metadata.schema());
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
            let (row_groups, pruned) = row_groups_for_predicate(
                metadata.metadata(),
                metadata.parquet_schema(),
                &file_schema,
                &plan.table_schema,
                plan.request.predicate.as_ref(),
            );
            plan.context.metrics.add_row_groups_pruned(pruned);
            if row_groups.is_empty() {
                plan.context.metrics.add_files_pruned(1);
                continue;
            }
            for row_group in row_groups {
                let row_count = usize::try_from(
                    metadata.metadata().row_group(row_group).num_rows(),
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
        }
    })
}

async fn parquet_metadata(
    file: &ObjectSource,
    snapshot: crate::storage::ObjectSnapshot,
    query: Option<QueryIo>,
    cache: &MetadataCache,
) -> Result<ArrowReaderMetadata> {
    if let Some(metadata) = cache.get(file, &snapshot) {
        return Ok(metadata);
    }
    let mut reader = SnapshotParquetReader::new(file, snapshot.clone(), query);
    let metadata = ArrowReaderMetadata::load_async(&mut reader, Default::default()).await?;
    cache.insert(file, &snapshot, metadata.clone());
    Ok(metadata)
}

fn resolve_schema(
    file_schemas: Vec<SchemaRef>,
    explicit: Option<SchemaRef>,
    union_by_name: bool,
) -> Result<SchemaRef> {
    let schema = if let Some(explicit) = explicit {
        for file in &file_schemas {
            validate_file_schema(file, &explicit, union_by_name)?;
        }
        explicit
    } else if union_by_name {
        let schemas = file_schemas
            .into_iter()
            .map(|schema| Schema::new(schema.fields().clone()));
        let merged = Schema::try_merge(schemas)?;
        let fields: Vec<_> = merged
            .fields()
            .iter()
            .map(|field| Arc::new(field.as_ref().clone().with_nullable(true)))
            .collect();
        Arc::new(Schema::new(fields))
    } else {
        let first = file_schemas
            .first()
            .ok_or_else(|| Error::Internal("missing Parquet schema".to_owned()))?;
        for schema in file_schemas.iter().skip(1) {
            validate_file_schema(schema, first, false)?;
        }
        Arc::clone(first)
    };
    Ok(schema)
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
mod tests {
    use std::{fs::File, sync::Arc};

    use arrow::{
        array::{Int64Array, StringArray},
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    };
    use futures::TryStreamExt;
    use parquet::{arrow::ArrowWriter, file::properties::WriterProperties};
    use tempfile::tempdir;

    use super::ParquetTable;
    use crate::{
        EngineConfig, ParquetOptions,
        datasource::parquet_scan::align_batch,
        datasource::{
            ComparisonOp, MetadataCache, PredicateValue, ScanPredicate, ScanRequest, TableProvider,
        },
        runtime::{MemoryPool, QueryContext},
    };

    #[test]
    fn alignment_fills_missing_union_columns_with_null() {
        let source_schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            source_schema,
            vec![Arc::new(Int64Array::from(vec![1_i64, 2]))],
        )
        .unwrap();
        let target = Arc::new(Schema::new(vec![
            Field::new("missing", DataType::Utf8, true),
            Field::new("a", DataType::Int64, false),
        ]));

        let aligned = align_batch(batch, &target, None, 0).unwrap();
        assert_eq!(aligned.num_rows(), 2);
        assert_eq!(aligned.column(0).null_count(), 2);
        assert_eq!(aligned.schema(), target);
    }

    #[tokio::test]
    async fn applies_projection_limit_and_row_group_pruning() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("events.parquet");
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(vec![1_i64, 2, 3, 4])),
                Arc::new(StringArray::from(vec!["a", "b", "c", "d"])),
            ],
        )
        .unwrap();
        let properties = WriterProperties::builder()
            .set_max_row_group_row_count(Some(2))
            .build();
        let mut writer =
            ArrowWriter::try_new(File::create(&path).unwrap(), schema, Some(properties)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        let config = EngineConfig::default();
        let table = ParquetTable::try_new_with_cache(
            vec![path.to_string_lossy().into_owned()],
            ParquetOptions::default(),
            &config,
            MetadataCache::new(config.metadata_cache_bytes),
        )
        .await
        .unwrap();
        let context = Arc::new(
            QueryContext::new(MemoryPool::new(16 * 1024 * 1024), directory.path()).unwrap(),
        );
        let mut request = ScanRequest::new(2);
        request.projection = Some(vec![1]);
        request.limit = Some(3);
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
            3
        );
        assert!(
            batches
                .iter()
                .all(|batch| batch.schema().field(0).name() == "name")
        );
        assert_eq!(context.metrics.snapshot().rows_scanned, 3);

        let pruning_context = Arc::new(
            QueryContext::new(MemoryPool::new(16 * 1024 * 1024), directory.path()).unwrap(),
        );
        let mut request = ScanRequest::new(2);
        request.predicate = Some(ScanPredicate::Comparison {
            column: 0,
            op: ComparisonOp::Gt,
            value: PredicateValue::Int64(10),
        });
        table.prepare(Arc::clone(&pruning_context)).await.unwrap();
        pruning_context.seal_object_snapshots();
        let batches = table
            .scan(request, Arc::clone(&pruning_context))
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert!(batches.is_empty());
        assert_eq!(pruning_context.metrics.snapshot().row_groups_pruned, 2);
    }

    #[tokio::test]
    async fn refreshes_object_identity_between_queries() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("changing.parquet");
        write_ids(&path, &[1]);
        let config = EngineConfig::default();
        let table = ParquetTable::try_new_with_cache(
            vec![path.to_string_lossy().into_owned()],
            ParquetOptions::default(),
            &config,
            MetadataCache::new(config.metadata_cache_bytes),
        )
        .await
        .unwrap();

        assert_eq!(scan_rows(&table, directory.path()).await, 1);
        write_ids(&path, &[1, 2, 3]);
        assert_eq!(scan_rows(&table, directory.path()).await, 3);
    }

    fn write_ids(path: &std::path::Path, values: &[i64]) {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(values.to_vec()))],
        )
        .unwrap();
        let mut writer = ArrowWriter::try_new(File::create(path).unwrap(), schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
    }

    async fn scan_rows(table: &ParquetTable, spill_root: &std::path::Path) -> usize {
        let context =
            Arc::new(QueryContext::new(MemoryPool::new(16 * 1024 * 1024), spill_root).unwrap());
        table.prepare(Arc::clone(&context)).await.unwrap();
        context.seal_object_snapshots();
        table
            .scan(ScanRequest::new(2), context)
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap()
            .iter()
            .map(RecordBatch::num_rows)
            .sum()
    }
}
