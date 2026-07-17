use std::sync::Arc;

use arrow::datatypes::SchemaRef;

use super::{FileSchema, ParquetTable};
use crate::{
    EngineConfig, Error, ParquetSchemaMode, Result,
    datasource::{MetadataCache, TableStatistics, native::NativePredicateSidecar},
    storage::ObjectSource,
};

impl ParquetTable {
    /// Builds a provider over an already fixed object list and trusted table
    /// metadata. Footer loading remains lazy in scan planning.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn from_fixed_files(
        files: Vec<ObjectSource>,
        schema: SchemaRef,
        statistics: TableStatistics,
        config: &EngineConfig,
        metadata_cache: MetadataCache,
    ) -> Result<Self> {
        Self::from_fixed_files_inner(files, None, schema, statistics, config, metadata_cache)
    }

    pub(in crate::datasource) fn from_fixed_files_with_predicate_sidecars(
        files: Vec<ObjectSource>,
        sidecars: Vec<Option<NativePredicateSidecar>>,
        schema: SchemaRef,
        statistics: TableStatistics,
        config: &EngineConfig,
        metadata_cache: MetadataCache,
    ) -> Result<Self> {
        Self::from_fixed_files_inner(
            files,
            Some(sidecars),
            schema,
            statistics,
            config,
            metadata_cache,
        )
    }

    fn from_fixed_files_inner(
        files: Vec<ObjectSource>,
        sidecars: Option<Vec<Option<NativePredicateSidecar>>>,
        schema: SchemaRef,
        statistics: TableStatistics,
        config: &EngineConfig,
        metadata_cache: MetadataCache,
    ) -> Result<Self> {
        if config.io_concurrency == 0 {
            return Err(Error::InvalidArgument(
                "I/O concurrency must be greater than zero".to_owned(),
            ));
        }
        if statistics.file_count != files.len() {
            return Err(Error::InvalidArgument(format!(
                "fixed Parquet statistics describe {} files, but {} files were provided",
                statistics.file_count,
                files.len()
            )));
        }
        if let Some(sidecars) = &sidecars {
            if sidecars.len() != files.len() {
                return Err(Error::InvalidArgument(format!(
                    "fixed Parquet provider received {} Native predicate companions for {} data files",
                    sidecars.len(),
                    files.len()
                )));
            }
            for (file, sidecar) in files.iter().zip(sidecars) {
                if let Some(sidecar) = sidecar
                    && sidecar.data_uri() != file.uri()
                {
                    return Err(Error::InvalidArgument(format!(
                        "Native predicate companion for '{}' was aligned with '{}'",
                        sidecar.data_uri(),
                        file.uri()
                    )));
                }
            }
        }

        let actual_bytes = files.iter().try_fold(0_u64, |bytes, file| {
            bytes.checked_add(file.snapshot().size).ok_or_else(|| {
                Error::ResourceExhausted("fixed Parquet file sizes overflow u64".to_owned())
            })
        })?;
        if let Some(expected_bytes) = statistics.total_byte_size
            && expected_bytes != actual_bytes
        {
            return Err(Error::InvalidArgument(format!(
                "fixed Parquet statistics report {expected_bytes} bytes, but object snapshots total {actual_bytes} bytes"
            )));
        }

        let file_schemas = files
            .iter()
            .map(|file| FileSchema {
                uri: file.uri().to_owned(),
                schema: Arc::clone(&schema),
            })
            .collect::<Vec<_>>();
        Ok(Self {
            files: files.into(),
            schema: Arc::clone(&schema),
            physical_schema: schema,
            statistics,
            hive: None,
            schema_mode: ParquetSchemaMode::Strict,
            file_schemas: file_schemas.into(),
            metadata_cache,
            io_concurrency: config.io_concurrency,
            parquet_scan: config.parquet_scan.clone(),
            _schema_reservation: None,
            query_metadata: None,
            fixed_files: true,
            native_predicate_sidecars: sidecars.map(Arc::from),
        })
    }
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

    use super::ParquetTable;
    use crate::{
        EngineConfig,
        datasource::{
            ComparisonOp, MetadataCache, PredicateValue, ScanPredicate, ScanRequest, TableProvider,
            TableStatistics,
        },
        runtime::{MemoryPool, QueryContext},
        storage::LocationResolver,
    };

    #[tokio::test]
    async fn fixed_files_skip_discovery_metadata_and_validate_statistics() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("not-parquet.rdbseg");
        std::fs::write(&path, b"footer loading must stay lazy").unwrap();
        let config = EngineConfig::default();
        let resolver = LocationResolver::with_memory_limit(config.s3.clone(), config.memory_limit);
        let files = resolver
            .resolve(&[path.to_string_lossy().into_owned()])
            .await
            .unwrap();
        let bytes = files[0].snapshot().size;
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let statistics = TableStatistics {
            row_count: Some(7),
            total_byte_size: Some(bytes),
            file_count: 1,
        };

        let table = ParquetTable::from_fixed_files(
            files,
            Arc::clone(&schema),
            statistics.clone(),
            &config,
            MetadataCache::new(config.metadata_cache_bytes),
        )
        .unwrap();
        assert_eq!(table.schema(), schema);
        assert_eq!(table.statistics(), statistics);
    }

    #[tokio::test]
    async fn fixed_files_reuse_projection_and_row_group_pruning() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("events.rdbseg");
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
        let mut writer = ArrowWriter::try_new(
            File::create(&path).unwrap(),
            Arc::clone(&schema),
            Some(properties),
        )
        .unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        let config = EngineConfig::default();
        let resolver = LocationResolver::with_memory_limit(config.s3.clone(), config.memory_limit);
        let files = resolver
            .resolve(&[path.to_string_lossy().into_owned()])
            .await
            .unwrap();
        let statistics = TableStatistics {
            row_count: Some(4),
            total_byte_size: Some(files[0].snapshot().size),
            file_count: 1,
        };
        let table = ParquetTable::from_fixed_files(
            files,
            schema,
            statistics,
            &config,
            MetadataCache::new(config.metadata_cache_bytes),
        )
        .unwrap();
        let projection_context = Arc::new(
            QueryContext::new(MemoryPool::new(16 * 1024 * 1024), directory.path()).unwrap(),
        );
        table
            .prepare(Arc::clone(&projection_context))
            .await
            .unwrap();
        projection_context.seal_object_snapshots();
        let mut request = ScanRequest::new(2);
        request.projection = Some(vec![1]);
        let projected = table
            .scan(request, Arc::clone(&projection_context))
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(
            projected.iter().map(RecordBatch::num_rows).sum::<usize>(),
            4
        );
        assert!(
            projected.iter().all(|batch| {
                batch.num_columns() == 1 && batch.schema().field(0).name() == "name"
            })
        );

        let pruning_context = Arc::new(
            QueryContext::new(MemoryPool::new(16 * 1024 * 1024), directory.path()).unwrap(),
        );
        table.prepare(Arc::clone(&pruning_context)).await.unwrap();
        pruning_context.seal_object_snapshots();
        let mut request = ScanRequest::new(2);
        request.projection = Some(vec![1]);
        request.predicate = Some(ScanPredicate::Comparison {
            column: 0,
            op: ComparisonOp::Gt,
            value: PredicateValue::Int64(10),
        });
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
}
