use std::sync::Arc;

use arrow::datatypes::{Schema, SchemaRef};
use async_trait::async_trait;
use futures::StreamExt;

use super::next_provider_id;
use super::parquet_mapping::{nullable_schema, remap_predicate, remap_projection, reorder_schema};
use crate::{
    EngineConfig, Error, ParquetOptions, ParquetSchemaMode, Result,
    datasource::{
        MetadataCache, ParquetTable, ScanRequest, ScanTask, TableProvider, TableSourceIdentity,
        TableStatistics, schema_evolution::align_batch_to_schema,
    },
    runtime::{
        QueryContext, RecordBatchStream, boxed_memory_batch_stream, boxed_record_batch_stream,
    },
};

#[derive(Clone, Debug)]
pub(crate) struct RegisteredParquetTable {
    id: u64,
    locations: Arc<[String]>,
    refresh_options: ParquetOptions,
    config: EngineConfig,
    metadata_cache: MetadataCache,
    schema_mode: ParquetSchemaMode,
    schema: SchemaRef,
    physical_schema: SchemaRef,
    statistics: TableStatistics,
}

impl RegisteredParquetTable {
    pub(crate) async fn try_new(
        locations: Vec<String>,
        options: ParquetOptions,
        config: &EngineConfig,
        metadata_cache: MetadataCache,
    ) -> Result<Self> {
        let schema_mode = options.effective_schema_mode()?;
        let discovered = ParquetTable::try_new_with_cache(
            locations.clone(),
            options.clone(),
            config,
            metadata_cache.clone(),
        )
        .await?;
        Ok(Self::from_discovered(
            locations,
            options,
            config,
            metadata_cache,
            schema_mode,
            discovered,
            None,
            None,
        ))
    }

    #[allow(clippy::too_many_arguments)]
    fn from_discovered(
        locations: Vec<String>,
        refresh_options: ParquetOptions,
        config: &EngineConfig,
        metadata_cache: MetadataCache,
        schema_mode: ParquetSchemaMode,
        discovered: ParquetTable,
        previous_schema: Option<&Schema>,
        previous_physical_schema: Option<&Schema>,
    ) -> Self {
        let mut schema = reorder_schema(&discovered.schema(), previous_schema);
        let mut physical_schema =
            reorder_schema(&discovered.physical_schema(), previous_physical_schema);
        if schema_mode == ParquetSchemaMode::UnionByName {
            schema = nullable_schema(&schema);
            physical_schema = nullable_schema(&physical_schema);
        }
        Self {
            id: next_provider_id(),
            locations: locations.into(),
            refresh_options,
            config: config.clone(),
            metadata_cache,
            schema_mode,
            schema,
            physical_schema,
            statistics: discovered.statistics(),
        }
    }

    async fn discover_for_query(
        &self,
        context: Arc<QueryContext>,
    ) -> Result<Arc<dyn TableProvider>> {
        let mut options = self.refresh_options.clone();
        options.schema = Some(Arc::clone(&self.physical_schema));
        options.union_by_name = false;
        options.schema_mode = self.schema_mode;
        let table = ParquetTable::try_new_with_cache_for_query(
            self.locations.to_vec(),
            options,
            &self.config,
            self.metadata_cache.clone(),
            Some(Arc::clone(&context)),
        )
        .await?;
        table.validate_compatible_with(&self.physical_schema, self.schema_mode)?;
        Ok(Arc::new(table))
    }

    fn remap_request(
        &self,
        provider: &Arc<dyn TableProvider>,
        request: ScanRequest,
    ) -> Result<(ScanRequest, SchemaRef)> {
        let target = request.projected_schema(&self.schema)?;
        let provider_schema = provider.schema();
        let projection = match request.projection {
            Some(indices) => Some(remap_projection(&self.schema, &provider_schema, &indices)?),
            None => Some(remap_projection(
                &self.schema,
                &provider_schema,
                &(0..self.schema.fields().len()).collect::<Vec<_>>(),
            )?),
        };
        let predicate = request
            .predicate
            .as_ref()
            .and_then(|predicate| remap_predicate(predicate, &self.schema, &provider_schema));
        Ok((
            ScanRequest {
                projection,
                predicate,
                limit: request.limit,
                batch_size: request.batch_size,
            },
            target,
        ))
    }
}

#[async_trait]
impl TableProvider for RegisteredParquetTable {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn statistics(&self) -> TableStatistics {
        self.statistics.clone()
    }

    fn source_identity(&self) -> Option<TableSourceIdentity> {
        Some(TableSourceIdentity::from_spec(
            "parquet",
            &self.locations,
            format!(
                "options={:?};schema_mode={:?};schema={:?};physical_schema={:?}",
                self.refresh_options, self.schema_mode, self.schema, self.physical_schema
            ),
        ))
    }

    fn explain_scan(&self) -> Option<String> {
        Some("format=parquet morsel=row_group metadata=singleflight".to_owned())
    }

    fn query_statistics(&self, context: &QueryContext) -> TableStatistics {
        context
            .prepared_provider(self.id)
            .map(|provider| provider.statistics())
            .unwrap_or_else(|| self.statistics())
    }

    async fn prepare(&self, context: Arc<QueryContext>) -> Result<()> {
        if context.prepared_provider(self.id).is_some() {
            return Ok(());
        }
        let provider = self.discover_for_query(Arc::clone(&context)).await?;
        provider.prepare(Arc::clone(&context)).await?;
        context.cache_prepared_provider(self.id, provider)
    }

    async fn refreshed(&self) -> Result<Option<Arc<dyn TableProvider>>> {
        let discovered = ParquetTable::try_new_with_cache(
            self.locations.to_vec(),
            self.refresh_options.clone(),
            &self.config,
            self.metadata_cache.clone(),
        )
        .await?;
        Ok(Some(Arc::new(Self::from_discovered(
            self.locations.to_vec(),
            self.refresh_options.clone(),
            &self.config,
            self.metadata_cache.clone(),
            self.schema_mode,
            discovered,
            Some(&self.schema),
            Some(&self.physical_schema),
        ))))
    }

    async fn scan(
        &self,
        request: ScanRequest,
        context: Arc<QueryContext>,
    ) -> Result<RecordBatchStream> {
        let provider = context.prepared_provider(self.id).ok_or_else(|| {
            Error::Internal("registered Parquet table scanned before query preparation".to_owned())
        })?;
        let (request, target) = self.remap_request(&provider, request)?;
        let mut input = provider.scan(request, context).await?;
        Ok(boxed_record_batch_stream(async_stream::try_stream! {
            while let Some(batch) = input.next().await {
                yield align_batch_to_schema(batch?, Arc::clone(&target), "registered Parquet table")?;
            }
        }))
    }

    async fn scan_tasks(
        &self,
        request: ScanRequest,
        context: Arc<QueryContext>,
        target_tasks: usize,
    ) -> Result<Vec<ScanTask>> {
        let provider = context.prepared_provider(self.id).ok_or_else(|| {
            Error::Internal("registered Parquet table scanned before query preparation".to_owned())
        })?;
        let (request, target) = self.remap_request(&provider, request)?;
        let tasks = provider.scan_tasks(request, context, target_tasks).await?;
        Ok(tasks
            .into_iter()
            .map(|task| {
                let id = task.id();
                let mut input = task.into_stream();
                let target = Arc::clone(&target);
                ScanTask::new(
                    id,
                    boxed_memory_batch_stream(async_stream::try_stream! {
                        while let Some(batch) = input.next().await {
                            let batch = batch?;
                            let aligned = align_batch_to_schema(
                                batch.batch().clone(),
                                Arc::clone(&target),
                                "registered Parquet table",
                            )?;
                            yield batch.replace(aligned, "registered Parquet alignment")?;
                        }
                    }),
                )
            })
            .collect())
    }
}
