use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use async_trait::async_trait;

use super::{
    csv_mapping::{remap_predicate, remap_projection, reorder_schema},
    next_provider_id,
};
use crate::{
    CsvHeader, CsvOptions, EngineConfig, Error, Result,
    datasource::{
        CsvTable, ScanRequest, ScanTask, TableProvider, TableSourceIdentity, TableStatistics,
    },
    runtime::{QueryContext, RecordBatchStream},
};

#[derive(Clone, Debug)]
pub(crate) struct RegisteredCsvTable {
    id: u64,
    locations: Arc<[String]>,
    refresh_options: CsvOptions,
    query_options: CsvOptions,
    schema_was_inferred: bool,
    config: EngineConfig,
    schema: SchemaRef,
    statistics: TableStatistics,
}

impl RegisteredCsvTable {
    pub(crate) async fn try_new(
        locations: Vec<String>,
        options: CsvOptions,
        config: &EngineConfig,
    ) -> Result<Self> {
        let discovered = CsvTable::try_new(locations.clone(), options.clone(), config).await?;
        Self::from_discovered(locations, options, config, discovered, None)
    }

    fn from_discovered(
        locations: Vec<String>,
        refresh_options: CsvOptions,
        config: &EngineConfig,
        discovered: CsvTable,
        previous_schema: Option<&arrow::datatypes::Schema>,
    ) -> Result<Self> {
        let physical_schema = discovered.schema();
        let schema = reorder_schema(&physical_schema, previous_schema)?;
        let statistics = discovered.statistics();
        let schema_was_inferred = refresh_options.schema.is_none();
        let mut query_options = refresh_options.clone();
        query_options.schema = Some(Arc::clone(&physical_schema));
        query_options.header = if discovered.has_header() {
            CsvHeader::Present
        } else {
            CsvHeader::Absent
        };
        Ok(Self {
            id: next_provider_id(),
            locations: locations.into(),
            refresh_options,
            query_options,
            schema_was_inferred,
            config: config.clone(),
            schema,
            statistics,
        })
    }

    async fn discover_for_query(
        &self,
        context: Arc<QueryContext>,
    ) -> Result<Arc<dyn TableProvider>> {
        let table = if self.schema_was_inferred {
            let registered_schema = self.query_options.schema.as_ref().ok_or_else(|| {
                Error::Internal("registered CSV table is missing its physical schema".to_owned())
            })?;
            CsvTable::try_new_for_query_with_registered_schema(
                self.locations.to_vec(),
                self.refresh_options.clone(),
                Arc::clone(registered_schema),
                &self.config,
                Some(Arc::clone(&context)),
            )
            .await?
        } else {
            CsvTable::try_new_for_query(
                self.locations.to_vec(),
                self.query_options.clone(),
                &self.config,
                Some(Arc::clone(&context)),
            )
            .await?
        };
        Ok(Arc::new(table))
    }

    fn remap_request(
        &self,
        provider: &Arc<dyn TableProvider>,
        request: ScanRequest,
    ) -> Result<ScanRequest> {
        let provider_schema = provider.schema();
        let logical_projection = request
            .projection
            .unwrap_or_else(|| (0..self.schema.fields().len()).collect());
        let projection = remap_projection(&self.schema, &provider_schema, &logical_projection)?;
        let predicate = request
            .predicate
            .as_ref()
            .map(|predicate| remap_predicate(predicate, &self.schema, &provider_schema))
            .transpose()?;
        Ok(ScanRequest {
            projection: Some(projection),
            predicate,
            limit: request.limit,
            batch_size: request.batch_size,
        })
    }
}

#[async_trait]
impl TableProvider for RegisteredCsvTable {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn statistics(&self) -> TableStatistics {
        self.statistics.clone()
    }

    fn source_identity(&self) -> Option<TableSourceIdentity> {
        Some(TableSourceIdentity::from_spec(
            "csv",
            &self.locations,
            format!(
                "refresh={:?};query={:?};schema={:?}",
                self.refresh_options, self.query_options, self.schema
            ),
        ))
    }

    fn explain_scan(&self) -> Option<String> {
        Some(format!(
            "format=csv codec={:?} record_morsel_target={} parser_lanes={}",
            self.refresh_options.compression,
            self.config.csv_scan.target_morsel_bytes,
            if self.config.csv_scan.parallel_single_file {
                "runtime"
            } else {
                "1"
            }
        ))
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
        let discovered = CsvTable::try_new(
            self.locations.to_vec(),
            self.refresh_options.clone(),
            &self.config,
        )
        .await?;
        Ok(Some(Arc::new(Self::from_discovered(
            self.locations.to_vec(),
            self.refresh_options.clone(),
            &self.config,
            discovered,
            Some(&self.schema),
        )?)))
    }

    async fn scan(
        &self,
        request: ScanRequest,
        context: Arc<QueryContext>,
    ) -> Result<RecordBatchStream> {
        let provider = context.prepared_provider(self.id).ok_or_else(|| {
            Error::Internal("registered CSV table scanned before query preparation".to_owned())
        })?;
        let request = self.remap_request(&provider, request)?;
        provider.scan(request, context).await
    }

    async fn scan_tasks(
        &self,
        request: ScanRequest,
        context: Arc<QueryContext>,
        target_tasks: usize,
    ) -> Result<Vec<ScanTask>> {
        let provider = context.prepared_provider(self.id).ok_or_else(|| {
            Error::Internal("registered CSV table scanned before query preparation".to_owned())
        })?;
        let request = self.remap_request(&provider, request)?;
        provider.scan_tasks(request, context, target_tasks).await
    }
}
