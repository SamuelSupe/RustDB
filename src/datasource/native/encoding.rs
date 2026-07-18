use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use futures::StreamExt;

use crate::{
    Result,
    runtime::{
        QueryContext, RecordBatchStream, boxed_memory_batch_stream, boxed_record_batch_stream,
    },
    storage::{decode_native_segment_batch, native_segment_encoding_required},
};

use super::super::{
    PredicateGuarantee, ScanPredicate, ScanRequest, ScanTask, TableProvider, TableSourceIdentity,
    TableStatistics,
};

pub(super) struct NativeEncodedTable {
    inner: Arc<dyn TableProvider>,
    schema: SchemaRef,
}

impl NativeEncodedTable {
    pub(super) fn wrap(
        inner: Arc<dyn TableProvider>,
        schema: SchemaRef,
    ) -> Result<Arc<dyn TableProvider>> {
        if !native_segment_encoding_required(&schema) {
            return Ok(inner);
        }
        Ok(Arc::new(Self { inner, schema }))
    }

    fn request(&self, mut request: ScanRequest) -> Result<ScanRequest> {
        request.reject_unsupported_exact("Native interval decoder")?;
        request.predicate = None;
        request.predicate_guarantee = PredicateGuarantee::BestEffort;
        request.dictionary_columns.clear();
        Ok(request)
    }
}

#[async_trait]
impl TableProvider for NativeEncodedTable {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn statistics(&self) -> TableStatistics {
        self.inner.statistics()
    }

    fn source_identity(&self) -> Option<TableSourceIdentity> {
        self.inner.source_identity()
    }

    fn explain_scan(&self) -> Option<String> {
        Some(match self.inner.explain_scan() {
            Some(inner) => format!("{inner} native_interval_encoding=fixed16"),
            None => "native_interval_encoding=fixed16".to_owned(),
        })
    }

    fn supports_exact_filter(&self, _predicate: &ScanPredicate) -> bool {
        false
    }

    fn query_statistics(&self, context: &QueryContext) -> TableStatistics {
        self.inner.query_statistics(context)
    }

    async fn scan(
        &self,
        request: ScanRequest,
        context: Arc<QueryContext>,
    ) -> Result<RecordBatchStream> {
        let schema = request.projected_schema(&self.schema)?;
        let mut input = self.inner.scan(self.request(request)?, context).await?;
        Ok(boxed_record_batch_stream(async_stream::try_stream! {
            while let Some(batch) = input.next().await {
                yield decode_native_segment_batch(&batch?, &schema)?;
            }
        }))
    }

    async fn scan_tasks(
        &self,
        request: ScanRequest,
        context: Arc<QueryContext>,
        target_tasks: usize,
    ) -> Result<Vec<ScanTask>> {
        let schema = request.projected_schema(&self.schema)?;
        let tasks = self
            .inner
            .scan_tasks(self.request(request)?, Arc::clone(&context), target_tasks)
            .await?;
        Ok(tasks
            .into_iter()
            .map(|task| {
                let id = task.id();
                let mut input = task.into_stream();
                let schema = Arc::clone(&schema);
                let context = Arc::clone(&context);
                ScanTask::new(
                    id,
                    boxed_memory_batch_stream(async_stream::try_stream! {
                        while let Some(envelope) = input.next().await {
                            let envelope = envelope?;
                            let workspace = context
                                .reserve_memory(envelope.memory_size().max(1), "native interval decode")
                                .await?;
                            let decoded = decode_native_segment_batch(envelope.batch(), &schema)?;
                            yield envelope.replace_with_reservation(
                                decoded,
                                workspace,
                                "native interval decode",
                            )?;
                        }
                    }),
                )
            })
            .collect())
    }
}
