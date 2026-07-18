use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use arrow::{array::BooleanArray, compute::filter_record_batch, datatypes::SchemaRef};
use async_trait::async_trait;
use futures::{StreamExt, stream};

use crate::{
    EngineConfig, Error, Result,
    runtime::{
        BatchEnvelope, MemoryReservation, QueryContext, RecordBatchStream,
        boxed_memory_batch_stream, boxed_record_batch_stream,
    },
    storage::{NativeDeleteVector, ObjectSource},
};

use super::super::{
    MetadataCache, ParquetTable, PredicateGuarantee, ScanPredicate, ScanRequest, ScanTask,
    TableProvider, TableStatistics,
};
use super::NativePredicateSidecar;

pub(super) struct DeleteAwareParquetTable {
    schema: SchemaRef,
    statistics: TableStatistics,
    segments: Vec<Segment>,
    _delete_vector_memory: MemoryReservation,
}

struct Segment {
    table: Arc<ParquetTable>,
    delete_vector: Option<Arc<NativeDeleteVector>>,
}

struct VisibleLimit {
    remaining: AtomicUsize,
}

impl DeleteAwareParquetTable {
    #[allow(clippy::too_many_arguments)]
    pub(super) fn try_new(
        files: Vec<ObjectSource>,
        sidecars: Vec<Option<NativePredicateSidecar>>,
        delete_vectors: Vec<Option<NativeDeleteVector>>,
        schema: SchemaRef,
        statistics: TableStatistics,
        config: &EngineConfig,
        metadata_cache: MetadataCache,
        delete_vector_memory: MemoryReservation,
    ) -> Result<Self> {
        if files.len() != sidecars.len() || files.len() != delete_vectors.len() {
            return Err(Error::Internal(
                "native segment and delete-vector counts do not match".to_owned(),
            ));
        }
        let mut segments = Vec::with_capacity(files.len());
        for ((file, sidecar), delete_vector) in files.into_iter().zip(sidecars).zip(delete_vectors)
        {
            let physical_rows = delete_vector.as_ref().map(NativeDeleteVector::row_count);
            let segment_statistics = TableStatistics {
                row_count: physical_rows,
                total_byte_size: Some(file.snapshot().size),
                file_count: 1,
            };
            let table = ParquetTable::from_fixed_files_with_predicate_sidecars(
                vec![file],
                vec![sidecar],
                Arc::clone(&schema),
                segment_statistics,
                config,
                metadata_cache.clone(),
            )?;
            segments.push(Segment {
                table: Arc::new(table),
                delete_vector: delete_vector.map(Arc::new),
            });
        }
        Ok(Self {
            schema,
            statistics,
            segments,
            _delete_vector_memory: delete_vector_memory,
        })
    }
}

#[async_trait]
impl TableProvider for DeleteAwareParquetTable {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn statistics(&self) -> TableStatistics {
        self.statistics.clone()
    }

    fn supports_exact_filter(&self, _predicate: &ScanPredicate) -> bool {
        false
    }

    async fn scan(
        &self,
        request: ScanRequest,
        context: Arc<QueryContext>,
    ) -> Result<RecordBatchStream> {
        let tasks = self
            .scan_tasks(request, Arc::clone(&context), self.segments.len().max(1))
            .await?;
        let streams = stream::iter(tasks.into_iter().map(ScanTask::into_stream));
        let mut merged = streams.flatten_unordered(self.segments.len().max(1));
        Ok(boxed_record_batch_stream(async_stream::try_stream! {
            while let Some(envelope) = merged.next().await {
                context.check_cancelled()?;
                yield envelope?.into_public();
            }
        }))
    }

    async fn scan_tasks(
        &self,
        mut request: ScanRequest,
        context: Arc<QueryContext>,
        _target_tasks: usize,
    ) -> Result<Vec<ScanTask>> {
        let limit = request.limit.map(|remaining| {
            Arc::new(VisibleLimit {
                remaining: AtomicUsize::new(remaining),
            })
        });
        // Physical offsets must remain contiguous. Predicate and limit remain
        // owned by the upper pipeline until the delete bitmap is applied.
        request.predicate = None;
        request.predicate_guarantee = PredicateGuarantee::BestEffort;
        request.limit = None;

        let mut tasks = Vec::with_capacity(self.segments.len());
        for segment in &self.segments {
            let mut segment_tasks = segment
                .table
                .scan_tasks(request.clone(), Arc::clone(&context), 1)
                .await?;
            let Some(task) = segment_tasks.pop() else {
                continue;
            };
            if !segment_tasks.is_empty() {
                return Err(Error::Internal(
                    "single Native segment produced more than one ordered scan task".to_owned(),
                ));
            }
            let input = task.into_stream();
            let vector = segment.delete_vector.clone();
            let task_context = Arc::clone(&context);
            let task_limit = limit.clone();
            let id = tasks.len();
            tasks.push(ScanTask::new(
                id,
                boxed_memory_batch_stream(async_stream::try_stream! {
                    let mut input = input;
                    let mut physical_offset = 0_u64;
                    while let Some(envelope) = input.next().await {
                        task_context.check_cancelled()?;
                        if task_limit.as_ref().is_some_and(|limit| limit.exhausted()) {
                            break;
                        }
                        let envelope = envelope?;
                        let physical_rows = u64::try_from(envelope.num_rows()).map_err(|_| {
                            Error::ResourceExhausted("native batch row count does not fit in u64".to_owned())
                        })?;
                        let mut envelope = match vector.as_deref() {
                            Some(vector) => filter_deleted(
                                envelope,
                                vector,
                                physical_offset,
                                &task_context,
                            ).await?,
                            None => envelope,
                        };
                        physical_offset = physical_offset.checked_add(physical_rows).ok_or_else(|| {
                            Error::ResourceExhausted("native physical row offset overflow".to_owned())
                        })?;
                        if envelope.num_rows() == 0 {
                            continue;
                        }
                        if let Some(limit) = task_limit.as_ref() {
                            let claimed = limit.claim(envelope.num_rows());
                            if claimed == 0 {
                                break;
                            }
                            if claimed < envelope.num_rows() {
                                let batch = envelope.batch().slice(0, claimed);
                                envelope = envelope.replace(batch, "native delete-vector limit")?;
                            }
                        }
                        yield envelope;
                    }
                    if !task_limit.as_ref().is_some_and(|limit| limit.exhausted())
                        && let Some(vector) = vector.as_deref()
                        && physical_offset != vector.row_count()
                    {
                        Err(Error::Execution(format!(
                            "native delete vector covers {} rows but its segment produced {physical_offset}",
                            vector.row_count()
                        )))?;
                    }
                }),
            ));
        }
        Ok(tasks)
    }
}

async fn filter_deleted(
    envelope: BatchEnvelope,
    vector: &NativeDeleteVector,
    start: u64,
    context: &QueryContext,
) -> Result<BatchEnvelope> {
    let rows = envelope.num_rows();
    let end = start
        .checked_add(u64::try_from(rows).map_err(|_| {
            Error::ResourceExhausted("native batch row count does not fit in u64".to_owned())
        })?)
        .ok_or_else(|| Error::ResourceExhausted("native row offset overflow".to_owned()))?;
    if end > vector.row_count() {
        return Err(Error::Execution(format!(
            "native segment produced row offset {end} beyond delete-vector row count {}",
            vector.row_count()
        )));
    }
    let kept = (start..end)
        .filter(|offset| !vector.contains(*offset))
        .count();
    if kept == rows {
        return Ok(envelope);
    }
    if kept == 0 {
        let batch = envelope.batch().slice(0, 0);
        return envelope.replace(batch, "native delete-vector filter");
    }
    let workspace_bytes = envelope
        .memory_size()
        .saturating_add(rows.div_ceil(8).saturating_mul(2))
        .saturating_add(256);
    let workspace = context
        .reserve_memory(workspace_bytes, "native delete-vector filter")
        .await?;
    let mask = BooleanArray::from_iter((start..end).map(|offset| Some(!vector.contains(offset))));
    let batch = filter_record_batch(envelope.batch(), &mask)?;
    envelope.replace_with_reservation(batch, workspace, "native delete-vector filter")
}

impl VisibleLimit {
    fn exhausted(&self) -> bool {
        self.remaining.load(Ordering::Relaxed) == 0
    }

    fn claim(&self, rows: usize) -> usize {
        let mut remaining = self.remaining.load(Ordering::Relaxed);
        loop {
            if remaining == 0 {
                return 0;
            }
            let claimed = remaining.min(rows);
            match self.remaining.compare_exchange_weak(
                remaining,
                remaining - claimed,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => return claimed,
                Err(actual) => remaining = actual,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use arrow::{
        array::Int64Array,
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    };

    use super::*;
    use crate::runtime::MemoryPool;

    #[tokio::test]
    async fn removes_physical_rows_and_reconciles_the_batch_lease() {
        let directory = tempfile::tempdir().unwrap();
        let context = QueryContext::new(MemoryPool::new(1 << 20), directory.path()).unwrap();
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(Int64Array::from(vec![10_i64, 20, 30, 40]))],
        )
        .unwrap();
        let envelope = BatchEnvelope::try_new(batch, &context.memory, "test input").unwrap();
        let vector = NativeDeleteVector::for_test(4, [1, 3]);

        let filtered = filter_deleted(envelope, &vector, 0, &context)
            .await
            .unwrap();
        let values = filtered
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(values.values(), &[10, 30]);
        drop(filtered);
        assert_eq!(context.memory.used(), 0);
    }
}
