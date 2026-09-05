use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use futures::{StreamExt, TryStreamExt, stream};

use crate::{
    Error, Result,
    runtime::{
        BatchEnvelope, MemoryBatchStream, QueryContext, RecordBatchStream,
        boxed_memory_batch_stream, estimate_schema_batch_bytes,
    },
    storage::{ObjectSnapshot, ObjectSource},
};

static NEXT_PROVIDER_ID: AtomicU64 = AtomicU64::new(1);

pub(crate) fn next_provider_id() -> u64 {
    NEXT_PROVIDER_ID.fetch_add(1, Ordering::Relaxed)
}

/// Coarse statistics available before a scan starts.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct TableStatistics {
    pub row_count: Option<u64>,
    pub total_byte_size: Option<u64>,
    pub file_count: usize,
}

/// Stable identity for scans that read the same object snapshot or unresolved
/// location specification with equivalent source semantics.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TableSourceIdentity {
    format: &'static str,
    sources: SourceSetIdentity,
    semantics: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum SourceSetIdentity {
    Objects(Vec<(String, ObjectSnapshot)>),
    Locations(Vec<String>),
}

impl TableSourceIdentity {
    pub(crate) fn from_objects(
        format: &'static str,
        files: &[ObjectSource],
        semantics: String,
    ) -> Self {
        let mut objects = files
            .iter()
            .map(|file| (file.uri().to_owned(), file.snapshot().clone()))
            .collect::<Vec<_>>();
        objects.sort_by(|left, right| left.0.cmp(&right.0));
        Self {
            format,
            sources: SourceSetIdentity::Objects(objects),
            semantics,
        }
    }

    pub(crate) fn from_spec(format: &'static str, locations: &[String], semantics: String) -> Self {
        let mut locations = locations.to_vec();
        locations.sort();
        Self {
            format,
            sources: SourceSetIdentity::Locations(locations),
            semantics,
        }
    }
}

/// A value that can be compared with file or row-group statistics.
#[derive(Clone, Debug, PartialEq)]
pub enum PredicateValue {
    Boolean(bool),
    Int64(i64),
    UInt64(u64),
    Float64(f64),
    Utf8(String),
    #[allow(dead_code)]
    Binary(Vec<u8>),
    Date32(i32),
    TimestampMicros(i64),
    Decimal128 {
        value: i128,
        precision: u8,
        scale: i8,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ComparisonOp {
    Eq,
    NotEq,
    Lt,
    LtEq,
    Gt,
    GtEq,
}

/// A conservative pushdown expression. Sources may ignore predicates they cannot
/// prove safe to apply; execution must still evaluate residual filters.
#[derive(Clone, Debug, PartialEq)]
pub enum ScanPredicate {
    Comparison {
        column: usize,
        op: ComparisonOp,
        value: PredicateValue,
    },
    IsNull {
        column: usize,
    },
    IsNotNull {
        column: usize,
    },
    And(Vec<ScanPredicate>),
    /// A same-column disjunction. Sources may prune it only when every branch
    /// is proven impossible; execution always retains the residual predicate.
    Or(Vec<ScanPredicate>),
}

/// Whether a source predicate is merely an optimization hint or is the
/// semantic owner of the SQL filter. Exact is crate-private because providers
/// outside RustDB must never be asked to uphold this internal contract.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) enum PredicateGuarantee {
    #[default]
    BestEffort,
    Exact,
}

#[derive(Clone, Debug)]
pub struct ScanRequest {
    pub projection: Option<Vec<usize>>,
    pub predicate: Option<ScanPredicate>,
    pub limit: Option<usize>,
    pub batch_size: usize,
    /// Optional source decode granularity for private sink pipelines. Sources
    /// may consume it only from `scan_tasks`; public scans retain `batch_size`.
    pub(crate) decode_batch_size: Option<usize>,
    /// Full provider-schema columns whose physical dictionary representation
    /// may be retained by sources that can prove it is safe. This is an
    /// internal execution hint; providers may ignore it.
    pub(crate) dictionary_columns: Vec<usize>,
    pub(crate) predicate_guarantee: PredicateGuarantee,
}

/// One independently pollable unit of scan work. File-backed providers should
/// expose their natural morsels here (Parquet row groups and CSV files) so the
/// execution pipeline can drive them on separate compute lanes.
pub(crate) struct ScanTask {
    id: usize,
    stream: MemoryBatchStream,
}

impl ScanTask {
    pub(crate) fn new(id: usize, stream: MemoryBatchStream) -> Self {
        Self { id, stream }
    }

    pub(crate) fn from_public(
        id: usize,
        mut stream: RecordBatchStream,
        context: Arc<QueryContext>,
        preclaim_bytes: usize,
        owner: &'static str,
    ) -> Self {
        let stream = boxed_memory_batch_stream(async_stream::try_stream! {
            while let Some(batch) = next_public_batch(
                &mut stream,
                &context,
                preclaim_bytes,
                owner,
            ).await? {
                yield batch;
            }
        });
        Self::new(id, stream)
    }

    pub(crate) fn id(&self) -> usize {
        self.id
    }

    pub(crate) fn into_stream(self) -> MemoryBatchStream {
        self.stream
    }
}

async fn next_public_batch(
    stream: &mut RecordBatchStream,
    context: &QueryContext,
    preclaim_bytes: usize,
    owner: &'static str,
) -> Result<Option<BatchEnvelope>> {
    let preclaim = preclaim_bytes.max(1);
    // A source can be exhausted without allocating another batch. Probe that
    // terminal state when admission is temporarily full; otherwise a
    // downstream operator retaining its state could wait forever for an EOF
    // reservation that EOF does not need.
    let reservation = match context.memory.try_reserve(preclaim) {
        Ok(reservation) => reservation,
        Err(_) => {
            tokio::select! {
                biased;
                _ = context.control.cancelled() => return Err(Error::Cancelled),
                next = stream.next() => {
                    let Some(batch) = next else {
                        return Ok(None);
                    };
                    // The source already allocated this batch before admission
                    // was available. Do not wait while retaining it; fail
                    // immediately if it cannot fit.
                    return BatchEnvelope::try_new(batch?, &context.memory, owner).map(Some);
                }
                reservation = context.reserve_memory(preclaim, owner) => reservation?,
            }
        }
    };
    let next = tokio::select! {
        _ = context.control.cancelled() => return Err(Error::Cancelled),
        next = stream.next() => next,
    };
    let Some(batch) = next else {
        return Ok(None);
    };
    Ok(Some(BatchEnvelope::from_reservation(
        batch?,
        reservation,
        owner,
    )?))
}

impl ScanRequest {
    pub fn new(batch_size: usize) -> Self {
        Self {
            projection: None,
            predicate: None,
            limit: None,
            batch_size,
            decode_batch_size: None,
            dictionary_columns: Vec::new(),
            predicate_guarantee: PredicateGuarantee::BestEffort,
        }
    }

    pub fn projected_schema(&self, schema: &SchemaRef) -> Result<SchemaRef> {
        match &self.projection {
            Some(indices) => Ok(Arc::new(schema.project(indices)?)),
            None => Ok(Arc::clone(schema)),
        }
    }

    pub(crate) fn reject_unsupported_exact(&self, provider: &str) -> Result<()> {
        if self.predicate_guarantee == PredicateGuarantee::Exact {
            return Err(Error::Internal(format!(
                "{provider} received an unsupported exact filter guarantee"
            )));
        }
        Ok(())
    }
}

#[async_trait]
pub trait TableProvider: Send + Sync {
    fn schema(&self) -> SchemaRef;

    fn statistics(&self) -> TableStatistics;

    /// Identifies providers whose scans are semantically interchangeable for
    /// one optimizer snapshot. Unknown providers retain pointer-only identity.
    fn source_identity(&self) -> Option<TableSourceIdentity> {
        None
    }

    /// Short source-specific details appended to a Scan in EXPLAIN.
    fn explain_scan(&self) -> Option<String> {
        None
    }

    /// Returns true only when this provider can make `predicate` the semantic
    /// filter for every object in the query snapshot.
    fn supports_exact_filter(&self, _predicate: &ScanPredicate) -> bool {
        false
    }

    /// Returns statistics for the object set fixed in `context`. Registered
    /// external tables override this after query preparation; the default is
    /// suitable for immutable and query-local providers.
    fn query_statistics(&self, _context: &QueryContext) -> TableStatistics {
        self.statistics()
    }

    /// Captures every external object this provider may scan. Execution calls
    /// this for all scans before any input stream is polled.
    async fn prepare(&self, _context: Arc<QueryContext>) -> Result<()> {
        Ok(())
    }

    /// Rebuilds a registered external table's schema and provider. Providers
    /// that are not refreshable return `None`.
    async fn refreshed(&self) -> Result<Option<Arc<dyn TableProvider>>> {
        Ok(None)
    }

    async fn scan(
        &self,
        request: ScanRequest,
        context: Arc<QueryContext>,
    ) -> Result<RecordBatchStream>;

    /// Produces independently pollable scan work. The compatibility default is
    /// one task; sources override this to expose file or row-group morsels.
    async fn scan_tasks(
        &self,
        request: ScanRequest,
        context: Arc<QueryContext>,
        _target_tasks: usize,
    ) -> Result<Vec<ScanTask>> {
        let schema = request.projected_schema(&self.schema())?;
        let preclaim = estimate_schema_batch_bytes(schema.as_ref(), request.batch_size);
        let stream = self.scan(request, Arc::clone(&context)).await?;
        Ok(vec![ScanTask::from_public(
            0,
            stream,
            context,
            preclaim,
            "table scan task",
        )])
    }
}

pub(super) async fn prepare_object_sources(
    files: &[ObjectSource],
    io_concurrency: usize,
    context: Arc<QueryContext>,
) -> Result<()> {
    stream::iter(files.iter().cloned().map(Ok::<_, Error>))
        .try_for_each_concurrent(io_concurrency, |file| {
            let context = Arc::clone(&context);
            async move {
                context.check_cancelled()?;
                if file.is_s3() {
                    context.metrics.add_s3_requests(1);
                }
                let snapshot = tokio::select! {
                    _ = context.control.cancelled() => Err(Error::Cancelled),
                    snapshot = file.head_snapshot() => snapshot,
                }?;
                context.register_object_snapshot(file.uri(), snapshot)
            }
        })
        .await
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::datatypes::{DataType, Field, Schema};

    use super::{ScanRequest, next_provider_id};

    #[test]
    fn provider_ids_share_one_monotonic_allocator() {
        let first = next_provider_id();
        let second = next_provider_id();
        assert!(second > first);
    }

    #[test]
    fn projection_preserves_requested_order() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Utf8, true),
        ]));
        let mut request = ScanRequest::new(1024);
        request.projection = Some(vec![1, 0]);

        let projected = request.projected_schema(&schema).unwrap();
        assert_eq!(projected.field(0).name(), "b");
        assert_eq!(projected.field(1).name(), "a");
    }

    #[test]
    fn non_parquet_provider_rejects_an_exact_guarantee() {
        let mut request = ScanRequest::new(1024);
        request.predicate_guarantee = super::PredicateGuarantee::Exact;
        let error = request.reject_unsupported_exact("CSV").unwrap_err();
        assert!(matches!(error, crate::Error::Internal(message) if message.contains("CSV")));
    }
}
