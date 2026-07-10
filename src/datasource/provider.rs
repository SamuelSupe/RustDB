use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use futures::{TryStreamExt, stream};

use crate::{
    Error, Result,
    runtime::{QueryContext, RecordBatchStream},
    storage::ObjectSource,
};

/// Coarse statistics available before a scan starts.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct TableStatistics {
    pub row_count: Option<u64>,
    pub total_byte_size: Option<u64>,
    pub file_count: usize,
}

/// A value that can be compared with file or row-group statistics.
#[derive(Clone, Debug, PartialEq)]
pub enum PredicateValue {
    Boolean(bool),
    Int64(i64),
    UInt64(u64),
    Float64(f64),
    Utf8(String),
    Date32(i32),
    Decimal128(i128),
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
}

#[derive(Clone, Debug)]
pub struct ScanRequest {
    pub projection: Option<Vec<usize>>,
    pub predicate: Option<ScanPredicate>,
    pub limit: Option<usize>,
    pub batch_size: usize,
}

impl ScanRequest {
    pub fn new(batch_size: usize) -> Self {
        Self {
            projection: None,
            predicate: None,
            limit: None,
            batch_size,
        }
    }

    pub fn projected_schema(&self, schema: &SchemaRef) -> Result<SchemaRef> {
        match &self.projection {
            Some(indices) => Ok(Arc::new(schema.project(indices)?)),
            None => Ok(Arc::clone(schema)),
        }
    }
}

#[async_trait]
pub trait TableProvider: Send + Sync {
    fn schema(&self) -> SchemaRef;

    fn statistics(&self) -> TableStatistics;

    /// Captures every external object this provider may scan. Execution calls
    /// this for all scans before any input stream is polled.
    async fn prepare(&self, _context: Arc<QueryContext>) -> Result<()> {
        Ok(())
    }

    async fn scan(
        &self,
        request: ScanRequest,
        context: Arc<QueryContext>,
    ) -> Result<RecordBatchStream>;
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

    use super::ScanRequest;

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
}
