use std::sync::Arc;

use arrow::{
    array::{Array, Int64Array},
    datatypes::{DataType, Field, Schema, SchemaRef},
    record_batch::RecordBatch,
};
use async_trait::async_trait;
use futures::{StreamExt, stream};

use crate::{
    Result,
    datasource::{ScanRequest, ScanTask, TableProvider, TableStatistics},
    runtime::{
        MemoryPool, QueryContext, RecordBatchStream, boxed_record_batch_stream,
        estimate_schema_batch_bytes,
    },
    sql::{BinaryOp, BoundExpr, ExprKind, LogicalPlan, PlanSchema, ScalarValue, StatementPlan},
};

#[derive(Clone)]
struct ProjectingTable {
    schema: SchemaRef,
    batch: RecordBatch,
}

#[async_trait]
impl TableProvider for ProjectingTable {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn statistics(&self) -> TableStatistics {
        TableStatistics::default()
    }

    async fn scan(
        &self,
        _request: ScanRequest,
        _context: Arc<QueryContext>,
    ) -> Result<RecordBatchStream> {
        unreachable!("scan_tasks is implemented")
    }

    async fn scan_tasks(
        &self,
        request: ScanRequest,
        context: Arc<QueryContext>,
        _target_tasks: usize,
    ) -> Result<Vec<ScanTask>> {
        let projection = request
            .projection
            .clone()
            .unwrap_or_else(|| (0..self.schema.fields().len()).collect());
        let schema = request.projected_schema(&self.schema)?;
        let columns = projection
            .iter()
            .map(|index| Arc::clone(self.batch.column(*index)))
            .collect();
        let batch = RecordBatch::try_new(Arc::clone(&schema), columns)?;
        let preclaim = estimate_schema_batch_bytes(schema.as_ref(), request.batch_size);
        Ok(vec![ScanTask::from_public(
            0,
            boxed_record_batch_stream(stream::once(async move { Ok(batch) })),
            context,
            preclaim,
            "compact filter test scan",
        )])
    }
}

#[tokio::test]
async fn projected_filter_runs_before_full_schema_expansion() {
    let schema = Arc::new(Schema::new(
        ["a", "unused_b", "filter_c", "unused_d"]
            .into_iter()
            .map(|name| Field::new(name, DataType::Int64, false))
            .collect::<Vec<_>>(),
    ));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![10, 20, 30])),
            Arc::new(Int64Array::from(vec![11, 21, 31])),
            Arc::new(Int64Array::from(vec![1, 0, 1])),
            Arc::new(Int64Array::from(vec![13, 23, 33])),
        ],
    )
    .unwrap();
    let scan = LogicalPlan::Scan {
        table_name: "compact".into(),
        provider: Arc::new(ProjectingTable {
            schema: Arc::clone(&schema),
            batch,
        }),
        statistics: TableStatistics::default(),
        projection: Some(vec![0, 2]),
        pushed_filter: None,
        limit: None,
        schema: PlanSchema::unqualified(Arc::clone(&schema)),
    };
    let predicate = BoundExpr {
        kind: ExprKind::Binary {
            left: Box::new(BoundExpr::column(2, DataType::Int64, "filter_c")),
            op: BinaryOp::Eq,
            right: Box::new(BoundExpr::literal(ScalarValue::Int64(1))),
        },
        data_type: DataType::Boolean,
        display_name: "filter_c = 1".into(),
    };
    let plan = LogicalPlan::Filter {
        input: Box::new(scan),
        predicate,
        schema: PlanSchema::unqualified(schema),
    };

    let temp = tempfile::tempdir().unwrap();
    let context = Arc::new(QueryContext::new(MemoryPool::new(16 << 20), temp.path()).unwrap());
    let mut output = super::super::runner::execute(StatementPlan::Query(plan), context)
        .await
        .unwrap();
    let batch = output.next().await.unwrap().unwrap();

    assert_eq!(batch.num_rows(), 2);
    assert_eq!(batch.num_columns(), 4);
    assert_eq!(
        batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values(),
        &[10, 30]
    );
    assert!(batch.column(1).is_null(0));
    assert_eq!(
        batch
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values(),
        &[1, 1]
    );
    assert!(batch.column(3).is_null(1));
    assert!(output.next().await.is_none());
}
