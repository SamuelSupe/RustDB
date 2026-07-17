use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;

use crate::{
    Result,
    datasource::{ScanRequest, TableProvider, TableStatistics},
    runtime::{QueryContext, RecordBatchStream},
    sql::{
        AggregateExpr, AggregateFunction, BinaryOp, BoundExpr, ExprKind, LogicalPlan, PlanSchema,
        ScalarValue,
    },
};

pub(super) fn scan() -> LogicalPlan {
    let schema = Arc::new(Schema::new(
        (0..4)
            .map(|index| Field::new(format!("c{index}"), DataType::Int64, false))
            .collect::<Vec<_>>(),
    ));
    LogicalPlan::Scan {
        table_name: "t".into(),
        provider: Arc::new(SchemaTable(Arc::clone(&schema))),
        statistics: TableStatistics::default(),
        projection: None,
        pushed_filter: None,
        exact_filter: None,
        limit: None,
        schema: PlanSchema::new_with_visibility(
            schema,
            vec![Some("t".into()); 4],
            vec![true, true, true, false],
        ),
    }
}

pub(super) fn aggregate(
    input: LogicalPlan,
    group_exprs: Vec<BoundExpr>,
    aggregate_exprs: Vec<AggregateExpr>,
) -> LogicalPlan {
    let fields = group_exprs
        .iter()
        .map(|expr| Field::new(expr.display_name.clone(), expr.data_type.clone(), true))
        .chain(
            aggregate_exprs
                .iter()
                .map(|expr| Field::new(expr.display_name.clone(), expr.data_type.clone(), true)),
        )
        .collect::<Vec<_>>();
    LogicalPlan::Aggregate {
        input: Box::new(input),
        group_exprs,
        aggregate_exprs,
        schema: PlanSchema::unqualified(Arc::new(Schema::new(fields))),
    }
}

pub(super) fn sum(expression: BoundExpr, distinct: bool) -> AggregateExpr {
    AggregateExpr {
        function: AggregateFunction::Sum,
        expr: Some(expression),
        distinct,
        data_type: DataType::Decimal128(38, 0),
        display_name: "sum".into(),
    }
}

pub(super) fn column(index: usize) -> BoundExpr {
    BoundExpr::column(index, DataType::Int64, format!("c{index}"))
}

pub(super) fn int(value: i64) -> BoundExpr {
    BoundExpr::literal(ScalarValue::Int64(value))
}

pub(super) fn binary(
    left: BoundExpr,
    op: BinaryOp,
    right: BoundExpr,
    data_type: DataType,
) -> BoundExpr {
    BoundExpr {
        kind: ExprKind::Binary {
            left: Box::new(left),
            op,
            right: Box::new(right),
        },
        data_type,
        display_name: "binary".into(),
    }
}

pub(super) fn referenced(expression: &BoundExpr) -> Vec<usize> {
    let mut columns = Vec::new();
    expression.referenced_columns(&mut columns);
    columns
}

pub(super) fn assert_column(expression: &BoundExpr, expected: usize) {
    assert!(matches!(&expression.kind, ExprKind::Column(index) if *index == expected));
}

pub(super) fn assert_columns(expressions: &[BoundExpr], expected: &[usize]) {
    let columns = expressions
        .iter()
        .map(|expression| match &expression.kind {
            ExprKind::Column(index) => *index,
            _ => panic!("expected direct column projection"),
        })
        .collect::<Vec<_>>();
    assert_eq!(columns, expected);
}

pub(super) fn assert_scan_projection(plan: &LogicalPlan, expected: &[usize]) {
    match plan {
        LogicalPlan::Scan { projection, .. } => assert_eq!(projection.as_deref(), Some(expected)),
        LogicalPlan::Filter { input, .. } => assert_scan_projection(input, expected),
        other => panic!("expected Scan/Filter chain, got {}", other.explain()),
    }
}

struct SchemaTable(SchemaRef);

#[async_trait]
impl TableProvider for SchemaTable {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.0)
    }

    fn statistics(&self) -> TableStatistics {
        TableStatistics::default()
    }

    async fn scan(
        &self,
        _request: ScanRequest,
        _context: Arc<QueryContext>,
    ) -> Result<RecordBatchStream> {
        panic!("optimizer-only provider must not execute")
    }
}
