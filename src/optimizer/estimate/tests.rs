use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;

use super::{
    EQUALITY_SELECTIVITY, RANGE_SELECTIVITY, SELECTIVITY_SCALE, estimate, multiply, selectivity,
};
use crate::{
    Error, Result,
    datasource::{ScanRequest, TableProvider, TableStatistics},
    runtime::{QueryContext, RecordBatchStream},
    sql::{BinaryOp, BoundExpr, ExprKind, LogicalPlan, PlanSchema, ScalarValue, UnaryOp},
};

#[test]
fn estimates_boolean_predicates_deterministically() {
    let equality = comparison(BinaryOp::Eq, 7);
    let range = comparison(BinaryOp::Lt, 9);

    assert_eq!(selectivity(&boolean(true)), SELECTIVITY_SCALE);
    assert_eq!(selectivity(&boolean(false)), 0);
    assert_eq!(selectivity(&BoundExpr::literal(ScalarValue::Null)), 0);
    assert_eq!(selectivity(&equality), EQUALITY_SELECTIVITY);
    assert_eq!(selectivity(&range), RANGE_SELECTIVITY);
    assert_eq!(
        selectivity(&binary(equality.clone(), BinaryOp::And, range)),
        multiply(EQUALITY_SELECTIVITY, RANGE_SELECTIVITY)
    );
    assert_eq!(
        selectivity(&binary(equality.clone(), BinaryOp::Or, equality.clone())),
        190_000
    );
    assert_eq!(
        selectivity(&BoundExpr {
            kind: ExprKind::Unary {
                op: UnaryOp::Not,
                expr: Box::new(equality),
            },
            data_type: DataType::Boolean,
            display_name: "not equality".to_owned(),
        }),
        SELECTIVITY_SCALE - EQUALITY_SELECTIVITY
    );
}

#[test]
fn not_keeps_unknown_predicates_conservative() {
    let column = BoundExpr::column(0, DataType::Boolean, "flag");
    let predicate = BoundExpr {
        kind: ExprKind::Unary {
            op: UnaryOp::Not,
            expr: Box::new(column),
        },
        data_type: DataType::Boolean,
        display_name: "not flag".to_owned(),
    };
    assert_eq!(selectivity(&predicate), SELECTIVITY_SCALE);

    let partially_unknown = binary(
        comparison(BinaryOp::Eq, 1),
        BinaryOp::And,
        BoundExpr::column(1, DataType::Boolean, "flag"),
    );
    let negated = BoundExpr {
        kind: ExprKind::Unary {
            op: UnaryOp::Not,
            expr: Box::new(partially_unknown),
        },
        data_type: DataType::Boolean,
        display_name: "not partially known predicate".to_owned(),
    };
    assert_eq!(selectivity(&negated), SELECTIVITY_SCALE);
}

#[test]
fn comparisons_with_null_never_pass_a_filter() {
    for op in [
        BinaryOp::Eq,
        BinaryOp::NotEq,
        BinaryOp::Lt,
        BinaryOp::LtEq,
        BinaryOp::Gt,
        BinaryOp::GtEq,
    ] {
        let comparison = binary(
            BoundExpr::column(0, DataType::Int64, "value"),
            op,
            BoundExpr::literal(ScalarValue::Null),
        );
        assert_eq!(selectivity(&comparison), 0, "{op}");

        let negated = BoundExpr {
            kind: ExprKind::Unary {
                op: UnaryOp::Not,
                expr: Box::new(comparison),
            },
            data_type: DataType::Boolean,
            display_name: format!("not null comparison {op}"),
        };
        assert_eq!(selectivity(&negated), 0, "NOT ({op} NULL)");
    }
}

#[test]
fn filter_projection_and_limit_update_rows_and_output_bytes() {
    let input = scan("items", Some(1_001), &[DataType::Int64, DataType::Utf8]);
    let input_estimate = estimate(&input);
    let filter_schema = input.schema().clone();
    let filtered = LogicalPlan::Filter {
        input: Box::new(input),
        predicate: comparison(BinaryOp::Eq, 1),
        schema: filter_schema,
    };
    let filtered_estimate = estimate(&filtered);
    assert_eq!(filtered_estimate.rows, Some(101));
    assert!(filtered_estimate.output_bytes < input_estimate.output_bytes);

    let projected_schema = one_column_schema("items", "items_0", DataType::Int64);
    let projected = LogicalPlan::Projection {
        input: Box::new(filtered),
        expressions: vec![BoundExpr::column(0, DataType::Int64, "items_0")],
        schema: projected_schema,
    };
    let projected_estimate = estimate(&projected);
    assert_eq!(projected_estimate.rows, Some(101));
    assert!(projected_estimate.output_bytes < filtered_estimate.output_bytes);

    let limit_schema = projected.schema().clone();
    let limited = LogicalPlan::Limit {
        input: Box::new(projected),
        offset: 10,
        limit: Some(20),
        schema: limit_schema,
    };
    let limited_estimate = estimate(&limited);
    assert_eq!(limited_estimate.rows, Some(20));
    assert!(limited_estimate.output_bytes < projected_estimate.output_bytes);
}

#[test]
fn append_requires_every_child_cardinality_to_be_known() {
    for inputs in [
        vec![
            scan("known", Some(5), &[DataType::Int64]),
            scan("unknown", None, &[DataType::Int64]),
        ],
        vec![
            scan("unknown", None, &[DataType::Int64]),
            scan("known", Some(5), &[DataType::Int64]),
        ],
    ] {
        let schema = inputs[0].schema().clone();
        let estimate = estimate(&LogicalPlan::Append { inputs, schema });
        assert_eq!(estimate.rows, None);
        assert_eq!(estimate.output_bytes, None);
    }
}

#[test]
fn append_saturates_known_cardinalities_instead_of_overflowing() {
    let inputs = vec![
        scan("large", Some(u64::MAX), &[DataType::Int64]),
        scan("one", Some(1), &[DataType::Int64]),
    ];
    let schema = inputs[0].schema().clone();
    let estimate = estimate(&LogicalPlan::Append { inputs, schema });
    assert_eq!(estimate.rows, Some(u64::MAX));
    assert_eq!(estimate.output_bytes, Some(u64::MAX));
}

#[test]
fn only_scalar_aggregate_has_a_cardinality_without_column_statistics() {
    let input = scan("items", Some(100), &[DataType::Int64]);
    let scalar = LogicalPlan::Aggregate {
        input: Box::new(input.clone()),
        group_exprs: Vec::new(),
        aggregate_exprs: Vec::new(),
        schema: PlanSchema::empty(),
    };
    assert_eq!(estimate(&scalar).rows, Some(1));

    let grouped = LogicalPlan::Aggregate {
        input: Box::new(input),
        group_exprs: vec![BoundExpr::column(0, DataType::Int64, "items_0")],
        aggregate_exprs: Vec::new(),
        schema: one_column_schema("items", "items_0", DataType::Int64),
    };
    assert_eq!(estimate(&grouped), Default::default());

    let empty_grouped = LogicalPlan::Aggregate {
        input: Box::new(scan("empty", Some(0), &[DataType::Int64])),
        group_exprs: vec![BoundExpr::column(0, DataType::Int64, "empty_0")],
        aggregate_exprs: Vec::new(),
        schema: one_column_schema("empty", "empty_0", DataType::Int64),
    };
    assert_eq!(estimate(&empty_grouped).rows, Some(0));
}

fn boolean(value: bool) -> BoundExpr {
    BoundExpr::literal(ScalarValue::Boolean(value))
}

fn comparison(op: BinaryOp, value: i64) -> BoundExpr {
    binary(
        BoundExpr::column(0, DataType::Int64, "value"),
        op,
        BoundExpr::literal(ScalarValue::Int64(value)),
    )
}

fn binary(left: BoundExpr, op: BinaryOp, right: BoundExpr) -> BoundExpr {
    BoundExpr {
        display_name: format!("{} {op} {}", left.display_name, right.display_name),
        kind: ExprKind::Binary {
            left: Box::new(left),
            op,
            right: Box::new(right),
        },
        data_type: DataType::Boolean,
    }
}

fn scan(name: &str, rows: Option<u64>, types: &[DataType]) -> LogicalPlan {
    let fields = types
        .iter()
        .enumerate()
        .map(|(index, data_type)| Field::new(format!("{name}_{index}"), data_type.clone(), true))
        .collect::<Vec<_>>();
    let schema = Arc::new(Schema::new(fields));
    let plan_schema = PlanSchema::new(
        Arc::clone(&schema),
        vec![Some(name.to_owned()); schema.fields().len()],
    );
    LogicalPlan::Scan {
        table_name: name.to_owned(),
        provider: Arc::new(StatsTable(Arc::clone(&schema))),
        statistics: TableStatistics {
            row_count: rows,
            total_byte_size: Some(1),
            file_count: 1,
        },
        projection: None,
        pushed_filter: None,
        exact_filter: None,
        limit: None,
        schema: plan_schema,
    }
}

fn one_column_schema(qualifier: &str, name: &str, data_type: DataType) -> PlanSchema {
    PlanSchema::new(
        Arc::new(Schema::new(vec![Field::new(name, data_type, true)])),
        vec![Some(qualifier.to_owned())],
    )
}

struct StatsTable(SchemaRef);

#[async_trait]
impl TableProvider for StatsTable {
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
        Err(Error::Internal(
            "statistics-only table was scanned".to_owned(),
        ))
    }
}
