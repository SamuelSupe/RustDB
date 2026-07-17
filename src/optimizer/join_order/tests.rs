use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;

use super::choose_build_sides;
use crate::{
    Error, Result,
    datasource::{ScanRequest, TableProvider, TableStatistics},
    runtime::{QueryContext, RecordBatchStream},
    sql::{BinaryOp, BoundExpr, ExprKind, JoinType, LogicalPlan, PlanSchema, ScalarValue},
};

#[test]
fn selective_left_input_becomes_the_right_build_side() {
    let left = scan("large", Some(1_000));
    let left_schema = left.schema().clone();
    let left = LogicalPlan::Filter {
        input: Box::new(left),
        predicate: equality(1),
        schema: left_schema,
    };
    let right = scan("medium", Some(150));
    let mut plan = inner_join(left, right, None);

    choose_build_sides(&mut plan).unwrap();

    let LogicalPlan::Projection {
        input, expressions, ..
    } = &plan
    else {
        panic!("selective left side should be restored through a projection")
    };
    let LogicalPlan::Join { left, right, .. } = input.as_ref() else {
        panic!("projection should wrap the swapped join")
    };
    assert_eq!(source_name(left), "medium");
    assert_eq!(source_name(right), "large");
    assert_eq!(column_indices(expressions), vec![1, 0]);
    assert_eq!(
        plan.schema()
            .arrow()
            .fields()
            .iter()
            .map(|field| field.name().as_str())
            .collect::<Vec<_>>(),
        vec!["large_id", "medium_id"]
    );
}

#[test]
fn output_width_is_considered_before_equal_row_counts() {
    let narrow = scan_with_types("narrow", Some(100), &[DataType::Int64]);
    let wide = scan_with_types(
        "wide",
        Some(100),
        &[DataType::Int64, DataType::Utf8, DataType::Utf8],
    );
    let mut plan = inner_join(narrow, wide, None);

    choose_build_sides(&mut plan).unwrap();

    let LogicalPlan::Projection { input, .. } = &plan else {
        panic!("narrow input should become the build side")
    };
    let LogicalPlan::Join { left, right, .. } = input.as_ref() else {
        panic!("projection should wrap the swapped join")
    };
    assert_eq!(source_name(left), "wide");
    assert_eq!(source_name(right), "narrow");
}

#[test]
fn unknown_inputs_keep_their_original_order() {
    let mut unknown = inner_join(scan("unknown", None), scan("known", Some(1)), None);
    choose_build_sides(&mut unknown).unwrap();
    assert_join_order(&unknown, "unknown", "known");
}

#[test]
fn infallible_residual_is_remapped_when_the_build_side_swaps() {
    let left = scan_with_types("small", Some(1), &[DataType::Int64, DataType::Int64]);
    let right = scan("large", Some(1_000));
    let residual = BoundExpr {
        kind: ExprKind::Binary {
            left: Box::new(BoundExpr::column(1, DataType::Int64, "small_payload")),
            op: BinaryOp::Gt,
            right: Box::new(BoundExpr::column(2, DataType::Int64, "large_id")),
        },
        data_type: DataType::Boolean,
        display_name: "small_payload > large_id".to_owned(),
    };
    let mut plan = inner_join(left, right, Some(residual));

    choose_build_sides(&mut plan).unwrap();

    let LogicalPlan::Projection {
        input, expressions, ..
    } = &plan
    else {
        panic!("swapped residual join should restore its original output")
    };
    assert_eq!(column_indices(expressions), vec![1, 2, 0]);
    let LogicalPlan::Join {
        left,
        right,
        residual: Some(residual),
        ..
    } = input.as_ref()
    else {
        panic!("projection should wrap the swapped residual join")
    };
    assert_eq!(source_name(left), "large");
    assert_eq!(source_name(right), "small");
    let ExprKind::Binary {
        left,
        op: BinaryOp::Gt,
        right,
    } = &residual.kind
    else {
        panic!("residual comparison shape changed")
    };
    assert!(matches!(&left.kind, ExprKind::Column(2)));
    assert!(matches!(&right.kind, ExprKind::Column(0)));
}

#[test]
fn fallible_residual_keeps_its_original_order() {
    let division = BoundExpr {
        kind: ExprKind::Binary {
            left: Box::new(BoundExpr::column(0, DataType::Int64, "small_id")),
            op: BinaryOp::Divide,
            right: Box::new(BoundExpr::literal(ScalarValue::Int64(0))),
        },
        data_type: DataType::Int64,
        display_name: "small_id / 0".to_owned(),
    };
    let residual = BoundExpr {
        kind: ExprKind::Binary {
            left: Box::new(division),
            op: BinaryOp::Gt,
            right: Box::new(BoundExpr::literal(ScalarValue::Int64(0))),
        },
        data_type: DataType::Boolean,
        display_name: "small_id / 0 > 0".to_owned(),
    };

    let mut plan = inner_join(
        scan("small", Some(1)),
        scan("large", Some(1_000)),
        Some(residual),
    );
    choose_build_sides(&mut plan).unwrap();
    assert_join_order(&plan, "small", "large");
}

fn inner_join(left: LogicalPlan, right: LogicalPlan, residual: Option<BoundExpr>) -> LogicalPlan {
    let schema = PlanSchema::join(left.schema(), right.schema());
    LogicalPlan::Join {
        left: Box::new(left),
        right: Box::new(right),
        on: vec![(
            BoundExpr::column(0, DataType::Int64, "left_id"),
            BoundExpr::column(0, DataType::Int64, "right_id"),
        )],
        null_equal_keys: false,
        residual,
        null_aware: None,
        join_type: JoinType::Inner,
        schema,
    }
}

fn equality(value: i64) -> BoundExpr {
    BoundExpr {
        kind: ExprKind::Binary {
            left: Box::new(BoundExpr::column(0, DataType::Int64, "id")),
            op: BinaryOp::Eq,
            right: Box::new(BoundExpr::literal(ScalarValue::Int64(value))),
        },
        data_type: DataType::Boolean,
        display_name: format!("id = {value}"),
    }
}

fn scan(name: &str, rows: Option<u64>) -> LogicalPlan {
    scan_with_types(name, rows, &[DataType::Int64])
}

fn scan_with_types(name: &str, rows: Option<u64>, types: &[DataType]) -> LogicalPlan {
    let fields = types
        .iter()
        .enumerate()
        .map(|(index, data_type)| {
            let suffix = if index == 0 {
                "id".to_owned()
            } else {
                format!("payload_{index}")
            };
            Field::new(format!("{name}_{suffix}"), data_type.clone(), true)
        })
        .collect::<Vec<_>>();
    let schema = Arc::new(Schema::new(fields));
    LogicalPlan::Scan {
        table_name: name.to_owned(),
        provider: Arc::new(StatsTable(Arc::clone(&schema))),
        statistics: TableStatistics {
            row_count: rows,
            total_byte_size: rows.map(|_| 1),
            file_count: 1,
        },
        projection: None,
        pushed_filter: None,
        exact_filter: None,
        limit: None,
        schema: PlanSchema::new(
            Arc::clone(&schema),
            vec![Some(name.to_owned()); schema.fields().len()],
        ),
    }
}

fn source_name(plan: &LogicalPlan) -> &str {
    match plan {
        LogicalPlan::Scan { table_name, .. } => table_name,
        LogicalPlan::Filter { input, .. } => source_name(input),
        _ => panic!("unexpected source plan"),
    }
}

fn column_indices(expressions: &[BoundExpr]) -> Vec<usize> {
    expressions
        .iter()
        .map(|expression| match expression.kind {
            ExprKind::Column(index) => index,
            _ => panic!("restore expression should be a column"),
        })
        .collect()
}

fn assert_join_order(plan: &LogicalPlan, left_name: &str, right_name: &str) {
    let LogicalPlan::Join { left, right, .. } = plan else {
        panic!("join should not have been swapped")
    };
    assert_eq!(source_name(left), left_name);
    assert_eq!(source_name(right), right_name);
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
