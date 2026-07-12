use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema};

use crate::sql::{
    BinaryOp, BoundExpr, ExprKind, JoinType, LogicalPlan, PlanSchema, ScalarFunction, ScalarValue,
};

use super::apply;

#[test]
fn only_structurally_infallible_predicates_can_move() {
    let column = BoundExpr::column(0, DataType::Int64, "value");
    let literal = BoundExpr::literal(ScalarValue::Int64(1));
    let comparison = binary(
        column.clone(),
        BinaryOp::Eq,
        literal.clone(),
        DataType::Boolean,
    );
    assert!(comparison.is_structurally_infallible());

    let divide = binary(column.clone(), BinaryOp::Divide, literal, DataType::Int64);
    assert!(!divide.is_structurally_infallible());
    assert!(
        !BoundExpr {
            kind: ExprKind::Cast {
                expr: Box::new(column.clone()),
            },
            data_type: DataType::Utf8,
            display_name: "CAST(value AS VARCHAR)".into(),
        }
        .is_structurally_infallible()
    );
    assert!(
        !BoundExpr {
            kind: ExprKind::ScalarFunction {
                function: ScalarFunction::Length,
                args: vec![column.clone()],
            },
            data_type: DataType::Int64,
            display_name: "length(value)".into(),
        }
        .is_structurally_infallible()
    );
    assert!(
        !BoundExpr {
            kind: ExprKind::Case {
                when_then: vec![(comparison, column.clone())],
                else_expr: Box::new(column),
            },
            data_type: DataType::Int64,
            display_name: "CASE".into(),
        }
        .is_structurally_infallible()
    );
}

#[test]
fn moves_an_infallible_left_predicate_below_an_infallible_inner_join() {
    let plan = relocate_over_inner(comparison(0, 7), safe_keys(), None);
    let LogicalPlan::Join { left, right, .. } = plan else {
        panic!("expected the filter to move below the join");
    };
    assert_filter_columns(*left, &[0]);
    assert!(matches!(*right, LogicalPlan::Empty { .. }));
}

#[test]
fn moves_and_rebases_an_infallible_right_predicate_below_an_inner_join() {
    let plan = relocate_over_inner(comparison(1, 7), safe_keys(), None);
    let LogicalPlan::Join { left, right, .. } = plan else {
        panic!("expected the filter to move below the join");
    };
    assert!(matches!(*left, LogicalPlan::Empty { .. }));
    assert_filter_columns(*right, &[0]);
}

#[test]
fn keeps_a_cross_side_predicate_above_an_inner_join() {
    let predicate = binary(column(0), BinaryOp::Eq, column(1), DataType::Boolean);
    assert_filter_above_join(relocate_over_inner(predicate, safe_keys(), None));
}

#[test]
fn keeps_a_predicate_above_a_potentially_failing_inner_join_key() {
    let key = BoundExpr {
        kind: ExprKind::Cast {
            expr: Box::new(column(0)),
        },
        data_type: DataType::Int64,
        display_name: "CAST(left_key AS BIGINT)".into(),
    };
    assert_filter_above_join(relocate_over_inner(
        comparison(0, 7),
        vec![(key, column(0))],
        None,
    ));
}

#[test]
fn keeps_a_predicate_above_a_potentially_failing_inner_join_residual() {
    let division = binary(
        column(0),
        BinaryOp::Divide,
        BoundExpr::literal(ScalarValue::Int64(0)),
        DataType::Int64,
    );
    let residual = binary(
        division,
        BinaryOp::Eq,
        BoundExpr::literal(ScalarValue::Int64(1)),
        DataType::Boolean,
    );
    assert_filter_above_join(relocate_over_inner(
        comparison(0, 7),
        safe_keys(),
        Some(residual),
    ));
}

fn relocate_over_inner(
    predicate: BoundExpr,
    on: Vec<(BoundExpr, BoundExpr)>,
    residual: Option<BoundExpr>,
) -> LogicalPlan {
    let left_schema = one_column_schema("left_key");
    let right_schema = one_column_schema("right_key");
    let schema = PlanSchema::join(&left_schema, &right_schema);
    let join = LogicalPlan::Join {
        left: Box::new(empty(left_schema)),
        right: Box::new(empty(right_schema)),
        on,
        residual,
        null_aware: None,
        join_type: JoinType::Inner,
        schema: schema.clone(),
    };
    apply(LogicalPlan::Filter {
        input: Box::new(join),
        predicate,
        schema,
    })
}

fn safe_keys() -> Vec<(BoundExpr, BoundExpr)> {
    vec![(column(0), column(0))]
}

fn empty(schema: PlanSchema) -> LogicalPlan {
    LogicalPlan::Empty {
        produce_one_row: false,
        schema,
    }
}

fn one_column_schema(name: &str) -> PlanSchema {
    PlanSchema::unqualified(Arc::new(Schema::new(vec![Field::new(
        name,
        DataType::Int64,
        false,
    )])))
}

fn comparison(index: usize, value: i64) -> BoundExpr {
    binary(
        column(index),
        BinaryOp::Eq,
        BoundExpr::literal(ScalarValue::Int64(value)),
        DataType::Boolean,
    )
}

fn column(index: usize) -> BoundExpr {
    BoundExpr::column(index, DataType::Int64, format!("column_{index}"))
}

fn assert_filter_columns(plan: LogicalPlan, expected: &[usize]) {
    let LogicalPlan::Filter { predicate, .. } = plan else {
        panic!("expected a filter below the join");
    };
    let mut columns = Vec::new();
    predicate.referenced_columns(&mut columns);
    assert_eq!(columns, expected);
}

fn assert_filter_above_join(plan: LogicalPlan) {
    let LogicalPlan::Filter { input, .. } = plan else {
        panic!("expected the filter to remain above the join");
    };
    assert!(matches!(*input, LogicalPlan::Join { .. }));
}

fn binary(left: BoundExpr, op: BinaryOp, right: BoundExpr, data_type: DataType) -> BoundExpr {
    BoundExpr {
        display_name: format!("{} {op} {}", left.display_name, right.display_name),
        kind: ExprKind::Binary {
            left: Box::new(left),
            op,
            right: Box::new(right),
        },
        data_type,
    }
}
