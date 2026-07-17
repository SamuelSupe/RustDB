use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema};

use super::apply;
use crate::sql::{BinaryOp, BoundExpr, ExprKind, LogicalPlan, PlanSchema, ScalarValue};

#[test]
fn merges_adjacent_infallible_filters_in_inner_to_outer_order() {
    let schema = schema();
    let plan = filter(
        filter(
            filter(empty(schema.clone()), comparison("inner", 1), &schema),
            comparison("middle", 2),
            &schema,
        ),
        comparison("outer", 3),
        &schema,
    );

    let LogicalPlan::Filter {
        input, predicate, ..
    } = apply(plan)
    else {
        panic!("expected one coalesced filter")
    };
    assert!(matches!(*input, LogicalPlan::Empty { .. }));
    assert_eq!(leaf_names(&predicate), ["inner", "middle", "outer"]);
}

#[test]
fn keeps_fallible_filter_boundaries_separate() {
    let schema = schema();
    let plan = filter(
        filter(
            filter(empty(schema.clone()), comparison("inner", 1), &schema),
            fallible("fallible"),
            &schema,
        ),
        comparison("outer", 3),
        &schema,
    );

    let plan = apply(plan);
    assert_eq!(filter_names(&plan), ["outer", "fallible", "inner"]);
}

#[test]
fn recurses_through_non_filter_nodes() {
    let schema = schema();
    let filters = filter(
        filter(empty(schema.clone()), comparison("inner", 1), &schema),
        comparison("outer", 2),
        &schema,
    );
    let plan = LogicalPlan::Projection {
        input: Box::new(filters),
        expressions: vec![BoundExpr::column(0, DataType::Int64, "value")],
        schema: schema.clone(),
    };

    let LogicalPlan::Projection { input, .. } = apply(plan) else {
        panic!("expected projection")
    };
    let LogicalPlan::Filter {
        input, predicate, ..
    } = *input
    else {
        panic!("expected one filter below projection")
    };
    assert!(matches!(*input, LogicalPlan::Empty { .. }));
    assert_eq!(leaf_names(&predicate), ["inner", "outer"]);
}

fn schema() -> PlanSchema {
    PlanSchema::unqualified(Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int64,
        true,
    )])))
}

fn empty(schema: PlanSchema) -> LogicalPlan {
    LogicalPlan::Empty {
        produce_one_row: false,
        schema,
    }
}

fn filter(input: LogicalPlan, predicate: BoundExpr, schema: &PlanSchema) -> LogicalPlan {
    LogicalPlan::Filter {
        input: Box::new(input),
        predicate,
        schema: schema.clone(),
    }
}

fn comparison(name: &str, value: i64) -> BoundExpr {
    binary(
        name,
        BoundExpr::column(0, DataType::Int64, "value"),
        BinaryOp::Eq,
        BoundExpr::literal(ScalarValue::Int64(value)),
        DataType::Boolean,
    )
}

fn fallible(name: &str) -> BoundExpr {
    let divide = binary(
        "divide",
        BoundExpr::column(0, DataType::Int64, "value"),
        BinaryOp::Divide,
        BoundExpr::literal(ScalarValue::Int64(0)),
        DataType::Int64,
    );
    binary(
        name,
        divide,
        BinaryOp::Eq,
        BoundExpr::literal(ScalarValue::Int64(1)),
        DataType::Boolean,
    )
}

fn binary(
    name: &str,
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
        display_name: name.into(),
    }
}

fn leaf_names(predicate: &BoundExpr) -> Vec<&str> {
    let mut names = Vec::new();
    collect_leaf_names(predicate, &mut names);
    names
}

fn collect_leaf_names<'a>(predicate: &'a BoundExpr, names: &mut Vec<&'a str>) {
    match &predicate.kind {
        ExprKind::Binary {
            left,
            op: BinaryOp::And,
            right,
        } => {
            collect_leaf_names(left, names);
            collect_leaf_names(right, names);
        }
        _ => names.push(predicate.display_name.as_str()),
    }
}

fn filter_names(plan: &LogicalPlan) -> Vec<&str> {
    let mut names = Vec::new();
    let mut plan = plan;
    while let LogicalPlan::Filter {
        input, predicate, ..
    } = plan
    {
        names.push(predicate.display_name.as_str());
        plan = input;
    }
    names
}
