use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema};

use super::super::push_required_columns;
use crate::sql::{
    AggregateExpr, AggregateFunction, BinaryOp, BoundExpr, ExprKind, LogicalPlan, PlanSchema,
    field_is_materialized,
};

#[path = "tests/support.rs"]
mod support;
use support::*;

#[test]
fn narrows_group_and_sum_input_and_preserves_schema_properties() {
    let mut plan = aggregate(scan(), vec![column(3)], vec![sum(column(1), false)]);

    push_required_columns(&mut plan);

    let LogicalPlan::Aggregate {
        input,
        group_exprs,
        aggregate_exprs,
        ..
    } = &plan
    else {
        panic!("expected aggregate")
    };
    assert_column(&group_exprs[0], 1);
    assert_column(aggregate_exprs[0].expr.as_ref().unwrap(), 0);
    let LogicalPlan::Projection {
        input,
        expressions,
        schema,
    } = input.as_ref()
    else {
        panic!("expected narrow aggregate input")
    };
    assert_columns(expressions, &[1, 3]);
    assert_eq!(schema.arrow().fields().len(), 2);
    assert_eq!(schema.qualifier(0), Some("t"));
    assert!(!schema.is_visible(1));
    assert!(
        schema
            .arrow()
            .fields()
            .iter()
            .all(|field| field_is_materialized(field))
    );
    assert_scan_projection(input, &[1, 3]);
}

#[test]
fn keeps_filter_columns_below_the_terminal_aggregate_projection() {
    let source = scan();
    let schema = source.schema().clone();
    let filtered = LogicalPlan::Filter {
        input: Box::new(source),
        predicate: binary(column(0), BinaryOp::Gt, int(10), DataType::Boolean),
        schema,
    };
    let mut plan = aggregate(filtered, Vec::new(), vec![sum(column(3), false)]);

    push_required_columns(&mut plan);

    let LogicalPlan::Aggregate {
        input,
        aggregate_exprs,
        ..
    } = &plan
    else {
        panic!("expected aggregate")
    };
    assert_column(aggregate_exprs[0].expr.as_ref().unwrap(), 0);
    let LogicalPlan::Projection {
        input, expressions, ..
    } = input.as_ref()
    else {
        panic!("expected terminal projection")
    };
    assert_columns(expressions, &[3]);
    let LogicalPlan::Filter {
        input, predicate, ..
    } = input.as_ref()
    else {
        panic!("filter must remain below the projection")
    };
    assert_eq!(referenced(predicate), vec![0]);
    assert_scan_projection(input, &[0, 3]);
}

#[test]
fn count_star_keeps_a_true_zero_column_scan() {
    let count = AggregateExpr {
        function: AggregateFunction::Count,
        expr: None,
        distinct: false,
        data_type: DataType::Int64,
        display_name: "count(*)".into(),
    };
    let mut plan = aggregate(scan(), Vec::new(), vec![count]);

    push_required_columns(&mut plan);

    let LogicalPlan::Aggregate { input, .. } = &plan else {
        panic!("expected aggregate")
    };
    let LogicalPlan::Scan {
        projection, schema, ..
    } = input.as_ref()
    else {
        panic!("bare COUNT(*) should not need a terminal projection")
    };
    assert_eq!(projection.as_deref(), Some(&[][..]));
    assert!(
        schema
            .arrow()
            .fields()
            .iter()
            .all(|field| !field_is_materialized(field))
    );
}

#[test]
fn reuses_direct_projection_and_remaps_duplicate_complex_references() {
    let source = scan();
    let projection_schema = PlanSchema::new_with_visibility(
        Arc::new(Schema::new(vec![
            Field::new("alias_a", DataType::Int64, false),
            Field::new("alias_b", DataType::Int64, false),
            Field::new("alias_c", DataType::Int64, false),
            Field::new("alias_d", DataType::Int64, false),
        ])),
        vec![
            Some("a".into()),
            Some("b".into()),
            Some("c".into()),
            Some("d".into()),
        ],
        vec![true, true, false, false],
    );
    let direct = LogicalPlan::Projection {
        input: Box::new(source),
        expressions: vec![column(2), column(1), column(2), column(3)],
        schema: projection_schema,
    };
    let comparison = binary(column(0), BinaryOp::Gt, int(0), DataType::Boolean);
    let group = BoundExpr {
        kind: ExprKind::Case {
            when_then: vec![(comparison, column(2))],
            else_expr: Box::new(column(0)),
        },
        data_type: DataType::Int64,
        display_name: "duplicate CASE".into(),
    };
    let cast = BoundExpr {
        kind: ExprKind::Cast {
            expr: Box::new(column(3)),
        },
        data_type: DataType::Int64,
        display_name: "CAST(alias_d AS BIGINT)".into(),
    };
    let mut plan = aggregate(direct, vec![group], vec![sum(cast, true)]);

    push_required_columns(&mut plan);

    let LogicalPlan::Aggregate {
        input,
        group_exprs,
        aggregate_exprs,
        ..
    } = &plan
    else {
        panic!("expected aggregate")
    };
    assert_eq!(referenced(&group_exprs[0]), vec![0, 0, 0]);
    assert_eq!(
        referenced(aggregate_exprs[0].expr.as_ref().unwrap()),
        vec![1]
    );
    assert!(aggregate_exprs[0].distinct);
    let LogicalPlan::Projection {
        input,
        expressions,
        schema,
    } = input.as_ref()
    else {
        panic!("expected reused projection")
    };
    assert_columns(expressions, &[2, 3]);
    assert_eq!(schema.arrow().field(0).name(), "alias_a");
    assert_eq!(schema.arrow().field(1).name(), "alias_d");
    assert_eq!(schema.qualifier(0), Some("a"));
    assert!(!schema.is_visible(1));
    assert_scan_projection(input, &[2, 3]);
}

#[test]
fn complex_breaker_input_falls_back_without_inserting_a_projection() {
    let source = scan();
    let schema = source.schema().clone();
    let limited = LogicalPlan::Limit {
        input: Box::new(source),
        offset: 0,
        limit: Some(10),
        schema,
    };
    let mut plan = aggregate(limited, vec![column(2)], vec![sum(column(3), false)]);

    push_required_columns(&mut plan);

    let LogicalPlan::Aggregate { input, .. } = &plan else {
        panic!("expected aggregate")
    };
    assert!(matches!(input.as_ref(), LogicalPlan::Limit { .. }));
}
