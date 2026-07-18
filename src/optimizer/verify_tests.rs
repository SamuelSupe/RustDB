use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema};

use super::executable;
use crate::{
    Error,
    sql::{BoundExpr, JoinType, LogicalPlan, PlanSchema, WindowExpr, WindowFrame, WindowFunction},
};

#[test]
fn rejects_a_physical_join_with_different_hash_key_types() {
    let left_schema = PlanSchema::unqualified(Arc::new(Schema::new(vec![Field::new(
        "amount",
        DataType::Decimal128(35, 2),
        false,
    )])));
    let right_schema = PlanSchema::unqualified(Arc::new(Schema::new(vec![Field::new(
        "amount",
        DataType::Decimal128(38, 6),
        false,
    )])));
    let schema = PlanSchema::join(&left_schema, &right_schema);
    let plan = LogicalPlan::Join {
        left: Box::new(LogicalPlan::Empty {
            produce_one_row: false,
            schema: left_schema,
        }),
        right: Box::new(LogicalPlan::Empty {
            produce_one_row: false,
            schema: right_schema,
        }),
        on: vec![(
            BoundExpr::column(0, DataType::Decimal128(35, 2), "left.amount"),
            BoundExpr::column(0, DataType::Decimal128(38, 6), "right.amount"),
        )],
        null_equal_keys: false,
        residual: None,
        null_aware: None,
        join_type: JoinType::Inner,
        schema,
    };

    let error = executable(&plan).unwrap_err();
    assert!(matches!(&error, Error::Internal(_)));
    assert!(
        error
            .to_string()
            .contains("physical join hash key types differ")
    );
}

#[test]
fn rejects_outer_refs_in_navigation_window_arguments() {
    let input_schema = PlanSchema::unqualified(Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int64,
        false,
    )])));
    let outer = BoundExpr::outer_ref(1, 0, DataType::Int64, "outer.value");
    let expression = WindowExpr {
        function: WindowFunction::Lead {
            expr: BoundExpr::column(0, DataType::Int64, "value"),
            offset: 1,
            default: outer,
        },
        partition_by: Vec::new(),
        order_by: Vec::new(),
        frame: WindowFrame::whole_partition(),
        data_type: DataType::Int64,
        display_name: "lead(value, 1, outer.value)".into(),
    };
    let output_schema = PlanSchema::unqualified(Arc::new(Schema::new(vec![
        Field::new("value", DataType::Int64, false),
        Field::new("lead", DataType::Int64, true),
    ])));
    let plan = LogicalPlan::Window {
        input: Box::new(LogicalPlan::Empty {
            produce_one_row: true,
            schema: input_schema,
        }),
        expressions: vec![expression],
        schema: output_schema,
    };

    let error = executable(&plan).unwrap_err();
    assert!(matches!(error, Error::Internal(_)));
    assert!(error.to_string().contains("OuterRef remained"));
}
