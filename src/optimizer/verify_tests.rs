use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema};

use super::executable;
use crate::{
    Error,
    sql::{BoundExpr, JoinType, LogicalPlan, PlanSchema},
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
