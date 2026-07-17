use std::sync::Arc;

use arrow::{
    array::{Array, BooleanArray, Decimal128Array, Int64Array, StringArray},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};

use super::{array_operand, try_evaluate};
use crate::sql::{BinaryOp, BoundExpr, ExprKind, ScalarValue};

#[test]
fn fixed_width_comparisons_preserve_values_and_nulls() {
    let batch = int_batch(vec![Some(1), None, Some(3)]);
    for (op, expected) in [
        (BinaryOp::Eq, vec![Some(false), None, Some(false)]),
        (BinaryOp::NotEq, vec![Some(true), None, Some(true)]),
        (BinaryOp::Lt, vec![Some(true), None, Some(false)]),
        (BinaryOp::LtEq, vec![Some(true), None, Some(false)]),
        (BinaryOp::Gt, vec![Some(false), None, Some(true)]),
        (BinaryOp::GtEq, vec![Some(false), None, Some(true)]),
    ] {
        let left = BoundExpr::column(0, DataType::Int64, "value");
        let right = BoundExpr::literal(ScalarValue::Int64(2));
        let result = try_evaluate(&left, op, &right, &batch)
            .expect("fixed-width comparison should use the scalar path")
            .unwrap();
        assert_eq!(booleans(&result), expected, "operator {op}");
    }
}

#[test]
fn literal_on_left_reverses_ordering_operator() {
    let batch = int_batch(vec![Some(1), None, Some(3)]);
    let literal = BoundExpr::literal(ScalarValue::Int64(2));
    let column = BoundExpr::column(0, DataType::Int64, "value");

    let result = try_evaluate(&literal, BinaryOp::Lt, &column, &batch)
        .expect("reverse comparison should use the scalar path")
        .unwrap();

    assert_eq!(booleans(&result), vec![Some(false), None, Some(true)]);
    assert!(std::ptr::eq(
        array_operand(&literal, BinaryOp::Lt, &column).unwrap(),
        &column,
    ));
}

#[test]
fn decimal_requires_an_exact_precision_and_scale_match() {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Decimal128(10, 2),
        true,
    )]));
    let values = Decimal128Array::from(vec![Some(100_i128), None, Some(300)])
        .with_precision_and_scale(10, 2)
        .unwrap();
    let batch = RecordBatch::try_new(schema, vec![Arc::new(values)]).unwrap();
    let column = BoundExpr::column(0, DataType::Decimal128(10, 2), "value");
    let exact = BoundExpr::literal(ScalarValue::Decimal128 {
        value: 200,
        precision: 10,
        scale: 2,
    });
    let mismatched = BoundExpr::literal(ScalarValue::Decimal128 {
        value: 200,
        precision: 8,
        scale: 2,
    });

    assert!(try_evaluate(&column, BinaryOp::Lt, &exact, &batch).is_some());
    assert!(try_evaluate(&column, BinaryOp::Lt, &mismatched, &batch).is_none());

    let fallback = comparison(column, BinaryOp::Lt, mismatched);
    let result = super::super::evaluate(&fallback, &batch).unwrap();
    assert_eq!(booleans(&result), vec![Some(true), None, Some(false)]);
}

#[test]
fn variable_width_literals_keep_the_existing_path() {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Utf8,
        false,
    )]));
    let batch =
        RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(vec!["a", "b"]))]).unwrap();
    let column = BoundExpr::column(0, DataType::Utf8, "value");
    let literal = BoundExpr::literal(ScalarValue::Utf8("b".into()));

    assert!(try_evaluate(&column, BinaryOp::Lt, &literal, &batch).is_none());
    let result =
        super::super::evaluate(&comparison(column, BinaryOp::Lt, literal), &batch).unwrap();
    assert_eq!(booleans(&result), vec![Some(true), Some(false)]);
}

#[test]
fn comparison_workspace_does_not_recharge_input_columns() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("left", DataType::Int64, false),
        Field::new("right", DataType::Int64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1_i64; 8192])),
            Arc::new(Int64Array::from(vec![2_i64; 8192])),
        ],
    )
    .unwrap();
    let scalar = comparison(
        BoundExpr::column(0, DataType::Int64, "left"),
        BinaryOp::Lt,
        BoundExpr::literal(ScalarValue::Int64(2)),
    );
    let arrays = comparison(
        BoundExpr::column(0, DataType::Int64, "left"),
        BinaryOp::Lt,
        BoundExpr::column(1, DataType::Int64, "right"),
    );

    let scalar_bytes = super::super::projection_workspace_bytes(&[scalar], &batch);
    let array_bytes = super::super::projection_workspace_bytes(&[arrays], &batch);
    assert_eq!(array_bytes, scalar_bytes);
}

fn int_batch(values: Vec<Option<i64>>) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int64,
        true,
    )]));
    RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(values))]).unwrap()
}

fn comparison(left: BoundExpr, op: BinaryOp, right: BoundExpr) -> BoundExpr {
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

fn booleans(array: &arrow::array::ArrayRef) -> Vec<Option<bool>> {
    array
        .as_any()
        .downcast_ref::<BooleanArray>()
        .unwrap()
        .iter()
        .collect()
}
