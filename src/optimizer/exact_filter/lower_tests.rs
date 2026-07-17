use arrow::datatypes::{DataType, Field, Schema};

use super::lower;
use crate::sql::{BinaryOp, BoundExpr, ExprKind, ScalarValue};

#[test]
fn admits_direct_integer_comparison_and_rejects_cast_or_float() {
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("score", DataType::Float64, true),
    ]);
    assert!(
        lower(
            &comparison(
                BoundExpr::column(0, DataType::Int64, "id"),
                BoundExpr::literal(ScalarValue::Int64(3)),
            ),
            &schema,
        )
        .is_some()
    );

    let cast = cast_column();
    assert!(
        lower(
            &comparison(cast, BoundExpr::literal(ScalarValue::Int64(3))),
            &schema,
        )
        .is_none()
    );
    assert!(
        lower(
            &comparison(
                BoundExpr::column(1, DataType::Float64, "score"),
                BoundExpr::literal(ScalarValue::Float64(3.0)),
            ),
            &schema,
        )
        .is_none()
    );
    let is_null = BoundExpr {
        kind: ExprKind::IsNull {
            expr: Box::new(BoundExpr::column(0, DataType::Int64, "id")),
            negated: false,
        },
        data_type: DataType::Boolean,
        display_name: "id IS NULL".into(),
    };
    assert!(matches!(
        lower(&is_null, &schema),
        Some(crate::datasource::ScanPredicate::IsNull { column: 0 })
    ));
}

#[test]
fn rejects_or_and_partial_conjunction() {
    let schema = Schema::new(vec![Field::new("id", DataType::Int64, true)]);
    let accepted = comparison(
        BoundExpr::column(0, DataType::Int64, "id"),
        BoundExpr::literal(ScalarValue::Int64(3)),
    );
    for op in [BinaryOp::Or, BinaryOp::And] {
        let expression = BoundExpr {
            kind: ExprKind::Binary {
                left: Box::new(accepted.clone()),
                op,
                right: Box::new(comparison(
                    cast_column(),
                    BoundExpr::literal(ScalarValue::Int64(4)),
                )),
            },
            data_type: DataType::Boolean,
            display_name: "predicate".into(),
        };
        assert!(lower(&expression, &schema).is_none());
    }
}

fn cast_column() -> BoundExpr {
    BoundExpr {
        kind: ExprKind::Cast {
            expr: Box::new(BoundExpr::column(0, DataType::Int64, "id")),
        },
        data_type: DataType::Int64,
        display_name: "CAST(id AS BIGINT)".into(),
    }
}

fn comparison(left: BoundExpr, right: BoundExpr) -> BoundExpr {
    BoundExpr {
        kind: ExprKind::Binary {
            left: Box::new(left),
            op: BinaryOp::Eq,
            right: Box::new(right),
        },
        data_type: DataType::Boolean,
        display_name: "comparison".into(),
    }
}
