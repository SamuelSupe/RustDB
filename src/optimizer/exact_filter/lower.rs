use arrow::datatypes::{DataType, Schema, TimeUnit};

use crate::datasource::{ComparisonOp, PredicateValue, ScanPredicate};
use crate::sql::{BinaryOp, BoundExpr, ExprKind, ScalarValue};

pub(super) fn lower(expression: &BoundExpr, schema: &Schema) -> Option<ScanPredicate> {
    match &expression.kind {
        ExprKind::Binary {
            left,
            op: BinaryOp::And,
            right,
        } => Some(ScanPredicate::And(vec![
            lower(left, schema)?,
            lower(right, schema)?,
        ])),
        ExprKind::Binary { left, op, right } => lower_comparison(left, *op, right, schema),
        ExprKind::IsNull { expr, negated } => {
            let column = direct_column(expr, schema)?;
            exact_type(schema.field(column).data_type()).then_some(())?;
            Some(if *negated {
                ScanPredicate::IsNotNull { column }
            } else {
                ScanPredicate::IsNull { column }
            })
        }
        _ => None,
    }
}

fn lower_comparison(
    left: &BoundExpr,
    op: BinaryOp,
    right: &BoundExpr,
    schema: &Schema,
) -> Option<ScanPredicate> {
    let op = comparison(op)?;
    let (column, op, value) =
        if let (Some(column), Some(value)) = (direct_column(left, schema), direct_literal(right)) {
            (column, op, value)
        } else {
            let (value, column) = (direct_literal(left)?, direct_column(right, schema)?);
            (column, reverse(op), value)
        };
    value_matches_column(column, &value, schema).then_some(ScanPredicate::Comparison {
        column,
        op,
        value,
    })
}

fn direct_column(expression: &BoundExpr, schema: &Schema) -> Option<usize> {
    let ExprKind::Column(column) = expression.kind else {
        return None;
    };
    let field = schema.fields().get(column)?;
    (field.data_type() == &expression.data_type).then_some(column)
}

fn direct_literal(expression: &BoundExpr) -> Option<PredicateValue> {
    let ExprKind::Literal(value) = &expression.kind else {
        return None;
    };
    match value {
        ScalarValue::Boolean(value) => Some(PredicateValue::Boolean(*value)),
        ScalarValue::Int64(value) => Some(PredicateValue::Int64(*value)),
        ScalarValue::UInt64(value) => Some(PredicateValue::UInt64(*value)),
        ScalarValue::Decimal128 {
            value,
            precision,
            scale,
        } => Some(PredicateValue::Decimal128 {
            value: *value,
            precision: *precision,
            scale: *scale,
        }),
        ScalarValue::Date32(value) => Some(PredicateValue::Date32(*value)),
        ScalarValue::TimestampMicrosecond(value) => Some(PredicateValue::TimestampMicros(*value)),
        ScalarValue::Null
        | ScalarValue::Float64(_)
        | ScalarValue::DayInterval(_)
        | ScalarValue::MonthInterval(_)
        | ScalarValue::Utf8(_)
        | ScalarValue::Binary(_) => None,
    }
}

fn value_matches_column(column: usize, value: &PredicateValue, schema: &Schema) -> bool {
    let Some(field) = schema.fields().get(column) else {
        return false;
    };
    matches!(
        (field.data_type(), value),
        (DataType::Boolean, PredicateValue::Boolean(_))
            | (DataType::Int64, PredicateValue::Int64(_))
            | (DataType::UInt64, PredicateValue::UInt64(_))
            | (DataType::Date32, PredicateValue::Date32(_))
            | (
                DataType::Timestamp(TimeUnit::Microsecond, None),
                PredicateValue::TimestampMicros(_)
            )
    ) || matches!(
        (field.data_type(), value),
        (
            DataType::Decimal128(precision, scale),
            PredicateValue::Decimal128 {
                precision: value_precision,
                scale: value_scale,
                ..
            }
        ) if precision == value_precision && scale == value_scale
    )
}

fn exact_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Boolean
            | DataType::Int64
            | DataType::UInt64
            | DataType::Date32
            | DataType::Timestamp(TimeUnit::Microsecond, None)
            | DataType::Decimal128(_, _)
    )
}

fn comparison(op: BinaryOp) -> Option<ComparisonOp> {
    match op {
        BinaryOp::Eq => Some(ComparisonOp::Eq),
        BinaryOp::NotEq => Some(ComparisonOp::NotEq),
        BinaryOp::Lt => Some(ComparisonOp::Lt),
        BinaryOp::LtEq => Some(ComparisonOp::LtEq),
        BinaryOp::Gt => Some(ComparisonOp::Gt),
        BinaryOp::GtEq => Some(ComparisonOp::GtEq),
        _ => None,
    }
}

fn reverse(op: ComparisonOp) -> ComparisonOp {
    match op {
        ComparisonOp::Eq => ComparisonOp::Eq,
        ComparisonOp::NotEq => ComparisonOp::NotEq,
        ComparisonOp::Lt => ComparisonOp::Gt,
        ComparisonOp::LtEq => ComparisonOp::GtEq,
        ComparisonOp::Gt => ComparisonOp::Lt,
        ComparisonOp::GtEq => ComparisonOp::LtEq,
    }
}

#[cfg(test)]
#[path = "lower_tests.rs"]
mod tests;
