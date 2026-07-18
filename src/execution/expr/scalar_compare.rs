use std::sync::Arc;

use arrow::{
    array::{ArrayRef, Scalar},
    compute::kernels::cmp,
    record_batch::RecordBatch,
};

use crate::{
    Result,
    sql::{BinaryOp, BoundExpr, ExprKind, ScalarValue},
};

use super::{evaluate, literal_array};

pub(super) fn try_evaluate(
    left: &BoundExpr,
    op: BinaryOp,
    right: &BoundExpr,
    batch: &RecordBatch,
) -> Option<Result<ArrayRef>> {
    let (array_expr, op, literal) = normalized(left, op, right)?;
    Some(evaluate_normalized(array_expr, op, literal, batch))
}

pub(super) fn array_operand<'a>(
    left: &'a BoundExpr,
    op: BinaryOp,
    right: &'a BoundExpr,
) -> Option<&'a BoundExpr> {
    normalized(left, op, right).map(|(array, _, _)| array)
}

fn normalized<'a>(
    left: &'a BoundExpr,
    op: BinaryOp,
    right: &'a BoundExpr,
) -> Option<(&'a BoundExpr, BinaryOp, &'a ScalarValue)> {
    if !is_comparison(op) {
        return None;
    }
    match (&left.kind, &right.kind) {
        (ExprKind::Literal(_), ExprKind::Literal(_)) => None,
        (_, ExprKind::Literal(literal)) if eligible(left, literal) => Some((left, op, literal)),
        (ExprKind::Literal(literal), _) if eligible(right, literal) => {
            Some((right, reverse(op), literal))
        }
        _ => None,
    }
}

fn evaluate_normalized(
    array_expr: &BoundExpr,
    op: BinaryOp,
    literal: &ScalarValue,
    batch: &RecordBatch,
) -> Result<ArrayRef> {
    let array = evaluate(array_expr, batch)?;
    let scalar = Scalar::new(literal_array(literal, 1)?);
    let result = match op {
        BinaryOp::Eq => cmp::eq(&array, &scalar),
        BinaryOp::NotEq => cmp::neq(&array, &scalar),
        BinaryOp::Lt => cmp::lt(&array, &scalar),
        BinaryOp::LtEq => cmp::lt_eq(&array, &scalar),
        BinaryOp::Gt => cmp::gt(&array, &scalar),
        BinaryOp::GtEq => cmp::gt_eq(&array, &scalar),
        _ => unreachable!("normalized accepts only comparisons"),
    }?;
    Ok(Arc::new(result))
}

fn eligible(array: &BoundExpr, literal: &ScalarValue) -> bool {
    array.data_type == literal.data_type()
        && matches!(
            literal,
            ScalarValue::Boolean(_)
                | ScalarValue::Int64(_)
                | ScalarValue::UInt64(_)
                | ScalarValue::Float64(_)
                | ScalarValue::Decimal128 { .. }
                | ScalarValue::Date32(_)
                | ScalarValue::TimestampMicrosecond(_)
                | ScalarValue::Timestamp { .. }
                | ScalarValue::Time { .. }
                | ScalarValue::Uuid(_)
        )
}

fn is_comparison(op: BinaryOp) -> bool {
    matches!(
        op,
        BinaryOp::Eq
            | BinaryOp::NotEq
            | BinaryOp::Lt
            | BinaryOp::LtEq
            | BinaryOp::Gt
            | BinaryOp::GtEq
    )
}

fn reverse(op: BinaryOp) -> BinaryOp {
    match op {
        BinaryOp::Eq | BinaryOp::NotEq => op,
        BinaryOp::Lt => BinaryOp::Gt,
        BinaryOp::LtEq => BinaryOp::GtEq,
        BinaryOp::Gt => BinaryOp::Lt,
        BinaryOp::GtEq => BinaryOp::LtEq,
        _ => unreachable!("reverse accepts only comparisons"),
    }
}

#[cfg(test)]
#[path = "scalar_compare/tests.rs"]
mod tests;
