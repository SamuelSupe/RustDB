use std::cmp::Ordering;

use arrow::{
    array::{
        Array, BooleanArray, Date32Array, Decimal128Array, Float64Array, Int64Array, StringArray,
        TimestampMicrosecondArray, UInt64Array,
    },
    datatypes::{DataType, TimeUnit},
};

use crate::sql::{BinaryOp, BoundExpr, ExprKind, LogicalPlan, ScalarValue, UnaryOp};

pub(super) fn fold_plan(plan: &mut LogicalPlan) {
    match plan {
        LogicalPlan::Empty { .. } => {}
        LogicalPlan::Scan { pushed_filter, .. } => {
            if let Some(predicate) = pushed_filter {
                fold_expr(predicate);
            }
        }
        LogicalPlan::Filter {
            input, predicate, ..
        } => {
            fold_plan(input);
            fold_expr(predicate);
        }
        LogicalPlan::Projection {
            input, expressions, ..
        } => {
            fold_plan(input);
            expressions.iter_mut().for_each(fold_expr);
        }
        LogicalPlan::Scalarize { input, .. } | LogicalPlan::Limit { input, .. } => {
            fold_plan(input);
        }
        LogicalPlan::DependentJoin {
            left,
            right,
            kind,
            guard,
            ..
        } => {
            fold_plan(left);
            fold_plan(right);
            if let crate::sql::DependentJoinKind::In { needle }
            | crate::sql::DependentJoinKind::InFilter { needle, .. } = kind
            {
                fold_expr(needle);
            }
            if let crate::sql::DependentJoinKind::GuardedScalar { expression } = kind {
                fold_expr(expression);
            }
            if let Some(guard) = guard {
                fold_expr(guard);
            }
        }
        LogicalPlan::Aggregate {
            input,
            group_exprs,
            aggregate_exprs,
            ..
        } => {
            fold_plan(input);
            group_exprs.iter_mut().for_each(fold_expr);
            for aggregate in aggregate_exprs {
                if let Some(expr) = &mut aggregate.expr {
                    fold_expr(expr);
                }
            }
        }
        LogicalPlan::Append { inputs, .. } => inputs.iter_mut().for_each(fold_plan),
        LogicalPlan::Window {
            input, expressions, ..
        } => {
            fold_plan(input);
            for expression in expressions {
                if let crate::sql::WindowFunction::Aggregate(aggregate) = &mut expression.function
                    && let Some(argument) = &mut aggregate.expr
                {
                    fold_expr(argument);
                }
                expression.partition_by.iter_mut().for_each(fold_expr);
                for order in &mut expression.order_by {
                    fold_expr(&mut order.expr);
                }
            }
        }
        LogicalPlan::Sort {
            input, expressions, ..
        } => {
            fold_plan(input);
            for expression in expressions {
                fold_expr(&mut expression.expr);
            }
        }
        LogicalPlan::Join {
            left,
            right,
            on,
            residual,
            null_aware,
            ..
        } => {
            fold_plan(left);
            fold_plan(right);
            for (left, right) in on {
                fold_expr(left);
                fold_expr(right);
            }
            if let Some(residual) = residual {
                fold_expr(residual);
            }
            if let Some((left, right)) = null_aware {
                fold_expr(left);
                fold_expr(right);
            }
        }
    }
}

fn fold_expr(expr: &mut BoundExpr) {
    match &mut expr.kind {
        ExprKind::Column(_)
        | ExprKind::OuterRef { .. }
        | ExprKind::DeferredGroup(_)
        | ExprKind::DeferredAggregate(_)
        | ExprKind::Literal(_) => {}
        ExprKind::Binary { left, right, .. } => {
            fold_expr(left);
            fold_expr(right);
        }
        ExprKind::Unary { expr, .. } | ExprKind::IsNull { expr, .. } | ExprKind::Cast { expr } => {
            fold_expr(expr)
        }
        ExprKind::Like { expr, pattern, .. } => {
            fold_expr(expr);
            fold_expr(pattern);
        }
        ExprKind::Case {
            when_then,
            else_expr,
        } => {
            for (when, then) in when_then {
                fold_expr(when);
                fold_expr(then);
            }
            fold_expr(else_expr);
        }
        ExprKind::ScalarFunction { args, .. } => args.iter_mut().for_each(fold_expr),
    }
    if let Some(replacement) = fold_current(expr) {
        *expr = replacement;
    }
}

fn fold_current(expr: &BoundExpr) -> Option<BoundExpr> {
    match &expr.kind {
        ExprKind::Binary { left, op, right } => fold_binary(left, *op, right, &expr.data_type),
        ExprKind::Unary { op, expr } => fold_unary(*op, expr),
        ExprKind::IsNull { expr, negated } => {
            let value = literal(expr)?;
            Some(BoundExpr::literal(ScalarValue::Boolean(
                matches!(value, ScalarValue::Null) ^ *negated,
            )))
        }
        ExprKind::Cast { expr: input } => {
            fold_cast(input, &expr.data_type).or_else(|| fold_evaluated(expr))
        }
        ExprKind::ScalarFunction { args, .. }
            if args
                .iter()
                .all(|arg| matches!(arg.kind, ExprKind::Literal(_))) =>
        {
            fold_evaluated(expr)
        }
        _ => None,
    }
}

fn fold_evaluated(expr: &BoundExpr) -> Option<BoundExpr> {
    let array = crate::execution::evaluate_constant_expression(expr).ok()?;
    let value = if array.is_null(0) {
        ScalarValue::Null
    } else {
        match array.data_type() {
            DataType::Boolean => {
                ScalarValue::Boolean(array.as_any().downcast_ref::<BooleanArray>()?.value(0))
            }
            DataType::Int64 => {
                ScalarValue::Int64(array.as_any().downcast_ref::<Int64Array>()?.value(0))
            }
            DataType::UInt64 => {
                ScalarValue::UInt64(array.as_any().downcast_ref::<UInt64Array>()?.value(0))
            }
            DataType::Float64 => {
                ScalarValue::Float64(array.as_any().downcast_ref::<Float64Array>()?.value(0))
            }
            DataType::Decimal128(precision, scale) => ScalarValue::Decimal128 {
                value: array.as_any().downcast_ref::<Decimal128Array>()?.value(0),
                precision: *precision,
                scale: *scale,
            },
            DataType::Date32 => {
                ScalarValue::Date32(array.as_any().downcast_ref::<Date32Array>()?.value(0))
            }
            DataType::Timestamp(TimeUnit::Microsecond, None) => ScalarValue::TimestampMicrosecond(
                array
                    .as_any()
                    .downcast_ref::<TimestampMicrosecondArray>()?
                    .value(0),
            ),
            DataType::Utf8 => ScalarValue::Utf8(
                array
                    .as_any()
                    .downcast_ref::<StringArray>()?
                    .value(0)
                    .to_owned(),
            ),
            _ => return None,
        }
    };
    let mut output = BoundExpr::literal(value);
    if output.data_type == expr.data_type {
        output.display_name.clone_from(&expr.display_name);
        Some(output)
    } else {
        None
    }
}

fn fold_binary(
    left: &BoundExpr,
    op: BinaryOp,
    right: &BoundExpr,
    output_type: &DataType,
) -> Option<BoundExpr> {
    if let Some(simplified) = simplify_boolean(left, op, right) {
        return Some(simplified);
    }
    let left = literal(left)?;
    let right = literal(right)?;
    let value = match op {
        BinaryOp::Eq => ScalarValue::Boolean(equal(left, right)?),
        BinaryOp::NotEq => ScalarValue::Boolean(!equal(left, right)?),
        BinaryOp::Lt => ScalarValue::Boolean(compare(left, right)? == Ordering::Less),
        BinaryOp::LtEq => ScalarValue::Boolean(compare(left, right)? != Ordering::Greater),
        BinaryOp::Gt => ScalarValue::Boolean(compare(left, right)? == Ordering::Greater),
        BinaryOp::GtEq => ScalarValue::Boolean(compare(left, right)? != Ordering::Less),
        BinaryOp::And => ScalarValue::Boolean(boolean(left)? && boolean(right)?),
        BinaryOp::Or => ScalarValue::Boolean(boolean(left)? || boolean(right)?),
        BinaryOp::Add
        | BinaryOp::Subtract
        | BinaryOp::Multiply
        | BinaryOp::Divide
        | BinaryOp::Modulo => arithmetic(left, op, right, output_type)?,
    };
    (value.data_type() == *output_type).then(|| BoundExpr::literal(value))
}

/// Only identities that still evaluate the non-constant side are safe. The
/// annihilator rules (`FALSE AND x`, `TRUE OR x`) could hide an overflow or a
/// divide-by-zero that execution currently observes, so they are intentional no-ops.
fn simplify_boolean(left: &BoundExpr, op: BinaryOp, right: &BoundExpr) -> Option<BoundExpr> {
    match (op, literal_boolean(left), literal_boolean(right)) {
        (BinaryOp::And, Some(true), _) => Some(right.clone()),
        (BinaryOp::And, _, Some(true)) => Some(left.clone()),
        (BinaryOp::Or, Some(false), _) => Some(right.clone()),
        (BinaryOp::Or, _, Some(false)) => Some(left.clone()),
        _ => None,
    }
}

fn fold_unary(op: UnaryOp, expr: &BoundExpr) -> Option<BoundExpr> {
    let value = literal(expr)?;
    let value = match (op, value) {
        (UnaryOp::Not, ScalarValue::Boolean(value)) => ScalarValue::Boolean(!value),
        (UnaryOp::Negate, ScalarValue::Int64(value)) => ScalarValue::Int64(value.checked_neg()?),
        (UnaryOp::Negate, ScalarValue::Float64(value)) => ScalarValue::Float64(-value),
        (
            UnaryOp::Negate,
            ScalarValue::Decimal128 {
                value,
                precision,
                scale,
            },
        ) => ScalarValue::Decimal128 {
            value: value.checked_neg()?,
            precision: *precision,
            scale: *scale,
        },
        _ => return None,
    };
    Some(BoundExpr::literal(value))
}

fn fold_cast(expr: &BoundExpr, target: &DataType) -> Option<BoundExpr> {
    let value = literal(expr)?;
    let value = match (value, target) {
        (value, target) if value.data_type() == *target => value.clone(),
        (ScalarValue::Int64(value), DataType::Float64) => ScalarValue::Float64(*value as f64),
        (ScalarValue::UInt64(value), DataType::Float64) => ScalarValue::Float64(*value as f64),
        (ScalarValue::Decimal128 { value, scale, .. }, DataType::Float64) => {
            ScalarValue::Float64(*value as f64 / 10_f64.powi(i32::from(*scale)))
        }
        (ScalarValue::Int64(value), DataType::Decimal128(precision, scale)) => {
            decimal_value(*value as i128, 0, *precision, *scale)?
        }
        (ScalarValue::UInt64(value), DataType::Decimal128(precision, scale)) => {
            decimal_value(i128::from(*value), 0, *precision, *scale)?
        }
        (
            ScalarValue::Decimal128 {
                value,
                scale: source_scale,
                ..
            },
            DataType::Decimal128(precision, scale),
        ) => decimal_value(*value, *source_scale, *precision, *scale)?,
        _ => return None,
    };
    Some(BoundExpr::literal(value))
}

fn arithmetic(
    left: &ScalarValue,
    op: BinaryOp,
    right: &ScalarValue,
    output_type: &DataType,
) -> Option<ScalarValue> {
    match (left, right) {
        (ScalarValue::Int64(left), ScalarValue::Int64(right)) => {
            Some(ScalarValue::Int64(integer_arithmetic(*left, op, *right)?))
        }
        (ScalarValue::UInt64(left), ScalarValue::UInt64(right)) => {
            Some(ScalarValue::UInt64(unsigned_arithmetic(*left, op, *right)?))
        }
        (ScalarValue::Float64(left), ScalarValue::Float64(right)) => {
            let result = float_arithmetic(*left, op, *right)?;
            result.is_finite().then_some(ScalarValue::Float64(result))
        }
        (
            ScalarValue::Decimal128 {
                value: left,
                scale: left_scale,
                ..
            },
            ScalarValue::Decimal128 {
                value: right,
                scale: right_scale,
                ..
            },
        ) => decimal_arithmetic(*left, *left_scale, op, *right, *right_scale, output_type),
        _ => None,
    }
}

fn integer_arithmetic(left: i64, op: BinaryOp, right: i64) -> Option<i64> {
    match op {
        BinaryOp::Add => left.checked_add(right),
        BinaryOp::Subtract => left.checked_sub(right),
        BinaryOp::Multiply => left.checked_mul(right),
        BinaryOp::Divide => left.checked_div(right),
        BinaryOp::Modulo => left.checked_rem(right),
        _ => None,
    }
}

fn unsigned_arithmetic(left: u64, op: BinaryOp, right: u64) -> Option<u64> {
    match op {
        BinaryOp::Add => left.checked_add(right),
        BinaryOp::Subtract => left.checked_sub(right),
        BinaryOp::Multiply => left.checked_mul(right),
        BinaryOp::Divide => left.checked_div(right),
        BinaryOp::Modulo => left.checked_rem(right),
        _ => None,
    }
}

fn float_arithmetic(left: f64, op: BinaryOp, right: f64) -> Option<f64> {
    if matches!(op, BinaryOp::Divide | BinaryOp::Modulo) && right == 0.0 {
        return None;
    }
    Some(match op {
        BinaryOp::Add => left + right,
        BinaryOp::Subtract => left - right,
        BinaryOp::Multiply => left * right,
        BinaryOp::Divide => left / right,
        BinaryOp::Modulo => left % right,
        _ => return None,
    })
}

fn decimal_arithmetic(
    left: i128,
    left_scale: i8,
    op: BinaryOp,
    right: i128,
    right_scale: i8,
    output_type: &DataType,
) -> Option<ScalarValue> {
    let DataType::Decimal128(precision, output_scale) = output_type else {
        return None;
    };
    let value =
        match op {
            BinaryOp::Add => rescale(left, left_scale, *output_scale)?.checked_add(rescale(
                right,
                right_scale,
                *output_scale,
            )?)?,
            BinaryOp::Subtract => rescale(left, left_scale, *output_scale)?
                .checked_sub(rescale(right, right_scale, *output_scale)?)?,
            BinaryOp::Multiply => {
                let value = left.checked_mul(right)?;
                rescale(value, left_scale.checked_add(right_scale)?, *output_scale)?
            }
            BinaryOp::Divide => {
                if right == 0 {
                    return None;
                }
                let exponent =
                    i16::from(*output_scale) + i16::from(right_scale) - i16::from(left_scale);
                scale_by_power(left, exponent)?.checked_div(right)?
            }
            BinaryOp::Modulo => {
                if right == 0 {
                    return None;
                }
                rescale(left, left_scale, *output_scale)?.checked_rem(rescale(
                    right,
                    right_scale,
                    *output_scale,
                )?)?
            }
            _ => return None,
        };
    fits_precision(value, *precision).then_some(ScalarValue::Decimal128 {
        value,
        precision: *precision,
        scale: *output_scale,
    })
}

fn decimal_value(value: i128, source_scale: i8, precision: u8, scale: i8) -> Option<ScalarValue> {
    let value = rescale(value, source_scale, scale)?;
    fits_precision(value, precision).then_some(ScalarValue::Decimal128 {
        value,
        precision,
        scale,
    })
}

fn rescale(value: i128, source: i8, target: i8) -> Option<i128> {
    scale_by_power(value, i16::from(target) - i16::from(source))
}

fn scale_by_power(value: i128, exponent: i16) -> Option<i128> {
    let factor = power_of_ten(exponent.unsigned_abs().into())?;
    if exponent >= 0 {
        value.checked_mul(factor)
    } else {
        value.checked_div(factor)
    }
}

fn power_of_ten(exponent: u32) -> Option<i128> {
    (0..exponent).try_fold(1_i128, |value, _| value.checked_mul(10))
}

fn fits_precision(value: i128, precision: u8) -> bool {
    let mut value = value.unsigned_abs();
    let mut digits = 1_u8;
    while value >= 10 {
        value /= 10;
        digits = digits.saturating_add(1);
    }
    digits <= precision
}

fn literal(expr: &BoundExpr) -> Option<&ScalarValue> {
    let ExprKind::Literal(value) = &expr.kind else {
        return None;
    };
    Some(value)
}

fn literal_boolean(expr: &BoundExpr) -> Option<bool> {
    boolean(literal(expr)?)
}

fn boolean(value: &ScalarValue) -> Option<bool> {
    let ScalarValue::Boolean(value) = value else {
        return None;
    };
    Some(*value)
}

fn equal(left: &ScalarValue, right: &ScalarValue) -> Option<bool> {
    Some(match (left, right) {
        (ScalarValue::Boolean(left), ScalarValue::Boolean(right)) => left == right,
        (ScalarValue::Int64(left), ScalarValue::Int64(right)) => left == right,
        (ScalarValue::UInt64(left), ScalarValue::UInt64(right)) => left == right,
        (ScalarValue::Float64(left), ScalarValue::Float64(right)) => left == right,
        (
            ScalarValue::Decimal128 { value: left, .. },
            ScalarValue::Decimal128 { value: right, .. },
        ) => left == right,
        (ScalarValue::Date32(left), ScalarValue::Date32(right)) => left == right,
        (ScalarValue::TimestampMicrosecond(left), ScalarValue::TimestampMicrosecond(right)) => {
            left == right
        }
        (ScalarValue::DayInterval(left), ScalarValue::DayInterval(right)) => left == right,
        (ScalarValue::MonthInterval(left), ScalarValue::MonthInterval(right)) => left == right,
        (ScalarValue::Utf8(left), ScalarValue::Utf8(right)) => left == right,
        _ => return None,
    })
}

fn compare(left: &ScalarValue, right: &ScalarValue) -> Option<Ordering> {
    match (left, right) {
        (ScalarValue::Boolean(left), ScalarValue::Boolean(right)) => left.partial_cmp(right),
        (ScalarValue::Int64(left), ScalarValue::Int64(right)) => left.partial_cmp(right),
        (ScalarValue::UInt64(left), ScalarValue::UInt64(right)) => left.partial_cmp(right),
        (ScalarValue::Float64(left), ScalarValue::Float64(right)) => left.partial_cmp(right),
        (
            ScalarValue::Decimal128 { value: left, .. },
            ScalarValue::Decimal128 { value: right, .. },
        ) => left.partial_cmp(right),
        (ScalarValue::Date32(left), ScalarValue::Date32(right)) => left.partial_cmp(right),
        (ScalarValue::TimestampMicrosecond(left), ScalarValue::TimestampMicrosecond(right)) => {
            left.partial_cmp(right)
        }
        (ScalarValue::DayInterval(left), ScalarValue::DayInterval(right)) => {
            left.partial_cmp(right)
        }
        (ScalarValue::MonthInterval(left), ScalarValue::MonthInterval(right)) => {
            left.partial_cmp(right)
        }
        (ScalarValue::Utf8(left), ScalarValue::Utf8(right)) => left.partial_cmp(right),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::DataType;

    use super::fold_expr;
    use crate::sql::{BoundExpr, ExprKind, ScalarFunction, ScalarValue};

    #[test]
    fn folds_constant_scalar_functions() {
        let mut expression = BoundExpr {
            kind: ExprKind::ScalarFunction {
                function: ScalarFunction::Lower,
                args: vec![BoundExpr::literal(ScalarValue::Utf8("AbC".into()))],
            },
            data_type: DataType::Utf8,
            display_name: "lower('AbC')".into(),
        };

        fold_expr(&mut expression);

        assert_eq!(
            expression.kind,
            ExprKind::Literal(ScalarValue::Utf8("abc".into()))
        );
    }
}
