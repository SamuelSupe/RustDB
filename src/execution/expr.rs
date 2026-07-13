use std::{cmp::Ordering, sync::Arc};

use arrow::{
    array::{
        Array, ArrayRef, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Float64Array,
        Int64Array, IntervalDayTimeArray, IntervalYearMonthArray, NullArray, StringArray,
        UInt64Array,
        types::{IntervalDayTimeType, IntervalYearMonthType},
    },
    compute::{
        filter_record_batch,
        kernels::{boolean, cmp, numeric},
    },
    record_batch::RecordBatch,
};

use crate::runtime::estimate_array_bytes;
use crate::sql::{BinaryOp, BoundExpr, ExprKind, ScalarValue, UnaryOp};
use crate::{Error, Result};

mod short_circuit;

pub(crate) fn evaluate(expr: &BoundExpr, batch: &RecordBatch) -> Result<ArrayRef> {
    match &expr.kind {
        ExprKind::Column(index) => batch.columns().get(*index).cloned().ok_or_else(|| {
            Error::Internal(format!(
                "column index {index} is outside the {}-column input schema while evaluating '{}'",
                batch.num_columns(),
                expr.display_name
            ))
        }),
        ExprKind::OuterRef { .. } => Err(Error::Internal(
            "OuterRef reached physical expression execution after decorrelation".into(),
        )),
        ExprKind::DeferredGroup(_) | ExprKind::DeferredAggregate(_) => Err(Error::Internal(
            "deferred aggregate result reached physical expression execution".into(),
        )),
        ExprKind::Literal(value) => literal_array(value, batch.num_rows()),
        ExprKind::Cast { expr: input } => {
            let input = evaluate(input, batch)?;
            super::functions::cast_array(&input, &expr.data_type)
        }
        ExprKind::ScalarFunction { function, args }
            if matches!(
                function,
                crate::sql::ScalarFunction::Coalesce | crate::sql::ScalarFunction::NullIf
            ) =>
        {
            short_circuit::null_function(*function, args, expr, batch)
        }
        ExprKind::ScalarFunction { function, args } => {
            let args = args
                .iter()
                .map(|arg| evaluate(arg, batch))
                .collect::<Result<Vec<_>>>()?;
            super::functions::evaluate(*function, &args, &expr.data_type)
        }
        ExprKind::Binary { left, op, right } if matches!(op, BinaryOp::And | BinaryOp::Or) => {
            short_circuit::boolean(*op, left, right, batch)
        }
        ExprKind::Binary { left, op, right } => {
            let left = evaluate(left, batch)?;
            let right = evaluate(right, batch)?;
            evaluate_binary(*op, left, right)
        }
        ExprKind::Unary { op, expr } => {
            let array = evaluate(expr, batch)?;
            match op {
                UnaryOp::Not => Ok(Arc::new(boolean::not(as_boolean(&array)?)?)),
                UnaryOp::Negate => Ok(numeric::neg(array.as_ref())?),
            }
        }
        ExprKind::IsNull { expr, negated } => {
            let array = evaluate(expr, batch)?;
            let result = if *negated {
                boolean::is_not_null(array.as_ref())
            } else {
                boolean::is_null(array.as_ref())
            }?;
            Ok(Arc::new(result))
        }
        ExprKind::Like {
            expr,
            pattern,
            negated,
            escape,
        } => {
            let expr = evaluate(expr, batch)?;
            let pattern = evaluate(pattern, batch)?;
            Ok(Arc::new(evaluate_like(&expr, &pattern, *negated, *escape)?))
        }
        ExprKind::Case {
            when_then,
            else_expr,
        } => short_circuit::case(when_then, else_expr, expr, batch),
    }
}

pub(crate) fn project(
    expressions: &[BoundExpr],
    schema: arrow::datatypes::SchemaRef,
    batch: &RecordBatch,
) -> Result<RecordBatch> {
    let columns = expressions
        .iter()
        .map(|expr| evaluate(expr, batch))
        .collect::<Result<Vec<_>>>()?;
    Ok(RecordBatch::try_new(schema, columns)?)
}

pub(crate) fn filter(predicate: &BoundExpr, batch: &RecordBatch) -> Result<RecordBatch> {
    let predicate = evaluate(predicate, batch)?;
    Ok(filter_record_batch(batch, as_boolean(&predicate)?)?)
}

pub(crate) fn projection_workspace_bytes(expressions: &[BoundExpr], batch: &RecordBatch) -> usize {
    let mut retained = 0usize;
    let mut peak = 0usize;
    for expression in expressions {
        let estimate = expression_memory(expression, batch);
        peak = peak.max(retained.saturating_add(estimate.peak));
        retained = retained.saturating_add(estimate.output);
    }
    peak.max(retained)
        .saturating_add(expressions.len().saturating_mul(256))
        .max(1)
}

pub(crate) fn filter_workspace_bytes(predicate: &BoundExpr, batch: &RecordBatch) -> usize {
    expression_memory(predicate, batch)
        .peak
        .saturating_add(batch.get_array_memory_size())
        .max(1)
}

#[derive(Clone, Copy)]
struct ExpressionMemory {
    output: usize,
    peak: usize,
}

fn expression_memory(expression: &BoundExpr, batch: &RecordBatch) -> ExpressionMemory {
    let output = expression_output_bytes(expression, batch);
    match &expression.kind {
        ExprKind::Column(_)
        | ExprKind::OuterRef { .. }
        | ExprKind::DeferredGroup(_)
        | ExprKind::DeferredAggregate(_)
        | ExprKind::Literal(_) => ExpressionMemory {
            output,
            peak: output,
        },
        ExprKind::Unary { expr, .. } | ExprKind::IsNull { expr, .. } | ExprKind::Cast { expr } => {
            let input = expression_memory(expr, batch);
            ExpressionMemory {
                output,
                peak: input.peak.max(input.output.saturating_add(output)),
            }
        }
        ExprKind::ScalarFunction { function, args } => {
            let mut retained = 0usize;
            let mut peak = output;
            for arg in args {
                let estimate = expression_memory(arg, batch);
                peak = peak.max(retained.saturating_add(estimate.peak));
                retained = retained.saturating_add(estimate.output);
            }
            let mut estimate = ExpressionMemory {
                output,
                peak: peak.max(retained.saturating_add(output)),
            };
            if matches!(
                function,
                crate::sql::ScalarFunction::Coalesce | crate::sql::ScalarFunction::NullIf
            ) {
                estimate.peak = estimate.peak.saturating_add(masked_input_bytes(batch));
            }
            estimate
        }
        ExprKind::Binary { left, op, right } => {
            let mut estimate = binary_expression_memory(left, right, output, batch);
            if matches!(op, BinaryOp::And | BinaryOp::Or) && !right.is_structurally_infallible() {
                estimate.peak = estimate.peak.saturating_add(masked_input_bytes(batch));
            }
            estimate
        }
        ExprKind::Like {
            expr: left,
            pattern: right,
            ..
        } => binary_expression_memory(left, right, output, batch),
        ExprKind::Case {
            when_then,
            else_expr,
        } => {
            // CASE retains the current output while each condition/result pair
            // and the next zip output are evaluated. Summing branch peaks is
            // conservative for variable-width branches without making every
            // projection pay for unrelated expressions.
            let mut current = expression_memory(else_expr, batch);
            let mut peak = current.peak;
            for (condition, result) in when_then.iter().rev() {
                let condition = expression_memory(condition, batch);
                let result = expression_memory(result, batch);
                peak = peak
                    .max(current.output.saturating_add(condition.peak))
                    .max(
                        current
                            .output
                            .saturating_add(condition.output)
                            .saturating_add(result.peak),
                    )
                    .max(
                        current
                            .output
                            .saturating_add(condition.output)
                            .saturating_add(result.output)
                            .saturating_add(output),
                    );
                current = ExpressionMemory { output, peak };
            }
            ExpressionMemory {
                output,
                peak: peak.saturating_add(masked_input_bytes(batch)),
            }
        }
    }
}

fn masked_input_bytes(batch: &RecordBatch) -> usize {
    batch
        .get_array_memory_size()
        .saturating_add(batch.num_rows().saturating_mul(std::mem::size_of::<u64>()))
        .saturating_add(512)
}

fn binary_expression_memory(
    left: &BoundExpr,
    right: &BoundExpr,
    output: usize,
    batch: &RecordBatch,
) -> ExpressionMemory {
    let left = expression_memory(left, batch);
    let right = expression_memory(right, batch);
    ExpressionMemory {
        output,
        peak: left.peak.max(left.output.saturating_add(right.peak)).max(
            left.output
                .saturating_add(right.output)
                .saturating_add(output),
        ),
    }
}

fn expression_output_bytes(expression: &BoundExpr, batch: &RecordBatch) -> usize {
    match &expression.kind {
        ExprKind::Column(index) => batch
            .columns()
            .get(*index)
            .map_or_else(
                || estimate_array_bytes(&expression.data_type, batch.num_rows()),
                |array| array.get_array_memory_size(),
            )
            .max(1),
        ExprKind::Literal(ScalarValue::Utf8(value)) => batch
            .num_rows()
            .saturating_mul(value.len().saturating_add(4))
            .saturating_add(batch.num_rows().div_ceil(8))
            .saturating_add(512)
            .max(1),
        ExprKind::Literal(ScalarValue::Binary(value)) => batch
            .num_rows()
            .saturating_mul(value.len().saturating_add(4))
            .saturating_add(batch.num_rows().div_ceil(8))
            .saturating_add(512)
            .max(1),
        ExprKind::Cast { expr }
            if matches!(
                &expression.data_type,
                arrow::datatypes::DataType::Utf8 | arrow::datatypes::DataType::LargeUtf8
            ) =>
        {
            estimate_array_bytes(&expression.data_type, batch.num_rows())
                .max(expression_output_bytes(expr, batch))
        }
        _ => estimate_array_bytes(&expression.data_type, batch.num_rows()),
    }
}

fn evaluate_binary(op: BinaryOp, left: ArrayRef, right: ArrayRef) -> Result<ArrayRef> {
    if matches!(op, BinaryOp::Divide | BinaryOp::Modulo) {
        ensure_non_zero(&right)?;
    }
    if left.data_type() != right.data_type()
        && matches!(
            left.data_type(),
            arrow::datatypes::DataType::Decimal128(_, _)
        )
        && matches!(
            right.data_type(),
            arrow::datatypes::DataType::Decimal128(_, _)
        )
        && matches!(
            op,
            BinaryOp::Eq
                | BinaryOp::NotEq
                | BinaryOp::Lt
                | BinaryOp::LtEq
                | BinaryOp::Gt
                | BinaryOp::GtEq
        )
    {
        return Ok(Arc::new(compare_decimals(op, &left, &right)?));
    }
    let result: ArrayRef = match op {
        BinaryOp::Eq => Arc::new(cmp::eq(&left, &right)?),
        BinaryOp::NotEq => Arc::new(cmp::neq(&left, &right)?),
        BinaryOp::Lt => Arc::new(cmp::lt(&left, &right)?),
        BinaryOp::LtEq => Arc::new(cmp::lt_eq(&left, &right)?),
        BinaryOp::Gt => Arc::new(cmp::gt(&left, &right)?),
        BinaryOp::GtEq => Arc::new(cmp::gt_eq(&left, &right)?),
        BinaryOp::And => Arc::new(boolean::and_kleene(
            as_boolean(&left)?,
            as_boolean(&right)?,
        )?),
        BinaryOp::Or => Arc::new(boolean::or_kleene(as_boolean(&left)?, as_boolean(&right)?)?),
        BinaryOp::Add => numeric::add(&left, &right)?,
        BinaryOp::Subtract => numeric::sub(&left, &right)?,
        BinaryOp::Multiply => numeric::mul(&left, &right)?,
        BinaryOp::Divide => numeric::div(&left, &right)?,
        BinaryOp::Modulo => numeric::rem(&left, &right)?,
    };
    Ok(result)
}

fn compare_decimals(op: BinaryOp, left: &ArrayRef, right: &ArrayRef) -> Result<BooleanArray> {
    let left = left
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .ok_or_else(|| {
            Error::Internal("left DECIMAL comparison operand is not Decimal128".into())
        })?;
    let right = right
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .ok_or_else(|| {
            Error::Internal("right DECIMAL comparison operand is not Decimal128".into())
        })?;
    if left.len() != right.len() {
        return Err(Error::Internal(
            "DECIMAL comparison operands have different lengths".into(),
        ));
    }
    let left_scale = decimal_scale(left.data_type())?;
    let right_scale = decimal_scale(right.data_type())?;
    Ok(BooleanArray::from_iter((0..left.len()).map(|row| {
        if left.is_null(row) || right.is_null(row) {
            return None;
        }
        let ordering =
            compare_decimal_values(left.value(row), left_scale, right.value(row), right_scale);
        Some(match op {
            BinaryOp::Eq => ordering.is_eq(),
            BinaryOp::NotEq => !ordering.is_eq(),
            BinaryOp::Lt => ordering.is_lt(),
            BinaryOp::LtEq => !ordering.is_gt(),
            BinaryOp::Gt => ordering.is_gt(),
            BinaryOp::GtEq => !ordering.is_lt(),
            _ => unreachable!("caller only passes comparison operators"),
        })
    })))
}

fn decimal_scale(data_type: &arrow::datatypes::DataType) -> Result<i8> {
    match data_type {
        arrow::datatypes::DataType::Decimal128(_, scale) => Ok(*scale),
        other => Err(Error::Internal(format!(
            "expected Decimal128 comparison operand, got {other}"
        ))),
    }
}

fn compare_decimal_values(left: i128, left_scale: i8, right: i128, right_scale: i8) -> Ordering {
    match (left.is_negative(), right.is_negative()) {
        (true, false) => Ordering::Less,
        (false, true) => Ordering::Greater,
        (false, false) => compare_decimal_magnitudes(
            left.unsigned_abs(),
            left_scale,
            right.unsigned_abs(),
            right_scale,
        ),
        (true, true) => compare_decimal_magnitudes(
            left.unsigned_abs(),
            left_scale,
            right.unsigned_abs(),
            right_scale,
        )
        .reverse(),
    }
}

fn compare_decimal_magnitudes(
    left: u128,
    left_scale: i8,
    right: u128,
    right_scale: i8,
) -> Ordering {
    if left == 0 || right == 0 {
        return left.cmp(&right);
    }
    let left_digits = left.to_string();
    let right_digits = right.to_string();
    let left_exponent = left_digits.len() as i16 - i16::from(left_scale);
    let right_exponent = right_digits.len() as i16 - i16::from(right_scale);
    match left_exponent.cmp(&right_exponent) {
        Ordering::Equal => {
            let width = left_digits.len().max(right_digits.len());
            let left = left_digits.bytes().chain(std::iter::repeat(b'0'));
            let right = right_digits.bytes().chain(std::iter::repeat(b'0'));
            left.zip(right)
                .take(width)
                .find_map(|(left, right)| (left != right).then(|| left.cmp(&right)))
                .unwrap_or(Ordering::Equal)
        }
        ordering => ordering,
    }
}

fn ensure_non_zero(array: &ArrayRef) -> Result<()> {
    let contains_zero = match array.data_type() {
        arrow::datatypes::DataType::Int64 => array
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("Int64 data type")
            .iter()
            .flatten()
            .any(|value| value == 0),
        arrow::datatypes::DataType::UInt64 => array
            .as_any()
            .downcast_ref::<UInt64Array>()
            .expect("UInt64 data type")
            .iter()
            .flatten()
            .any(|value| value == 0),
        arrow::datatypes::DataType::Float64 => array
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("Float64 data type")
            .iter()
            .flatten()
            .any(|value| value == 0.0),
        arrow::datatypes::DataType::Decimal128(_, _) => array
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .expect("Decimal128 data type")
            .iter()
            .flatten()
            .any(|value| value == 0),
        _ => false,
    };
    if contains_zero {
        Err(Error::Execution("division by zero".into()))
    } else {
        Ok(())
    }
}

fn literal_array(value: &ScalarValue, len: usize) -> Result<ArrayRef> {
    let array: ArrayRef =
        match value {
            ScalarValue::Null => Arc::new(NullArray::new(len)),
            ScalarValue::Boolean(value) => Arc::new(BooleanArray::from(vec![Some(*value); len])),
            ScalarValue::Int64(value) => Arc::new(Int64Array::from(vec![Some(*value); len])),
            ScalarValue::UInt64(value) => Arc::new(UInt64Array::from(vec![Some(*value); len])),
            ScalarValue::Float64(value) => Arc::new(Float64Array::from(vec![Some(*value); len])),
            ScalarValue::Decimal128 {
                value,
                precision,
                scale,
            } => Arc::new(
                Decimal128Array::from(vec![Some(*value); len])
                    .with_precision_and_scale(*precision, *scale)?,
            ),
            ScalarValue::Date32(value) => Arc::new(Date32Array::from(vec![Some(*value); len])),
            ScalarValue::TimestampMicrosecond(value) => {
                super::functions::timestamp_literal(*value, len)
            }
            ScalarValue::DayInterval(days) => Arc::new(IntervalDayTimeArray::from(vec![
                Some(
                    IntervalDayTimeType::make_value(*days, 0)
                );
                len
            ])),
            ScalarValue::MonthInterval(months) => Arc::new(IntervalYearMonthArray::from(vec![
                Some(
                    IntervalYearMonthType::make_value(0, *months)
                );
                len
            ])),
            ScalarValue::Utf8(value) => Arc::new(StringArray::from_iter_values(
                std::iter::repeat_n(value.as_str(), len),
            )),
            ScalarValue::Binary(value) => Arc::new(BinaryArray::from_iter_values(
                std::iter::repeat_n(value.as_slice(), len),
            )),
        };
    Ok(array)
}

fn evaluate_like(
    values: &ArrayRef,
    patterns: &ArrayRef,
    negated: bool,
    escape: Option<char>,
) -> Result<BooleanArray> {
    let values = values
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| Error::Internal(format!("LIKE input has type {}", values.data_type())))?;
    let patterns = patterns
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| {
            Error::Internal(format!("LIKE pattern has type {}", patterns.data_type()))
        })?;
    let output = (0..values.len())
        .map(|row| {
            if values.is_null(row) || patterns.is_null(row) {
                Ok(None)
            } else {
                Ok(Some(
                    like_matches(values.value(row), patterns.value(row), escape)? ^ negated,
                ))
            }
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(BooleanArray::from(output))
}

fn like_matches(value: &str, pattern: &str, escape: Option<char>) -> Result<bool> {
    let tokens = like_tokens(pattern, escape)?;
    let value = value.chars().collect::<Vec<_>>();
    let mut previous = vec![false; value.len() + 1];
    previous[0] = true;
    for token in tokens {
        let mut current = vec![false; value.len() + 1];
        if token == LikeToken::Any {
            current[0] = previous[0];
        }
        for index in 1..=value.len() {
            current[index] = match token {
                LikeToken::Any => previous[index] || current[index - 1],
                LikeToken::One => previous[index - 1],
                LikeToken::Literal(expected) => previous[index - 1] && value[index - 1] == expected,
            };
        }
        previous = current;
    }
    Ok(previous[value.len()])
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum LikeToken {
    Any,
    One,
    Literal(char),
}

fn like_tokens(pattern: &str, escape: Option<char>) -> Result<Vec<LikeToken>> {
    let mut characters = pattern.chars();
    let mut tokens = Vec::with_capacity(pattern.len());
    while let Some(character) = characters.next() {
        if escape == Some(character) {
            let literal = characters.next().ok_or_else(|| {
                Error::InvalidArgument("LIKE pattern ends with its ESCAPE character".into())
            })?;
            tokens.push(LikeToken::Literal(literal));
        } else {
            tokens.push(match character {
                '%' => LikeToken::Any,
                '_' => LikeToken::One,
                literal => LikeToken::Literal(literal),
            });
        }
    }
    Ok(tokens)
}

fn as_boolean(array: &ArrayRef) -> Result<&BooleanArray> {
    array
        .as_any()
        .downcast_ref::<BooleanArray>()
        .ok_or_else(|| {
            Error::Execution(format!("expected BOOLEAN array, got {}", array.data_type()))
        })
}

#[cfg(test)]
mod tests;
