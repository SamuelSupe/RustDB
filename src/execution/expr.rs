use std::{cmp::Ordering, sync::Arc};

use arrow::{
    array::{
        Array, ArrayRef, BinaryArray, BooleanArray, Date32Array, Decimal128Array,
        FixedSizeBinaryBuilder, Float64Array, Int64Array, IntervalDayTimeArray,
        IntervalMonthDayNanoArray, IntervalYearMonthArray, NullArray, StringArray,
        Time32MillisecondArray, Time32SecondArray, Time64MicrosecondArray, Time64NanosecondArray,
        TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
        TimestampSecondArray, UInt64Array,
        types::{IntervalDayTimeType, IntervalMonthDayNanoType, IntervalYearMonthType},
    },
    compute::{
        filter_record_batch,
        kernels::{boolean, cmp, numeric},
    },
    record_batch::{RecordBatch, RecordBatchOptions},
};

use crate::runtime::estimate_array_bytes;
use crate::sql::{BinaryOp, BoundExpr, ExprKind, ScalarValue, UnaryOp};
use crate::{Error, Result};

mod like;
mod scalar_compare;
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
            if let Some(result) = scalar_compare::try_evaluate(left, *op, right, batch) {
                return result;
            }
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
            let result = match &pattern.kind {
                ExprKind::Literal(ScalarValue::Utf8(pattern)) => {
                    like::evaluate_literal(&expr, pattern, *negated, *escape)?
                }
                _ => {
                    let pattern = evaluate(pattern, batch)?;
                    like::evaluate(&expr, &pattern, *negated, *escape)?
                }
            };
            Ok(Arc::new(result))
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
    projected_batch(schema, columns, batch.num_rows())
}

/// Builds an internal Aggregate-input batch while retaining dictionary arrays
/// explicitly requested by the grouped scan fast path. The logical field name,
/// nullability and metadata are preserved; Aggregate materializes its public
/// output back to the logical value type.
pub(crate) fn project_preserving_dictionaries(
    expressions: &[BoundExpr],
    logical_schema: arrow::datatypes::SchemaRef,
    batch: &RecordBatch,
    dictionary_columns: &[usize],
) -> Result<RecordBatch> {
    let mut columns = expressions
        .iter()
        .map(|expression| evaluate(expression, batch))
        .collect::<Result<Vec<_>>>()?;
    if columns.len() != logical_schema.fields().len() {
        return Err(Error::Internal(format!(
            "projection produced {} columns for a {}-field schema",
            columns.len(),
            logical_schema.fields().len()
        )));
    }
    let fields = logical_schema
        .fields()
        .iter()
        .zip(&columns)
        .enumerate()
        .map(|(index, (field, column))| match column.data_type() {
            arrow::datatypes::DataType::Dictionary(_, value)
                if dictionary_columns.contains(&index) && value.as_ref() == field.data_type() =>
            {
                Arc::new(
                    field
                        .as_ref()
                        .clone()
                        .with_data_type(column.data_type().clone()),
                )
            }
            _ => Arc::clone(field),
        })
        .collect::<Vec<_>>();
    for (index, column) in columns.iter_mut().enumerate() {
        if dictionary_columns.contains(&index) {
            continue;
        }
        if matches!(column.data_type(), arrow::datatypes::DataType::Dictionary(_, value)
            if value.as_ref() == logical_schema.field(index).data_type())
        {
            *column =
                arrow::compute::cast(column.as_ref(), logical_schema.field(index).data_type())?;
        }
    }
    let schema = Arc::new(arrow::datatypes::Schema::new_with_metadata(
        fields,
        logical_schema.metadata().clone(),
    ));
    projected_batch(schema, columns, batch.num_rows())
}

fn projected_batch(
    schema: arrow::datatypes::SchemaRef,
    columns: Vec<ArrayRef>,
    rows: usize,
) -> Result<RecordBatch> {
    let options = RecordBatchOptions::new().with_row_count(Some(rows));
    Ok(RecordBatch::try_new_with_options(
        schema, columns, &options,
    )?)
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
        // A projected column is an Arc clone of an input array. The input
        // envelope already owns its buffers, so only the RecordBatch/ArrayRef
        // container overhead below is new projection workspace.
        ExprKind::Column(_)
        | ExprKind::OuterRef { .. }
        | ExprKind::DeferredGroup(_)
        | ExprKind::DeferredAggregate(_) => ExpressionMemory { output: 0, peak: 0 },
        ExprKind::Literal(_) => ExpressionMemory {
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
            if let Some(input) = scalar_compare::array_operand(left, *op, right) {
                let input = expression_memory(input, batch);
                return ExpressionMemory {
                    output,
                    peak: input.peak.max(input.output.saturating_add(output)),
                };
            }
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
        } => {
            if let ExprKind::Literal(ScalarValue::Utf8(pattern)) = &right.kind {
                let left = expression_memory(left, batch);
                let workspace = like::literal_workspace_bytes(pattern);
                ExpressionMemory {
                    output,
                    peak: left
                        .peak
                        .max(left.output.saturating_add(output).saturating_add(workspace)),
                }
            } else {
                binary_expression_memory(left, right, output, batch)
            }
        }
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
    if matches!(left.data_type(), arrow::datatypes::DataType::Interval(_))
        && matches!(right.data_type(), arrow::datatypes::DataType::Interval(_))
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
        return Ok(Arc::new(compare_intervals(op, &left, &right)?));
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

fn compare_intervals(op: BinaryOp, left: &ArrayRef, right: &ArrayRef) -> Result<BooleanArray> {
    if left.len() != right.len() {
        return Err(Error::Internal(
            "INTERVAL comparison operands have different lengths".into(),
        ));
    }
    Ok(BooleanArray::from_iter(
        (0..left.len())
            .map(|row| {
                if left.is_null(row) || right.is_null(row) {
                    return Ok(None);
                }
                let ordering =
                    super::value::cell(left, row)?.compare(&super::value::cell(right, row)?)?;
                Ok(Some(match op {
                    BinaryOp::Eq => ordering.is_eq(),
                    BinaryOp::NotEq => !ordering.is_eq(),
                    BinaryOp::Lt => ordering.is_lt(),
                    BinaryOp::LtEq => !ordering.is_gt(),
                    BinaryOp::Gt => ordering.is_gt(),
                    BinaryOp::GtEq => !ordering.is_lt(),
                    _ => unreachable!("caller only passes comparison operators"),
                }))
            })
            .collect::<Result<Vec<_>>>()?,
    ))
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
            ScalarValue::Timestamp {
                value,
                unit,
                timezone,
            } => match unit {
                arrow::datatypes::TimeUnit::Second => Arc::new(
                    TimestampSecondArray::from(vec![Some(*value); len])
                        .with_timezone_opt(timezone.clone()),
                ),
                arrow::datatypes::TimeUnit::Millisecond => Arc::new(
                    TimestampMillisecondArray::from(vec![Some(*value); len])
                        .with_timezone_opt(timezone.clone()),
                ),
                arrow::datatypes::TimeUnit::Microsecond => Arc::new(
                    TimestampMicrosecondArray::from(vec![Some(*value); len])
                        .with_timezone_opt(timezone.clone()),
                ),
                arrow::datatypes::TimeUnit::Nanosecond => Arc::new(
                    TimestampNanosecondArray::from(vec![Some(*value); len])
                        .with_timezone_opt(timezone.clone()),
                ),
            },
            ScalarValue::Time { value, unit } => match unit {
                arrow::datatypes::TimeUnit::Second => Arc::new(Time32SecondArray::from(vec![
                    Some(
                        i32::try_from(*value).map_err(|_| {
                            Error::Execution("TIME(second) literal is out of range".into())
                        })?
                    );
                    len
                ])),
                arrow::datatypes::TimeUnit::Millisecond => {
                    Arc::new(Time32MillisecondArray::from(vec![
                        Some(
                            i32::try_from(*value).map_err(|_| {
                                Error::Execution("TIME(millisecond) literal is out of range".into())
                            })?
                        );
                        len
                    ]))
                }
                arrow::datatypes::TimeUnit::Microsecond => {
                    Arc::new(Time64MicrosecondArray::from(vec![Some(*value); len]))
                }
                arrow::datatypes::TimeUnit::Nanosecond => {
                    Arc::new(Time64NanosecondArray::from(vec![Some(*value); len]))
                }
            },
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
            ScalarValue::MonthDayNanoInterval {
                months,
                days,
                nanoseconds,
            } => Arc::new(IntervalMonthDayNanoArray::from(vec![
                Some(
                    IntervalMonthDayNanoType::make_value(*months, *days, *nanoseconds,)
                );
                len
            ])),
            ScalarValue::Utf8(value) => Arc::new(StringArray::from_iter_values(
                std::iter::repeat_n(value.as_str(), len),
            )),
            ScalarValue::Binary(value) => Arc::new(BinaryArray::from_iter_values(
                std::iter::repeat_n(value.as_slice(), len),
            )),
            ScalarValue::Uuid(value) => {
                let mut builder = FixedSizeBinaryBuilder::with_capacity(len, 16);
                for _ in 0..len {
                    builder.append_value(value)?;
                }
                Arc::new(builder.finish())
            }
        };
    Ok(array)
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
mod projection_memory_tests;

#[cfg(test)]
mod tests;
