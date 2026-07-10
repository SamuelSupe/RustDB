use arrow::datatypes::{DataType, IntervalUnit};
use sqlparser::ast::{DataType as SqlDataType, ExactNumberInfo};

use crate::{Error, Result};

use super::{BinaryOp, BoundExpr, ExprKind, ScalarValue};

pub(super) fn cast(expr: BoundExpr, data_type: &SqlDataType) -> Result<BoundExpr> {
    let target = arrow_type(data_type)?;
    let display_name = format!("CAST({} AS {data_type})", expr.display_name);
    Ok(BoundExpr {
        kind: ExprKind::Cast {
            expr: Box::new(expr),
        },
        data_type: target,
        display_name,
    })
}

pub(super) fn cast_if_needed(expr: BoundExpr, target: &DataType) -> BoundExpr {
    if &expr.data_type == target {
        return expr;
    }
    let display_name = expr.display_name.clone();
    BoundExpr {
        kind: ExprKind::Cast {
            expr: Box::new(expr),
        },
        data_type: target.clone(),
        display_name,
    }
}

pub(super) fn coerce_comparison(
    left: BoundExpr,
    right: BoundExpr,
) -> Result<(BoundExpr, BoundExpr)> {
    if left.data_type == right.data_type {
        return Ok((left, right));
    }
    if left.data_type == DataType::Null {
        let target = right.data_type.clone();
        return Ok((cast_if_needed(left, &target), right));
    }
    if right.data_type == DataType::Null {
        let target = left.data_type.clone();
        return Ok((left, cast_if_needed(right, &target)));
    }
    if is_decimal(&left.data_type) || is_decimal(&right.data_type) {
        let left = decimal_operand(left)?;
        let right = decimal_operand(right)?;
        let target = common_decimal(&left.data_type, &right.data_type)?;
        return Ok((
            cast_if_needed(left, &target),
            cast_if_needed(right, &target),
        ));
    }
    if is_numeric(&left.data_type) && is_numeric(&right.data_type) {
        let target = if is_float(&left.data_type) || is_float(&right.data_type) {
            DataType::Float64
        } else if is_unsigned(&left.data_type) && is_unsigned(&right.data_type) {
            DataType::UInt64
        } else {
            DataType::Int64
        };
        return Ok((
            cast_if_needed(left, &target),
            cast_if_needed(right, &target),
        ));
    }
    if is_string(&left.data_type) && is_string(&right.data_type) {
        return Ok((
            cast_if_needed(left, &DataType::Utf8),
            cast_if_needed(right, &DataType::Utf8),
        ));
    }
    Err(Error::InvalidArgument(format!(
        "cannot compare {} with {}",
        left.data_type, right.data_type
    )))
}

pub(super) fn coerce_arithmetic(
    left: BoundExpr,
    right: BoundExpr,
    op: BinaryOp,
) -> Result<(BoundExpr, BoundExpr, DataType)> {
    let day_interval = DataType::Interval(IntervalUnit::DayTime);
    let month_interval = DataType::Interval(IntervalUnit::YearMonth);
    if left.data_type == DataType::Date32
        && matches!(
            &right.data_type,
            DataType::Interval(IntervalUnit::DayTime | IntervalUnit::YearMonth)
        )
        && matches!(op, BinaryOp::Add | BinaryOp::Subtract)
    {
        return Ok((left, right, DataType::Date32));
    }
    if matches!(
        &left.data_type,
        DataType::Interval(IntervalUnit::DayTime | IntervalUnit::YearMonth)
    ) && right.data_type == DataType::Date32
        && op == BinaryOp::Add
    {
        return Ok((left, right, DataType::Date32));
    }
    if left.data_type == day_interval
        && right.data_type == day_interval
        && matches!(op, BinaryOp::Add | BinaryOp::Subtract)
    {
        return Ok((left, right, day_interval));
    }
    if left.data_type == month_interval
        && right.data_type == month_interval
        && matches!(op, BinaryOp::Add | BinaryOp::Subtract)
    {
        return Ok((left, right, month_interval));
    }
    if is_decimal(&left.data_type) || is_decimal(&right.data_type) {
        let left = decimal_operand(left)?;
        let right = decimal_operand(right)?;
        let data_type = decimal_arithmetic_type(&left.data_type, &right.data_type, op)?;
        return Ok((left, right, data_type));
    }
    if !is_numeric(&left.data_type) || !is_numeric(&right.data_type) {
        return Err(Error::Unsupported(format!(
            "operator {op} does not support {} and {}",
            left.data_type, right.data_type
        )));
    }
    let target =
        if op == BinaryOp::Divide || is_float(&left.data_type) || is_float(&right.data_type) {
            DataType::Float64
        } else if is_unsigned(&left.data_type) && is_unsigned(&right.data_type) {
            DataType::UInt64
        } else {
            DataType::Int64
        };
    Ok((
        cast_if_needed(left, &target),
        cast_if_needed(right, &target),
        target,
    ))
}

pub(super) fn common_case_type(left: &DataType, right: &DataType) -> Result<DataType> {
    if left == right || right == &DataType::Null {
        return Ok(left.clone());
    }
    if left == &DataType::Null {
        return Ok(right.clone());
    }
    if is_decimal(left) || is_decimal(right) {
        if is_float(left) || is_float(right) {
            return Err(Error::InvalidArgument(
                "mixing DECIMAL and floating-point CASE branches requires an explicit CAST".into(),
            ));
        }
        if is_decimal(left) && is_decimal(right) {
            return common_decimal(left, right);
        }
        if is_decimal(left) && is_integer(right) {
            return Ok(left.clone());
        }
        if is_integer(left) && is_decimal(right) {
            return Ok(right.clone());
        }
    }
    if is_numeric(left) && is_numeric(right) {
        return Ok(if is_float(left) || is_float(right) {
            DataType::Float64
        } else if is_unsigned(left) && is_unsigned(right) {
            DataType::UInt64
        } else {
            DataType::Int64
        });
    }
    if is_string(left) && is_string(right) {
        return Ok(DataType::Utf8);
    }
    Err(Error::InvalidArgument(format!(
        "CASE branches have incompatible types {left} and {right}"
    )))
}

pub(super) fn is_numeric(data_type: &DataType) -> bool {
    is_integer(data_type) || is_float(data_type) || is_decimal(data_type)
}

pub(super) fn is_string(data_type: &DataType) -> bool {
    matches!(data_type, DataType::Utf8 | DataType::LargeUtf8)
}

fn arrow_type(data_type: &SqlDataType) -> Result<DataType> {
    let output = match data_type {
        SqlDataType::Bool | SqlDataType::Boolean => DataType::Boolean,
        SqlDataType::TinyInt(_)
        | SqlDataType::Int2(_)
        | SqlDataType::SmallInt(_)
        | SqlDataType::MediumInt(_)
        | SqlDataType::Int(_)
        | SqlDataType::Int4(_)
        | SqlDataType::Int8(_)
        | SqlDataType::Int16
        | SqlDataType::Int32
        | SqlDataType::Int64
        | SqlDataType::Integer(_)
        | SqlDataType::BigInt(_)
        | SqlDataType::Signed
        | SqlDataType::SignedInteger => DataType::Int64,
        SqlDataType::TinyIntUnsigned(_)
        | SqlDataType::UTinyInt
        | SqlDataType::Int2Unsigned(_)
        | SqlDataType::SmallIntUnsigned(_)
        | SqlDataType::USmallInt
        | SqlDataType::MediumIntUnsigned(_)
        | SqlDataType::IntUnsigned(_)
        | SqlDataType::Int4Unsigned(_)
        | SqlDataType::IntegerUnsigned(_)
        | SqlDataType::UInt8
        | SqlDataType::UInt16
        | SqlDataType::UInt32
        | SqlDataType::UInt64
        | SqlDataType::BigIntUnsigned(_)
        | SqlDataType::Unsigned
        | SqlDataType::UnsignedInteger => DataType::UInt64,
        SqlDataType::Float(_)
        | SqlDataType::Float4
        | SqlDataType::Float32
        | SqlDataType::Float64
        | SqlDataType::Real
        | SqlDataType::Float8
        | SqlDataType::Double(_)
        | SqlDataType::DoublePrecision => DataType::Float64,
        SqlDataType::Numeric(info)
        | SqlDataType::Decimal(info)
        | SqlDataType::DecimalUnsigned(info)
        | SqlDataType::Dec(info)
        | SqlDataType::DecUnsigned(info) => decimal_type(info)?,
        SqlDataType::Character(_)
        | SqlDataType::Char(_)
        | SqlDataType::CharacterVarying(_)
        | SqlDataType::CharVarying(_)
        | SqlDataType::Varchar(_)
        | SqlDataType::Nvarchar(_)
        | SqlDataType::Text
        | SqlDataType::String(_) => DataType::Utf8,
        SqlDataType::Date | SqlDataType::Date32 => DataType::Date32,
        other => {
            return Err(Error::Unsupported(format!(
                "CAST target type {other} is not supported"
            )));
        }
    };
    Ok(output)
}

fn decimal_type(info: &ExactNumberInfo) -> Result<DataType> {
    let (precision, scale) = match info {
        ExactNumberInfo::None => (18_u64, 3_i64),
        ExactNumberInfo::Precision(precision) => (*precision, 0),
        ExactNumberInfo::PrecisionAndScale(precision, scale) => (*precision, *scale),
    };
    if !(1..=38).contains(&precision) || scale < 0 || scale > precision as i64 {
        return Err(Error::InvalidArgument(format!(
            "DECIMAL precision/scale ({precision}, {scale}) is outside Decimal128 bounds"
        )));
    }
    Ok(DataType::Decimal128(precision as u8, scale as i8))
}

fn decimal_operand(expr: BoundExpr) -> Result<BoundExpr> {
    if is_decimal(&expr.data_type) {
        return Ok(expr);
    }
    if expr.data_type == DataType::Null {
        return Ok(cast_if_needed(expr, &DataType::Decimal128(1, 0)));
    }
    let precision = match (&expr.kind, &expr.data_type) {
        (ExprKind::Literal(ScalarValue::Int64(value)), _) => {
            decimal_digits(u128::from(value.unsigned_abs()))
        }
        (ExprKind::Literal(ScalarValue::UInt64(value)), _) => decimal_digits(u128::from(*value)),
        (_, data_type) if is_unsigned(data_type) => 20,
        (_, data_type) if is_integer(data_type) => 19,
        (_, data_type) if is_float(data_type) => {
            return Err(Error::InvalidArgument(
                "mixing DECIMAL and floating-point values requires an explicit CAST".into(),
            ));
        }
        _ => {
            return Err(Error::InvalidArgument(format!(
                "cannot use {} as a DECIMAL operand",
                expr.data_type
            )));
        }
    };
    Ok(cast_if_needed(expr, &DataType::Decimal128(precision, 0)))
}

fn decimal_digits(mut value: u128) -> u8 {
    let mut digits = 1;
    while value >= 10 {
        value /= 10;
        digits += 1;
    }
    digits
}

fn decimal_arithmetic_type(left: &DataType, right: &DataType, op: BinaryOp) -> Result<DataType> {
    let (left_precision, left_scale) = decimal_parts(left)?;
    let (right_precision, right_scale) = decimal_parts(right)?;
    let (precision, scale) = match op {
        BinaryOp::Add | BinaryOp::Subtract => {
            let scale = left_scale.max(right_scale);
            let integer = (left_precision as i16 - i16::from(left_scale))
                .max(right_precision as i16 - i16::from(right_scale));
            ((i16::from(scale) + integer + 1).min(38) as u8, scale)
        }
        BinaryOp::Multiply => {
            let scale = left_scale.saturating_add(right_scale);
            if scale > 38 {
                return Err(Error::InvalidArgument(
                    "DECIMAL multiplication result scale exceeds 38".into(),
                ));
            }
            (
                left_precision
                    .saturating_add(right_precision.saturating_add(1))
                    .min(38),
                scale,
            )
        }
        BinaryOp::Divide => {
            let scale = left_scale.saturating_add(4).min(38);
            let multiplier = scale - left_scale + right_scale;
            (
                (u16::from(multiplier as u8) + u16::from(left_precision)).min(38) as u8,
                scale,
            )
        }
        BinaryOp::Modulo => {
            let scale = left_scale.max(right_scale);
            let integer = (left_precision as i16 - i16::from(left_scale))
                .min(right_precision as i16 - i16::from(right_scale));
            ((i16::from(scale) + integer).min(38) as u8, scale)
        }
        _ => {
            return Err(Error::Internal(format!(
                "{op} is not a DECIMAL arithmetic operator"
            )));
        }
    };
    Ok(DataType::Decimal128(precision, scale))
}

fn common_decimal(left: &DataType, right: &DataType) -> Result<DataType> {
    let (left_precision, left_scale) = decimal_parts(left)?;
    let (right_precision, right_scale) = decimal_parts(right)?;
    let scale = left_scale.max(right_scale);
    let integer = (left_precision as i16 - i16::from(left_scale))
        .max(right_precision as i16 - i16::from(right_scale));
    let precision = i16::from(scale) + integer;
    if precision > 38 {
        return Err(Error::InvalidArgument(
            "DECIMAL values require more than 38 digits for a lossless common type".into(),
        ));
    }
    Ok(DataType::Decimal128(precision.max(1) as u8, scale))
}

fn decimal_parts(data_type: &DataType) -> Result<(u8, i8)> {
    match data_type {
        DataType::Decimal128(precision, scale) => Ok((*precision, *scale)),
        other => Err(Error::Internal(format!("expected DECIMAL, got {other}"))),
    }
}

fn is_decimal(data_type: &DataType) -> bool {
    matches!(data_type, DataType::Decimal128(_, _))
}

fn is_integer(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
    )
}

fn is_float(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Float16 | DataType::Float32 | DataType::Float64
    )
}

fn is_unsigned(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64
    )
}
