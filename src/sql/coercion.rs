use arrow::datatypes::{DataType, IntervalUnit, TimeUnit};
use sqlparser::ast::{DataType as SqlDataType, ExactNumberInfo, TimezoneInfo};

use crate::{Error, Result};

use super::{BinaryOp, BoundExpr, ExprKind, ScalarValue};

pub(super) fn cast(expr: BoundExpr, data_type: &SqlDataType) -> Result<BoundExpr> {
    let target = arrow_type(data_type)?;
    validate_temporal_cast(&expr.data_type, &target)?;
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
        if is_float(&left.data_type) || is_float(&right.data_type) {
            return Ok((
                cast_if_needed(left, &DataType::Float64),
                cast_if_needed(right, &DataType::Float64),
            ));
        }
        let left = decimal_operand(left)?;
        let right = decimal_operand(right)?;
        return match lossless_common_decimal(&left.data_type, &right.data_type)? {
            Some(target) => Ok((
                cast_if_needed(left, &target),
                cast_if_needed(right, &target),
            )),
            None => Ok((left, right)),
        };
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
    if left.data_type == DataType::Date32 && is_utf8_literal(&right) {
        return Ok((left, cast_if_needed(right, &DataType::Date32)));
    }
    if right.data_type == DataType::Date32 && is_utf8_literal(&left) {
        return Ok((cast_if_needed(left, &DataType::Date32), right));
    }
    if matches!(left.data_type, DataType::Timestamp(_, None)) && is_utf8_literal(&right) {
        let target = left.data_type.clone();
        return Ok((left, cast_if_needed(right, &target)));
    }
    if matches!(right.data_type, DataType::Timestamp(_, None)) && is_utf8_literal(&left) {
        let target = right.data_type.clone();
        return Ok((cast_if_needed(left, &target), right));
    }
    if left.data_type == DataType::Date32
        && let DataType::Timestamp(unit, None) = &right.data_type
    {
        let target = DataType::Timestamp(*unit, None);
        return Ok((cast_if_needed(left, &target), right));
    }
    if right.data_type == DataType::Date32
        && let DataType::Timestamp(unit, None) = &left.data_type
    {
        let target = DataType::Timestamp(*unit, None);
        return Ok((left, cast_if_needed(right, &target)));
    }
    if let (
        DataType::Timestamp(left_unit, left_zone),
        DataType::Timestamp(right_unit, right_zone),
    ) = (&left.data_type, &right.data_type)
    {
        if left_zone != right_zone {
            return Err(Error::InvalidArgument(format!(
                "cannot compare TIMESTAMP values with different timezones ({left_zone:?} and {right_zone:?})"
            )));
        }
        let unit = finer_time_unit(*left_unit, *right_unit);
        let target = DataType::Timestamp(unit, left_zone.clone());
        return Ok((
            cast_if_needed(left, &target),
            cast_if_needed(right, &target),
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
    if matches!(&left.data_type, DataType::Timestamp(_, _))
        && matches!(
            &right.data_type,
            DataType::Interval(IntervalUnit::DayTime | IntervalUnit::YearMonth)
        )
        && matches!(op, BinaryOp::Add | BinaryOp::Subtract)
    {
        let output = left.data_type.clone();
        return Ok((left, right, output));
    }
    if matches!(
        &left.data_type,
        DataType::Interval(IntervalUnit::DayTime | IntervalUnit::YearMonth)
    ) && matches!(&right.data_type, DataType::Timestamp(_, _))
        && op == BinaryOp::Add
    {
        let output = right.data_type.clone();
        return Ok((left, right, output));
    }
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
        // DuckDB's `/` operator always produces floating-point output, also
        // for two exact DECIMAL operands. Other DECIMAL arithmetic stays
        // exact unless one side is already floating point.
        if op == BinaryOp::Divide || is_float(&left.data_type) || is_float(&right.data_type) {
            return Ok((
                cast_if_needed(left, &DataType::Float64),
                cast_if_needed(right, &DataType::Float64),
                DataType::Float64,
            ));
        }
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
            return Ok(DataType::Float64);
        }
        if is_decimal(left) && is_decimal(right) {
            return common_decimal(left, right);
        }
        if is_decimal(left) && is_integer(right) {
            return common_decimal(left, &integer_decimal_type(right)?);
        }
        if is_integer(left) && is_decimal(right) {
            return common_decimal(&integer_decimal_type(left)?, right);
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
    if left == &DataType::Date32
        && let DataType::Timestamp(unit, None) = right
    {
        return Ok(DataType::Timestamp(*unit, None));
    }
    if right == &DataType::Date32
        && let DataType::Timestamp(unit, None) = left
    {
        return Ok(DataType::Timestamp(*unit, None));
    }
    if let (DataType::Timestamp(left_unit, left_zone), DataType::Timestamp(right_unit, right_zone)) =
        (left, right)
        && left_zone == right_zone
    {
        return Ok(DataType::Timestamp(
            finer_time_unit(*left_unit, *right_unit),
            left_zone.clone(),
        ));
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
        | SqlDataType::UBigInt
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
        SqlDataType::Binary(_)
        | SqlDataType::Varbinary(_)
        | SqlDataType::Blob(_)
        | SqlDataType::TinyBlob
        | SqlDataType::MediumBlob
        | SqlDataType::LongBlob
        | SqlDataType::Bytes(_) => DataType::Binary,
        SqlDataType::Date | SqlDataType::Date32 => DataType::Date32,
        SqlDataType::Timestamp(precision, timezone) => {
            if precision.is_some_and(|precision| precision > 6) {
                return Err(Error::InvalidArgument(
                    "TIMESTAMP precision above 6 cannot be represented by the microsecond engine type"
                        .into(),
                ));
            }
            if precision.is_some_and(|precision| precision < 6) {
                return Err(Error::Unsupported(
                    "precision-qualified TIMESTAMP casts below microseconds are not supported; use TIMESTAMP or TIMESTAMP(6)"
                        .into(),
                ));
            }
            let timezone = match timezone {
                TimezoneInfo::None | TimezoneInfo::WithoutTimeZone => None,
                _ => {
                    return Err(Error::Unsupported(
                        "timezone-aware TIMESTAMP cast targets are not supported".into(),
                    ));
                }
            };
            DataType::Timestamp(TimeUnit::Microsecond, timezone)
        }
        SqlDataType::TimestampNtz(precision) => {
            if precision.is_some_and(|precision| precision > 6) {
                return Err(Error::InvalidArgument(
                    "TIMESTAMP precision above 6 cannot be represented by the microsecond engine type"
                        .into(),
                ));
            }
            if precision.is_some_and(|precision| precision < 6) {
                return Err(Error::Unsupported(
                    "precision-qualified TIMESTAMP casts below microseconds are not supported; use TIMESTAMP or TIMESTAMP(6)"
                        .into(),
                ));
            }
            DataType::Timestamp(TimeUnit::Microsecond, None)
        }
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
    lossless_common_decimal(left, right)?.ok_or_else(|| {
        Error::InvalidArgument(
            "DECIMAL values require more than 38 digits for a lossless common type".into(),
        )
    })
}

fn lossless_common_decimal(left: &DataType, right: &DataType) -> Result<Option<DataType>> {
    let (left_precision, left_scale) = decimal_parts(left)?;
    let (right_precision, right_scale) = decimal_parts(right)?;
    let scale = left_scale.max(right_scale);
    let integer = (left_precision as i16 - i16::from(left_scale))
        .max(right_precision as i16 - i16::from(right_scale));
    let precision = i16::from(scale) + integer;
    if precision > 38 {
        return Ok(None);
    }
    Ok(Some(DataType::Decimal128(precision.max(1) as u8, scale)))
}

fn decimal_parts(data_type: &DataType) -> Result<(u8, i8)> {
    match data_type {
        DataType::Decimal128(precision, scale) => Ok((*precision, *scale)),
        other => Err(Error::Internal(format!("expected DECIMAL, got {other}"))),
    }
}

fn integer_decimal_type(data_type: &DataType) -> Result<DataType> {
    let precision = match data_type {
        DataType::Int8 => 3,
        DataType::Int16 => 5,
        DataType::Int32 => 10,
        DataType::Int64 => 19,
        DataType::UInt8 => 3,
        DataType::UInt16 => 5,
        DataType::UInt32 => 10,
        DataType::UInt64 => 20,
        other => {
            return Err(Error::Internal(format!(
                "expected integer CASE operand, got {other}"
            )));
        }
    };
    Ok(DataType::Decimal128(precision, 0))
}

fn is_decimal(data_type: &DataType) -> bool {
    matches!(data_type, DataType::Decimal128(_, _))
}

pub(super) fn is_integer(data_type: &DataType) -> bool {
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

fn is_utf8_literal(expr: &BoundExpr) -> bool {
    matches!(&expr.kind, ExprKind::Literal(ScalarValue::Utf8(_)))
}

fn validate_temporal_cast(source: &DataType, target: &DataType) -> Result<()> {
    let source_temporal = matches!(source, DataType::Date32 | DataType::Timestamp(_, _));
    let target_temporal = matches!(target, DataType::Date32 | DataType::Timestamp(_, _));
    if !source_temporal && !target_temporal {
        return Ok(());
    }
    if source == &DataType::Null {
        return Ok(());
    }
    if target == &DataType::Date32 && is_integer(source) {
        return Ok(());
    }
    let supported = matches!(
        (source, target),
        (DataType::Utf8 | DataType::LargeUtf8, DataType::Date32)
            | (
                DataType::Utf8 | DataType::LargeUtf8,
                DataType::Timestamp(_, None)
            )
            | (DataType::Date32, DataType::Utf8 | DataType::LargeUtf8)
            | (DataType::Date32, DataType::Timestamp(_, None))
            | (DataType::Date32, DataType::Date32)
            | (
                DataType::Timestamp(_, None),
                DataType::Utf8 | DataType::LargeUtf8
            )
            | (DataType::Timestamp(_, None), DataType::Date32)
            | (DataType::Timestamp(_, None), DataType::Timestamp(_, None))
    );
    if supported {
        Ok(())
    } else {
        Err(Error::InvalidArgument(format!(
            "strict temporal CAST does not support {source} to {target}"
        )))
    }
}

fn finer_time_unit(left: TimeUnit, right: TimeUnit) -> TimeUnit {
    use TimeUnit::{Microsecond, Millisecond, Nanosecond, Second};
    match (left, right) {
        (Nanosecond, _) | (_, Nanosecond) => Nanosecond,
        (Microsecond, _) | (_, Microsecond) => Microsecond,
        (Millisecond, _) | (_, Millisecond) => Millisecond,
        (Second, Second) => Second,
    }
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
