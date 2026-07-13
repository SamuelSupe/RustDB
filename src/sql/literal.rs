use sqlparser::ast::{
    DataType as SqlDataType, DateTimeField, Expr, Interval, TimezoneInfo, TypedString,
    UnaryOperator, Value,
};

use crate::{Error, Result};

use super::temporal::{parse_date32, parse_timestamp_microsecond};
use super::{BoundExpr, ScalarValue};

pub(super) fn bind_value(value: &Value) -> Result<BoundExpr> {
    let value = match value {
        Value::Null => ScalarValue::Null,
        Value::Boolean(value) => ScalarValue::Boolean(*value),
        Value::Number(value, _) if value.contains('.') && !contains_exponent(value) => {
            parse_decimal(value)?
        }
        Value::Number(value, _) if contains_exponent(value) => {
            ScalarValue::Float64(value.parse().map_err(|_| {
                Error::InvalidArgument(format!("invalid floating-point literal '{value}'"))
            })?)
        }
        Value::Number(value, _) => match value.parse::<i64>() {
            Ok(value) => ScalarValue::Int64(value),
            Err(_) => ScalarValue::UInt64(value.parse().map_err(|_| {
                Error::InvalidArgument(format!("integer literal '{value}' is out of range"))
            })?),
        },
        Value::HexStringLiteral(value) => ScalarValue::Binary(parse_hex(value)?),
        value if string_value(value).is_some() => ScalarValue::Utf8(
            string_value(value)
                .expect("checked string literal")
                .to_owned(),
        ),
        other => {
            return Err(Error::Unsupported(format!(
                "literal {other} is not supported"
            )));
        }
    };
    Ok(BoundExpr::literal(value))
}

fn parse_hex(value: &str) -> Result<Vec<u8>> {
    if !value.len().is_multiple_of(2) {
        return Err(Error::InvalidArgument(
            "hexadecimal binary literal must contain an even number of digits".into(),
        ));
    }
    value
        .as_bytes()
        .chunks_exact(2)
        .map(|digits| {
            let text = std::str::from_utf8(digits).expect("hex literal is ASCII");
            u8::from_str_radix(text, 16).map_err(|_| {
                Error::InvalidArgument(format!(
                    "hexadecimal binary literal contains invalid digits '{text}'"
                ))
            })
        })
        .collect()
}

pub(super) fn bind_typed_string(value: &TypedString) -> Result<BoundExpr> {
    let literal = string_value(&value.value.value).ok_or_else(|| {
        Error::InvalidArgument(format!("{} literal must contain a string", value.data_type))
    })?;
    match value.data_type {
        SqlDataType::Date | SqlDataType::Date32 => Ok(BoundExpr::literal(ScalarValue::Date32(
            parse_date32(literal)?,
        ))),
        SqlDataType::Timestamp(precision, TimezoneInfo::None | TimezoneInfo::WithoutTimeZone)
        | SqlDataType::TimestampNtz(precision) => Ok(BoundExpr::literal(
            ScalarValue::TimestampMicrosecond(timestamp_with_precision(literal, precision)?),
        )),
        SqlDataType::Timestamp(_, _) => Err(Error::Unsupported(
            "timezone-aware TIMESTAMP literals are not supported".into(),
        )),
        _ => Err(Error::Unsupported(format!(
            "typed literal {} is not supported",
            value.data_type
        ))),
    }
}

fn timestamp_with_precision(literal: &str, precision: Option<u64>) -> Result<i64> {
    let precision = precision.unwrap_or(6);
    if precision > 6 {
        return Err(Error::InvalidArgument(
            "TIMESTAMP precision above 6 cannot be represented by the microsecond engine type"
                .into(),
        ));
    }
    let value = parse_timestamp_microsecond(literal)?;
    let factor = 10_i128.pow(6 - u32::try_from(precision).expect("precision is at most six"));
    let magnitude = i128::from(value).abs();
    let rounded = (magnitude + factor / 2) / factor * factor;
    let rounded = if value.is_negative() {
        -rounded
    } else {
        rounded
    };
    i64::try_from(rounded)
        .map_err(|_| Error::InvalidArgument("TIMESTAMP literal is out of range".into()))
}

pub(super) fn bind_interval(interval: &Interval) -> Result<BoundExpr> {
    if interval.last_field.is_some()
        || interval.leading_precision.is_some()
        || interval.fractional_seconds_precision.is_some()
    {
        return Err(Error::Unsupported(
            "interval ranges and precision qualifiers are not supported".into(),
        ));
    }
    let raw = interval_value(interval.value.as_ref())?;
    let value = match interval.leading_field.as_ref() {
        Some(DateTimeField::Day) => ScalarValue::DayInterval(parse_integer(&raw, "day")?),
        Some(DateTimeField::Month) => ScalarValue::MonthInterval(parse_integer(&raw, "month")?),
        Some(DateTimeField::Year) => ScalarValue::MonthInterval(
            parse_integer(&raw, "year")?
                .checked_mul(12)
                .ok_or_else(|| Error::InvalidArgument("year INTERVAL is out of range".into()))?,
        ),
        Some(field) => {
            return Err(Error::Unsupported(format!(
                "INTERVAL {field} is not supported; expected YEAR, MONTH, or DAY"
            )));
        }
        None => parse_interval_suffix(&raw)?,
    };
    Ok(BoundExpr::literal(value))
}

pub(super) fn string_value(value: &Value) -> Option<&str> {
    match value {
        Value::SingleQuotedString(value)
        | Value::DoubleQuotedString(value)
        | Value::EscapedStringLiteral(value)
        | Value::NationalStringLiteral(value) => Some(value),
        _ => None,
    }
}

fn contains_exponent(value: &str) -> bool {
    value.bytes().any(|byte| matches!(byte, b'e' | b'E'))
}

fn parse_decimal(value: &str) -> Result<ScalarValue> {
    let (integer, fraction) = value.split_once('.').ok_or_else(|| {
        Error::Internal("fixed-point literal did not contain a decimal point".into())
    })?;
    if (!integer.is_empty() && !integer.bytes().all(|byte| byte.is_ascii_digit()))
        || !fraction.bytes().all(|byte| byte.is_ascii_digit())
    {
        return Err(Error::InvalidArgument(format!(
            "invalid decimal literal '{value}'"
        )));
    }
    let scale = i8::try_from(fraction.len()).map_err(|_| {
        Error::InvalidArgument(format!("decimal literal '{value}' exceeds 38 digits"))
    })?;
    let integer_digits = integer.trim_start_matches('0').len();
    let precision = integer_digits.saturating_add(fraction.len()).max(1);
    let precision = u8::try_from(precision).map_err(|_| {
        Error::InvalidArgument(format!("decimal literal '{value}' exceeds 38 digits"))
    })?;
    if precision > 38 || scale > 38 {
        return Err(Error::InvalidArgument(format!(
            "decimal literal '{value}' exceeds Decimal128 precision"
        )));
    }
    let digits = format!("{integer}{fraction}");
    let raw = if digits.is_empty() {
        0
    } else {
        digits.parse::<i128>().map_err(|_| {
            Error::InvalidArgument(format!("decimal literal '{value}' is out of range"))
        })?
    };
    Ok(ScalarValue::Decimal128 {
        value: raw,
        precision,
        scale,
    })
}

fn interval_value(expr: &Expr) -> Result<String> {
    match expr {
        Expr::Value(value) => match &value.value {
            Value::Number(value, _) => Ok(value.clone()),
            value if string_value(value).is_some() => Ok(string_value(value)
                .expect("checked string literal")
                .to_owned()),
            other => Err(Error::InvalidArgument(format!(
                "INTERVAL value {other} is not a string or integer"
            ))),
        },
        Expr::UnaryOp {
            op: UnaryOperator::Minus,
            expr,
        } => Ok(format!("-{}", interval_value(expr)?)),
        _ => Err(Error::InvalidArgument(
            "INTERVAL value must be a constant integer".into(),
        )),
    }
}

fn parse_integer(value: &str, unit: &str) -> Result<i32> {
    value.trim().parse().map_err(|_| {
        Error::InvalidArgument(format!(
            "{unit} INTERVAL value '{value}' is not a 32-bit integer"
        ))
    })
}

fn parse_interval_suffix(value: &str) -> Result<ScalarValue> {
    let mut parts = value.split_whitespace();
    let number = parts.next().unwrap_or_default();
    let unit = parts.next().unwrap_or_default();
    if parts.next().is_some() {
        return Err(Error::Unsupported(
            "an unqualified INTERVAL must use '<integer> year|month|day'".into(),
        ));
    }
    match unit.to_ascii_lowercase().as_str() {
        "day" | "days" => Ok(ScalarValue::DayInterval(parse_integer(number, "day")?)),
        "month" | "months" => Ok(ScalarValue::MonthInterval(parse_integer(number, "month")?)),
        "year" | "years" => Ok(ScalarValue::MonthInterval(
            parse_integer(number, "year")?
                .checked_mul(12)
                .ok_or_else(|| Error::InvalidArgument("year INTERVAL is out of range".into()))?,
        )),
        _ => Err(Error::Unsupported(
            "an unqualified INTERVAL must use '<integer> year|month|day'".into(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::parse_decimal;
    use crate::sql::ScalarValue;
    use crate::sql::temporal::parse_date32;

    #[test]
    fn parses_exact_decimal_literals() {
        assert_eq!(
            parse_decimal("0.0625").unwrap(),
            ScalarValue::Decimal128 {
                value: 625,
                precision: 4,
                scale: 4,
            }
        );
    }

    #[test]
    fn validates_date_literals() {
        assert_eq!(parse_date32("1970-01-01").unwrap(), 0);
        assert_eq!(parse_date32("2000-02-29").unwrap(), 11_016);
        assert!(parse_date32("1998-02-29").is_err());
    }
}
