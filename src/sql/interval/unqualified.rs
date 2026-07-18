use crate::{Error, Result};
use sqlparser::ast::DateTimeField;

use super::{Parts, finish, parse};
use crate::sql::ScalarValue;

pub(super) fn parse(raw: &str) -> Result<ScalarValue> {
    let tokens = raw.split_whitespace().collect::<Vec<_>>();
    if tokens.len() == 1 {
        if tokens[0].contains(':') {
            let fields = tokens[0].matches(':').count();
            let leading = if fields == 1 {
                DateTimeField::Minute
            } else {
                DateTimeField::Hour
            };
            if matches!(fields, 1 | 2) {
                return super::qualified::parse(
                    raw,
                    &leading,
                    Some(&DateTimeField::Second),
                    None,
                    None,
                );
            }
        }
        let (_, unsigned) = parse::strip_sign(raw)?;
        if unsigned.contains('-') {
            return super::qualified::parse(
                raw,
                &DateTimeField::Year,
                Some(&DateTimeField::Month),
                None,
                None,
            );
        }
    }
    if tokens.len() == 2 && tokens[1].contains(':') {
        return super::qualified::parse(
            raw,
            &DateTimeField::Day,
            Some(&DateTimeField::Second),
            None,
            None,
        );
    }
    if tokens.is_empty() || !tokens.len().is_multiple_of(2) {
        return Err(Error::InvalidArgument(
            "unqualified INTERVAL must contain '<number> <unit>' pairs".into(),
        ));
    }
    let mut parts = Parts::default();
    let mut compound = tokens.len() > 2;
    for pair in tokens.chunks_exact(2) {
        let number = pair[0];
        match pair[1].to_ascii_lowercase().as_str() {
            "year" | "years" => {
                parts.months = add(
                    parts.months,
                    parse::signed_integer(number, "year")?
                        .checked_mul(12)
                        .ok_or_else(parse::overflow)?,
                )?;
            }
            "month" | "months" => {
                parts.months = add(parts.months, parse::signed_integer(number, "month")?)?;
            }
            "day" | "days" => {
                parts.days = add(parts.days, parse::signed_integer(number, "day")?)?;
            }
            "hour" | "hours" => {
                compound = true;
                parts.nanos = add(
                    parts.nanos,
                    parse::hours_to_nanos(parse::signed_integer(number, "hour")?)?,
                )?;
            }
            "minute" | "minutes" => {
                compound = true;
                parts.nanos = add(
                    parts.nanos,
                    parse::minutes_to_nanos(parse::signed_integer(number, "minute")?)?,
                )?;
            }
            "second" | "seconds" => {
                compound = true;
                let (sign, value) = parse::strip_sign(number)?;
                parts.nanos = add(parts.nanos, parse::seconds(value, false, None)? * sign)?;
            }
            unit => {
                return Err(Error::InvalidArgument(format!(
                    "unknown INTERVAL unit '{unit}'"
                )));
            }
        }
    }
    finish(parts, compound)
}

fn add(left: i128, right: i128) -> Result<i128> {
    left.checked_add(right).ok_or_else(parse::overflow)
}
