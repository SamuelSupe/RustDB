use sqlparser::ast::DateTimeField;

use crate::{Error, Result};

use super::{Parts, finish, parse};
use crate::sql::ScalarValue;

pub(super) fn parse(
    raw: &str,
    leading: &DateTimeField,
    last: Option<&DateTimeField>,
    leading_precision: Option<u64>,
    fractional_precision: Option<u64>,
) -> Result<ScalarValue> {
    let (sign, value) = parse::strip_sign(raw)?;
    let leading_text = value.split(['-', ' ', ':', '.']).next().unwrap_or_default();
    parse::check_leading_precision(leading_text, leading_precision)?;
    let (mut parts, compound) = fields(value, leading, last, fractional_precision)?;
    parts.months *= sign;
    parts.days *= sign;
    parts.nanos *= sign;
    finish(parts, compound)
}

fn fields(
    value: &str,
    leading: &DateTimeField,
    last: Option<&DateTimeField>,
    precision: Option<u64>,
) -> Result<(Parts, bool)> {
    Ok(match (leading, last) {
        (DateTimeField::Year, None) => (months(parse::unsigned(value, "year")?, 12)?, false),
        (DateTimeField::Month, None) => (months(parse::unsigned(value, "month")?, 1)?, false),
        (DateTimeField::Day, None) => (days(parse::unsigned(value, "day")?), false),
        (DateTimeField::Hour, None) => (
            nanos(parse::hours_to_nanos(parse::unsigned(value, "hour")?)?),
            true,
        ),
        (DateTimeField::Minute, None) => (
            nanos(parse::minutes_to_nanos(parse::unsigned(value, "minute")?)?),
            true,
        ),
        (DateTimeField::Second, None) => (nanos(parse::seconds(value, false, precision)?), true),
        (DateTimeField::Year, Some(DateTimeField::Month)) => {
            let (year, month) = parse::split_once(value, '-', "YEAR TO MONTH")?;
            let month = parse::bounded(month, "month", 12)?;
            let total = parse::unsigned(year, "year")?
                .checked_mul(12)
                .and_then(|value| value.checked_add(month))
                .ok_or_else(parse::overflow)?;
            (months(total, 1)?, false)
        }
        (DateTimeField::Day, Some(DateTimeField::Hour)) => {
            let (day, hour) = parse::split_space(value, "DAY TO HOUR")?;
            (
                day_time(
                    parse::unsigned(day, "day")?,
                    parse::hours_to_nanos(parse::bounded(hour, "hour", 24)?)?,
                ),
                true,
            )
        }
        (DateTimeField::Day, Some(DateTimeField::Minute)) => {
            let (day, time) = parse::split_space(value, "DAY TO MINUTE")?;
            let [hour, minute] = parse::colon_parts::<2>(time, "DAY TO MINUTE")?;
            (
                day_time(
                    parse::unsigned(day, "day")?,
                    parse::clock_nanos(hour, minute, None, precision)?,
                ),
                true,
            )
        }
        (DateTimeField::Day, Some(DateTimeField::Second)) => {
            let (day, time) = parse::split_space(value, "DAY TO SECOND")?;
            let [hour, minute, second] = parse::colon_parts::<3>(time, "DAY TO SECOND")?;
            (
                day_time(
                    parse::unsigned(day, "day")?,
                    parse::clock_nanos(hour, minute, Some(second), precision)?,
                ),
                true,
            )
        }
        (DateTimeField::Hour, Some(DateTimeField::Minute)) => {
            let [hour, minute] = parse::colon_parts::<2>(value, "HOUR TO MINUTE")?;
            (
                nanos(add(
                    parse::hours_to_nanos(parse::unsigned(hour, "hour")?)?,
                    parse::minutes_to_nanos(parse::bounded(minute, "minute", 60)?)?,
                )?),
                true,
            )
        }
        (DateTimeField::Hour, Some(DateTimeField::Second)) => {
            let [hour, minute, second] = parse::colon_parts::<3>(value, "HOUR TO SECOND")?;
            let clock = add(
                parse::hours_to_nanos(parse::unsigned(hour, "hour")?)?,
                parse::minutes_to_nanos(parse::bounded(minute, "minute", 60)?)?,
            )?;
            (
                nanos(add(clock, parse::seconds(second, true, precision)?)?),
                true,
            )
        }
        (DateTimeField::Minute, Some(DateTimeField::Second)) => {
            let [minute, second] = parse::colon_parts::<2>(value, "MINUTE TO SECOND")?;
            (
                nanos(add(
                    parse::minutes_to_nanos(parse::unsigned(minute, "minute")?)?,
                    parse::seconds(second, true, precision)?,
                )?),
                true,
            )
        }
        _ => {
            return Err(Error::Unsupported(format!(
                "INTERVAL range {}{} is not supported",
                leading,
                last.map(|field| format!(" TO {field}")).unwrap_or_default()
            )));
        }
    })
}

fn months(value: i128, multiplier: i128) -> Result<Parts> {
    Ok(Parts {
        months: value.checked_mul(multiplier).ok_or_else(parse::overflow)?,
        ..Parts::default()
    })
}

fn days(value: i128) -> Parts {
    Parts {
        days: value,
        ..Parts::default()
    }
}

fn nanos(value: i128) -> Parts {
    Parts {
        nanos: value,
        ..Parts::default()
    }
}

fn day_time(day: i128, nanos: i128) -> Parts {
    Parts {
        days: day,
        nanos,
        ..Parts::default()
    }
}

fn add(left: i128, right: i128) -> Result<i128> {
    left.checked_add(right).ok_or_else(parse::overflow)
}
