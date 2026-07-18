use arrow::datatypes::TimeUnit;
use chrono::{LocalResult, NaiveDateTime, TimeZone, Utc};

use crate::{Error, Result};

pub(crate) const MICROS_PER_DAY: i64 = 86_400_000_000;

pub(crate) fn parse_date32(value: &str) -> Result<i32> {
    let (year, month, day) = parse_date(value)?;
    days_from_civil(year, month, day)
        .try_into()
        .map_err(|_| invalid_date(value))
}

#[cfg(test)]
pub(crate) fn parse_timestamp_microsecond(value: &str) -> Result<i64> {
    parse_timestamp_at_precision(value, 6).map(|(value, _)| value)
}

pub(crate) fn parse_timestamp_at_precision(value: &str, precision: u64) -> Result<(i64, TimeUnit)> {
    if precision > 9 {
        return Err(Error::InvalidArgument(
            "TIMESTAMP precision above 9 is not supported".into(),
        ));
    }
    let value = value.trim();
    let (date, time) = value
        .split_once(' ')
        .or_else(|| value.split_once('T'))
        .unwrap_or((value, "00:00:00"));
    let days = i64::from(parse_date32(date)?);
    let (clock, fraction) = time.split_once('.').unwrap_or((time, ""));
    let mut parts = clock.split(':');
    let hour = parse_part(parts.next(), "hour", value)?;
    let minute = parse_part(parts.next(), "minute", value)?;
    let second = parse_part(parts.next(), "second", value)?;
    if parts.next().is_some() || hour > 23 || minute > 59 || second > 59 {
        return Err(invalid_timestamp(value));
    }
    let nanos = parse_fraction_nanos(fraction, value, "TIMESTAMP")?;
    let seconds = i128::from(hour * 3_600 + minute * 60 + second);
    let timestamp =
        i128::from(days) * 86_400_000_000_000_i128 + seconds * 1_000_000_000 + i128::from(nanos);
    scaled_temporal(timestamp, precision, "TIMESTAMP", value)
}

pub(crate) fn parse_time_at_precision(value: &str, precision: u64) -> Result<(i64, TimeUnit)> {
    if precision > 9 {
        return Err(Error::InvalidArgument(
            "TIME precision above 9 is not supported".into(),
        ));
    }
    let value = value.trim();
    let (clock, fraction) = value.split_once('.').unwrap_or((value, ""));
    let mut parts = clock.split(':');
    let hour = parse_part(parts.next(), "hour", value)?;
    let minute = parse_part(parts.next(), "minute", value)?;
    let second = parse_part(parts.next(), "second", value)?;
    if parts.next().is_some() || hour > 23 || minute > 59 || second > 59 {
        return Err(Error::InvalidArgument(format!(
            "TIME literal '{value}' is not a valid time"
        )));
    }
    let nanos = parse_fraction_nanos(fraction, value, "TIME")?;
    let total = i128::from(hour * 3_600 + minute * 60 + second) * 1_000_000_000 + i128::from(nanos);
    let output = scaled_temporal(total, precision, "TIME", value)?;
    if to_nanos(output.0, output.1)? >= 86_400_000_000_000 {
        return Err(Error::InvalidArgument(format!(
            "TIME literal '{value}' rounds past the end of day"
        )));
    }
    Ok(output)
}

pub(crate) fn parse_timestamptz_at_precision(
    value: &str,
    precision: u64,
) -> Result<(i64, TimeUnit)> {
    let value = value.trim();
    let normalized = if value.contains('T') {
        value.to_owned()
    } else if let Some((date, rest)) = value.split_once(' ') {
        format!("{date}T{rest}")
    } else {
        value.to_owned()
    };
    let datetime = chrono::DateTime::parse_from_rfc3339(&normalized)
        .map(|value| value.with_timezone(&Utc))
        .or_else(|offset_error| {
            let (local, zone) = value.rsplit_once(' ').ok_or(offset_error)?;
            let timezone = zone.parse::<chrono_tz::Tz>().map_err(|_| offset_error)?;
            let naive = NaiveDateTime::parse_from_str(local, "%Y-%m-%d %H:%M:%S%.f")
                .or_else(|_| NaiveDateTime::parse_from_str(local, "%Y-%m-%dT%H:%M:%S%.f"))
                .map_err(|_| offset_error)?;
            match timezone.from_local_datetime(&naive) {
                LocalResult::Single(value) => Ok(value.with_timezone(&Utc)),
                LocalResult::Ambiguous(_, later) => Ok(later.with_timezone(&Utc)),
                LocalResult::None => Err(offset_error),
            }
        })
        .map_err(|_| {
            Error::InvalidArgument(format!(
                "TIMESTAMPTZ literal '{value}' requires an RFC3339 offset or an IANA timezone"
            ))
        })?;
    let nanos = i128::from(datetime.timestamp()) * 1_000_000_000
        + i128::from(datetime.timestamp_subsec_nanos());
    scaled_temporal(nanos, precision, "TIMESTAMPTZ", value)
}

pub(crate) fn format_date32(days: i32) -> String {
    let (year, month, day) = civil_from_days(i64::from(days));
    format!("{year:04}-{month:02}-{day:02}")
}

pub(crate) fn format_timestamp_microsecond(value: i64) -> String {
    format_timestamp(value, TimeUnit::Microsecond)
}

pub(crate) fn format_timestamp(value: i64, unit: TimeUnit) -> String {
    let units_per_second = match unit {
        TimeUnit::Second => 1,
        TimeUnit::Millisecond => 1_000,
        TimeUnit::Microsecond => 1_000_000,
        TimeUnit::Nanosecond => 1_000_000_000,
    };
    let seconds = value.div_euclid(units_per_second);
    let subsecond = value.rem_euclid(units_per_second);
    let days = seconds.div_euclid(86_400);
    let within_day_seconds = seconds.rem_euclid(86_400);
    let (year, month, day) = civil_from_days(days);
    let hour = within_day_seconds / 3_600;
    let minute = within_day_seconds % 3_600 / 60;
    let second = within_day_seconds % 60;
    let mut output = format!("{year:04}-{month:02}-{day:02} {hour:02}:{minute:02}:{second:02}");
    if subsecond != 0 {
        let width = match unit {
            TimeUnit::Second => 0,
            TimeUnit::Millisecond => 3,
            TimeUnit::Microsecond => 6,
            TimeUnit::Nanosecond => 9,
        };
        let fraction = format!("{subsecond:0width$}");
        output.push('.');
        output.push_str(fraction.trim_end_matches('0'));
    }
    output
}

pub(crate) fn format_time(value: i64, unit: TimeUnit) -> Result<String> {
    let nanos = to_nanos(value, unit)?;
    if !(0..86_400_000_000_000).contains(&nanos) {
        return Err(Error::Execution("TIME value is outside one day".into()));
    }
    let seconds = nanos / 1_000_000_000;
    let hour = seconds / 3_600;
    let minute = seconds % 3_600 / 60;
    let second = seconds % 60;
    let fraction = nanos % 1_000_000_000;
    let mut output = format!("{hour:02}:{minute:02}:{second:02}");
    if fraction != 0 {
        let fraction = format!("{fraction:09}");
        output.push('.');
        output.push_str(fraction.trim_end_matches('0'));
    }
    Ok(output)
}

pub(crate) fn timestamp_to_date32(value: i64) -> Result<i32> {
    value
        .div_euclid(MICROS_PER_DAY)
        .try_into()
        .map_err(|_| Error::Execution("TIMESTAMP is outside the Date32 range".into()))
}

pub(crate) fn date32_to_timestamp(value: i32) -> Result<i64> {
    i64::from(value)
        .checked_mul(MICROS_PER_DAY)
        .ok_or_else(|| Error::Execution("DATE is outside the microsecond TIMESTAMP range".into()))
}

pub(crate) fn truncate_date(days: i32, part: super::DateTimePart) -> i32 {
    let (year, month, _) = civil_from_days(i64::from(days));
    match part {
        super::DateTimePart::Year => days_from_civil(year, 1, 1) as i32,
        super::DateTimePart::Month => days_from_civil(year, month, 1) as i32,
        _ => days,
    }
}

pub(crate) fn truncate_timestamp(value: i64, part: super::DateTimePart) -> Result<i64> {
    let days = value.div_euclid(MICROS_PER_DAY);
    let within_day = value.rem_euclid(MICROS_PER_DAY);
    let (year, month, _) = civil_from_days(days);
    let day_start = match part {
        super::DateTimePart::Year => days_from_civil(year, 1, 1),
        super::DateTimePart::Month => days_from_civil(year, month, 1),
        _ => days,
    };
    let within_day = match part {
        super::DateTimePart::Year | super::DateTimePart::Month | super::DateTimePart::Day => 0,
        super::DateTimePart::Hour => within_day / 3_600_000_000 * 3_600_000_000,
        super::DateTimePart::Minute => within_day / 60_000_000 * 60_000_000,
        super::DateTimePart::Second => within_day / 1_000_000 * 1_000_000,
    };
    day_start
        .checked_mul(MICROS_PER_DAY)
        .and_then(|value| value.checked_add(within_day))
        .ok_or_else(|| Error::Execution("date_trunc TIMESTAMP result is out of range".into()))
}

fn parse_date(value: &str) -> Result<(i32, u32, u32)> {
    let (year_sign, value_without_sign) = value
        .strip_prefix('-')
        .map_or((1_i64, value), |value| (-1_i64, value));
    let mut parts = value_without_sign.split('-');
    let year = parts
        .next()
        .filter(|part| !part.is_empty())
        .ok_or_else(|| invalid_date(value))?
        .parse::<i64>()
        .ok()
        .and_then(|year| year.checked_mul(year_sign))
        .and_then(|year| i32::try_from(year).ok())
        .ok_or_else(|| Error::InvalidArgument(format!("'{value}' has an invalid year")))?;
    let month = parse_part(parts.next(), "month", value)?;
    let day = parse_part(parts.next(), "day", value)?;
    if parts.next().is_some()
        || !(1..=12).contains(&month)
        || day == 0
        || day > days_in_month(year, month)
    {
        return Err(invalid_date(value));
    }
    Ok((year, month, day))
}

fn parse_part(part: Option<&str>, name: &str, value: &str) -> Result<u32> {
    part.filter(|part| !part.is_empty())
        .ok_or_else(|| Error::InvalidArgument(format!("'{value}' has no {name}")))?
        .parse()
        .map_err(|_| Error::InvalidArgument(format!("'{value}' has an invalid {name}")))
}

fn parse_fraction_nanos(fraction: &str, value: &str, kind: &str) -> Result<i64> {
    if fraction.is_empty() {
        return Ok(0);
    }
    if fraction.len() > 9 || !fraction.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(Error::InvalidArgument(format!(
            "{kind} literal '{value}' must have at most 9 fractional digits"
        )));
    }
    let parsed: i64 = fraction.parse().map_err(|_| invalid_timestamp(value))?;
    Ok(parsed * 10_i64.pow(9 - fraction.len() as u32))
}

fn scaled_temporal(
    nanos: i128,
    precision: u64,
    kind: &str,
    literal: &str,
) -> Result<(i64, TimeUnit)> {
    let precision_factor = 10_i128.pow(9 - precision as u32);
    let magnitude = nanos.abs();
    let rounded = (magnitude + precision_factor / 2) / precision_factor * precision_factor;
    let rounded = if nanos.is_negative() {
        -rounded
    } else {
        rounded
    };
    let unit = match precision {
        0 => TimeUnit::Second,
        1..=3 => TimeUnit::Millisecond,
        4..=6 => TimeUnit::Microsecond,
        _ => TimeUnit::Nanosecond,
    };
    let divisor = match unit {
        TimeUnit::Second => 1_000_000_000,
        TimeUnit::Millisecond => 1_000_000,
        TimeUnit::Microsecond => 1_000,
        TimeUnit::Nanosecond => 1,
    };
    let value = i64::try_from(rounded / divisor).map_err(|_| {
        Error::InvalidArgument(format!("{kind} literal '{literal}' is out of range"))
    })?;
    Ok((value, unit))
}

fn to_nanos(value: i64, unit: TimeUnit) -> Result<i64> {
    let factor = match unit {
        TimeUnit::Second => 1_000_000_000,
        TimeUnit::Millisecond => 1_000_000,
        TimeUnit::Microsecond => 1_000,
        TimeUnit::Nanosecond => 1,
    };
    value
        .checked_mul(factor)
        .ok_or_else(|| Error::Execution("temporal value overflows nanoseconds".into()))
}

fn days_in_month(year: i32, month: u32) -> u32 {
    match month {
        2 if year % 4 == 0 && (year % 100 != 0 || year % 400 == 0) => 29,
        2 => 28,
        4 | 6 | 9 | 11 => 30,
        _ => 31,
    }
}

fn days_from_civil(year: i32, month: u32, day: u32) -> i64 {
    let adjusted_year = i64::from(year) - i64::from(month <= 2);
    let era = if adjusted_year >= 0 {
        adjusted_year
    } else {
        adjusted_year - 399
    } / 400;
    let year_of_era = adjusted_year - era * 400;
    let shifted_month = i64::from(month) + if month > 2 { -3 } else { 9 };
    let day_of_year = (153 * shifted_month + 2) / 5 + i64::from(day) - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    era * 146_097 + day_of_era - 719_468
}

fn civil_from_days(days: i64) -> (i32, u32, u32) {
    let days = days + 719_468;
    let era = if days >= 0 { days } else { days - 146_096 } / 146_097;
    let day_of_era = days - era * 146_097;
    let year_of_era =
        (day_of_era - day_of_era / 1_460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let mut year = year_of_era + era * 400;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_prime = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * month_prime + 2) / 5 + 1;
    let month = month_prime + if month_prime < 10 { 3 } else { -9 };
    year += i64::from(month <= 2);
    (year as i32, month as u32, day as u32)
}

fn invalid_date(value: &str) -> Error {
    Error::InvalidArgument(format!(
        "DATE literal '{value}' is not a valid calendar date"
    ))
}

fn invalid_timestamp(value: &str) -> Error {
    Error::InvalidArgument(format!(
        "TIMESTAMP literal '{value}' is not a valid timestamp"
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn timestamp_round_trips_at_microsecond_precision() {
        for value in [
            "1970-01-01 00:00:00",
            "1969-12-31 23:59:59.999999",
            "2000-02-29 12:34:56.1234",
        ] {
            let parsed = parse_timestamp_microsecond(value).unwrap();
            assert_eq!(
                parse_timestamp_microsecond(&format_timestamp_microsecond(parsed)).unwrap(),
                parsed
            );
        }
    }

    #[test]
    fn date32_boundaries_round_trip() {
        for value in [i32::MIN, i32::MAX] {
            assert_eq!(parse_date32(&format_date32(value)).unwrap(), value);
        }
    }

    #[test]
    fn timestamp_boundaries_round_trip() {
        for value in [i64::MIN, i64::MAX] {
            assert_eq!(
                parse_timestamp_microsecond(&format_timestamp_microsecond(value)).unwrap(),
                value
            );
        }
    }
}
