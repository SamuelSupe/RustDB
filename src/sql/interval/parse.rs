use crate::{Error, Result};

const NANOS_PER_SECOND: i128 = 1_000_000_000;
const SECONDS_PER_MINUTE: i128 = 60;
const MINUTES_PER_HOUR: i128 = 60;

pub(super) fn clock_nanos(
    hour: &str,
    minute: &str,
    second: Option<&str>,
    fractional_precision: Option<u64>,
) -> Result<i128> {
    let hour = bounded(hour, "hour", 24)?;
    let minute = bounded(minute, "minute", 60)?;
    let second = second
        .map(|value| seconds(value, true, fractional_precision))
        .transpose()?
        .unwrap_or(0);
    hours_to_nanos(hour)?
        .checked_add(minutes_to_nanos(minute)?)
        .and_then(|value| value.checked_add(second))
        .ok_or_else(overflow)
}

pub(super) fn seconds(value: &str, bounded: bool, precision: Option<u64>) -> Result<i128> {
    let split = value.split_once('.');
    let (whole, fraction) = split.unwrap_or((value, ""));
    let whole = unsigned(whole, "second")?;
    if bounded && whole >= 60 {
        return Err(Error::InvalidArgument(format!(
            "second INTERVAL component '{value}' must be below 60"
        )));
    }
    if split.is_some() && fraction.is_empty()
        || !fraction.bytes().all(|byte| byte.is_ascii_digit())
        || value.matches('.').count() > 1
    {
        return Err(Error::InvalidArgument(format!(
            "second INTERVAL value '{value}' is invalid"
        )));
    }
    let mut digits = fraction.as_bytes().iter().copied();
    let mut nanos = 0_i128;
    for _ in 0..9 {
        nanos *= 10;
        if let Some(digit) = digits.next() {
            nanos += i128::from(digit - b'0');
        }
    }
    if digits.next().is_some_and(|digit| digit >= b'5') {
        nanos += 1;
    }
    let precision = precision.unwrap_or(9);
    if precision < 9 {
        let quantum = 10_i128.pow(u32::try_from(9 - precision).expect("precision <= 9"));
        nanos = ((nanos + quantum / 2) / quantum) * quantum;
    }
    whole
        .checked_mul(NANOS_PER_SECOND)
        .and_then(|value| value.checked_add(nanos))
        .ok_or_else(overflow)
}

pub(super) fn strip_sign(value: &str) -> Result<(i128, &str)> {
    let value = value.trim();
    let (sign, value) = match value.as_bytes().first() {
        Some(b'-') => (-1, &value[1..]),
        Some(b'+') => (1, &value[1..]),
        _ => (1, value),
    };
    if value.is_empty() {
        Err(Error::InvalidArgument("INTERVAL value is empty".into()))
    } else {
        Ok((sign, value))
    }
}

pub(super) fn signed_integer(value: &str, unit: &str) -> Result<i128> {
    value.parse().map_err(|_| {
        Error::InvalidArgument(format!("{unit} INTERVAL value '{value}' is not an integer"))
    })
}

pub(super) fn unsigned(value: &str, unit: &str) -> Result<i128> {
    if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(Error::InvalidArgument(format!(
            "{unit} INTERVAL value '{value}' is not an unsigned integer"
        )));
    }
    value
        .parse()
        .map_err(|_| Error::InvalidArgument(format!("{unit} INTERVAL value is out of range")))
}

pub(super) fn bounded(value: &str, unit: &str, upper: i128) -> Result<i128> {
    let value = unsigned(value, unit)?;
    if value >= upper {
        Err(Error::InvalidArgument(format!(
            "{unit} INTERVAL component must be below {upper}"
        )))
    } else {
        Ok(value)
    }
}

pub(super) fn split_once<'a>(
    value: &'a str,
    delimiter: char,
    range: &str,
) -> Result<(&'a str, &'a str)> {
    let (left, right) = value.split_once(delimiter).ok_or_else(|| {
        Error::InvalidArgument(format!("{range} INTERVAL '{value}' has invalid syntax"))
    })?;
    if right.contains(delimiter) {
        return Err(Error::InvalidArgument(format!(
            "{range} INTERVAL '{value}' has invalid syntax"
        )));
    }
    Ok((left, right))
}

pub(super) fn split_space<'a>(value: &'a str, range: &str) -> Result<(&'a str, &'a str)> {
    let mut parts = value.split_whitespace();
    let left = parts.next().unwrap_or_default();
    let right = parts.next().unwrap_or_default();
    if left.is_empty() || right.is_empty() || parts.next().is_some() {
        return Err(Error::InvalidArgument(format!(
            "{range} INTERVAL '{value}' has invalid syntax"
        )));
    }
    Ok((left, right))
}

pub(super) fn colon_parts<'a, const N: usize>(value: &'a str, range: &str) -> Result<[&'a str; N]> {
    value
        .split(':')
        .collect::<Vec<_>>()
        .try_into()
        .map_err(|_| {
            Error::InvalidArgument(format!("{range} INTERVAL '{value}' has invalid syntax"))
        })
}

pub(super) fn hours_to_nanos(hours: i128) -> Result<i128> {
    hours
        .checked_mul(MINUTES_PER_HOUR)
        .and_then(|value| value.checked_mul(SECONDS_PER_MINUTE))
        .and_then(|value| value.checked_mul(NANOS_PER_SECOND))
        .ok_or_else(overflow)
}

pub(super) fn minutes_to_nanos(minutes: i128) -> Result<i128> {
    minutes
        .checked_mul(SECONDS_PER_MINUTE)
        .and_then(|value| value.checked_mul(NANOS_PER_SECOND))
        .ok_or_else(overflow)
}

pub(super) fn validate_fractional_precision(precision: Option<u64>) -> Result<()> {
    if precision.is_some_and(|precision| precision > 9) {
        Err(Error::InvalidArgument(
            "INTERVAL fractional second precision above 9 is not supported".into(),
        ))
    } else {
        Ok(())
    }
}

pub(super) fn check_leading_precision(value: &str, precision: Option<u64>) -> Result<()> {
    if let Some(precision) = precision
        && value.len() > usize::try_from(precision).unwrap_or(usize::MAX)
    {
        return Err(Error::InvalidArgument(format!(
            "INTERVAL leading field '{value}' exceeds precision {precision}"
        )));
    }
    Ok(())
}

pub(super) fn overflow() -> Error {
    Error::InvalidArgument("INTERVAL value is out of Arrow range".into())
}
