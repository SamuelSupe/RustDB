use std::sync::Arc;

use arrow::{
    array::{
        Array, ArrayRef, StringArray, TimestampMicrosecondArray, TimestampMillisecondArray,
        TimestampNanosecondArray, TimestampSecondArray,
    },
    datatypes::{DataType, TimeUnit},
};
use chrono::{LocalResult, NaiveDateTime, TimeZone, Utc};
use chrono_tz::Tz;

use crate::{Error, Result};

pub(super) fn at_time_zone(
    timezone: Tz,
    attach: bool,
    array: &ArrayRef,
    output_type: &DataType,
) -> Result<ArrayRef> {
    let DataType::Timestamp(unit, _) = array.data_type() else {
        return Err(Error::Internal(
            "AT TIME ZONE input is not TIMESTAMP".into(),
        ));
    };
    let values = (0..array.len())
        .map(|row| {
            if array.is_null(row) {
                return Ok(None);
            }
            let super::super::value::CellValue::Int64(value) =
                super::super::value::cell(array, row)?
            else {
                return Err(Error::Internal("TIMESTAMP cell is not Int64".into()));
            };
            let naive = numeric_to_naive(value, *unit)?;
            let shifted = if attach {
                resolve_local(timezone, naive, row)?
                    .with_timezone(&Utc)
                    .naive_utc()
            } else {
                Utc.from_utc_datetime(&naive)
                    .with_timezone(&timezone)
                    .naive_local()
            };
            Ok(Some(naive_to_numeric(shifted, *unit)?))
        })
        .collect::<Result<Vec<_>>>()?;
    timestamp_array(values, output_type)
}

pub(super) fn string_to_timestamp(array: &ArrayRef, target: &DataType) -> Result<ArrayRef> {
    let values = array
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| Error::Internal(format!("expected Utf8, got {}", array.data_type())))?;
    let DataType::Timestamp(unit, _) = target else {
        return Err(Error::Internal(
            "TIMESTAMPTZ target is not TIMESTAMP".into(),
        ));
    };
    let precision = precision(*unit);
    let output = (0..values.len())
        .map(|row| {
            values
                .is_valid(row)
                .then(|| {
                    crate::sql::temporal::parse_timestamptz_at_precision(
                        values.value(row),
                        precision,
                    )
                    .map(|value| value.0)
                })
                .transpose()
                .map_err(|error| {
                    Error::Execution(format!(
                        "strict CAST to TIMESTAMPTZ failed at row {row}: {error}"
                    ))
                })
        })
        .collect::<Result<Vec<_>>>()?;
    timestamp_array(output, target)
}

pub(super) fn timestamp_to_string(array: &ArrayRef) -> Result<ArrayRef> {
    let DataType::Timestamp(unit, Some(zone)) = array.data_type() else {
        return Err(Error::Internal(
            "timezone formatter received a naive TIMESTAMP".into(),
        ));
    };
    let timezone = zone.parse::<Tz>().map_err(|_| {
        Error::Execution(format!("TIMESTAMP carries unknown IANA timezone '{zone}'"))
    })?;
    let values = (0..array.len())
        .map(|row| {
            if array.is_null(row) {
                return Ok(None);
            }
            let super::super::value::CellValue::Int64(value) =
                super::super::value::cell(array, row)?
            else {
                return Err(Error::Internal("TIMESTAMP cell is not Int64".into()));
            };
            let local = Utc
                .from_utc_datetime(&numeric_to_naive(value, *unit)?)
                .with_timezone(&timezone);
            let formatted = local.format("%Y-%m-%d %H:%M:%S%.f%:z").to_string();
            Ok(Some(trim_fraction(formatted)))
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(Arc::new(StringArray::from_iter(
        values.iter().map(Option::as_deref),
    )))
}

fn resolve_local(timezone: Tz, value: NaiveDateTime, row: usize) -> Result<chrono::DateTime<Tz>> {
    match timezone.from_local_datetime(&value) {
        LocalResult::Single(value) => Ok(value),
        LocalResult::Ambiguous(_, later) => Ok(later),
        LocalResult::None => Err(Error::Execution(format!(
            "AT TIME ZONE '{timezone}' encountered a nonexistent local time at row {row}"
        ))),
    }
}

fn numeric_to_naive(value: i64, unit: TimeUnit) -> Result<NaiveDateTime> {
    let divisor = units_per_second(unit);
    let seconds = value.div_euclid(divisor);
    let remainder = value.rem_euclid(divisor);
    let nanos = remainder
        .checked_mul(1_000_000_000 / divisor)
        .ok_or_else(|| Error::Execution("TIMESTAMP fraction overflowed".into()))?;
    chrono::DateTime::from_timestamp(seconds, nanos as u32)
        .map(|value| value.naive_utc())
        .ok_or_else(|| Error::Execution("TIMESTAMP is outside the timezone library range".into()))
}

fn naive_to_numeric(value: NaiveDateTime, unit: TimeUnit) -> Result<i64> {
    let value = value.and_utc();
    value
        .timestamp()
        .checked_mul(units_per_second(unit))
        .and_then(|seconds| {
            seconds.checked_add(
                i64::from(value.timestamp_subsec_nanos())
                    / (1_000_000_000 / units_per_second(unit)),
            )
        })
        .ok_or_else(|| Error::Execution("timezone conversion overflowed TIMESTAMP".into()))
}

fn timestamp_array(values: Vec<Option<i64>>, output: &DataType) -> Result<ArrayRef> {
    let DataType::Timestamp(unit, timezone) = output else {
        return Err(Error::Internal("timezone output is not TIMESTAMP".into()));
    };
    Ok(match unit {
        TimeUnit::Second => {
            Arc::new(TimestampSecondArray::from(values).with_timezone_opt(timezone.clone()))
                as ArrayRef
        }
        TimeUnit::Millisecond => {
            Arc::new(TimestampMillisecondArray::from(values).with_timezone_opt(timezone.clone()))
        }
        TimeUnit::Microsecond => {
            Arc::new(TimestampMicrosecondArray::from(values).with_timezone_opt(timezone.clone()))
        }
        TimeUnit::Nanosecond => {
            Arc::new(TimestampNanosecondArray::from(values).with_timezone_opt(timezone.clone()))
        }
    })
}

fn units_per_second(unit: TimeUnit) -> i64 {
    match unit {
        TimeUnit::Second => 1,
        TimeUnit::Millisecond => 1_000,
        TimeUnit::Microsecond => 1_000_000,
        TimeUnit::Nanosecond => 1_000_000_000,
    }
}

fn precision(unit: TimeUnit) -> u64 {
    match unit {
        TimeUnit::Second => 0,
        TimeUnit::Millisecond => 3,
        TimeUnit::Microsecond => 6,
        TimeUnit::Nanosecond => 9,
    }
}

fn trim_fraction(value: String) -> String {
    let Some(dot) = value.find('.') else {
        return value;
    };
    let offset = value[dot..]
        .find(['+', '-'])
        .map(|index| dot + index)
        .unwrap_or(value.len());
    let (clock, suffix) = value.split_at(offset);
    format!(
        "{}{suffix}",
        clock.trim_end_matches('0').trim_end_matches('.')
    )
}
