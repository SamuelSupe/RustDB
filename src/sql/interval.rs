mod parse;
mod qualified;
mod unqualified;

use sqlparser::ast::{DateTimeField, IntervalFields};

use crate::{Error, Result};

use super::ScalarValue;

#[derive(Default)]
struct Parts {
    months: i128,
    days: i128,
    nanos: i128,
}

pub(super) fn parse_interval(
    raw: &str,
    leading: Option<&DateTimeField>,
    last: Option<&DateTimeField>,
    leading_precision: Option<u64>,
    fractional_precision: Option<u64>,
) -> Result<ScalarValue> {
    if leading_precision == Some(0) {
        return Err(Error::InvalidArgument(
            "INTERVAL leading precision must be positive".into(),
        ));
    }
    parse::validate_fractional_precision(fractional_precision)?;
    match (leading, last) {
        (None, None) => unqualified::parse(raw),
        (Some(leading), last) => {
            qualified::parse(raw, leading, last, leading_precision, fractional_precision)
        }
        (None, Some(_)) => Err(Error::InvalidArgument(
            "INTERVAL trailing field requires a leading field".into(),
        )),
    }
}

pub(super) fn parse_cast_interval(
    raw: &str,
    fields: Option<&IntervalFields>,
    fractional_precision: Option<u64>,
) -> Result<ScalarValue> {
    let (leading, last) = match fields {
        None => return parse_interval(raw, None, None, None, fractional_precision),
        Some(IntervalFields::Year) => (DateTimeField::Year, None),
        Some(IntervalFields::Month) => (DateTimeField::Month, None),
        Some(IntervalFields::Day) => (DateTimeField::Day, None),
        Some(IntervalFields::Hour) => (DateTimeField::Hour, None),
        Some(IntervalFields::Minute) => (DateTimeField::Minute, None),
        Some(IntervalFields::Second) => (DateTimeField::Second, None),
        Some(IntervalFields::YearToMonth) => (DateTimeField::Year, Some(DateTimeField::Month)),
        Some(IntervalFields::DayToHour) => (DateTimeField::Day, Some(DateTimeField::Hour)),
        Some(IntervalFields::DayToMinute) => (DateTimeField::Day, Some(DateTimeField::Minute)),
        Some(IntervalFields::DayToSecond) => (DateTimeField::Day, Some(DateTimeField::Second)),
        Some(IntervalFields::HourToMinute) => (DateTimeField::Hour, Some(DateTimeField::Minute)),
        Some(IntervalFields::HourToSecond) => (DateTimeField::Hour, Some(DateTimeField::Second)),
        Some(IntervalFields::MinuteToSecond) => {
            (DateTimeField::Minute, Some(DateTimeField::Second))
        }
    };
    parse_interval(
        raw,
        Some(&leading),
        last.as_ref(),
        None,
        fractional_precision,
    )
}

fn finish(parts: Parts, force_month_day_nano: bool) -> Result<ScalarValue> {
    let months = i32::try_from(parts.months).map_err(|_| parse::overflow())?;
    let days = i32::try_from(parts.days).map_err(|_| parse::overflow())?;
    let nanoseconds = i64::try_from(parts.nanos).map_err(|_| parse::overflow())?;
    if !force_month_day_nano && days == 0 && nanoseconds == 0 {
        Ok(ScalarValue::MonthInterval(months))
    } else if !force_month_day_nano && months == 0 && nanoseconds == 0 {
        Ok(ScalarValue::DayInterval(days))
    } else {
        Ok(ScalarValue::MonthDayNanoInterval {
            months,
            days,
            nanoseconds,
        })
    }
}

#[cfg(test)]
mod tests {
    use sqlparser::ast::DateTimeField;

    use super::parse_interval;
    use crate::sql::ScalarValue;

    #[test]
    fn parses_standard_compound_ranges() {
        assert_eq!(
            parse_interval(
                "2-3",
                Some(&DateTimeField::Year),
                Some(&DateTimeField::Month),
                None,
                None,
            )
            .unwrap(),
            ScalarValue::MonthInterval(27)
        );
        assert_eq!(
            parse_interval(
                "-1 02:03:04.5",
                Some(&DateTimeField::Day),
                Some(&DateTimeField::Second),
                None,
                None,
            )
            .unwrap(),
            ScalarValue::MonthDayNanoInterval {
                months: 0,
                days: -1,
                nanoseconds: -7_384_500_000_000,
            }
        );
    }

    #[test]
    fn rejects_out_of_range_trailing_fields() {
        assert!(
            parse_interval(
                "1 24:00",
                Some(&DateTimeField::Day),
                Some(&DateTimeField::Minute),
                None,
                None,
            )
            .is_err()
        );
        assert!(
            parse_interval(
                "1-12",
                Some(&DateTimeField::Year),
                Some(&DateTimeField::Month),
                None,
                None,
            )
            .is_err()
        );
    }

    #[test]
    fn cast_style_compact_intervals_infer_their_range() {
        assert_eq!(
            super::parse_cast_interval("2:03.5", None, None).unwrap(),
            ScalarValue::MonthDayNanoInterval {
                months: 0,
                days: 0,
                nanoseconds: 123_500_000_000,
            }
        );
        assert_eq!(
            super::parse_cast_interval("1-2", None, None).unwrap(),
            ScalarValue::MonthInterval(14)
        );
    }
}
