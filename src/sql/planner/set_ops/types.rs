use arrow::datatypes::{DataType, IntervalUnit, TimeUnit};

pub(super) fn supports_distinct_key(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Null
            | DataType::Boolean
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Float16
            | DataType::Float32
            | DataType::Float64
            | DataType::Decimal128(_, _)
            | DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Binary
            | DataType::LargeBinary
            | DataType::Date32
            | DataType::Date64
            | DataType::Time32(_)
            | DataType::Time64(_)
            | DataType::Timestamp(_, _)
            | DataType::Interval(_)
            | DataType::FixedSizeBinary(16)
    )
}

pub(super) fn is_nested(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::List(_)
            | DataType::ListView(_)
            | DataType::FixedSizeList(_, _)
            | DataType::LargeList(_)
            | DataType::LargeListView(_)
            | DataType::Struct(_)
            | DataType::Map(_, _)
            | DataType::Union(_, _)
            | DataType::RunEndEncoded(_, _)
    )
}

pub(super) fn common_set_type(
    left: &DataType,
    right: &DataType,
) -> std::result::Result<DataType, String> {
    let left = dictionary_value(left);
    let right = dictionary_value(right);
    // An untyped NULL-only set column still needs an executable physical key
    // type. SQL integer literals are canonicalized to Int64 by the binder, so
    // use the same deterministic default without changing any value.
    if left == &DataType::Null && right == &DataType::Null {
        return Ok(DataType::Int64);
    }
    if left == right || right == &DataType::Null {
        return Ok(left.clone());
    }
    if left == &DataType::Null {
        return Ok(right.clone());
    }
    if let Some(integer) = common_integer(left, right) {
        return Ok(integer);
    }
    if let Some(decimal) = common_decimal_compatible(left, right)? {
        return Ok(decimal);
    }
    match (left, right) {
        (
            DataType::Float16 | DataType::Float32 | DataType::Float64,
            DataType::Float16 | DataType::Float32 | DataType::Float64,
        ) => Ok(DataType::Float64),
        (DataType::Utf8, DataType::LargeUtf8) | (DataType::LargeUtf8, DataType::Utf8) => {
            Ok(DataType::LargeUtf8)
        }
        (DataType::Binary, DataType::LargeBinary) | (DataType::LargeBinary, DataType::Binary) => {
            Ok(DataType::LargeBinary)
        }
        (DataType::Date32, DataType::Timestamp(unit, None))
        | (DataType::Timestamp(unit, None), DataType::Date32) => {
            Ok(DataType::Timestamp(*unit, None))
        }
        (
            DataType::Time32(left_unit) | DataType::Time64(left_unit),
            DataType::Time32(right_unit) | DataType::Time64(right_unit),
        ) => Ok(time_type(finer_time_unit(*left_unit, *right_unit))),
        (DataType::Interval(_), DataType::Interval(_)) => {
            Ok(DataType::Interval(IntervalUnit::MonthDayNano))
        }
        (DataType::Timestamp(left_unit, left_tz), DataType::Timestamp(right_unit, right_tz))
            if left_tz.is_some() == right_tz.is_some() =>
        {
            let timezone = match (left_tz, right_tz) {
                (Some(left), Some(right)) if left == right => Some(left.clone()),
                (Some(_), Some(_)) => Some("UTC".into()),
                (None, None) => None,
                _ => unreachable!("timezone presence was checked"),
            };
            Ok(DataType::Timestamp(
                finer_time_unit(*left_unit, *right_unit),
                timezone,
            ))
        }
        (DataType::Timestamp(_, _), DataType::Timestamp(_, _)) => {
            Err("cannot align TIMESTAMP WITH TIME ZONE with TIMESTAMP WITHOUT TIME ZONE".into())
        }
        _ => Err("no lossless common type exists".into()),
    }
}

fn dictionary_value(data_type: &DataType) -> &DataType {
    match data_type {
        DataType::Dictionary(_, value) => dictionary_value(value),
        other => other,
    }
}

fn common_integer(left: &DataType, right: &DataType) -> Option<DataType> {
    let (left_signed, left_bits) = integer_kind(left)?;
    let (right_signed, right_bits) = integer_kind(right)?;
    if left_signed == right_signed {
        return integer_type(left_signed, left_bits.max(right_bits));
    }
    let (signed_bits, unsigned_bits) = if left_signed {
        (left_bits, right_bits)
    } else {
        (right_bits, left_bits)
    };
    [8, 16, 32, 64]
        .into_iter()
        .find(|bits| *bits >= signed_bits && *bits > unsigned_bits)
        .and_then(|bits| integer_type(true, bits))
        .or(Some(DataType::Decimal128(
            integer_precision(left).max(integer_precision(right)),
            0,
        )))
}

fn integer_kind(data_type: &DataType) -> Option<(bool, u8)> {
    match data_type {
        DataType::Int8 => Some((true, 8)),
        DataType::Int16 => Some((true, 16)),
        DataType::Int32 => Some((true, 32)),
        DataType::Int64 => Some((true, 64)),
        DataType::UInt8 => Some((false, 8)),
        DataType::UInt16 => Some((false, 16)),
        DataType::UInt32 => Some((false, 32)),
        DataType::UInt64 => Some((false, 64)),
        _ => None,
    }
}

fn integer_type(signed: bool, bits: u8) -> Option<DataType> {
    match (signed, bits) {
        (true, 8) => Some(DataType::Int8),
        (true, 16) => Some(DataType::Int16),
        (true, 32) => Some(DataType::Int32),
        (true, 64) => Some(DataType::Int64),
        (false, 8) => Some(DataType::UInt8),
        (false, 16) => Some(DataType::UInt16),
        (false, 32) => Some(DataType::UInt32),
        (false, 64) => Some(DataType::UInt64),
        _ => None,
    }
}

fn integer_precision(data_type: &DataType) -> u8 {
    match data_type {
        DataType::Int8 | DataType::UInt8 => 3,
        DataType::Int16 | DataType::UInt16 => 5,
        DataType::Int32 | DataType::UInt32 => 10,
        DataType::Int64 => 19,
        DataType::UInt64 => 20,
        _ => 0,
    }
}

fn common_decimal_compatible(
    left: &DataType,
    right: &DataType,
) -> std::result::Result<Option<DataType>, String> {
    let Some((left_precision, left_scale)) = decimal_spec(left)? else {
        return Ok(None);
    };
    let Some((right_precision, right_scale)) = decimal_spec(right)? else {
        return Ok(None);
    };
    if !matches!(left, DataType::Decimal128(_, _)) && !matches!(right, DataType::Decimal128(_, _)) {
        return Ok(None);
    }
    let scale = left_scale.max(right_scale);
    let integer_digits = (i16::from(left_precision) - i16::from(left_scale))
        .max(i16::from(right_precision) - i16::from(right_scale));
    let precision = integer_digits + i16::from(scale);
    if precision > 38 {
        return Err(format!(
            "common Decimal128 requires precision {precision}, exceeding 38"
        ));
    }
    Ok(Some(DataType::Decimal128(
        u8::try_from(precision).map_err(|_| "invalid decimal precision".to_owned())?,
        scale,
    )))
}

fn decimal_spec(data_type: &DataType) -> std::result::Result<Option<(u8, i8)>, String> {
    if let DataType::Decimal128(precision, scale) = data_type {
        if *precision == 0
            || *precision > 38
            || *scale < 0
            || i16::from(*scale) > i16::from(*precision)
        {
            return Err(format!(
                "invalid Decimal128 precision/scale ({precision}, {scale})"
            ));
        }
        return Ok(Some((*precision, *scale)));
    }
    let precision = integer_precision(data_type);
    Ok((precision != 0).then_some((precision, 0)))
}

fn finer_time_unit(left: TimeUnit, right: TimeUnit) -> TimeUnit {
    if time_unit_rank(left) >= time_unit_rank(right) {
        left
    } else {
        right
    }
}

fn time_type(unit: TimeUnit) -> DataType {
    match unit {
        TimeUnit::Second | TimeUnit::Millisecond => DataType::Time32(unit),
        TimeUnit::Microsecond | TimeUnit::Nanosecond => DataType::Time64(unit),
    }
}

fn time_unit_rank(unit: TimeUnit) -> u8 {
    match unit {
        TimeUnit::Second => 0,
        TimeUnit::Millisecond => 1,
        TimeUnit::Microsecond => 2,
        TimeUnit::Nanosecond => 3,
    }
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::{DataType, Field, TimeUnit};

    use super::{common_set_type, is_nested, supports_distinct_key};

    #[test]
    fn finds_lossless_common_scalar_types() {
        assert_eq!(
            common_set_type(&DataType::Null, &DataType::Null).unwrap(),
            DataType::Int64
        );
        assert_eq!(
            common_set_type(&DataType::Int16, &DataType::UInt16).unwrap(),
            DataType::Int32
        );
        assert_eq!(
            common_set_type(&DataType::Int64, &DataType::UInt64).unwrap(),
            DataType::Decimal128(20, 0)
        );
        assert_eq!(
            common_set_type(&DataType::Decimal128(10, 2), &DataType::Decimal128(12, 4),).unwrap(),
            DataType::Decimal128(12, 4)
        );
        assert_eq!(
            common_set_type(
                &DataType::Date32,
                &DataType::Timestamp(TimeUnit::Microsecond, None),
            )
            .unwrap(),
            DataType::Timestamp(TimeUnit::Microsecond, None)
        );
        assert_eq!(
            common_set_type(
                &DataType::Time32(TimeUnit::Millisecond),
                &DataType::Time64(TimeUnit::Nanosecond),
            )
            .unwrap(),
            DataType::Time64(TimeUnit::Nanosecond)
        );
        assert_eq!(
            common_set_type(
                &DataType::Timestamp(TimeUnit::Millisecond, Some("America/New_York".into()),),
                &DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            )
            .unwrap(),
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
        );
        assert_eq!(
            common_set_type(
                &DataType::Interval(arrow::datatypes::IntervalUnit::YearMonth),
                &DataType::Interval(arrow::datatypes::IntervalUnit::DayTime),
            )
            .unwrap(),
            DataType::Interval(arrow::datatypes::IntervalUnit::MonthDayNano)
        );
        for data_type in [
            DataType::Time64(TimeUnit::Nanosecond),
            DataType::FixedSizeBinary(16),
            DataType::Interval(arrow::datatypes::IntervalUnit::MonthDayNano),
        ] {
            assert!(supports_distinct_key(&data_type));
        }
    }

    #[test]
    fn rejects_lossy_or_nested_distinct_types() {
        assert!(common_set_type(&DataType::Int64, &DataType::Float64).is_err());
        let nested = DataType::List(Field::new("item", DataType::Int64, true).into());
        assert!(is_nested(&nested));
        assert!(!supports_distinct_key(&nested));
    }
}
