use arrow::datatypes::{DataType, TimeUnit};

use super::ParquetSchemaMode;

pub(super) fn canonical_type(data_type: &DataType) -> DataType {
    match data_type {
        DataType::Dictionary(_, value) => canonical_type(value),
        _ => data_type.clone(),
    }
}

pub(super) fn merge_types(
    left: &DataType,
    right: &DataType,
    mode: ParquetSchemaMode,
) -> std::result::Result<DataType, String> {
    if left == right {
        if matches!(left, DataType::Decimal128(_, _)) {
            decimal_spec(left)?;
        }
        return Ok(left.clone());
    }
    if mode != ParquetSchemaMode::SafeWidening {
        return Err(format!("types differ under {mode:?} mode"));
    }
    if left == &DataType::Null {
        return Ok(right.clone());
    }
    if right == &DataType::Null {
        return Ok(left.clone());
    }
    if let Some(data_type) = merge_integers(left, right) {
        return Ok(data_type);
    }
    if let Some(data_type) = merge_decimal_compatible(left, right)? {
        return Ok(data_type);
    }

    match (left, right) {
        (DataType::Float16, DataType::Float32)
        | (DataType::Float32, DataType::Float16)
        | (DataType::Float16, DataType::Float64)
        | (DataType::Float64, DataType::Float16)
        | (DataType::Float32, DataType::Float64)
        | (DataType::Float64, DataType::Float32) => Ok(DataType::Float64),
        (DataType::Utf8, DataType::LargeUtf8) | (DataType::LargeUtf8, DataType::Utf8) => {
            Ok(DataType::LargeUtf8)
        }
        (DataType::Binary, DataType::LargeBinary) | (DataType::LargeBinary, DataType::Binary) => {
            Ok(DataType::LargeBinary)
        }
        (DataType::Timestamp(left_unit, left_tz), DataType::Timestamp(right_unit, right_tz))
            if left_tz == right_tz =>
        {
            Ok(DataType::Timestamp(
                finer_time_unit(left_unit, right_unit),
                left_tz.clone(),
            ))
        }
        (DataType::Timestamp(_, left_tz), DataType::Timestamp(_, right_tz)) => Err(format!(
            "timestamp timezones differ ({left_tz:?} versus {right_tz:?})"
        )),
        _ => Err("no lossless top-level widening rule exists".to_owned()),
    }
}

fn merge_integers(left: &DataType, right: &DataType) -> Option<DataType> {
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
    for bits in [8, 16, 32, 64] {
        if bits >= signed_bits && bits > unsigned_bits {
            return integer_type(true, bits);
        }
    }
    Some(DataType::Decimal128(
        integer_precision(left).max(integer_precision(right)),
        0,
    ))
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

fn merge_decimal_compatible(
    left: &DataType,
    right: &DataType,
) -> std::result::Result<Option<DataType>, String> {
    let Some(left) = decimal_spec(left)? else {
        return Ok(None);
    };
    let Some(right) = decimal_spec(right)? else {
        return Ok(None);
    };
    let scale = left.1.max(right.1);
    let left_precision = i16::from(left.0) + i16::from(scale) - i16::from(left.1);
    let right_precision = i16::from(right.0) + i16::from(scale) - i16::from(right.1);
    let precision = left_precision.max(right_precision);
    if precision > 38 {
        return Err(format!(
            "merged Decimal128 requires precision {precision}, exceeding 38"
        ));
    }
    let precision = u8::try_from(precision)
        .map_err(|_| format!("merged Decimal128 has invalid precision {precision}"))?;
    Ok(Some(DataType::Decimal128(precision, scale)))
}

fn decimal_spec(data_type: &DataType) -> std::result::Result<Option<(u8, i8)>, String> {
    if let DataType::Decimal128(precision, scale) = data_type {
        if *precision == 0
            || *precision > 38
            || *scale > 38
            || (*scale > 0
                && u8::try_from(*scale)
                    .ok()
                    .is_some_and(|scale| scale > *precision))
        {
            return Err(format!(
                "invalid Decimal128 precision/scale ({precision}, {scale})"
            ));
        }
        return Ok(Some((*precision, *scale)));
    }
    let precision = integer_precision(data_type);
    if precision == 0 {
        return Ok(None);
    }
    Ok(Some((precision, 0)))
}

fn finer_time_unit(left: &TimeUnit, right: &TimeUnit) -> TimeUnit {
    if time_unit_rank(left) >= time_unit_rank(right) {
        *left
    } else {
        *right
    }
}

fn time_unit_rank(unit: &TimeUnit) -> u8 {
    match unit {
        TimeUnit::Second => 0,
        TimeUnit::Millisecond => 1,
        TimeUnit::Microsecond => 2,
        TimeUnit::Nanosecond => 3,
    }
}
