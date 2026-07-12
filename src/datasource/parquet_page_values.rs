use std::cmp::Ordering;

use arrow::datatypes::{DataType, TimeUnit};
use parquet::file::page_index::column_index::{
    ByteArrayColumnIndex, ColumnIndexMetaData, PrimitiveColumnIndex,
};

use super::{ComparisonOp, PredicateValue};

pub(super) fn comparison_excludes(
    index: &ColumnIndexMetaData,
    data_type: &DataType,
    op: ComparisonOp,
    value: &PredicateValue,
    page: usize,
) -> bool {
    match (index, data_type, value) {
        (
            ColumnIndexMetaData::BOOLEAN(index),
            DataType::Boolean,
            PredicateValue::Boolean(value),
        ) => typed_excludes(index, page, op, value),
        (
            ColumnIndexMetaData::INT32(index),
            DataType::Int8 | DataType::Int16 | DataType::Int32,
            PredicateValue::Int64(value),
        ) => i32::try_from(*value)
            .ok()
            .is_some_and(|value| typed_excludes(index, page, op, &value)),
        (ColumnIndexMetaData::INT64(index), DataType::Int64, PredicateValue::Int64(value)) => {
            typed_excludes(index, page, op, value)
        }
        (
            ColumnIndexMetaData::INT32(index),
            DataType::UInt8 | DataType::UInt16 | DataType::UInt32,
            PredicateValue::UInt64(value),
        ) => u32::try_from(*value)
            .ok()
            .is_some_and(|value| unsigned32_excludes(index, page, op, value)),
        (ColumnIndexMetaData::INT64(index), DataType::UInt64, PredicateValue::UInt64(value)) => {
            unsigned64_excludes(index, page, op, *value)
        }
        (ColumnIndexMetaData::FLOAT(index), DataType::Float32, PredicateValue::Float64(value)) => {
            value.is_finite() && float32_excludes(index, page, op, *value)
        }
        (ColumnIndexMetaData::DOUBLE(index), DataType::Float64, PredicateValue::Float64(value)) => {
            value.is_finite()
                && index.min_value(page).is_some_and(|min| min.is_finite())
                && index.max_value(page).is_some_and(|max| max.is_finite())
                && typed_excludes(index, page, op, value)
        }
        (ColumnIndexMetaData::INT32(index), DataType::Date32, PredicateValue::Date32(value)) => {
            typed_excludes(index, page, op, value)
        }
        (
            ColumnIndexMetaData::INT32(index),
            DataType::Decimal128(_, file_scale),
            PredicateValue::Decimal128 { value, scale, .. },
        ) => rescale_decimal(*value, *scale, *file_scale)
            .is_some_and(|value| integer_decimal_excludes(index, page, op, value)),
        (
            ColumnIndexMetaData::INT64(index),
            DataType::Decimal128(_, file_scale),
            PredicateValue::Decimal128 { value, scale, .. },
        ) => rescale_decimal(*value, *scale, *file_scale)
            .is_some_and(|value| integer_decimal_excludes(index, page, op, value)),
        (
            ColumnIndexMetaData::INT64(index),
            DataType::Timestamp(unit, _),
            PredicateValue::TimestampMicros(value),
        ) => timestamp_excludes(index, page, op, *value, *unit),
        (
            ColumnIndexMetaData::BYTE_ARRAY(index),
            DataType::Utf8 | DataType::LargeUtf8,
            PredicateValue::Utf8(value),
        ) => byte_excludes(index, page, op, value.as_bytes()),
        (
            ColumnIndexMetaData::BYTE_ARRAY(index),
            DataType::Binary | DataType::LargeBinary,
            PredicateValue::Binary(value),
        ) => byte_excludes(index, page, op, value),
        (
            ColumnIndexMetaData::FIXED_LEN_BYTE_ARRAY(index),
            DataType::FixedSizeBinary(_),
            PredicateValue::Binary(value),
        ) => fixed_byte_excludes(index, page, op, value),
        (
            ColumnIndexMetaData::FIXED_LEN_BYTE_ARRAY(index),
            DataType::Decimal128(precision, file_scale),
            PredicateValue::Decimal128 { value, scale, .. },
        ) => rescale_decimal(*value, *scale, *file_scale)
            .is_some_and(|value| decimal_excludes(index, page, op, value, *precision)),
        _ => false,
    }
}

fn typed_excludes<T: PartialOrd + PartialEq>(
    index: &PrimitiveColumnIndex<T>,
    page: usize,
    op: ComparisonOp,
    value: &T,
) -> bool {
    excludes(index.min_value(page), index.max_value(page), op, value)
}

fn unsigned32_excludes(
    index: &PrimitiveColumnIndex<i32>,
    page: usize,
    op: ComparisonOp,
    value: u32,
) -> bool {
    let min = index.min_value(page).map(|value| *value as u32);
    let max = index.max_value(page).map(|value| *value as u32);
    excludes(min.as_ref(), max.as_ref(), op, &value)
}

fn unsigned64_excludes(
    index: &PrimitiveColumnIndex<i64>,
    page: usize,
    op: ComparisonOp,
    value: u64,
) -> bool {
    let min = index.min_value(page).map(|value| *value as u64);
    let max = index.max_value(page).map(|value| *value as u64);
    excludes(min.as_ref(), max.as_ref(), op, &value)
}

fn float32_excludes(
    index: &PrimitiveColumnIndex<f32>,
    page: usize,
    op: ComparisonOp,
    value: f64,
) -> bool {
    let min = index.min_value(page).copied().map(f64::from);
    let max = index.max_value(page).copied().map(f64::from);
    if min.is_some_and(|value| !value.is_finite()) || max.is_some_and(|value| !value.is_finite()) {
        return false;
    }
    excludes(min.as_ref(), max.as_ref(), op, &value)
}

fn timestamp_excludes(
    index: &PrimitiveColumnIndex<i64>,
    page: usize,
    op: ComparisonOp,
    micros: i64,
    unit: TimeUnit,
) -> bool {
    let scale = match unit {
        TimeUnit::Second => 1_000_000_000_i128,
        TimeUnit::Millisecond => 1_000_000_i128,
        TimeUnit::Microsecond => 1_000_i128,
        TimeUnit::Nanosecond => 1_i128,
    };
    let min = index
        .min_value(page)
        .map(|value| i128::from(*value) * scale);
    let max = index
        .max_value(page)
        .map(|value| i128::from(*value) * scale);
    let value = i128::from(micros) * 1_000;
    excludes(min.as_ref(), max.as_ref(), op, &value)
}

fn integer_decimal_excludes<T>(
    index: &PrimitiveColumnIndex<T>,
    page: usize,
    op: ComparisonOp,
    value: i128,
) -> bool
where
    T: Copy + Into<i128>,
{
    let min = index.min_value(page).copied().map(Into::into);
    let max = index.max_value(page).copied().map(Into::into);
    excludes(min.as_ref(), max.as_ref(), op, &value)
}

/// Converts the predicate's unscaled integer to the scale used by one file.
/// A value that needs rounding, or that overflows Decimal128 while scaling, is
/// not safe for metadata pruning and therefore falls back to the residual.
fn rescale_decimal(value: i128, source_scale: i8, target_scale: i8) -> Option<i128> {
    let difference = i16::from(target_scale) - i16::from(source_scale);
    if difference == 0 {
        return Some(value);
    }
    let exponent = u32::from(difference.unsigned_abs());
    let factor = 10_i128.checked_pow(exponent)?;
    if difference > 0 {
        value.checked_mul(factor)
    } else if value % factor == 0 {
        Some(value / factor)
    } else {
        None
    }
}

fn byte_excludes(
    index: &ByteArrayColumnIndex,
    page: usize,
    op: ComparisonOp,
    value: &[u8],
) -> bool {
    excludes_bytes(index.min_value(page), index.max_value(page), op, value)
}

fn fixed_byte_excludes(
    index: &ByteArrayColumnIndex,
    page: usize,
    op: ComparisonOp,
    value: &[u8],
) -> bool {
    excludes_bytes(index.min_value(page), index.max_value(page), op, value)
}

fn decimal_excludes(
    index: &ByteArrayColumnIndex,
    page: usize,
    op: ComparisonOp,
    value: i128,
    precision: u8,
) -> bool {
    let Some(expected) = decimal_byte_width(precision) else {
        return false;
    };
    let min = index
        .min_value(page)
        .filter(|value| value.len() == expected)
        .and_then(decimal128);
    let max = index
        .max_value(page)
        .filter(|value| value.len() == expected)
        .and_then(decimal128);
    excludes(min.as_ref(), max.as_ref(), op, &value)
}

fn decimal_byte_width(precision: u8) -> Option<usize> {
    if !(1..=38).contains(&precision) {
        return None;
    }
    let required = 10_u128.checked_pow(u32::from(precision))?;
    (1_usize..=16).find(|bytes| required <= (1_u128 << (bytes * 8 - 1)))
}

fn decimal128(bytes: &[u8]) -> Option<i128> {
    if bytes.is_empty() || bytes.len() > 16 {
        return None;
    }
    let fill = if bytes[0] & 0x80 == 0 { 0 } else { 0xff };
    let mut value = [fill; 16];
    value[16 - bytes.len()..].copy_from_slice(bytes);
    Some(i128::from_be_bytes(value))
}

fn excludes<T: PartialOrd + PartialEq>(
    min: Option<&T>,
    max: Option<&T>,
    op: ComparisonOp,
    value: &T,
) -> bool {
    let (Some(min), Some(max)) = (min, max) else {
        return false;
    };
    excludes_ordering(
        min.partial_cmp(value),
        max.partial_cmp(value),
        min == max,
        op,
    )
}

fn excludes_bytes(min: Option<&[u8]>, max: Option<&[u8]>, op: ComparisonOp, value: &[u8]) -> bool {
    let (Some(min), Some(max)) = (min, max) else {
        return false;
    };
    excludes_ordering(Some(min.cmp(value)), Some(max.cmp(value)), min == max, op)
}

fn excludes_ordering(
    min: Option<Ordering>,
    max: Option<Ordering>,
    min_equals_max: bool,
    op: ComparisonOp,
) -> bool {
    match op {
        ComparisonOp::Eq => {
            matches!(min, Some(Ordering::Greater)) || matches!(max, Some(Ordering::Less))
        }
        ComparisonOp::NotEq => min_equals_max && matches!(min, Some(Ordering::Equal)),
        ComparisonOp::Lt => matches!(min, Some(Ordering::Equal | Ordering::Greater)),
        ComparisonOp::LtEq => matches!(min, Some(Ordering::Greater)),
        ComparisonOp::Gt => matches!(max, Some(Ordering::Equal | Ordering::Less)),
        ComparisonOp::GtEq => matches!(max, Some(Ordering::Less)),
    }
}

#[cfg(test)]
mod tests {
    use super::{decimal_byte_width, decimal128, rescale_decimal};

    #[test]
    fn decodes_signed_big_endian_decimal() {
        assert_eq!(decimal128(&[0x00, 0x7f]), Some(127));
        assert_eq!(decimal128(&[0xff, 0x80]), Some(-128));
    }

    #[test]
    fn decimal_width_matches_parquet_precision_boundaries() {
        assert_eq!(decimal_byte_width(0), None);
        assert_eq!(decimal_byte_width(1), Some(1));
        assert_eq!(decimal_byte_width(2), Some(1));
        assert_eq!(decimal_byte_width(3), Some(2));
        assert_eq!(decimal_byte_width(9), Some(4));
        assert_eq!(decimal_byte_width(10), Some(5));
        assert_eq!(decimal_byte_width(18), Some(8));
        assert_eq!(decimal_byte_width(19), Some(9));
        assert_eq!(decimal_byte_width(38), Some(16));
        assert_eq!(decimal_byte_width(39), None);
    }

    #[test]
    fn decimal_predicates_rescale_only_when_exact() {
        assert_eq!(rescale_decimal(1, 0, 2), Some(100));
        assert_eq!(rescale_decimal(10_000, 4, 2), Some(100));
        assert_eq!(rescale_decimal(1_001, 3, 2), None);
        assert_eq!(rescale_decimal(i128::MAX, 0, 2), None);
    }
}
