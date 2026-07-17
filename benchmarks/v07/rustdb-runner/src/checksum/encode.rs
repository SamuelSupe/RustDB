use arrow::{
    array::{
        Array, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Float32Array, Float64Array,
        Int8Array, Int16Array, Int32Array, Int64Array, LargeBinaryArray, LargeStringArray,
        RecordBatch, StringArray, TimestampMicrosecondArray, TimestampMillisecondArray,
        TimestampNanosecondArray, TimestampSecondArray, UInt8Array, UInt16Array, UInt32Array,
        UInt64Array,
    },
    datatypes::{DataType, TimeUnit},
};
use sha2::{Digest, Sha256};

const CANONICAL_NAN_BITS: u64 = 0x7ff8_0000_0000_0000;

macro_rules! encode_integer {
    ($array:expr, $array_type:ty, $row:expr, $digest:expr) => {{
        let values = $array
            .as_any()
            .downcast_ref::<$array_type>()
            .ok_or_else(|| "invalid integer array".to_owned())?;
        write_present(b'i', values.value($row).to_string().as_bytes(), $digest)
    }};
}

pub(super) fn hash_row(batch: &RecordBatch, row: usize) -> Result<[u8; 32], String> {
    let column_count = u32::try_from(batch.num_columns())
        .map_err(|_| "checksum row has more than u32::MAX columns".to_owned())?;
    let mut digest = Sha256::new();
    digest.update(column_count.to_le_bytes());
    for column in batch.columns() {
        encode_value(column.as_ref(), row, &mut digest)?;
    }
    Ok(digest.finalize().into())
}

fn encode_value(array: &dyn Array, row: usize, digest: &mut Sha256) -> Result<(), String> {
    let tag = type_tag(array.data_type())?;
    if matches!(array.data_type(), DataType::Null) || array.is_null(row) {
        digest.update([tag, 0]);
        return Ok(());
    }

    match array.data_type() {
        DataType::Boolean => encode_bool(array, row, digest),
        DataType::Int8 => encode_integer!(array, Int8Array, row, digest),
        DataType::Int16 => encode_integer!(array, Int16Array, row, digest),
        DataType::Int32 => encode_integer!(array, Int32Array, row, digest),
        DataType::Int64 => encode_integer!(array, Int64Array, row, digest),
        DataType::UInt8 => encode_integer!(array, UInt8Array, row, digest),
        DataType::UInt16 => encode_integer!(array, UInt16Array, row, digest),
        DataType::UInt32 => encode_integer!(array, UInt32Array, row, digest),
        DataType::UInt64 => encode_integer!(array, UInt64Array, row, digest),
        DataType::Float32 => encode_float32(array, row, digest),
        DataType::Float64 => encode_float64(array, row, digest),
        DataType::Decimal128(_, scale) => encode_decimal(array, row, *scale, digest),
        DataType::Utf8 => write_present(
            b's',
            array
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("type checked")
                .value(row)
                .as_bytes(),
            digest,
        ),
        DataType::LargeUtf8 => write_present(
            b's',
            array
                .as_any()
                .downcast_ref::<LargeStringArray>()
                .expect("type checked")
                .value(row)
                .as_bytes(),
            digest,
        ),
        DataType::Binary => write_present(
            b'x',
            array
                .as_any()
                .downcast_ref::<BinaryArray>()
                .expect("type checked")
                .value(row),
            digest,
        ),
        DataType::LargeBinary => write_present(
            b'x',
            array
                .as_any()
                .downcast_ref::<LargeBinaryArray>()
                .expect("type checked")
                .value(row),
            digest,
        ),
        DataType::Date32 => {
            let value = array
                .as_any()
                .downcast_ref::<Date32Array>()
                .expect("type checked")
                .value(row)
                .to_le_bytes();
            write_present(b'a', &value, digest)
        }
        DataType::Timestamp(TimeUnit::Second, _) => {
            encode_timestamp::<TimestampSecondArray>(array, row, 1_000_000, digest)
        }
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            encode_timestamp::<TimestampMillisecondArray>(array, row, 1_000, digest)
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            encode_timestamp::<TimestampMicrosecondArray>(array, row, 1, digest)
        }
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            let value = array
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .ok_or_else(|| "invalid TimestampNanosecond array".to_owned())?
                .value(row);
            if value % 1_000 != 0 {
                return Err(
                    "nanosecond Timestamp cannot be represented exactly in checksum v2".to_owned(),
                );
            }
            write_present(b't', &(value / 1_000).to_le_bytes(), digest)
        }
        DataType::Null => unreachable!("handled before value encoding"),
        unsupported => Err(format!(
            "checksum does not yet support result type {unsupported}"
        )),
    }
}

fn type_tag(data_type: &DataType) -> Result<u8, String> {
    match data_type {
        // DuckDB exports an untyped SQL NULL as Int32. Canonicalize Arrow Null
        // to the same logical family while retaining explicit typed NULLs.
        DataType::Null => Ok(b'i'),
        DataType::Boolean => Ok(b'b'),
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64 => Ok(b'i'),
        DataType::Float32 | DataType::Float64 => Ok(b'f'),
        DataType::Decimal128(_, _) => Ok(b'd'),
        DataType::Utf8 | DataType::LargeUtf8 => Ok(b's'),
        DataType::Binary | DataType::LargeBinary => Ok(b'x'),
        DataType::Date32 => Ok(b'a'),
        DataType::Timestamp(_, _) => Ok(b't'),
        unsupported => Err(format!(
            "checksum does not yet support result type {unsupported}"
        )),
    }
}

fn encode_bool(array: &dyn Array, row: usize, digest: &mut Sha256) -> Result<(), String> {
    let value = array
        .as_any()
        .downcast_ref::<BooleanArray>()
        .ok_or_else(|| "invalid Boolean array".to_owned())?
        .value(row);
    write_present(b'b', &[u8::from(value)], digest)
}

fn encode_float32(array: &dyn Array, row: usize, digest: &mut Sha256) -> Result<(), String> {
    let value = array
        .as_any()
        .downcast_ref::<Float32Array>()
        .ok_or_else(|| "invalid Float32 array".to_owned())?
        .value(row);
    encode_float(f64::from(value), digest)
}

fn encode_float64(array: &dyn Array, row: usize, digest: &mut Sha256) -> Result<(), String> {
    let value = array
        .as_any()
        .downcast_ref::<Float64Array>()
        .ok_or_else(|| "invalid Float64 array".to_owned())?
        .value(row);
    encode_float(value, digest)
}

fn encode_float(value: f64, digest: &mut Sha256) -> Result<(), String> {
    let bits = if value.is_nan() {
        CANONICAL_NAN_BITS
    } else if value == 0.0 {
        0
    } else {
        value.to_bits()
    };
    write_present(b'f', &bits.to_le_bytes(), digest)
}

fn encode_decimal(
    array: &dyn Array,
    row: usize,
    scale: i8,
    digest: &mut Sha256,
) -> Result<(), String> {
    let value = array
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .ok_or_else(|| "invalid Decimal128 array".to_owned())?
        .value(row);
    write_present(b'd', decimal_text(value, scale).as_bytes(), digest)
}

fn decimal_text(value: i128, scale: i8) -> String {
    let negative = value.is_negative();
    let mut digits = value.unsigned_abs().to_string();
    if scale > 0 {
        let scale = usize::try_from(scale).expect("positive i8 fits usize");
        if digits.len() <= scale {
            digits.insert_str(0, &"0".repeat(scale + 1 - digits.len()));
        }
        digits.insert(digits.len() - scale, '.');
        while digits.ends_with('0') {
            digits.pop();
        }
        if digits.ends_with('.') {
            digits.pop();
        }
    } else if scale < 0 {
        digits.push_str(&"0".repeat(usize::from(scale.unsigned_abs())));
    }
    if negative && digits != "0" {
        digits.insert(0, '-');
    }
    digits
}

trait TimestampArray {
    fn value_at(array: &dyn Array, row: usize) -> Option<i64>;
}

macro_rules! timestamp_array {
    ($array_type:ty) => {
        impl TimestampArray for $array_type {
            fn value_at(array: &dyn Array, row: usize) -> Option<i64> {
                array
                    .as_any()
                    .downcast_ref::<$array_type>()
                    .map(|values| values.value(row))
            }
        }
    };
}

timestamp_array!(TimestampSecondArray);
timestamp_array!(TimestampMillisecondArray);
timestamp_array!(TimestampMicrosecondArray);

fn encode_timestamp<T: TimestampArray>(
    array: &dyn Array,
    row: usize,
    multiplier: i64,
    digest: &mut Sha256,
) -> Result<(), String> {
    let value = T::value_at(array, row)
        .ok_or_else(|| "invalid Timestamp array".to_owned())?
        .checked_mul(multiplier)
        .ok_or_else(|| "Timestamp overflows checksum microsecond representation".to_owned())?;
    write_present(b't', &value.to_le_bytes(), digest)
}

fn write_present(tag: u8, bytes: &[u8], digest: &mut Sha256) -> Result<(), String> {
    let length = u64::try_from(bytes.len())
        .map_err(|_| "checksum value is larger than u64::MAX".to_owned())?;
    digest.update([tag, 1]);
    digest.update(length.to_le_bytes());
    digest.update(bytes);
    Ok(())
}
