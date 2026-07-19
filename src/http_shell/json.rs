use arrow::{
    array::{
        Array, BinaryArray, BinaryViewArray, BooleanArray, Date32Array, Date64Array,
        FixedSizeBinaryArray, Float32Array, Float64Array, Int8Array, Int16Array, Int32Array,
        Int64Array, LargeBinaryArray, LargeStringArray, StringArray, StringViewArray, UInt8Array,
        UInt16Array, UInt32Array, UInt64Array,
    },
    compute::cast,
    datatypes::DataType,
    datatypes::SchemaRef,
    record_batch::RecordBatch,
    util::display::array_value_to_string,
};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use serde_json::{Value, json};

use crate::{Error, Result};

use super::types::{SchemaColumn, number};

pub(crate) fn schema_columns(schema: &SchemaRef) -> Vec<SchemaColumn> {
    schema
        .fields()
        .iter()
        .map(|field| SchemaColumn {
            name: field.name().clone(),
            data_type: field.data_type().to_string(),
            nullable: field.is_nullable(),
        })
        .collect()
}

pub(crate) fn batch_rows(
    batch: &RecordBatch,
    offset: usize,
    length: usize,
) -> Result<Vec<Vec<Value>>> {
    let end = offset
        .checked_add(length)
        .filter(|end| *end <= batch.num_rows())
        .ok_or_else(|| Error::Internal("HTTP result slice is outside its batch".into()))?;
    (offset..end)
        .map(|row| {
            batch
                .columns()
                .iter()
                .map(|column| value(column.as_ref(), row))
                .collect()
        })
        .collect()
}

fn value(array: &dyn Array, row: usize) -> Result<Value> {
    if array.is_null(row) {
        return Ok(Value::Null);
    }
    macro_rules! primitive {
        ($kind:ty) => {{
            let array = array.as_any().downcast_ref::<$kind>().ok_or_else(|| {
                Error::Internal(format!("invalid Arrow array for {}", array.data_type()))
            })?;
            Ok(json!(array.value(row)))
        }};
    }
    match array.data_type() {
        DataType::Boolean => primitive!(BooleanArray),
        DataType::Int8 => primitive!(Int8Array),
        DataType::Int16 => primitive!(Int16Array),
        DataType::Int32 => primitive!(Int32Array),
        DataType::Int64 => primitive!(Int64Array),
        DataType::UInt8 => primitive!(UInt8Array),
        DataType::UInt16 => primitive!(UInt16Array),
        DataType::UInt32 => primitive!(UInt32Array),
        DataType::UInt64 => primitive!(UInt64Array),
        DataType::Float32 => float_value(f64::from(
            array
                .as_any()
                .downcast_ref::<Float32Array>()
                .ok_or_else(|| Error::Internal("invalid Float32 array".into()))?
                .value(row),
        )),
        DataType::Float64 => float_value(
            array
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| Error::Internal("invalid Float64 array".into()))?
                .value(row),
        ),
        DataType::Decimal128(_, _) | DataType::Decimal256(_, _) => {
            number(array_value_to_string(array, row)?)
        }
        DataType::Utf8 => Ok(Value::String(
            array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| Error::Internal("invalid Utf8 array".into()))?
                .value(row)
                .to_owned(),
        )),
        DataType::LargeUtf8 => Ok(Value::String(
            array
                .as_any()
                .downcast_ref::<LargeStringArray>()
                .ok_or_else(|| Error::Internal("invalid LargeUtf8 array".into()))?
                .value(row)
                .to_owned(),
        )),
        DataType::Utf8View => Ok(Value::String(
            array
                .as_any()
                .downcast_ref::<StringViewArray>()
                .ok_or_else(|| Error::Internal("invalid Utf8View array".into()))?
                .value(row)
                .to_owned(),
        )),
        DataType::Binary => binary(
            array
                .as_any()
                .downcast_ref::<BinaryArray>()
                .ok_or_else(|| Error::Internal("invalid Binary array".into()))?
                .value(row),
        ),
        DataType::LargeBinary => binary(
            array
                .as_any()
                .downcast_ref::<LargeBinaryArray>()
                .ok_or_else(|| Error::Internal("invalid LargeBinary array".into()))?
                .value(row),
        ),
        DataType::BinaryView => binary(
            array
                .as_any()
                .downcast_ref::<BinaryViewArray>()
                .ok_or_else(|| Error::Internal("invalid BinaryView array".into()))?
                .value(row),
        ),
        DataType::FixedSizeBinary(_) => binary(
            array
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .ok_or_else(|| Error::Internal("invalid FixedSizeBinary array".into()))?
                .value(row),
        ),
        DataType::Date32 => {
            let array = array
                .as_any()
                .downcast_ref::<Date32Array>()
                .ok_or_else(|| Error::Internal("invalid Date32 array".into()))?;
            let _ = array.value(row);
            Ok(Value::String(array_value_to_string(array, row)?))
        }
        DataType::Date64 => {
            let array = array
                .as_any()
                .downcast_ref::<Date64Array>()
                .ok_or_else(|| Error::Internal("invalid Date64 array".into()))?;
            let _ = array.value(row);
            Ok(Value::String(array_value_to_string(array, row)?))
        }
        DataType::Timestamp(_, _)
        | DataType::Time32(_)
        | DataType::Time64(_)
        | DataType::Duration(_)
        | DataType::Interval(_) => Ok(Value::String(array_value_to_string(array, row)?)),
        DataType::Dictionary(_, value_type) => {
            let decoded = cast(array, value_type.as_ref())?;
            value(decoded.as_ref(), row)
        }
        DataType::Null => Ok(Value::Null),
        _ => structured_or_string(array_value_to_string(array, row)?),
    }
}

fn float_value(value: f64) -> Result<Value> {
    if value.is_nan() {
        return Ok(Value::String("NaN".into()));
    }
    if value == f64::INFINITY {
        return Ok(Value::String("Infinity".into()));
    }
    if value == f64::NEG_INFINITY {
        return Ok(Value::String("-Infinity".into()));
    }
    serde_json::Number::from_f64(value)
        .map(Value::Number)
        .ok_or_else(|| Error::Internal("finite float could not be encoded as JSON".into()))
}

fn binary(value: &[u8]) -> Result<Value> {
    Ok(Value::String(STANDARD.encode(value)))
}

fn structured_or_string(value: String) -> Result<Value> {
    Ok(serde_json::from_str(&value).unwrap_or(Value::String(value)))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::{
        array::{ArrayRef, Float64Array, Int64Array},
        datatypes::{Field, Schema},
        record_batch::RecordBatch,
    };
    use serde_json::json;

    use super::batch_rows;

    #[test]
    fn preserves_large_integers_and_special_floats() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("i", arrow::datatypes::DataType::Int64, false),
            Field::new("f", arrow::datatypes::DataType::Float64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![i64::MAX])) as ArrayRef,
                Arc::new(Float64Array::from(vec![f64::INFINITY])) as ArrayRef,
            ],
        )
        .unwrap();
        assert_eq!(
            batch_rows(&batch, 0, 1).unwrap(),
            vec![vec![json!(i64::MAX), json!("Infinity")]]
        );
    }
}
