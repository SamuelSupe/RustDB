use std::sync::Arc;

use arrow::array::{Array, ArrayRef, FixedSizeBinaryArray, FixedSizeBinaryBuilder, StringArray};

use crate::{Error, Result};

pub(super) fn string_to_uuid(array: &ArrayRef) -> Result<ArrayRef> {
    let values = array
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| Error::Internal(format!("expected Utf8, got {}", array.data_type())))?;
    let mut builder = FixedSizeBinaryBuilder::with_capacity(values.len(), 16);
    for row in 0..values.len() {
        if values.is_null(row) {
            builder.append_null();
            continue;
        }
        let value = uuid::Uuid::parse_str(values.value(row)).map_err(|error| {
            Error::Execution(format!("strict CAST to UUID failed at row {row}: {error}"))
        })?;
        builder.append_value(value.as_bytes())?;
    }
    Ok(Arc::new(builder.finish()))
}

pub(super) fn uuid_to_string(array: &ArrayRef) -> Result<ArrayRef> {
    let values = array
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .ok_or_else(|| {
            Error::Internal(format!(
                "expected FixedSizeBinary, got {}",
                array.data_type()
            ))
        })?;
    if values.value_length() != 16 {
        return Err(Error::Internal(
            "UUID physical value is not 16 bytes".into(),
        ));
    }
    Ok(Arc::new(StringArray::from_iter((0..values.len()).map(
        |row| {
            values.is_valid(row).then(|| {
                let bytes: [u8; 16] = values
                    .value(row)
                    .try_into()
                    .expect("UUID length was validated");
                uuid::Uuid::from_bytes(bytes).to_string()
            })
        },
    ))))
}
