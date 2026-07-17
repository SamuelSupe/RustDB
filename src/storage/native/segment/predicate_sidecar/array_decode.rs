use std::sync::Arc;

use arrow::{
    array::{
        ArrayRef, Date32Builder, Decimal128Builder, Int8Builder, Int16Builder, Int32Builder,
        Int64Builder,
    },
    datatypes::DataType,
};

use super::{
    PredicateSidecarError, PredicateType, Result,
    bitpack::{PackedValues, bit_width},
    format::{BlockParts, Encoding, parse},
};

pub(super) fn decode_selected_array(
    bytes: &[u8],
    data_type: &DataType,
    selection: &[bool],
) -> Result<ArrayRef> {
    let expected = PredicateType::from_arrow(data_type)?;
    let parts = parse(bytes)?;
    if parts.data_type != expected {
        return Err(corrupt(format!(
            "predicate block type mismatch: expected {expected:?}, found {:?}",
            parts.data_type
        )));
    }
    if selection.len() != parts.row_count {
        return Err(corrupt(format!(
            "selection has {} rows, expected {}",
            selection.len(),
            parts.row_count
        )));
    }
    let capacity = selection.iter().filter(|selected| **selected).count();

    match expected {
        PredicateType::Int8 => {
            let mut builder = Int8Builder::with_capacity(capacity);
            visit_selected(&parts, selection, |value| {
                builder.append_option(value.map(|value| value as i8));
            })?;
            Ok(Arc::new(builder.finish()))
        }
        PredicateType::Int16 => {
            let mut builder = Int16Builder::with_capacity(capacity);
            visit_selected(&parts, selection, |value| {
                builder.append_option(value.map(|value| value as i16));
            })?;
            Ok(Arc::new(builder.finish()))
        }
        PredicateType::Int32 => {
            let mut builder = Int32Builder::with_capacity(capacity);
            visit_selected(&parts, selection, |value| {
                builder.append_option(value.map(|value| value as i32));
            })?;
            Ok(Arc::new(builder.finish()))
        }
        PredicateType::Int64 => {
            let mut builder = Int64Builder::with_capacity(capacity);
            visit_selected(&parts, selection, |value| {
                builder.append_option(value);
            })?;
            Ok(Arc::new(builder.finish()))
        }
        PredicateType::Date32 => {
            let mut builder = Date32Builder::with_capacity(capacity);
            visit_selected(&parts, selection, |value| {
                builder.append_option(value.map(|value| value as i32));
            })?;
            Ok(Arc::new(builder.finish()))
        }
        PredicateType::Decimal128 { precision, scale } => {
            let mut builder = Decimal128Builder::with_capacity(capacity);
            visit_selected(&parts, selection, |value| {
                builder.append_option(value.map(i128::from));
            })?;
            let array = builder
                .finish()
                .with_precision_and_scale(precision, scale)
                .map_err(|error| corrupt(format!("invalid decimal metadata: {error}")))?;
            Ok(Arc::new(array))
        }
    }
}

fn visit_selected(
    parts: &BlockParts<'_>,
    selection: &[bool],
    append: impl FnMut(Option<i64>),
) -> Result<()> {
    match parts.encoding {
        Encoding::Dictionary => visit_dictionary(parts, selection, append),
        Encoding::FrameOfReference => visit_frame(parts, selection, append),
    }
}

fn visit_dictionary(
    parts: &BlockParts<'_>,
    selection: &[bool],
    mut append: impl FnMut(Option<i64>),
) -> Result<()> {
    let expected_width = bit_width(
        u64::try_from(parts.auxiliary_count.saturating_sub(1))
            .map_err(|_| PredicateSidecarError::TooLarge)?,
    );
    if parts.bit_width != expected_width {
        return Err(corrupt(format!(
            "dictionary uses bit width {}, expected {expected_width}",
            parts.bit_width
        )));
    }
    let dictionary_bytes = parts
        .auxiliary_count
        .checked_mul(8)
        .ok_or(PredicateSidecarError::TooLarge)?;
    let dictionary = parts
        .payload
        .get(..dictionary_bytes)
        .ok_or_else(|| corrupt("truncated dictionary"))?;
    validate_dictionary(parts.data_type, dictionary)?;
    let ids = PackedValues::new(
        &parts.payload[dictionary_bytes..],
        parts.row_count,
        parts.bit_width,
    )?;

    for (row, id) in ids.enumerate() {
        let valid = is_valid(parts.validity, row);
        let value = if valid {
            let id = usize::try_from(id).map_err(|_| PredicateSidecarError::TooLarge)?;
            Some(dictionary_value(dictionary, id)?)
        } else {
            if id != 0 {
                return Err(corrupt("null dictionary row has a non-zero id"));
            }
            None
        };
        if selection[row] {
            append(value);
        }
    }
    Ok(())
}

fn validate_dictionary(data_type: PredicateType, dictionary: &[u8]) -> Result<()> {
    let mut previous = None;
    for value in dictionary
        .chunks_exact(8)
        .map(|bytes| i64::from_le_bytes(bytes.try_into().expect("eight-byte dictionary value")))
    {
        data_type.validate_value(value)?;
        if previous.is_some_and(|previous| previous >= value) {
            return Err(corrupt("dictionary values are not strictly ordered"));
        }
        previous = Some(value);
    }
    Ok(())
}

fn dictionary_value(dictionary: &[u8], id: usize) -> Result<i64> {
    let start = id.checked_mul(8).ok_or(PredicateSidecarError::TooLarge)?;
    let end = start
        .checked_add(8)
        .ok_or(PredicateSidecarError::TooLarge)?;
    dictionary
        .get(start..end)
        .and_then(|bytes| bytes.try_into().ok())
        .map(i64::from_le_bytes)
        .ok_or_else(|| corrupt(format!("dictionary id {id} is out of range")))
}

fn visit_frame(
    parts: &BlockParts<'_>,
    selection: &[bool],
    mut append: impl FnMut(Option<i64>),
) -> Result<()> {
    if parts.auxiliary_count != 0 || parts.null_count == parts.row_count {
        return Err(corrupt("invalid frame-of-reference metadata"));
    }
    let base_bytes: [u8; 8] = parts
        .payload
        .get(..8)
        .and_then(|bytes| bytes.try_into().ok())
        .ok_or_else(|| corrupt("missing frame base"))?;
    let base = i64::from_le_bytes(base_bytes);
    let deltas = PackedValues::new(&parts.payload[8..], parts.row_count, parts.bit_width)?;
    let mut max_delta = 0_u64;

    for (row, delta) in deltas.enumerate() {
        max_delta = max_delta.max(delta);
        let valid = is_valid(parts.validity, row);
        let value = if valid {
            let value = i64::try_from(i128::from(base) + i128::from(delta))
                .map_err(|_| corrupt("frame delta overflows i64"))?;
            parts.data_type.validate_value(value)?;
            Some(value)
        } else {
            if delta != 0 {
                return Err(corrupt("null frame row has a non-zero delta"));
            }
            None
        };
        if selection[row] {
            append(value);
        }
    }
    let expected_width = bit_width(max_delta);
    if parts.bit_width != expected_width {
        return Err(corrupt(format!(
            "frame uses bit width {}, expected {expected_width}",
            parts.bit_width
        )));
    }
    Ok(())
}

fn is_valid(validity: &[u8], row: usize) -> bool {
    validity[row / 8] & (1 << (row % 8)) != 0
}

fn corrupt(message: impl Into<String>) -> PredicateSidecarError {
    PredicateSidecarError::Corrupt(message.into())
}
