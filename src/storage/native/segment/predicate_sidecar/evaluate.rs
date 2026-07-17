use super::{
    Predicate, PredicateSidecarError, PredicateType, Result,
    bitpack::{PackedValues, bit_width},
    codec::compare,
    format::{BlockParts, Encoding, parse},
};

#[cfg(test)]
pub(super) fn evaluate_block(bytes: &[u8], predicate: Predicate) -> Result<Vec<bool>> {
    evaluate_all_block(bytes, &[predicate])
}

#[cfg(test)]
pub(super) fn evaluate_all_block(bytes: &[u8], predicates: &[Predicate]) -> Result<Vec<bool>> {
    let parts = parse(bytes)?;
    evaluate_parts(&parts, predicates)
}

pub(super) fn evaluate_all_block_typed(
    bytes: &[u8],
    expected: PredicateType,
    predicates: &[Predicate],
) -> Result<Vec<bool>> {
    let parts = parse(bytes)?;
    if parts.data_type != expected {
        return Err(corrupt(format!(
            "predicate block type mismatch: expected {expected:?}, found {:?}",
            parts.data_type
        )));
    }
    evaluate_parts(&parts, predicates)
}

fn evaluate_parts(parts: &BlockParts<'_>, predicates: &[Predicate]) -> Result<Vec<bool>> {
    match parts.encoding {
        Encoding::Dictionary => evaluate_dictionary(parts, predicates),
        Encoding::FrameOfReference => evaluate_frame(parts, predicates),
    }
}

fn evaluate_dictionary(parts: &BlockParts<'_>, predicates: &[Predicate]) -> Result<Vec<bool>> {
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
    validate_dictionary(dictionary)?;

    let ids = PackedValues::new(
        &parts.payload[dictionary_bytes..],
        parts.row_count,
        parts.bit_width,
    )?;
    let mut selection = Vec::with_capacity(parts.row_count);
    for (row, id) in ids.enumerate() {
        let valid = is_valid(parts.validity, row);
        if !valid {
            if id != 0 {
                return Err(corrupt("null dictionary row has a non-zero id"));
            }
            selection.push(evaluate_all_values(None, predicates));
            continue;
        }
        let id = usize::try_from(id).map_err(|_| PredicateSidecarError::TooLarge)?;
        let value = dictionary_value(dictionary, id)?;
        parts.data_type.validate_value(value)?;
        selection.push(evaluate_all_values(Some(value), predicates));
    }
    Ok(selection)
}

fn validate_dictionary(dictionary: &[u8]) -> Result<()> {
    let mut previous = None;
    for value in dictionary
        .chunks_exact(8)
        .map(|bytes| i64::from_le_bytes(bytes.try_into().expect("eight-byte dictionary value")))
    {
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
    let bytes = dictionary
        .get(start..end)
        .and_then(|bytes| bytes.try_into().ok())
        .ok_or_else(|| corrupt(format!("dictionary id {id} is out of range")))?;
    Ok(i64::from_le_bytes(bytes))
}

fn evaluate_frame(parts: &BlockParts<'_>, predicates: &[Predicate]) -> Result<Vec<bool>> {
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
    let mut selection = Vec::with_capacity(parts.row_count);
    let mut max_delta = 0_u64;
    for (row, delta) in deltas.enumerate() {
        max_delta = max_delta.max(delta);
        let valid = is_valid(parts.validity, row);
        if !valid {
            if delta != 0 {
                return Err(corrupt("null frame row has a non-zero delta"));
            }
            selection.push(evaluate_all_values(None, predicates));
            continue;
        }
        let value = i64::try_from(i128::from(base) + i128::from(delta))
            .map_err(|_| corrupt("frame delta overflows i64"))?;
        parts.data_type.validate_value(value)?;
        selection.push(evaluate_all_values(Some(value), predicates));
    }
    let expected_width = bit_width(max_delta);
    if parts.bit_width != expected_width {
        return Err(corrupt(format!(
            "frame uses bit width {}, expected {expected_width}",
            parts.bit_width
        )));
    }
    Ok(selection)
}

fn evaluate_value(value: Option<i64>, predicate: Predicate) -> bool {
    match predicate {
        Predicate::IsNull => value.is_none(),
        Predicate::IsNotNull => value.is_some(),
        Predicate::Compare { op, value: rhs } => value.is_some_and(|lhs| compare(lhs, rhs, op)),
    }
}

fn evaluate_all_values(value: Option<i64>, predicates: &[Predicate]) -> bool {
    predicates
        .iter()
        .copied()
        .all(|predicate| evaluate_value(value, predicate))
}

fn is_valid(validity: &[u8], row: usize) -> bool {
    validity[row / 8] & (1 << (row % 8)) != 0
}

fn corrupt(message: impl Into<String>) -> PredicateSidecarError {
    PredicateSidecarError::Corrupt(message.into())
}
