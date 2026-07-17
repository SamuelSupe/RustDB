use super::{
    DecodedPredicateBlock, PredicateSidecarError, Result,
    bitpack::{bit_width, unpack},
    format::{BlockParts, Encoding, parse},
};

pub(super) fn decode_block(bytes: &[u8]) -> Result<DecodedPredicateBlock> {
    let parts = parse(bytes)?;
    let encoded = match parts.encoding {
        Encoding::Dictionary => decode_dictionary(&parts)?,
        Encoding::FrameOfReference => decode_frame(&parts)?,
    };
    let mut values = Vec::with_capacity(parts.row_count);
    for (row, value) in encoded.into_iter().enumerate() {
        if is_valid(parts.validity, row) {
            parts.data_type.validate_value(value)?;
            values.push(Some(value));
        } else {
            values.push(None);
        }
    }
    debug_assert_eq!(
        values.iter().filter(|value| value.is_none()).count(),
        parts.null_count
    );
    Ok(DecodedPredicateBlock {
        data_type: parts.data_type,
        values,
    })
}

fn decode_dictionary(parts: &BlockParts<'_>) -> Result<Vec<i64>> {
    let expected_width = bit_width(
        u64::try_from(parts.auxiliary_count.saturating_sub(1))
            .map_err(|_| PredicateSidecarError::TooLarge)?,
    );
    if parts.bit_width != expected_width {
        return Err(PredicateSidecarError::Corrupt(format!(
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
        .ok_or_else(|| PredicateSidecarError::Corrupt("truncated dictionary".to_owned()))?
        .chunks_exact(8)
        .map(|bytes| i64::from_le_bytes(bytes.try_into().expect("eight-byte chunk")))
        .collect::<Vec<_>>();
    if dictionary.windows(2).any(|pair| pair[0] >= pair[1]) {
        return Err(PredicateSidecarError::Corrupt(
            "dictionary values are not strictly ordered".to_owned(),
        ));
    }
    unpack(
        &parts.payload[dictionary_bytes..],
        parts.row_count,
        parts.bit_width,
    )?
    .into_iter()
    .enumerate()
    .map(|(row, id)| {
        let id = usize::try_from(id).map_err(|_| PredicateSidecarError::TooLarge)?;
        if !is_valid(parts.validity, row) {
            return if id == 0 {
                Ok(dictionary.first().copied().unwrap_or(0))
            } else {
                Err(PredicateSidecarError::Corrupt(
                    "null dictionary row has a non-zero id".to_owned(),
                ))
            };
        }
        dictionary.get(id).copied().ok_or_else(|| {
            PredicateSidecarError::Corrupt(format!("dictionary id {id} is out of range"))
        })
    })
    .collect()
}

fn decode_frame(parts: &BlockParts<'_>) -> Result<Vec<i64>> {
    if parts.auxiliary_count != 0 {
        return Err(PredicateSidecarError::Corrupt(
            "frame-of-reference block has dictionary entries".to_owned(),
        ));
    }
    let base_bytes: [u8; 8] = parts
        .payload
        .get(..8)
        .and_then(|bytes| bytes.try_into().ok())
        .ok_or_else(|| PredicateSidecarError::Corrupt("missing frame base".to_owned()))?;
    if parts.null_count == parts.row_count {
        return Err(PredicateSidecarError::Corrupt(
            "frame-of-reference block has no valid values".to_owned(),
        ));
    }
    let deltas = unpack(&parts.payload[8..], parts.row_count, parts.bit_width)?;
    let expected_width = deltas.iter().copied().max().map_or(0, bit_width);
    if parts.bit_width != expected_width {
        return Err(PredicateSidecarError::Corrupt(format!(
            "frame uses bit width {}, expected {expected_width}",
            parts.bit_width
        )));
    }
    let base = i64::from_le_bytes(base_bytes);
    deltas
        .into_iter()
        .enumerate()
        .map(|(row, delta)| {
            if !is_valid(parts.validity, row) && delta != 0 {
                return Err(PredicateSidecarError::Corrupt(
                    "null frame row has a non-zero delta".to_owned(),
                ));
            }
            i64::try_from(i128::from(base) + i128::from(delta))
                .map_err(|_| PredicateSidecarError::Corrupt("frame delta overflows i64".to_owned()))
        })
        .collect()
}

fn is_valid(validity: &[u8], row: usize) -> bool {
    validity[row / 8] & (1 << (row % 8)) != 0
}
