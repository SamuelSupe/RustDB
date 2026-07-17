use std::collections::BTreeMap;

use super::{
    PredicateSidecarError, PredicateType, Result,
    bitpack::{bit_width, pack, packed_len},
    format::{DIRECTORY_ENTRY_BYTES, Encoding, Header, assemble},
};

pub(super) fn encode_block(
    data_type: PredicateType,
    values: &[Option<i64>],
) -> Result<Option<Vec<u8>>> {
    data_type.validate()?;
    for value in values.iter().flatten() {
        data_type.validate_value(*value)?;
    }
    let row_count = u32::try_from(values.len()).map_err(|_| PredicateSidecarError::TooLarge)?;
    let (validity, null_count) = build_validity(values)?;
    let dictionary = dictionary_candidate(values)?;
    let frame = frame_candidate(values)?;
    let candidate = match frame {
        Some(frame) if frame.payload.len() < dictionary.payload.len() => frame,
        _ => dictionary,
    };
    let bytes = assemble(
        Header {
            data_type,
            encoding: candidate.encoding,
            bit_width: candidate.bit_width,
            row_count,
            null_count,
            auxiliary_count: candidate.auxiliary_count,
            validity_len: u32::try_from(validity.len())
                .map_err(|_| PredicateSidecarError::TooLarge)?,
            payload_len: u32::try_from(candidate.payload.len())
                .map_err(|_| PredicateSidecarError::TooLarge)?,
        },
        &validity,
        &candidate.payload,
    )?;
    let raw_bytes = values
        .len()
        .checked_mul(data_type.raw_width())
        .ok_or(PredicateSidecarError::TooLarge)?;
    let retained_bytes = bytes
        .len()
        .checked_add(DIRECTORY_ENTRY_BYTES)
        .ok_or(PredicateSidecarError::TooLarge)?;
    Ok((retained_bytes.saturating_mul(2) <= raw_bytes).then_some(bytes))
}

struct Candidate {
    encoding: Encoding,
    bit_width: u8,
    auxiliary_count: u32,
    payload: Vec<u8>,
}

fn dictionary_candidate(values: &[Option<i64>]) -> Result<Candidate> {
    let mut dictionary = values.iter().flatten().copied().collect::<Vec<_>>();
    dictionary.sort_unstable();
    dictionary.dedup();
    let ids_by_value = dictionary
        .iter()
        .enumerate()
        .map(|(id, value)| (*value, id as u64))
        .collect::<BTreeMap<_, _>>();
    let ids = values
        .iter()
        .map(|value| {
            value
                .and_then(|value| ids_by_value.get(&value).copied())
                .unwrap_or(0)
        })
        .collect::<Vec<_>>();
    let width = bit_width(dictionary.len().saturating_sub(1) as u64);
    let dictionary_bytes = dictionary
        .len()
        .checked_mul(8)
        .ok_or(PredicateSidecarError::TooLarge)?;
    let capacity = dictionary_bytes
        .checked_add(packed_len(values.len(), width)?)
        .ok_or(PredicateSidecarError::TooLarge)?;
    let mut payload = Vec::with_capacity(capacity);
    for value in &dictionary {
        payload.extend_from_slice(&value.to_le_bytes());
    }
    payload.extend_from_slice(&pack(&ids, width)?);
    Ok(Candidate {
        encoding: Encoding::Dictionary,
        bit_width: width,
        auxiliary_count: u32::try_from(dictionary.len())
            .map_err(|_| PredicateSidecarError::TooLarge)?,
        payload,
    })
}

fn frame_candidate(values: &[Option<i64>]) -> Result<Option<Candidate>> {
    let Some(base) = values.iter().flatten().copied().min() else {
        return Ok(None);
    };
    let deltas = values
        .iter()
        .map(|value| match value {
            Some(value) => u64::try_from(i128::from(*value) - i128::from(base))
                .map_err(|_| PredicateSidecarError::TooLarge),
            None => Ok(0),
        })
        .collect::<Result<Vec<_>>>()?;
    let width = deltas.iter().copied().max().map_or(0, bit_width);
    let mut payload = Vec::with_capacity(
        8_usize
            .checked_add(packed_len(values.len(), width)?)
            .ok_or(PredicateSidecarError::TooLarge)?,
    );
    payload.extend_from_slice(&base.to_le_bytes());
    payload.extend_from_slice(&pack(&deltas, width)?);
    Ok(Some(Candidate {
        encoding: Encoding::FrameOfReference,
        bit_width: width,
        auxiliary_count: 0,
        payload,
    }))
}

fn build_validity(values: &[Option<i64>]) -> Result<(Vec<u8>, u32)> {
    let mut validity = vec![0_u8; values.len().div_ceil(8)];
    let mut null_count = 0_u32;
    for (row, value) in values.iter().enumerate() {
        if value.is_some() {
            validity[row / 8] |= 1 << (row % 8);
        } else {
            null_count = null_count
                .checked_add(1)
                .ok_or(PredicateSidecarError::TooLarge)?;
        }
    }
    Ok((validity, null_count))
}
