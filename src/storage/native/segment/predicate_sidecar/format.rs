use super::{PredicateSidecarError, PredicateType, Result};

#[cfg(test)]
pub(super) const DIRECTORY_ENTRY_BYTES: usize = 32;
const MAGIC: &[u8; 8] = b"RDBPSC01";
const VERSION: u16 = 1;
const HEADER_BYTES: usize = 40;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Encoding {
    Dictionary,
    FrameOfReference,
}

impl Encoding {
    #[cfg(test)]
    fn tag(self) -> u8 {
        match self {
            Self::Dictionary => 1,
            Self::FrameOfReference => 2,
        }
    }

    fn from_tag(tag: u8) -> Result<Self> {
        match tag {
            1 => Ok(Self::Dictionary),
            2 => Ok(Self::FrameOfReference),
            _ => Err(PredicateSidecarError::Corrupt(format!(
                "unknown encoding tag {tag}"
            ))),
        }
    }
}

pub(super) struct BlockParts<'a> {
    pub(super) data_type: PredicateType,
    pub(super) encoding: Encoding,
    pub(super) bit_width: u8,
    pub(super) row_count: usize,
    pub(super) null_count: usize,
    pub(super) auxiliary_count: usize,
    pub(super) validity: &'a [u8],
    pub(super) payload: &'a [u8],
}

#[cfg(test)]
pub(super) struct Header {
    pub(super) data_type: PredicateType,
    pub(super) encoding: Encoding,
    pub(super) bit_width: u8,
    pub(super) row_count: u32,
    pub(super) null_count: u32,
    pub(super) auxiliary_count: u32,
    pub(super) validity_len: u32,
    pub(super) payload_len: u32,
}

#[cfg(test)]
pub(super) fn assemble(header: Header, validity: &[u8], payload: &[u8]) -> Result<Vec<u8>> {
    if usize::try_from(header.validity_len).ok() != Some(validity.len())
        || usize::try_from(header.payload_len).ok() != Some(payload.len())
    {
        return Err(PredicateSidecarError::Corrupt(
            "header length does not match block data".to_owned(),
        ));
    }
    let total = HEADER_BYTES
        .checked_add(validity.len())
        .and_then(|size| size.checked_add(payload.len()))
        .ok_or(PredicateSidecarError::TooLarge)?;
    let mut bytes = Vec::with_capacity(total);
    let (type_tag, precision, scale) = header.data_type.format_parts();
    bytes.extend_from_slice(MAGIC);
    bytes.extend_from_slice(&VERSION.to_le_bytes());
    bytes.extend_from_slice(&(HEADER_BYTES as u16).to_le_bytes());
    bytes.push(type_tag);
    bytes.push(header.encoding.tag());
    bytes.push(header.bit_width);
    bytes.push(precision);
    bytes.push(scale as u8);
    bytes.extend_from_slice(&[0; 3]);
    bytes.extend_from_slice(&header.row_count.to_le_bytes());
    bytes.extend_from_slice(&header.null_count.to_le_bytes());
    bytes.extend_from_slice(&header.validity_len.to_le_bytes());
    bytes.extend_from_slice(&header.auxiliary_count.to_le_bytes());
    bytes.extend_from_slice(&header.payload_len.to_le_bytes());
    bytes.extend_from_slice(validity);
    bytes.extend_from_slice(payload);
    Ok(bytes)
}

pub(super) fn parse(bytes: &[u8]) -> Result<BlockParts<'_>> {
    if bytes.len() < HEADER_BYTES || bytes.get(..8) != Some(MAGIC) {
        return Err(PredicateSidecarError::Corrupt(
            "missing predicate block header".to_owned(),
        ));
    }
    if read_u16(bytes, 8)? != VERSION || usize::from(read_u16(bytes, 10)?) != HEADER_BYTES {
        return Err(PredicateSidecarError::Corrupt(
            "unsupported predicate block version or header length".to_owned(),
        ));
    }
    if bytes[17..20].iter().any(|byte| *byte != 0) {
        return Err(PredicateSidecarError::Corrupt(
            "non-zero reserved header bytes".to_owned(),
        ));
    }
    let data_type = PredicateType::from_format(bytes[12], bytes[15], bytes[16] as i8)?;
    let encoding = Encoding::from_tag(bytes[13])?;
    let bit_width = bytes[14];
    if bit_width > 64 {
        return Err(PredicateSidecarError::Corrupt(format!(
            "invalid bit width {bit_width}"
        )));
    }
    let row_count = usize_from_u32(read_u32(bytes, 20)?)?;
    let null_count = usize_from_u32(read_u32(bytes, 24)?)?;
    let validity_len = usize_from_u32(read_u32(bytes, 28)?)?;
    let auxiliary_count = usize_from_u32(read_u32(bytes, 32)?)?;
    let payload_len = usize_from_u32(read_u32(bytes, 36)?)?;
    let expected_validity = row_count.div_ceil(8);
    if validity_len != expected_validity || null_count > row_count {
        return Err(PredicateSidecarError::Corrupt(
            "invalid validity or null count".to_owned(),
        ));
    }
    let payload_offset = HEADER_BYTES
        .checked_add(validity_len)
        .ok_or(PredicateSidecarError::TooLarge)?;
    let total = payload_offset
        .checked_add(payload_len)
        .ok_or(PredicateSidecarError::TooLarge)?;
    if total != bytes.len() {
        return Err(PredicateSidecarError::Corrupt(
            "block length does not match header".to_owned(),
        ));
    }
    let validity = &bytes[HEADER_BYTES..payload_offset];
    validate_validity(validity, row_count, null_count)?;
    Ok(BlockParts {
        data_type,
        encoding,
        bit_width,
        row_count,
        null_count,
        auxiliary_count,
        validity,
        payload: &bytes[payload_offset..],
    })
}

fn validate_validity(validity: &[u8], row_count: usize, null_count: usize) -> Result<()> {
    if !row_count.is_multiple_of(8) {
        let used = row_count % 8;
        let padding_mask = !((1_u8 << used) - 1);
        if validity.last().is_some_and(|byte| byte & padding_mask != 0) {
            return Err(PredicateSidecarError::Corrupt(
                "non-zero validity padding".to_owned(),
            ));
        }
    }
    let valid_count = validity
        .iter()
        .map(|byte| byte.count_ones() as usize)
        .sum::<usize>();
    if row_count.saturating_sub(valid_count) != null_count {
        return Err(PredicateSidecarError::Corrupt(
            "null count does not match validity bitmap".to_owned(),
        ));
    }
    Ok(())
}

fn read_u16(bytes: &[u8], offset: usize) -> Result<u16> {
    let value: [u8; 2] = bytes
        .get(offset..offset + 2)
        .and_then(|bytes| bytes.try_into().ok())
        .ok_or_else(|| PredicateSidecarError::Corrupt("truncated header".to_owned()))?;
    Ok(u16::from_le_bytes(value))
}

fn read_u32(bytes: &[u8], offset: usize) -> Result<u32> {
    let value: [u8; 4] = bytes
        .get(offset..offset + 4)
        .and_then(|bytes| bytes.try_into().ok())
        .ok_or_else(|| PredicateSidecarError::Corrupt("truncated header".to_owned()))?;
    Ok(u32::from_le_bytes(value))
}

fn usize_from_u32(value: u32) -> Result<usize> {
    usize::try_from(value).map_err(|_| PredicateSidecarError::TooLarge)
}
