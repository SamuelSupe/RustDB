use super::super::{PredicateSidecarError, Result};

const MAGIC: &[u8; 8] = b"RDBPRD01";
pub(crate) const FILE_FORMAT_VERSION: u16 = 1;
pub(super) const HEADER_BYTES: usize = 128;
pub(super) const DIRECTORY_ENTRY_BYTES: usize = 64;

pub(super) struct Header {
    pub(super) schema_fingerprint: [u8; 32],
    pub(super) segment_sha256: [u8; 32],
    pub(super) segment_rows: u64,
    pub(super) row_group_count: u32,
    pub(super) indexed_column_count: u32,
    pub(super) block_count: u32,
    pub(super) row_group_rows_offset: u64,
    pub(super) column_ordinals_offset: u64,
    pub(super) directory_offset: u64,
}

impl Header {
    pub(super) fn parse(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < HEADER_BYTES || bytes.get(..8) != Some(MAGIC) {
            return Err(corrupt("missing predicate sidecar header"));
        }
        if read_u16(bytes, 8)? != FILE_FORMAT_VERSION
            || usize::from(read_u16(bytes, 10)?) != HEADER_BYTES
            || usize::from(read_u16(bytes, 12)?) != DIRECTORY_ENTRY_BYTES
            || read_u16(bytes, 14)? != 0
            || read_u32(bytes, 100)? != 0
        {
            return Err(corrupt("unsupported predicate sidecar header"));
        }
        Ok(Self {
            schema_fingerprint: read_array(bytes, 16)?,
            segment_sha256: read_array(bytes, 48)?,
            segment_rows: read_u64(bytes, 80)?,
            row_group_count: read_u32(bytes, 88)?,
            indexed_column_count: read_u32(bytes, 92)?,
            block_count: read_u32(bytes, 96)?,
            row_group_rows_offset: read_u64(bytes, 104)?,
            column_ordinals_offset: read_u64(bytes, 112)?,
            directory_offset: read_u64(bytes, 120)?,
        })
    }

    #[cfg(test)]
    pub(super) fn write(&self, bytes: &mut Vec<u8>) {
        bytes.extend_from_slice(MAGIC);
        bytes.extend_from_slice(&FILE_FORMAT_VERSION.to_le_bytes());
        bytes.extend_from_slice(&(HEADER_BYTES as u16).to_le_bytes());
        bytes.extend_from_slice(&(DIRECTORY_ENTRY_BYTES as u16).to_le_bytes());
        bytes.extend_from_slice(&0_u16.to_le_bytes());
        bytes.extend_from_slice(&self.schema_fingerprint);
        bytes.extend_from_slice(&self.segment_sha256);
        bytes.extend_from_slice(&self.segment_rows.to_le_bytes());
        bytes.extend_from_slice(&self.row_group_count.to_le_bytes());
        bytes.extend_from_slice(&self.indexed_column_count.to_le_bytes());
        bytes.extend_from_slice(&self.block_count.to_le_bytes());
        bytes.extend_from_slice(&0_u32.to_le_bytes());
        bytes.extend_from_slice(&self.row_group_rows_offset.to_le_bytes());
        bytes.extend_from_slice(&self.column_ordinals_offset.to_le_bytes());
        bytes.extend_from_slice(&self.directory_offset.to_le_bytes());
    }

    pub(super) fn validate_layout(&self, file_len: u64) -> Result<()> {
        if self.segment_rows == 0
            || self.row_group_count == 0
            || self.indexed_column_count == 0
            || self.block_count == 0
            || self.row_group_rows_offset != HEADER_BYTES as u64
        {
            return Err(corrupt("invalid predicate sidecar counts"));
        }
        let columns = self.row_group_rows_offset + u64::from(self.row_group_count) * 4;
        let directory = columns + u64::from(self.indexed_column_count) * 4;
        if self.column_ordinals_offset != columns
            || self.directory_offset != directory
            || self.payload_offset()? > file_len
            || u64::from(self.block_count)
                > u64::from(self.row_group_count) * u64::from(self.indexed_column_count)
        {
            return Err(corrupt("invalid predicate sidecar section offsets"));
        }
        Ok(())
    }

    pub(super) fn payload_offset(&self) -> Result<u64> {
        self.directory_offset
            .checked_add(u64::from(self.block_count) * DIRECTORY_ENTRY_BYTES as u64)
            .ok_or(PredicateSidecarError::TooLarge)
    }
}

#[derive(Clone)]
pub(super) struct DirectoryEntry {
    pub(super) row_group: u32,
    pub(super) column_ordinal: u32,
    pub(super) row_count: u32,
    pub(super) offset: u64,
    pub(super) length: u64,
    pub(super) sha256: [u8; 32],
}

impl DirectoryEntry {
    pub(super) fn parse(bytes: &[u8], offset: usize) -> Result<Self> {
        if read_u32(bytes, offset + 12)? != 0 {
            return Err(corrupt("non-zero predicate directory reserved bytes"));
        }
        Ok(Self {
            row_group: read_u32(bytes, offset)?,
            column_ordinal: read_u32(bytes, offset + 4)?,
            row_count: read_u32(bytes, offset + 8)?,
            offset: read_u64(bytes, offset + 16)?,
            length: read_u64(bytes, offset + 24)?,
            sha256: read_array(bytes, offset + 32)?,
        })
    }

    #[cfg(test)]
    pub(super) fn write(&self, bytes: &mut Vec<u8>) {
        bytes.extend_from_slice(&self.row_group.to_le_bytes());
        bytes.extend_from_slice(&self.column_ordinal.to_le_bytes());
        bytes.extend_from_slice(&self.row_count.to_le_bytes());
        bytes.extend_from_slice(&0_u32.to_le_bytes());
        bytes.extend_from_slice(&self.offset.to_le_bytes());
        bytes.extend_from_slice(&self.length.to_le_bytes());
        bytes.extend_from_slice(&self.sha256);
    }
}

pub(super) fn validate_row_groups(segment_rows: u64, rows: &[u32]) -> Result<()> {
    if rows.is_empty() || rows.contains(&0) {
        return Err(corrupt("predicate sidecar has invalid row groups"));
    }
    let total = rows.iter().try_fold(0_u64, |total, rows| {
        total
            .checked_add(u64::from(*rows))
            .ok_or(PredicateSidecarError::TooLarge)
    })?;
    if total != segment_rows {
        return Err(corrupt("predicate sidecar segment row count mismatch"));
    }
    Ok(())
}

pub(super) fn read_u32_table(bytes: &[u8], offset: u64, count: u32) -> Result<Vec<u32>> {
    let start = usize_from_u64(offset)?;
    let byte_len = usize::try_from(count)
        .map_err(|_| PredicateSidecarError::TooLarge)?
        .checked_mul(4)
        .ok_or(PredicateSidecarError::TooLarge)?;
    bytes
        .get(start..start + byte_len)
        .ok_or_else(|| corrupt("truncated predicate sidecar table"))?
        .chunks_exact(4)
        .map(|chunk| {
            Ok(u32::from_le_bytes(
                chunk.try_into().expect("four-byte chunk"),
            ))
        })
        .collect()
}

#[cfg(test)]
pub(super) fn decode_sha256(value: &str) -> Result<[u8; 32]> {
    if value.len() != 64 {
        return Err(corrupt("invalid SHA-256 binding"));
    }
    let mut decoded = [0_u8; 32];
    for (output, pair) in decoded.iter_mut().zip(value.as_bytes().chunks_exact(2)) {
        let high = hex(pair[0]).ok_or_else(|| corrupt("invalid SHA-256 binding"))?;
        let low = hex(pair[1]).ok_or_else(|| corrupt("invalid SHA-256 binding"))?;
        *output = (high << 4) | low;
    }
    Ok(decoded)
}

pub(super) fn encode_sha256(bytes: &[u8; 32]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(64);
    for byte in bytes {
        encoded.push(HEX[usize::from(byte >> 4)] as char);
        encoded.push(HEX[usize::from(byte & 0x0f)] as char);
    }
    encoded
}

#[cfg(test)]
fn hex(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}
fn read_u16(bytes: &[u8], offset: usize) -> Result<u16> {
    Ok(u16::from_le_bytes(read_array(bytes, offset)?))
}
fn read_u32(bytes: &[u8], offset: usize) -> Result<u32> {
    Ok(u32::from_le_bytes(read_array(bytes, offset)?))
}
fn read_u64(bytes: &[u8], offset: usize) -> Result<u64> {
    Ok(u64::from_le_bytes(read_array(bytes, offset)?))
}
fn read_array<const N: usize>(bytes: &[u8], offset: usize) -> Result<[u8; N]> {
    bytes
        .get(offset..offset + N)
        .and_then(|bytes| bytes.try_into().ok())
        .ok_or_else(|| corrupt("truncated predicate sidecar"))
}
pub(super) fn usize_from_u64(value: u64) -> Result<usize> {
    usize::try_from(value).map_err(|_| PredicateSidecarError::TooLarge)
}
pub(super) fn corrupt(message: impl Into<String>) -> PredicateSidecarError {
    PredicateSidecarError::Corrupt(message.into())
}
