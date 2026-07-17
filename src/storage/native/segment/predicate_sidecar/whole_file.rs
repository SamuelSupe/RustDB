#[cfg(test)]
use std::collections::{BTreeMap, BTreeSet};

#[cfg(test)]
use sha2::{Digest, Sha256};

#[cfg(test)]
use super::{EncodedPredicateBlock, PredicateSidecarError, Result};

mod format;
mod index;

#[cfg(test)]
pub(crate) use format::FILE_FORMAT_VERSION;
pub(crate) use index::PredicateSidecarIndex;

#[cfg(test)]
use format::{
    DIRECTORY_ENTRY_BYTES, DirectoryEntry, HEADER_BYTES, Header, decode_sha256, usize_from_u64,
    validate_row_groups,
};

#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg(test)]
pub(crate) struct PredicateSidecarBlock {
    row_group: u32,
    column_ordinal: u32,
    row_count: u32,
    block: EncodedPredicateBlock,
}

#[cfg(test)]
impl PredicateSidecarBlock {
    pub(crate) fn new(
        row_group: u32,
        column_ordinal: u32,
        block: EncodedPredicateBlock,
    ) -> Result<Self> {
        let row_count =
            u32::try_from(block.row_count()?).map_err(|_| PredicateSidecarError::TooLarge)?;
        Ok(Self {
            row_group,
            column_ordinal,
            row_count,
            block,
        })
    }

    pub(crate) fn retained_bytes(&self) -> usize {
        self.block
            .as_bytes()
            .len()
            .saturating_add(DIRECTORY_ENTRY_BYTES)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg(test)]
pub(crate) struct PredicateSidecarFile {
    schema_fingerprint: String,
    segment_sha256: String,
    segment_rows: u64,
    row_group_rows: Vec<u32>,
    indexed_column_ordinals: Vec<u32>,
    blocks: BTreeMap<(u32, u32), EncodedPredicateBlock>,
}

#[cfg(test)]
impl PredicateSidecarFile {
    pub(crate) fn new(
        schema_fingerprint: &str,
        segment_sha256: &str,
        segment_rows: u64,
        row_group_rows: Vec<u32>,
        blocks: Vec<PredicateSidecarBlock>,
    ) -> Result<Self> {
        decode_sha256(schema_fingerprint)?;
        decode_sha256(segment_sha256)?;
        validate_row_groups(segment_rows, &row_group_rows)?;
        if blocks.is_empty() {
            return Err(format::corrupt("predicate sidecar contains no blocks"));
        }
        let row_group_count =
            u32::try_from(row_group_rows.len()).map_err(|_| PredicateSidecarError::TooLarge)?;
        let mut indexed = BTreeSet::new();
        let mut by_key = BTreeMap::new();
        for entry in blocks {
            if entry.row_group >= row_group_count
                || entry.row_count != row_group_rows[entry.row_group as usize]
            {
                return Err(format::corrupt(
                    "predicate block row-group binding mismatch",
                ));
            }
            indexed.insert(entry.column_ordinal);
            if by_key
                .insert((entry.row_group, entry.column_ordinal), entry.block)
                .is_some()
            {
                return Err(format::corrupt("duplicate predicate block directory key"));
            }
        }
        Ok(Self {
            schema_fingerprint: schema_fingerprint.to_ascii_lowercase(),
            segment_sha256: segment_sha256.to_ascii_lowercase(),
            segment_rows,
            row_group_rows,
            indexed_column_ordinals: indexed.into_iter().collect(),
            blocks: by_key,
        })
    }

    #[cfg(test)]
    pub(crate) fn from_bytes(bytes: &[u8]) -> Result<Self> {
        let prefix_len = PredicateSidecarIndex::metadata_prefix_len(bytes)?;
        let prefix = bytes
            .get(..prefix_len)
            .ok_or_else(|| format::corrupt("truncated predicate sidecar metadata"))?;
        let index = PredicateSidecarIndex::from_metadata(
            prefix,
            u64::try_from(bytes.len()).map_err(|_| PredicateSidecarError::TooLarge)?,
        )?;
        let mut blocks = BTreeMap::new();
        for (&key, entry) in index.entries() {
            let range = entry.byte_range()?;
            let block_bytes = bytes
                .get(range)
                .ok_or_else(|| format::corrupt("predicate block range is outside the file"))?;
            blocks.insert(key, index.decode_block(entry, block_bytes)?);
        }
        Ok(Self {
            schema_fingerprint: index.schema_fingerprint().to_owned(),
            segment_sha256: index.segment_sha256().to_owned(),
            segment_rows: index.segment_rows(),
            row_group_rows: index.row_group_rows().to_vec(),
            indexed_column_ordinals: index.indexed_column_ordinals().to_vec(),
            blocks,
        })
    }

    pub(crate) fn to_bytes(&self) -> Result<Vec<u8>> {
        let layout = self.layout()?;
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(layout.total_bytes)
            .map_err(|_| PredicateSidecarError::TooLarge)?;
        layout.header.write(&mut bytes);
        for rows in &self.row_group_rows {
            bytes.extend_from_slice(&rows.to_le_bytes());
        }
        for ordinal in &self.indexed_column_ordinals {
            bytes.extend_from_slice(&ordinal.to_le_bytes());
        }
        let mut offset = layout.header.payload_offset()?;
        for (&(row_group, column_ordinal), block) in &self.blocks {
            let length = u64::try_from(block.as_bytes().len())
                .map_err(|_| PredicateSidecarError::TooLarge)?;
            DirectoryEntry {
                row_group,
                column_ordinal,
                row_count: self.row_group_rows[row_group as usize],
                offset,
                length,
                sha256: Sha256::digest(block.as_bytes()).into(),
            }
            .write(&mut bytes);
            offset = offset
                .checked_add(length)
                .ok_or(PredicateSidecarError::TooLarge)?;
        }
        for block in self.blocks.values() {
            bytes.extend_from_slice(block.as_bytes());
        }
        debug_assert_eq!(bytes.len(), layout.total_bytes);
        Ok(bytes)
    }

    pub(crate) fn encoded_len(&self) -> Result<usize> {
        Ok(self.layout()?.total_bytes)
    }

    #[cfg(test)]
    pub(crate) fn schema_fingerprint(&self) -> &str {
        &self.schema_fingerprint
    }
    #[cfg(test)]
    pub(crate) fn segment_sha256(&self) -> &str {
        &self.segment_sha256
    }
    #[cfg(test)]
    pub(crate) fn segment_rows(&self) -> u64 {
        self.segment_rows
    }
    pub(crate) fn row_group_rows(&self) -> &[u32] {
        &self.row_group_rows
    }
    pub(crate) fn indexed_column_ordinals(&self) -> &[u32] {
        &self.indexed_column_ordinals
    }
    #[cfg(test)]
    pub(crate) fn block(
        &self,
        row_group: u32,
        column_ordinal: u32,
    ) -> Option<&EncodedPredicateBlock> {
        self.blocks.get(&(row_group, column_ordinal))
    }

    fn layout(&self) -> Result<Layout> {
        let row_group_count = u32::try_from(self.row_group_rows.len())
            .map_err(|_| PredicateSidecarError::TooLarge)?;
        let indexed_column_count = u32::try_from(self.indexed_column_ordinals.len())
            .map_err(|_| PredicateSidecarError::TooLarge)?;
        let block_count =
            u32::try_from(self.blocks.len()).map_err(|_| PredicateSidecarError::TooLarge)?;
        let row_group_rows_offset = HEADER_BYTES as u64;
        let column_ordinals_offset = row_group_rows_offset + u64::from(row_group_count) * 4;
        let directory_offset = column_ordinals_offset + u64::from(indexed_column_count) * 4;
        let payload_offset = directory_offset
            .checked_add(u64::from(block_count) * DIRECTORY_ENTRY_BYTES as u64)
            .ok_or(PredicateSidecarError::TooLarge)?;
        let total = self
            .blocks
            .values()
            .try_fold(payload_offset, |total, block| {
                total
                    .checked_add(
                        u64::try_from(block.as_bytes().len())
                            .map_err(|_| PredicateSidecarError::TooLarge)?,
                    )
                    .ok_or(PredicateSidecarError::TooLarge)
            })?;
        Ok(Layout {
            header: Header {
                schema_fingerprint: decode_sha256(&self.schema_fingerprint)?,
                segment_sha256: decode_sha256(&self.segment_sha256)?,
                segment_rows: self.segment_rows,
                row_group_count,
                indexed_column_count,
                block_count,
                row_group_rows_offset,
                column_ordinals_offset,
                directory_offset,
            },
            total_bytes: usize_from_u64(total)?,
        })
    }
}

#[cfg(test)]
struct Layout {
    header: Header,
    total_bytes: usize,
}

#[cfg(test)]
#[path = "whole_file/tests.rs"]
mod tests;
