use std::collections::BTreeMap;
#[cfg(test)]
use std::ops::Range;

#[cfg(test)]
use sha2::{Digest, Sha256};

#[cfg(test)]
use super::super::EncodedPredicateBlock;
use super::{
    super::{PredicateSidecarError, Result},
    format::{
        DIRECTORY_ENTRY_BYTES, DirectoryEntry, Header, corrupt, encode_sha256, read_u32_table,
        usize_from_u64, validate_row_groups,
    },
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct PredicateBlockEntry {
    row_count: u32,
    offset: u64,
    length: u64,
    sha256: [u8; 32],
}

impl PredicateBlockEntry {
    pub(crate) fn row_count(&self) -> u32 {
        self.row_count
    }
    pub(crate) fn offset(&self) -> u64 {
        self.offset
    }
    pub(crate) fn length(&self) -> u64 {
        self.length
    }
    pub(crate) fn sha256(&self) -> String {
        encode_sha256(&self.sha256)
    }
    #[cfg(test)]
    pub(crate) fn byte_range(&self) -> Result<Range<usize>> {
        let start = usize_from_u64(self.offset)?;
        let end = usize_from_u64(
            self.offset
                .checked_add(self.length)
                .ok_or(PredicateSidecarError::TooLarge)?,
        )?;
        Ok(start..end)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct PredicateSidecarIndex {
    schema_fingerprint: String,
    segment_sha256: String,
    segment_rows: u64,
    row_group_rows: Vec<u32>,
    indexed_column_ordinals: Vec<u32>,
    entries: BTreeMap<(u32, u32), PredicateBlockEntry>,
}

impl PredicateSidecarIndex {
    pub(crate) fn metadata_prefix_len(header_bytes: &[u8]) -> Result<usize> {
        usize_from_u64(Header::parse(header_bytes)?.payload_offset()?)
    }

    pub(crate) fn from_metadata(prefix: &[u8], total_file_len: u64) -> Result<Self> {
        let header = Header::parse(prefix)?;
        header.validate_layout(total_file_len)?;
        if header.payload_offset()? != prefix.len() as u64 {
            return Err(corrupt("predicate sidecar metadata prefix length mismatch"));
        }
        let row_group_rows =
            read_u32_table(prefix, header.row_group_rows_offset, header.row_group_count)?;
        validate_row_groups(header.segment_rows, &row_group_rows)?;
        let indexed_column_ordinals = read_u32_table(
            prefix,
            header.column_ordinals_offset,
            header.indexed_column_count,
        )?;
        if indexed_column_ordinals.is_empty()
            || indexed_column_ordinals
                .windows(2)
                .any(|pair| pair[0] >= pair[1])
        {
            return Err(corrupt("indexed column ordinals are empty or unordered"));
        }
        let mut entries = BTreeMap::new();
        let mut payload_cursor = header.payload_offset()?;
        let mut previous_key = None;
        for entry_index in 0..header.block_count {
            let directory_offset =
                header.directory_offset + u64::from(entry_index) * DIRECTORY_ENTRY_BYTES as u64;
            let entry = DirectoryEntry::parse(prefix, usize_from_u64(directory_offset)?)?;
            let key = (entry.row_group, entry.column_ordinal);
            if entry.row_group >= header.row_group_count
                || indexed_column_ordinals
                    .binary_search(&entry.column_ordinal)
                    .is_err()
                || entry.row_count != row_group_rows[entry.row_group as usize]
                || entry.offset != payload_cursor
                || entry.length == 0
                || previous_key.is_some_and(|previous| previous >= key)
            {
                return Err(corrupt("invalid predicate block directory entry"));
            }
            payload_cursor = entry
                .offset
                .checked_add(entry.length)
                .ok_or(PredicateSidecarError::TooLarge)?;
            if payload_cursor > total_file_len {
                return Err(corrupt("predicate block range is outside the file"));
            }
            previous_key = Some(key);
            entries.insert(
                key,
                PredicateBlockEntry {
                    row_count: entry.row_count,
                    offset: entry.offset,
                    length: entry.length,
                    sha256: entry.sha256,
                },
            );
        }
        if payload_cursor != total_file_len
            || indexed_column_ordinals
                .iter()
                .any(|ordinal| !entries.keys().any(|(_, candidate)| candidate == ordinal))
        {
            return Err(corrupt(
                "predicate sidecar has trailing or unreferenced data",
            ));
        }
        Ok(Self {
            schema_fingerprint: encode_sha256(&header.schema_fingerprint),
            segment_sha256: encode_sha256(&header.segment_sha256),
            segment_rows: header.segment_rows,
            row_group_rows,
            indexed_column_ordinals,
            entries,
        })
    }

    #[cfg(test)]
    pub(crate) fn decode_block(
        &self,
        entry: &PredicateBlockEntry,
        bytes: &[u8],
    ) -> Result<EncodedPredicateBlock> {
        if u64::try_from(bytes.len()).ok() != Some(entry.length)
            || Sha256::digest(bytes).as_slice() != entry.sha256
        {
            return Err(corrupt("predicate block checksum or length mismatch"));
        }
        let block = EncodedPredicateBlock::from_bytes(bytes.to_vec())?;
        if u32::try_from(block.row_count()?).ok() != Some(entry.row_count) {
            return Err(corrupt("predicate block row count mismatch"));
        }
        Ok(block)
    }

    pub(crate) fn entry(
        &self,
        row_group: u32,
        column_ordinal: u32,
    ) -> Option<&PredicateBlockEntry> {
        self.entries.get(&(row_group, column_ordinal))
    }
    pub(crate) fn schema_fingerprint(&self) -> &str {
        &self.schema_fingerprint
    }
    pub(crate) fn segment_sha256(&self) -> &str {
        &self.segment_sha256
    }
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
    pub(super) fn entries(&self) -> &BTreeMap<(u32, u32), PredicateBlockEntry> {
        &self.entries
    }
}
