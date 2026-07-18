use std::{fs::OpenOptions, io::Write, os::unix::fs::OpenOptionsExt, path::Path};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{Error, Result};

use super::super::{disk_budget::DiskBudget, io};

const MAGIC: &[u8; 8] = b"RDBDV001";
pub(super) const FORMAT_VERSION: u32 = 1;
const HEADER_BYTES: usize = 8 + 4 + 8 + 8;
const MAX_FILE_BYTES: usize = 512 * 1024 * 1024;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(in crate::storage::native) struct DeleteVectorDescriptor {
    owner_version: u64,
    owner_snapshot_id: String,
    format_version: u32,
    row_count: u64,
    deleted_rows: u64,
    bytes: u64,
    sha256: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct DeleteVector {
    row_count: u64,
    deleted_rows: u64,
    bits: Vec<u8>,
}

impl DeleteVectorDescriptor {
    pub(in crate::storage::native) fn owner_version(&self) -> u64 {
        self.owner_version
    }

    pub(in crate::storage::native) fn owner_snapshot_id(&self) -> &str {
        &self.owner_snapshot_id
    }

    pub(in crate::storage::native) fn format_version(&self) -> u32 {
        self.format_version
    }

    pub(in crate::storage::native) fn row_count(&self) -> u64 {
        self.row_count
    }

    pub(in crate::storage::native) fn deleted_rows(&self) -> u64 {
        self.deleted_rows
    }

    pub(in crate::storage::native) fn bytes(&self) -> u64 {
        self.bytes
    }

    pub(in crate::storage::native) fn sha256(&self) -> &str {
        &self.sha256
    }
}

impl DeleteVector {
    pub(crate) fn memory_size_for_rows(row_count: u64) -> Result<usize> {
        bit_bytes(row_count)
    }

    pub(crate) fn empty(row_count: u64) -> Result<Self> {
        Self::from_offsets(row_count, std::iter::empty())
    }

    #[allow(dead_code)]
    pub(in crate::storage::native) fn from_offsets(
        row_count: u64,
        offsets: impl IntoIterator<Item = u64>,
    ) -> Result<Self> {
        let bit_bytes = bit_bytes(row_count)?;
        let total_bytes = HEADER_BYTES.checked_add(bit_bytes).ok_or_else(|| {
            Error::ResourceExhausted("native delete vector size overflow".to_owned())
        })?;
        if total_bytes > MAX_FILE_BYTES {
            return Err(Error::ResourceExhausted(format!(
                "native delete vector requires {total_bytes} bytes, exceeding the {MAX_FILE_BYTES} byte per-vector limit"
            )));
        }
        let mut bits = vec![0_u8; bit_bytes];
        let mut deleted_rows = 0_u64;
        for offset in offsets {
            if offset >= row_count {
                return Err(Error::InvalidArgument(format!(
                    "native row offset {offset} is outside segment row count {row_count}"
                )));
            }
            let byte = usize::try_from(offset / 8).map_err(|_| {
                Error::ResourceExhausted("native row offset does not fit in usize".to_owned())
            })?;
            let mask = 1_u8 << (offset % 8);
            if bits[byte] & mask == 0 {
                bits[byte] |= mask;
                deleted_rows += 1;
            }
        }
        Ok(Self {
            row_count,
            deleted_rows,
            bits,
        })
    }

    pub(crate) fn row_count(&self) -> u64 {
        self.row_count
    }

    pub(crate) fn deleted_rows(&self) -> u64 {
        self.deleted_rows
    }

    pub(crate) fn memory_size(&self) -> usize {
        self.bits.capacity()
    }

    pub(crate) fn contains(&self, offset: u64) -> bool {
        if offset >= self.row_count {
            return false;
        }
        let byte = usize::try_from(offset / 8).expect("bounded delete-vector offset");
        self.bits[byte] & (1_u8 << (offset % 8)) != 0
    }

    pub(crate) fn mark_deleted(&mut self, offset: u64) -> Result<bool> {
        if offset >= self.row_count {
            return Err(Error::InvalidArgument(format!(
                "native row offset {offset} is outside segment row count {}",
                self.row_count
            )));
        }
        let byte = usize::try_from(offset / 8).map_err(|_| {
            Error::ResourceExhausted("native row offset does not fit in usize".to_owned())
        })?;
        let mask = 1_u8 << (offset % 8);
        if self.bits[byte] & mask != 0 {
            return Ok(false);
        }
        self.bits[byte] |= mask;
        self.deleted_rows = self.deleted_rows.checked_add(1).ok_or_else(|| {
            Error::ResourceExhausted("native deleted-row count overflow".to_owned())
        })?;
        Ok(true)
    }

    #[allow(dead_code)]
    pub(in crate::storage::native) fn write(
        &self,
        path: &Path,
        owner_version: u64,
        owner_snapshot_id: &str,
        budget: &DiskBudget,
    ) -> Result<DeleteVectorDescriptor> {
        let bytes = self.encode()?;
        let digest = format!("{:x}", Sha256::digest(&bytes));
        let file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .mode(0o600)
            .open(path)
            .map_err(|error| Error::io(Some(path.to_path_buf()), error))?;
        let mut file = super::super::disk_budget::QuotaFile::new(file, budget.clone());
        file.write_all(&bytes)
            .map_err(|error| Error::io(Some(path.to_path_buf()), error))?;
        file.flush()
            .map_err(|error| Error::io(Some(path.to_path_buf()), error))?;
        file.sync_all()
            .map_err(|error| Error::io(Some(path.to_path_buf()), error))?;
        io::sync_dir(
            path.parent()
                .ok_or_else(|| Error::native_storage(path, "delete vector path has no parent"))?,
        )?;
        Ok(DeleteVectorDescriptor {
            owner_version,
            owner_snapshot_id: owner_snapshot_id.to_owned(),
            format_version: FORMAT_VERSION,
            row_count: self.row_count,
            deleted_rows: self.deleted_rows,
            bytes: u64::try_from(bytes.len()).map_err(|_| {
                Error::ResourceExhausted("delete vector byte count does not fit in u64".to_owned())
            })?,
            sha256: digest,
        })
    }

    pub(in crate::storage::native) fn read(
        path: &Path,
        descriptor: &DeleteVectorDescriptor,
    ) -> Result<Self> {
        let size = usize::try_from(descriptor.bytes).map_err(|_| {
            Error::native_storage(path, "delete vector byte count does not fit in usize")
        })?;
        if size > MAX_FILE_BYTES {
            return Err(Error::native_storage(
                path,
                "delete vector exceeds the per-vector size limit",
            ));
        }
        let bytes = io::read_bounded(path, size, "native delete vector")?;
        if bytes.len() != size {
            return Err(Error::native_storage(
                path,
                "native delete vector byte size mismatch",
            ));
        }
        if format!("{:x}", Sha256::digest(&bytes)) != descriptor.sha256 {
            return Err(Error::native_storage(
                path,
                "native delete vector checksum mismatch",
            ));
        }
        let vector = Self::decode(path, &bytes)?;
        if vector.row_count != descriptor.row_count
            || vector.deleted_rows != descriptor.deleted_rows
        {
            return Err(Error::native_storage(
                path,
                "native delete vector descriptor mismatch",
            ));
        }
        Ok(vector)
    }

    fn encode(&self) -> Result<Vec<u8>> {
        let capacity = HEADER_BYTES.checked_add(self.bits.len()).ok_or_else(|| {
            Error::ResourceExhausted("native delete vector size overflow".to_owned())
        })?;
        let mut bytes = Vec::with_capacity(capacity);
        bytes.extend_from_slice(MAGIC);
        bytes.extend_from_slice(&FORMAT_VERSION.to_le_bytes());
        bytes.extend_from_slice(&self.row_count.to_le_bytes());
        bytes.extend_from_slice(&self.deleted_rows.to_le_bytes());
        bytes.extend_from_slice(&self.bits);
        Ok(bytes)
    }

    fn decode(path: &Path, bytes: &[u8]) -> Result<Self> {
        if bytes.len() < HEADER_BYTES || &bytes[..MAGIC.len()] != MAGIC {
            return Err(Error::native_storage(
                path,
                "invalid native delete vector header",
            ));
        }
        let version = u32::from_le_bytes(bytes[8..12].try_into().expect("fixed header"));
        if version != FORMAT_VERSION {
            return Err(Error::native_storage(
                path,
                format!("unsupported native delete vector version {version}"),
            ));
        }
        let row_count = u64::from_le_bytes(bytes[12..20].try_into().expect("fixed header"));
        let deleted_rows = u64::from_le_bytes(bytes[20..28].try_into().expect("fixed header"));
        let expected = HEADER_BYTES
            .checked_add(bit_bytes(row_count)?)
            .ok_or_else(|| Error::native_storage(path, "native delete vector size overflow"))?;
        if bytes.len() != expected {
            return Err(Error::native_storage(
                path,
                "native delete vector payload size mismatch",
            ));
        }
        let bits = bytes[HEADER_BYTES..].to_vec();
        let actual_deleted: u64 = bits.iter().map(|byte| u64::from(byte.count_ones())).sum();
        if actual_deleted != deleted_rows || deleted_rows > row_count {
            return Err(Error::native_storage(
                path,
                "native delete vector deleted-row count mismatch",
            ));
        }
        if row_count % 8 != 0 && bits.last().is_some_and(|byte| byte >> (row_count % 8) != 0) {
            return Err(Error::native_storage(
                path,
                "native delete vector sets bits beyond the segment row count",
            ));
        }
        Ok(Self {
            row_count,
            deleted_rows,
            bits,
        })
    }

    #[cfg(test)]
    pub(crate) fn for_test(row_count: u64, offsets: impl IntoIterator<Item = u64>) -> Self {
        Self::from_offsets(row_count, offsets).unwrap()
    }
}

fn bit_bytes(row_count: u64) -> Result<usize> {
    let bytes = row_count.checked_add(7).ok_or_else(|| {
        Error::ResourceExhausted("native delete vector row count overflow".to_owned())
    })? / 8;
    usize::try_from(bytes).map_err(|_| {
        Error::ResourceExhausted("native delete vector does not fit in memory".to_owned())
    })
}

#[cfg(test)]
mod tests {
    use uuid::Uuid;

    use super::*;

    #[test]
    fn persists_a_deduplicated_checksummed_bitmap() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("rows.rdbdel");
        let vector = DeleteVector::from_offsets(17, [0, 3, 3, 16]).unwrap();
        let descriptor = vector
            .write(
                &path,
                2,
                &Uuid::new_v4().to_string(),
                &DiskBudget::unlimited(),
            )
            .unwrap();
        let loaded = DeleteVector::read(&path, &descriptor).unwrap();

        assert_eq!(loaded.deleted_rows(), 3);
        assert!(loaded.contains(0));
        assert!(loaded.contains(3));
        assert!(loaded.contains(16));
        assert!(!loaded.contains(4));

        std::fs::write(&path, b"corrupt").unwrap();
        assert!(DeleteVector::read(&path, &descriptor).is_err());
    }
}
