use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

pub(in crate::storage::native) const FORMAT_VERSION: u32 = 1;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(in crate::storage::native) struct PredicateSidecarDescriptor {
    format_version: u32,
    bytes: u64,
    sha256: String,
    row_count: u64,
    row_group_count: u64,
    indexed_column_ordinals: Vec<u32>,
}

impl PredicateSidecarDescriptor {
    #[cfg(test)]
    pub(in crate::storage::native) fn new(
        format_version: u32,
        bytes: u64,
        sha256: impl Into<String>,
        row_count: u64,
        row_group_count: u64,
        indexed_column_ordinals: Vec<u32>,
    ) -> Self {
        Self {
            format_version,
            bytes,
            sha256: sha256.into(),
            row_count,
            row_group_count,
            indexed_column_ordinals,
        }
    }

    pub(in crate::storage::native) fn format_version(&self) -> u32 {
        self.format_version
    }

    pub(in crate::storage::native) fn bytes(&self) -> u64 {
        self.bytes
    }

    pub(in crate::storage::native) fn sha256(&self) -> &str {
        &self.sha256
    }

    pub(in crate::storage::native) fn row_count(&self) -> u64 {
        self.row_count
    }

    pub(in crate::storage::native) fn row_group_count(&self) -> u64 {
        self.row_group_count
    }

    pub(in crate::storage::native) fn indexed_column_ordinals(&self) -> &[u32] {
        &self.indexed_column_ordinals
    }
}

/// Query-facing binding between one immutable Native segment and its optional
/// predicate companion. Paths are kept together so callers never align files
/// by resolver ordering.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct NativePredicateSidecarBinding {
    data_path: PathBuf,
    sidecar_path: PathBuf,
    descriptor: PredicateSidecarDescriptor,
    segment_sha256: String,
    schema_fingerprint: String,
    segment_rows: u64,
}

impl NativePredicateSidecarBinding {
    pub(super) fn new(
        data_path: PathBuf,
        sidecar_path: PathBuf,
        descriptor: PredicateSidecarDescriptor,
        segment_sha256: impl Into<String>,
        schema_fingerprint: impl Into<String>,
        segment_rows: u64,
    ) -> Self {
        Self {
            data_path,
            sidecar_path,
            descriptor,
            segment_sha256: segment_sha256.into(),
            schema_fingerprint: schema_fingerprint.into(),
            segment_rows,
        }
    }

    pub(crate) fn data_path(&self) -> &Path {
        &self.data_path
    }

    pub(crate) fn sidecar_path(&self) -> &Path {
        &self.sidecar_path
    }

    pub(crate) fn format_version(&self) -> u32 {
        self.descriptor.format_version()
    }

    pub(crate) fn sidecar_bytes(&self) -> u64 {
        self.descriptor.bytes()
    }

    pub(crate) fn sidecar_sha256(&self) -> &str {
        self.descriptor.sha256()
    }

    pub(crate) fn row_group_count(&self) -> u64 {
        self.descriptor.row_group_count()
    }

    pub(crate) fn indexed_column_ordinals(&self) -> &[u32] {
        self.descriptor.indexed_column_ordinals()
    }

    pub(crate) fn segment_sha256(&self) -> &str {
        &self.segment_sha256
    }

    pub(crate) fn schema_fingerprint(&self) -> &str {
        &self.schema_fingerprint
    }

    pub(crate) fn segment_rows(&self) -> u64 {
        self.segment_rows
    }
}
