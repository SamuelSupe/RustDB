use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SegmentMetadata {
    format_version: u32,
    schema_fingerprint: String,
    rows: u64,
    bytes: u64,
    sha256: String,
}

impl SegmentMetadata {
    pub(super) fn new(
        format_version: u32,
        schema_fingerprint: String,
        rows: u64,
        bytes: u64,
        sha256: String,
    ) -> Self {
        Self {
            format_version,
            schema_fingerprint,
            rows,
            bytes,
            sha256,
        }
    }

    pub(crate) fn format_version(&self) -> u32 {
        self.format_version
    }

    pub(crate) fn schema_fingerprint(&self) -> &str {
        &self.schema_fingerprint
    }

    pub(crate) fn rows(&self) -> u64 {
        self.rows
    }

    pub(crate) fn bytes(&self) -> u64 {
        self.bytes
    }

    pub(crate) fn sha256(&self) -> &str {
        &self.sha256
    }
}
