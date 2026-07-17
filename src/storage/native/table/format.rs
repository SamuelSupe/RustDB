use serde::{Deserialize, Serialize};

use super::{NativeSegment, SnapshotOperation};
use crate::storage::native::manifest::TableReference;

pub(super) const FORMAT_VERSION: u32 = 2;
const LEGACY_FORMAT_VERSION: u32 = 1;
pub(super) const SCHEMA_ENCODING: &str = "arrow-ipc-flatbuffer-hex-v1";

pub(super) fn supported_format_version(version: u32) -> bool {
    matches!(version, LEGACY_FORMAT_VERSION | FORMAT_VERSION)
}

pub(super) fn supports_predicate_sidecars(version: u32) -> bool {
    version == FORMAT_VERSION
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct StoredSchema {
    pub(super) encoding: String,
    pub(super) ipc_hex: String,
    pub(super) sha256: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct TableManifest {
    pub(super) database_id: String,
    pub(super) format_version: u32,
    pub(super) table_id: String,
    pub(super) version: u64,
    pub(super) snapshot_id: String,
    pub(super) parent: Option<TableReference>,
    pub(super) operation: SnapshotOperation,
    pub(super) schema: StoredSchema,
    pub(super) source_bytes: u64,
    pub(super) row_count: u64,
    pub(super) segment_bytes: u64,
    pub(super) segments: Vec<NativeSegment>,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ManifestEnvelope {
    pub(super) manifest: TableManifest,
    pub(super) sha256: String,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct SnapshotMarker {
    pub(super) database_id: String,
    pub(super) table_id: String,
    pub(super) version: u64,
    pub(super) snapshot_id: String,
}
