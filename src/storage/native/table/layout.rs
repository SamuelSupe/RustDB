use std::path::{Path, PathBuf};

pub(super) fn snapshot_directory(
    root: &Path,
    table_id: &str,
    version: u64,
    snapshot_id: &str,
) -> PathBuf {
    root.join("tables")
        .join(table_id)
        .join("snapshots")
        .join(format!("{version:020}-{snapshot_id}"))
}

pub(super) fn manifest_path(directory: &Path) -> PathBuf {
    directory.join("manifest.json")
}

pub(super) fn marker_path(directory: &Path) -> PathBuf {
    directory.join(".rustdb-snapshot")
}

pub(super) fn segment_path(
    root: &Path,
    table_id: &str,
    owner_version: u64,
    owner_snapshot_id: &str,
    segment_id: &str,
) -> PathBuf {
    snapshot_directory(root, table_id, owner_version, owner_snapshot_id)
        .join("segments")
        .join(format!("{segment_id}.rdbseg"))
}

pub(super) fn predicate_sidecar_path(
    root: &Path,
    table_id: &str,
    owner_version: u64,
    owner_snapshot_id: &str,
    segment_id: &str,
) -> PathBuf {
    snapshot_directory(root, table_id, owner_version, owner_snapshot_id)
        .join("segments")
        .join(format!("{segment_id}.rdbpred"))
}
