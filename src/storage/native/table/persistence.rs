use std::path::Path;

use serde::Serialize;

use crate::{Error, Result};

use super::{
    TableSnapshot,
    format::{ManifestEnvelope, SCHEMA_ENCODING, SnapshotMarker, StoredSchema, TableManifest},
    layout, schema, verify,
};
use crate::storage::native::{disk_budget::DiskBudget, io, manifest::TableReference};

pub(super) const MAX_SNAPSHOT_MARKER_BYTES: usize = 4 * 1024;
// A 64 MiB envelope supports very wide schemas and hundreds of thousands of
// segment references while keeping corrupt JSON bounded.
pub(super) const MAX_TABLE_MANIFEST_BYTES: usize = 64 * 1024 * 1024;

#[derive(Serialize)]
struct ManifestEnvelopeRef<'a> {
    manifest: &'a TableManifest,
    sha256: &'a str,
}

struct EncodedStagedMetadata {
    marker: Vec<u8>,
    manifest: Vec<u8>,
    manifest_sha256: String,
}

impl EncodedStagedMetadata {
    fn bytes(&self) -> Result<u64> {
        let bytes = self
            .marker
            .len()
            .checked_add(self.manifest.len())
            .ok_or_else(|| {
                Error::ResourceExhausted("native snapshot metadata size overflow".to_owned())
            })?;
        u64::try_from(bytes).map_err(|_| {
            Error::ResourceExhausted("native snapshot metadata size does not fit in u64".to_owned())
        })
    }
}

#[cfg(test)]
pub(in crate::storage::native) fn write_staged(
    directory: &Path,
    snapshot: &mut TableSnapshot,
) -> Result<()> {
    write_staged_with_budget(directory, snapshot, &DiskBudget::unlimited())
}

pub(in crate::storage::native) fn write_staged_with_budget(
    directory: &Path,
    snapshot: &mut TableSnapshot,
    budget: &DiskBudget,
) -> Result<()> {
    verify::snapshot(snapshot, directory)?;
    let marker_path = layout::marker_path(directory);
    let manifest_path = layout::manifest_path(directory);
    let encoded = encode_staged(directory, snapshot)?;
    let metadata_bytes = encoded.bytes()?;
    budget.reserve_metadata(metadata_bytes, directory)?;
    io::atomic_create(&marker_path, &encoded.marker)?;
    io::atomic_create(&manifest_path, &encoded.manifest)?;
    io::sync_dir(directory)?;
    snapshot.manifest_sha256 = encoded.manifest_sha256;
    Ok(())
}

#[cfg(test)]
pub(in crate::storage::native) fn staged_metadata_bytes(
    directory: &Path,
    snapshot: &TableSnapshot,
) -> Result<u64> {
    encode_staged(directory, snapshot)?.bytes()
}

fn encode_staged(directory: &Path, snapshot: &TableSnapshot) -> Result<EncodedStagedMetadata> {
    let marker_path = layout::marker_path(directory);
    let manifest_path = layout::manifest_path(directory);
    let schema_bytes = schema::encode(&snapshot.schema);
    schema::validate_encoded_size(&manifest_path, schema_bytes.len())?;
    let manifest = TableManifest {
        database_id: snapshot.database_id.clone(),
        format_version: super::format::FORMAT_VERSION,
        table_id: snapshot.table_id.clone(),
        version: snapshot.version,
        snapshot_id: snapshot.snapshot_id.clone(),
        parent: snapshot.parent.clone(),
        operation: snapshot.operation,
        schema: StoredSchema {
            encoding: SCHEMA_ENCODING.to_owned(),
            ipc_hex: schema::encode_hex(&schema_bytes),
            sha256: schema::sha256(&schema_bytes),
        },
        source_bytes: snapshot.source_bytes,
        row_count: snapshot.row_count,
        segment_bytes: snapshot.segment_bytes,
        segments: snapshot.segments.to_vec(),
    };
    let checksum = io::json_sha256(
        &manifest_path,
        &manifest,
        MAX_TABLE_MANIFEST_BYTES,
        "table manifest",
    )?;
    let envelope = ManifestEnvelopeRef {
        manifest: &manifest,
        sha256: &checksum,
    };
    let manifest = io::encode_json_bounded(
        &manifest_path,
        &envelope,
        MAX_TABLE_MANIFEST_BYTES,
        "table manifest",
        true,
        true,
    )?;
    let marker = encode_marker(directory, snapshot)?;
    io::validate_size(
        &marker_path,
        marker.len(),
        MAX_SNAPSHOT_MARKER_BYTES,
        "snapshot marker",
    )?;
    Ok(EncodedStagedMetadata {
        marker,
        manifest,
        manifest_sha256: checksum,
    })
}

pub(in crate::storage::native) fn load(
    root: &Path,
    database_id: &str,
    reference: &TableReference,
) -> Result<TableSnapshot> {
    io::require_directory(&root.join("tables"))?;
    io::require_directory(&root.join("tables").join(reference.table_id()))?;
    io::require_directory(
        &root
            .join("tables")
            .join(reference.table_id())
            .join("snapshots"),
    )?;
    let directory = layout::snapshot_directory(
        root,
        reference.table_id(),
        reference.version(),
        reference.snapshot_id(),
    );
    io::require_directory(&directory)?;
    let path = layout::manifest_path(&directory);
    let marker = read_marker(&directory)?;
    let bytes = io::read_bounded(&path, MAX_TABLE_MANIFEST_BYTES, "table manifest")?;
    let envelope: ManifestEnvelope = serde_json::from_slice(&bytes).map_err(|error| {
        Error::native_storage(&path, format!("invalid table manifest: {error}"))
    })?;
    let actual = io::json_sha256(
        &path,
        &envelope.manifest,
        MAX_TABLE_MANIFEST_BYTES,
        "table manifest",
    )?;
    if actual != envelope.sha256 || actual != reference.manifest_sha256() {
        return Err(Error::native_storage(
            &path,
            "table manifest checksum mismatch",
        ));
    }
    let manifest = envelope.manifest;
    if manifest.database_id != database_id
        || manifest.database_id != marker.database_id
        || manifest.table_id != reference.table_id()
        || manifest.table_id != marker.table_id
        || manifest.version != reference.version()
        || manifest.version != marker.version
        || manifest.snapshot_id != reference.snapshot_id()
        || manifest.snapshot_id != marker.snapshot_id
        || !super::format::supported_format_version(manifest.format_version)
        || manifest.schema.encoding != SCHEMA_ENCODING
    {
        return Err(Error::native_storage(
            &path,
            "table snapshot identity or format mismatch",
        ));
    }
    if !super::format::supports_predicate_sidecars(manifest.format_version)
        && manifest
            .segments
            .iter()
            .any(|segment| segment.predicate_sidecar().is_some())
    {
        return Err(Error::native_storage(
            &path,
            "legacy table manifest declares a predicate sidecar",
        ));
    }
    let schema = schema::decode(&path, &manifest.schema.ipc_hex, &manifest.schema.sha256)?;
    let mut snapshot = TableSnapshot {
        database_id: manifest.database_id,
        table_id: manifest.table_id,
        version: manifest.version,
        snapshot_id: manifest.snapshot_id,
        parent: manifest.parent,
        operation: manifest.operation,
        schema,
        schema_fingerprint: manifest.schema.sha256,
        source_bytes: manifest.source_bytes,
        row_count: manifest.row_count,
        segment_bytes: manifest.segment_bytes,
        storage_bytes: 0,
        segments: manifest.segments.into(),
        verified_segment_fingerprints: Vec::new().into(),
        manifest_sha256: actual,
        location_leases: Default::default(),
    };
    snapshot.initialize_location_leases();
    verify::snapshot(&snapshot, &path)?;
    verify::segment_owners(root, &snapshot)?;
    let mut verified_segment_fingerprints = Vec::with_capacity(snapshot.segments().len());
    for segment in snapshot.segments() {
        verified_segment_fingerprints.push(verify::segment_file(root, &snapshot, segment)?);
    }
    snapshot.set_verified_segment_fingerprints(verified_segment_fingerprints)?;
    snapshot.storage_bytes =
        crate::storage::native::disk_budget::snapshot_storage_bytes(root, &snapshot)?;
    Ok(snapshot)
}

fn encode_marker(directory: &Path, snapshot: &TableSnapshot) -> Result<Vec<u8>> {
    let marker = SnapshotMarker {
        database_id: snapshot.database_id.clone(),
        table_id: snapshot.table_id.clone(),
        version: snapshot.version,
        snapshot_id: snapshot.snapshot_id.clone(),
    };
    io::encode_json_bounded(
        directory,
        &marker,
        MAX_SNAPSHOT_MARKER_BYTES,
        "snapshot marker",
        true,
        true,
    )
}

pub(super) fn read_marker(directory: &Path) -> Result<SnapshotMarker> {
    let path = layout::marker_path(directory);
    let bytes = io::read_bounded(&path, MAX_SNAPSHOT_MARKER_BYTES, "snapshot marker")?;
    serde_json::from_slice(&bytes).map_err(|error| {
        Error::native_storage(directory, format!("invalid snapshot marker: {error}"))
    })
}
