use std::{
    collections::{BTreeMap, BTreeSet},
    path::{Path, PathBuf},
    sync::Arc,
};

use arrow::datatypes::SchemaRef;
use serde::{Deserialize, Serialize};

use crate::{Error, Result};

use super::{manifest::TableReference, schema, segment::SegmentMetadata};

mod format;
mod layout;
mod persistence;
mod recovery;
mod sidecar;
mod verify;

#[cfg(test)]
pub(super) use persistence::staged_metadata_bytes;
#[cfg(test)]
pub(super) use persistence::write_staged;
pub(super) use persistence::{load, write_staged_with_budget};
pub(super) use recovery::{
    SnapshotRemoval, prune_inherited_manifests, recover_orphans, remove_snapshot,
};
pub(crate) use sidecar::NativePredicateSidecarBinding;
pub(in crate::storage::native) use sidecar::PredicateSidecarDescriptor;

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum SnapshotOperation {
    Import,
    Append,
    Replace,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct NativeSegment {
    segment_id: String,
    owner_version: u64,
    owner_snapshot_id: String,
    format_version: u32,
    schema_fingerprint: String,
    rows: u64,
    bytes: u64,
    sha256: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    predicate_sidecar: Option<PredicateSidecarDescriptor>,
}

impl NativeSegment {
    pub(super) fn new(
        segment_id: impl Into<String>,
        owner_version: u64,
        owner_snapshot_id: impl Into<String>,
        metadata: SegmentMetadata,
    ) -> Self {
        Self {
            segment_id: segment_id.into(),
            owner_version,
            owner_snapshot_id: owner_snapshot_id.into(),
            format_version: metadata.format_version(),
            schema_fingerprint: metadata.schema_fingerprint().to_owned(),
            rows: metadata.rows(),
            bytes: metadata.bytes(),
            sha256: metadata.sha256().to_owned(),
            predicate_sidecar: None,
        }
    }

    #[cfg(test)]
    pub(in crate::storage::native) fn with_predicate_sidecar(
        mut self,
        descriptor: PredicateSidecarDescriptor,
    ) -> Self {
        self.predicate_sidecar = Some(descriptor);
        self
    }

    pub(super) fn segment_id(&self) -> &str {
        &self.segment_id
    }
    pub(super) fn owner_version(&self) -> u64 {
        self.owner_version
    }
    pub(super) fn owner_snapshot_id(&self) -> &str {
        &self.owner_snapshot_id
    }
    pub(super) fn format_version(&self) -> u32 {
        self.format_version
    }
    pub(super) fn schema_fingerprint(&self) -> &str {
        &self.schema_fingerprint
    }
    pub(super) fn rows(&self) -> u64 {
        self.rows
    }
    pub(super) fn bytes(&self) -> u64 {
        self.bytes
    }
    pub(super) fn sha256(&self) -> &str {
        &self.sha256
    }
    pub(in crate::storage::native) fn predicate_sidecar(
        &self,
    ) -> Option<&PredicateSidecarDescriptor> {
        self.predicate_sidecar.as_ref()
    }
}

#[derive(Clone, Debug)]
pub(crate) struct TableSnapshot {
    database_id: String,
    table_id: String,
    version: u64,
    snapshot_id: String,
    parent: Option<TableReference>,
    operation: SnapshotOperation,
    schema: SchemaRef,
    schema_fingerprint: String,
    source_bytes: u64,
    row_count: u64,
    segment_bytes: u64,
    storage_bytes: u64,
    segments: Arc<[NativeSegment]>,
    /// Process-local identities captured by the full segment verification
    /// that made this snapshot visible. These are never persisted and only
    /// seed the query verifier; every query still re-reads the current file
    /// identity before trusting the seed.
    verified_segment_fingerprints: Arc<[Option<String>]>,
    manifest_sha256: String,
    location_leases: BTreeMap<SnapshotLocation, Arc<()>>,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(super) struct SnapshotLocation {
    version: u64,
    snapshot_id: String,
}

impl TableSnapshot {
    #[allow(clippy::too_many_arguments)]
    pub(super) fn new(
        database_id: impl Into<String>,
        table_id: impl Into<String>,
        version: u64,
        snapshot_id: impl Into<String>,
        parent: Option<TableReference>,
        operation: SnapshotOperation,
        schema: SchemaRef,
        source_bytes: u64,
        segments: Vec<NativeSegment>,
    ) -> Result<Self> {
        let schema_bytes = schema::encode(&schema);
        let row_count = checked_sum(segments.iter().map(NativeSegment::rows), "row count")?;
        let segment_bytes =
            checked_sum(segments.iter().map(NativeSegment::bytes), "segment bytes")?;
        let verified_segment_fingerprints = vec![None; segments.len()].into();
        let mut snapshot = Self {
            database_id: database_id.into(),
            table_id: table_id.into(),
            version,
            snapshot_id: snapshot_id.into(),
            parent,
            operation,
            schema,
            schema_fingerprint: schema::sha256(&schema_bytes),
            source_bytes,
            row_count,
            segment_bytes,
            storage_bytes: 0,
            segments: segments.into(),
            verified_segment_fingerprints,
            manifest_sha256: String::new(),
            location_leases: BTreeMap::new(),
        };
        snapshot.initialize_location_leases();
        verify::snapshot(&snapshot, Path::new("native table snapshot"))?;
        Ok(snapshot)
    }

    pub(super) fn database_id(&self) -> &str {
        &self.database_id
    }
    pub(crate) fn table_id(&self) -> &str {
        &self.table_id
    }
    pub(crate) fn version(&self) -> u64 {
        self.version
    }
    pub(crate) fn snapshot_id(&self) -> &str {
        &self.snapshot_id
    }
    pub(super) fn parent(&self) -> Option<&TableReference> {
        self.parent.as_ref()
    }
    pub(super) fn operation(&self) -> SnapshotOperation {
        self.operation
    }
    pub(crate) fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
    pub(crate) fn schema_fingerprint(&self) -> &str {
        &self.schema_fingerprint
    }
    pub(super) fn source_bytes(&self) -> u64 {
        self.source_bytes
    }
    pub(crate) fn row_count(&self) -> u64 {
        self.row_count
    }
    pub(crate) fn segment_bytes(&self) -> u64 {
        self.segment_bytes
    }
    pub(crate) fn storage_bytes(&self) -> u64 {
        self.storage_bytes
    }
    pub(in crate::storage::native) fn set_storage_bytes(&mut self, bytes: u64) {
        self.storage_bytes = bytes;
    }
    pub(super) fn segments(&self) -> &[NativeSegment] {
        &self.segments
    }
    pub(crate) fn manifest_sha256(&self) -> &str {
        &self.manifest_sha256
    }

    pub(crate) fn segment_verification_keys(&self) -> Vec<String> {
        self.segments
            .iter()
            .map(|segment| {
                let segment_key = format!(
                    "{}:{}:{}:{}:{}:{}:{}",
                    self.snapshot_id,
                    segment.segment_id(),
                    segment.owner_version(),
                    segment.owner_snapshot_id(),
                    segment.rows(),
                    segment.bytes(),
                    segment.sha256()
                );
                match segment.predicate_sidecar() {
                    Some(sidecar) => format!(
                        "{segment_key}:predicate:{}:{}:{}:{}:{}:{:?}",
                        sidecar.format_version(),
                        sidecar.bytes(),
                        sidecar.sha256(),
                        sidecar.row_count(),
                        sidecar.row_group_count(),
                        sidecar.indexed_column_ordinals()
                    ),
                    None => segment_key,
                }
            })
            .collect()
    }

    pub(crate) fn verified_segment_fingerprints(&self) -> &[Option<String>] {
        &self.verified_segment_fingerprints
    }

    pub(in crate::storage::native) fn set_verified_segment_fingerprints(
        &mut self,
        fingerprints: Vec<Option<String>>,
    ) -> Result<()> {
        if fingerprints.len() != self.segments.len() {
            return Err(Error::Internal(
                "native segment verification returned an invalid fingerprint count".to_owned(),
            ));
        }
        self.verified_segment_fingerprints = fingerprints.into();
        Ok(())
    }

    pub(crate) fn segment_paths(&self, root: &Path) -> Vec<std::path::PathBuf> {
        self.segments
            .iter()
            .map(|segment| {
                layout::segment_path(
                    root,
                    self.table_id(),
                    segment.owner_version(),
                    segment.owner_snapshot_id(),
                    segment.segment_id(),
                )
            })
            .collect()
    }

    pub(crate) fn predicate_sidecar_paths(&self, root: &Path) -> Vec<PathBuf> {
        self.segments
            .iter()
            .filter(|segment| segment.predicate_sidecar().is_some())
            .map(|segment| {
                layout::predicate_sidecar_path(
                    root,
                    self.table_id(),
                    segment.owner_version(),
                    segment.owner_snapshot_id(),
                    segment.segment_id(),
                )
            })
            .collect()
    }

    pub(crate) fn predicate_sidecar_bindings(
        &self,
        root: &Path,
    ) -> Vec<NativePredicateSidecarBinding> {
        self.segments
            .iter()
            .filter_map(|segment| {
                let descriptor = segment.predicate_sidecar()?.clone();
                let data_path = layout::segment_path(
                    root,
                    self.table_id(),
                    segment.owner_version(),
                    segment.owner_snapshot_id(),
                    segment.segment_id(),
                );
                let sidecar_path = layout::predicate_sidecar_path(
                    root,
                    self.table_id(),
                    segment.owner_version(),
                    segment.owner_snapshot_id(),
                    segment.segment_id(),
                );
                Some(NativePredicateSidecarBinding::new(
                    data_path,
                    sidecar_path,
                    descriptor,
                    segment.sha256(),
                    segment.schema_fingerprint(),
                    segment.rows(),
                ))
            })
            .collect()
    }

    pub(crate) fn segment_file_fingerprints(
        &self,
        root: &Path,
        mut check_cancelled: impl FnMut() -> Result<()>,
    ) -> Result<Vec<Option<String>>> {
        verify::segment_fingerprints_cancelable(root, self, &mut check_cancelled)
    }

    pub(crate) fn verify_segment_indices(
        &self,
        root: &Path,
        indices: &[usize],
        mut check_cancelled: impl FnMut() -> Result<()>,
    ) -> Result<Vec<Option<String>>> {
        verify::segment_indices_cancelable(root, self, indices, &mut check_cancelled)
    }

    #[cfg(test)]
    pub(crate) fn full_verification_count_for_test(path: &Path) -> usize {
        verify::full_verification_count(path)
    }

    pub(super) fn final_directory(&self, root: &Path) -> std::path::PathBuf {
        layout::snapshot_directory(root, self.table_id(), self.version(), self.snapshot_id())
    }

    pub(super) fn table_reference(&self) -> TableReference {
        TableReference::new(
            self.table_id.clone(),
            self.version,
            self.snapshot_id.clone(),
            self.manifest_sha256.clone(),
        )
    }

    pub(super) fn reachable_locations(&self) -> Vec<SnapshotLocation> {
        let mut locations = BTreeSet::from([SnapshotLocation {
            version: self.version,
            snapshot_id: self.snapshot_id.clone(),
        }]);
        locations.extend(self.segments.iter().map(|segment| SnapshotLocation {
            version: segment.owner_version(),
            snapshot_id: segment.owner_snapshot_id().to_owned(),
        }));
        locations.into_iter().collect()
    }

    pub(super) fn initialize_location_leases(&mut self) {
        self.location_leases = self
            .reachable_locations()
            .into_iter()
            .map(|location| (location, Arc::new(())))
            .collect();
    }

    pub(super) fn inherit_location_leases(&mut self, previous: &Self) {
        self.location_leases = self
            .reachable_locations()
            .into_iter()
            .map(|location| {
                let lease = previous
                    .location_leases
                    .get(&location)
                    .cloned()
                    .unwrap_or_else(|| Arc::new(()));
                (location, lease)
            })
            .collect();
    }

    pub(super) fn location_lease(&self, location: &SnapshotLocation) -> std::sync::Weak<()> {
        Arc::downgrade(
            self.location_leases
                .get(location)
                .expect("reachable native location must have a lifetime lease"),
        )
    }

    pub(in crate::storage::native) fn reachable_directories(&self, root: &Path) -> Vec<PathBuf> {
        self.reachable_locations()
            .into_iter()
            .map(|location| {
                layout::snapshot_directory(
                    root,
                    self.table_id(),
                    location.version,
                    &location.snapshot_id,
                )
            })
            .collect()
    }
}

pub(super) fn location_directory(
    root: &Path,
    table_id: &str,
    location: &SnapshotLocation,
) -> PathBuf {
    layout::snapshot_directory(root, table_id, location.version, &location.snapshot_id)
}

fn checked_sum(mut values: impl Iterator<Item = u64>, name: &str) -> Result<u64> {
    values.try_fold(0_u64, |total, value| {
        total
            .checked_add(value)
            .ok_or_else(|| Error::ResourceExhausted(format!("native table {name} overflow")))
    })
}

#[cfg(test)]
mod sidecar_tests;
#[cfg(test)]
mod tests;
