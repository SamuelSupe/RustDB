use sha2::{Digest, Sha256};

use crate::{Error, Result};

use super::NativeWritePlan;
use crate::storage::native::{
    StagedSnapshot,
    disk_budget::DiskBudget,
    segment::{
        predicate_file_writer,
        predicate_sidecar::{FILE_FORMAT_VERSION, PredicateSidecarArtifact},
    },
    table::{NativeSegment, PredicateSidecarDescriptor, TableSnapshot, staged_metadata_bytes},
};

const MAX_PENDING_BYTES: usize = 64 * 1024 * 1024;

#[derive(Default)]
pub(super) struct PendingSidecars {
    entries: Vec<PendingSidecar>,
    bytes: usize,
}

struct PendingSidecar {
    new_segment_index: usize,
    segment_id: String,
    descriptor: PredicateSidecarDescriptor,
    artifact: PredicateSidecarArtifact,
}

impl PendingSidecars {
    pub(super) fn retain(
        &mut self,
        new_segment_index: usize,
        segment_id: String,
        row_count: u64,
        artifact: PredicateSidecarArtifact,
    ) {
        let Some(next_bytes) = self.bytes.checked_add(artifact.bytes.len()) else {
            return;
        };
        if next_bytes > MAX_PENDING_BYTES {
            return;
        }
        let Ok(artifact_bytes) = u64::try_from(artifact.bytes.len()) else {
            return;
        };
        let descriptor = PredicateSidecarDescriptor::new(
            u32::from(FILE_FORMAT_VERSION),
            artifact_bytes,
            format!("{:x}", Sha256::digest(&artifact.bytes)),
            row_count,
            artifact.row_group_count,
            artifact.indexed_column_ordinals.clone(),
        );
        self.bytes = next_bytes;
        self.entries.push(PendingSidecar {
            new_segment_index,
            segment_id,
            descriptor,
            artifact,
        });
    }

    pub(super) fn admit_and_write(
        self,
        disk_budget: &DiskBudget,
        plan: &NativeWritePlan,
        staging: &StagedSnapshot,
        database_id: &str,
        source_bytes: u64,
        mut segments: Vec<NativeSegment>,
    ) -> Result<Vec<NativeSegment>> {
        let inherited = plan.inherited_segments.len();
        let mut admitted_bytes = 0_u64;
        let mut admitted = Vec::new();
        for pending in self.entries {
            let index = inherited
                .checked_add(pending.new_segment_index)
                .ok_or_else(|| Error::Internal("native segment index overflow".to_owned()))?;
            let Some(segment) = segments.get(index) else {
                return Err(Error::Internal(
                    "native predicate sidecar lost its segment".to_owned(),
                ));
            };
            if segment.segment_id() != pending.segment_id {
                return Err(Error::Internal(
                    "native predicate sidecar segment binding changed".to_owned(),
                ));
            }
            let mut tentative = segments.clone();
            tentative[index] = segment
                .clone()
                .with_predicate_sidecar(pending.descriptor.clone());
            let snapshot = snapshot(plan, database_id, source_bytes, tentative.clone())?;
            let metadata_bytes = staged_metadata_bytes(staging.snapshot_directory(), &snapshot)?;
            let sidecar_bytes = pending.descriptor.bytes();
            let required = admitted_bytes
                .checked_add(sidecar_bytes)
                .and_then(|bytes| bytes.checked_add(metadata_bytes));
            if required.is_some_and(|bytes| bytes <= disk_budget.remaining()) {
                admitted_bytes += sidecar_bytes;
                segments = tentative;
                admitted.push(pending);
            }
        }

        for pending in admitted {
            predicate_file_writer::write(
                &staging.predicate_sidecar_path(&pending.segment_id),
                &pending.artifact,
                pending.descriptor.sha256(),
                disk_budget,
            )?;
        }
        Ok(segments)
    }
}

fn snapshot(
    plan: &NativeWritePlan,
    database_id: &str,
    source_bytes: u64,
    segments: Vec<NativeSegment>,
) -> Result<TableSnapshot> {
    TableSnapshot::new(
        database_id,
        plan.table_id.clone(),
        plan.version,
        plan.snapshot_id.clone(),
        plan.parent.clone(),
        plan.operation,
        plan.schema.clone(),
        source_bytes,
        segments,
    )
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::datatypes::{DataType, Field, Schema};
    use serde_json::json;
    use uuid::Uuid;

    use super::*;
    use crate::storage::native::{
        disk_budget::DiskBudget,
        schema,
        segment::predicate_sidecar::PredicateSidecarArtifact,
        table::{SnapshotOperation, write_staged_with_budget},
    };

    #[test]
    fn optional_sidecars_leave_exact_room_for_staged_metadata() {
        let root = tempfile::tempdir().unwrap();
        std::fs::create_dir(root.path().join("staging")).unwrap();
        let database_id = Uuid::new_v4().to_string();
        let staging = StagedSnapshot::begin(root.path(), &database_id).unwrap();
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let table_id = Uuid::new_v4().to_string();
        let snapshot_id = Uuid::new_v4().to_string();
        let schema_fingerprint = schema::sha256(&schema::encode(&schema));
        let segment_ids = [Uuid::new_v4().to_string(), Uuid::new_v4().to_string()];
        let segments = segment_ids
            .iter()
            .map(|segment_id| {
                serde_json::from_value(json!({
                    "segment_id": segment_id,
                    "owner_version": 1,
                    "owner_snapshot_id": snapshot_id,
                    "format_version": crate::storage::native::segment::FORMAT_VERSION,
                    "schema_fingerprint": schema_fingerprint,
                    "rows": 1,
                    "bytes": 1,
                    "sha256": "11".repeat(32),
                }))
                .unwrap()
            })
            .collect::<Vec<NativeSegment>>();
        let plan = NativeWritePlan {
            name: "events".to_owned(),
            expected_generation: 0,
            table_id,
            version: 1,
            snapshot_id,
            parent: None,
            operation: SnapshotOperation::Import,
            schema,
            inherited_source_bytes: 0,
            new_source_bytes: 2,
            retained_old_source_bytes: 0,
            retained_old_storage_bytes: 0,
            inherited_storage_bytes: 0,
            new_snapshot_limit: 0,
            inherited_segments: Vec::new(),
        };
        let source_bytes = 2;
        let base_snapshot = snapshot(&plan, &database_id, source_bytes, segments.clone()).unwrap();
        let metadata_bytes =
            staged_metadata_bytes(staging.snapshot_directory(), &base_snapshot).unwrap();
        let disk_budget = DiskBudget::new(metadata_bytes);
        let mut pending = PendingSidecars::default();
        for (index, segment_id) in segment_ids.iter().enumerate() {
            pending.retain(
                index,
                segment_id.clone(),
                1,
                PredicateSidecarArtifact {
                    bytes: vec![index as u8; 8],
                    row_group_count: 1,
                    indexed_column_ordinals: vec![0],
                    _reservation: None,
                },
            );
        }

        let admitted = pending
            .admit_and_write(
                &disk_budget,
                &plan,
                &staging,
                &database_id,
                source_bytes,
                segments,
            )
            .unwrap();

        assert!(
            admitted
                .iter()
                .all(|segment| segment.predicate_sidecar().is_none())
        );
        assert!(
            segment_ids
                .iter()
                .all(|segment_id| { !staging.predicate_sidecar_path(segment_id).exists() })
        );
        let mut final_snapshot = snapshot(&plan, &database_id, source_bytes, admitted).unwrap();
        write_staged_with_budget(
            staging.snapshot_directory(),
            &mut final_snapshot,
            &disk_budget,
        )
        .unwrap();
        assert_eq!(disk_budget.used(), metadata_bytes);

        staging.abort().unwrap();
    }
}
