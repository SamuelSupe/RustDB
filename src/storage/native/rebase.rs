use std::{collections::BTreeMap, sync::Arc};

use uuid::Uuid;

use crate::{Error, Result};

use super::{
    NativeDatabase, NativePublishedSnapshot, NativeWriteMode,
    table::{DeleteVector, NativeSegment, TableSnapshot},
};

pub(super) fn write(
    database: &NativeDatabase,
    name: &str,
    base: &Arc<TableSnapshot>,
    current: &Arc<TableSnapshot>,
    transaction: &Arc<TableSnapshot>,
) -> Result<Option<NativePublishedSnapshot>> {
    if base.table_id() != current.table_id()
        || base.table_id() != transaction.table_id()
        || base.schema().as_ref() != current.schema().as_ref()
        || base.schema().as_ref() != transaction.schema().as_ref()
    {
        return Ok(None);
    }
    let base_segments = segments_by_id(base);
    let current_segments = segments_by_id(current);
    let transaction_segments = segments_by_id(transaction);
    for (id, base_segment) in &base_segments {
        if !current_segments
            .get(id)
            .is_some_and(|segment| same_data_file(base_segment, segment))
            || !transaction_segments
                .get(id)
                .is_some_and(|segment| same_data_file(base_segment, segment))
        {
            return Ok(None);
        }
    }

    let root = database.path();
    let base_vectors = vectors_by_id(base, root)?;
    let current_vectors = vectors_by_id(current, root)?;
    let transaction_vectors = vectors_by_id(transaction, root)?;
    let mut merged_vectors = Vec::new();
    for (id, segment) in &base_segments {
        let base_vector = base_vectors.get(id).and_then(Option::as_ref);
        let current_vector = current_vectors.get(id).and_then(Option::as_ref);
        let transaction_vector = transaction_vectors.get(id).and_then(Option::as_ref);
        let mut merged = DeleteVector::empty(segment.rows())?;
        let mut transaction_changed = false;
        for offset in 0..segment.rows() {
            let base_deleted = base_vector.is_some_and(|vector| vector.contains(offset));
            let current_deleted = current_vector.is_some_and(|vector| vector.contains(offset));
            let transaction_deleted =
                transaction_vector.is_some_and(|vector| vector.contains(offset));
            if !base_deleted && current_deleted && transaction_deleted {
                return Ok(None);
            }
            if !base_deleted && transaction_deleted {
                transaction_changed = true;
            }
            if current_deleted || transaction_deleted {
                merged.mark_deleted(offset)?;
            }
        }
        if transaction_changed {
            merged_vectors.push((id.clone(), merged));
        }
    }

    let mut merged_segments = current.segments().to_vec();
    for segment in transaction.segments() {
        if base_segments.contains_key(segment.segment_id()) {
            continue;
        }
        if current_segments.contains_key(segment.segment_id()) {
            return Ok(None);
        }
        merged_segments.push(segment.clone());
    }
    let delta_source_bytes = transaction
        .source_bytes()
        .checked_sub(base.source_bytes())
        .ok_or_else(|| Error::Internal("transaction source bytes regressed".to_owned()))?;
    let mode = if merged_vectors.is_empty() {
        NativeWriteMode::Append
    } else {
        NativeWriteMode::Update
    };
    let schemas = database.state.lock().catalog.schemas().clone();
    let mut plan = database.plan_transaction_write(
        name,
        mode,
        database.catalog_generation(),
        Some(Arc::clone(current)),
        &schemas,
        transaction.schema(),
        delta_source_bytes,
    )?;
    plan.inherited_segments = merged_segments;
    let transaction_storage_delta = transaction
        .storage_bytes()
        .saturating_sub(base.storage_bytes());
    plan.inherited_storage_bytes = plan
        .inherited_storage_bytes
        .checked_add(transaction_storage_delta)
        .ok_or_else(|| {
            Error::ResourceExhausted("rebased storage byte count overflow".to_owned())
        })?;
    plan.snapshot_id = Uuid::new_v4().to_string();

    let mut writer = database.start_write(plan)?;
    for (segment_id, vector) in merged_vectors {
        if let Err(error) = writer.write_delete_vector(&segment_id, &vector) {
            return match writer.abort() {
                Ok(()) => Err(error),
                Err(cleanup) => Err(Error::native_storage(
                    database.path(),
                    format!("{error}; rebased write cleanup failed: {cleanup}"),
                )),
            };
        }
    }
    let prepared = writer.finish()?;
    database
        .publish_transaction_write_under_publication_gate(prepared)
        .map(Some)
}

fn segments_by_id(snapshot: &TableSnapshot) -> BTreeMap<String, NativeSegment> {
    snapshot
        .segments()
        .iter()
        .map(|segment| (segment.segment_id().to_owned(), segment.clone()))
        .collect()
}

fn vectors_by_id(
    snapshot: &TableSnapshot,
    root: &std::path::Path,
) -> Result<BTreeMap<String, Option<DeleteVector>>> {
    Ok(snapshot
        .segments()
        .iter()
        .map(|segment| segment.segment_id().to_owned())
        .zip(snapshot.load_delete_vectors(root)?)
        .collect())
}

fn same_data_file(left: &NativeSegment, right: &NativeSegment) -> bool {
    left.segment_id() == right.segment_id()
        && left.owner_version() == right.owner_version()
        && left.owner_snapshot_id() == right.owner_snapshot_id()
        && left.format_version() == right.format_version()
        && left.schema_fingerprint() == right.schema_fingerprint()
        && left.rows() == right.rows()
        && left.bytes() == right.bytes()
        && left.sha256() == right.sha256()
        && left.predicate_sidecar() == right.predicate_sidecar()
}
