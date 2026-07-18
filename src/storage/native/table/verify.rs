use std::{collections::HashSet, fs, fs::File, io::Read, path::Path};

#[cfg(test)]
use std::{
    collections::HashMap,
    path::PathBuf,
    sync::{Mutex, OnceLock},
};

use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use sha2::{Digest, Sha256};
use uuid::Uuid;

use super::{NativeSegment, TableSnapshot, layout};
use crate::{Error, Result, storage::LocalFileIdentity};

pub(super) fn snapshot(snapshot: &TableSnapshot, path: &Path) -> Result<()> {
    Uuid::parse_str(snapshot.database_id()).map_err(|error| {
        Error::native_storage(path, format!("invalid database id in snapshot: {error}"))
    })?;
    Uuid::parse_str(snapshot.table_id()).map_err(|error| {
        Error::native_storage(path, format!("invalid table id in snapshot: {error}"))
    })?;
    Uuid::parse_str(snapshot.snapshot_id())
        .map_err(|error| Error::native_storage(path, format!("invalid snapshot id: {error}")))?;
    if snapshot.version() == 0 {
        return Err(Error::native_storage(
            path,
            "snapshot version must be positive",
        ));
    }
    match (snapshot.operation(), snapshot.parent()) {
        (super::SnapshotOperation::Import, None) if snapshot.version() == 1 => {}
        (
            super::SnapshotOperation::Append
            | super::SnapshotOperation::Replace
            | super::SnapshotOperation::Delete
            | super::SnapshotOperation::Update
            | super::SnapshotOperation::Truncate,
            Some(parent),
        ) if parent.table_id() == snapshot.table_id()
            && parent.version().checked_add(1) == Some(snapshot.version()) =>
        {
            parent.validate(path, "parent")?;
        }
        _ => {
            return Err(Error::native_storage(
                path,
                "snapshot operation, version, and parent are inconsistent",
            ));
        }
    }
    let physical_rows = checked_sum(
        path,
        snapshot.segments().iter().map(NativeSegment::rows),
        "physical row count",
    )?;
    let deleted_rows = checked_sum(
        path,
        snapshot
            .segments()
            .iter()
            .filter_map(|segment| segment.delete_vector().map(|vector| vector.deleted_rows())),
        "deleted row count",
    )?;
    let visible_rows = physical_rows
        .checked_sub(deleted_rows)
        .ok_or_else(|| Error::native_storage(path, "deleted rows exceed physical rows"))?;
    let bytes = checked_sum(
        path,
        snapshot.segments().iter().map(NativeSegment::bytes),
        "segment bytes",
    )?;
    if physical_rows != snapshot.physical_row_count()
        || visible_rows != snapshot.row_count()
        || deleted_rows != snapshot.deleted_row_count()
        || bytes != snapshot.segment_bytes()
    {
        return Err(Error::native_storage(
            path,
            "snapshot totals do not match its segment list",
        ));
    }
    let sidecar_bytes = checked_sum(
        path,
        snapshot
            .segments()
            .iter()
            .filter_map(|segment| segment.predicate_sidecar().map(|sidecar| sidecar.bytes())),
        "predicate sidecar bytes",
    )?;
    let delete_vector_bytes = checked_sum(
        path,
        snapshot
            .segments()
            .iter()
            .filter_map(|segment| segment.delete_vector().map(|vector| vector.bytes())),
        "delete vector bytes",
    )?;
    if delete_vector_bytes != snapshot.delete_vector_bytes() {
        return Err(Error::native_storage(
            path,
            "snapshot delete vector bytes do not match its segment list",
        ));
    }
    let managed_bytes = bytes
        .checked_add(sidecar_bytes)
        .and_then(|bytes| bytes.checked_add(delete_vector_bytes))
        .ok_or_else(|| Error::native_storage(path, "snapshot managed data byte count overflow"))?;
    let limit = super::super::write_plan::storage_limit(snapshot.source_bytes(), 2, "final")?;
    if managed_bytes > limit {
        return Err(Error::ResourceExhausted(format!(
            "native snapshot uses {managed_bytes} data bytes for {} source bytes, exceeding the 2x plus metadata allowance final-size limit",
            snapshot.source_bytes()
        )));
    }
    let mut segment_ids = HashSet::with_capacity(snapshot.segments().len());
    for segment in snapshot.segments() {
        if !segment_ids.insert(segment.segment_id()) {
            return Err(Error::native_storage(
                path,
                "duplicate segment id in snapshot",
            ));
        }
        segment_descriptor(snapshot, segment, path)?;
    }
    Ok(())
}

pub(super) fn segment_file(
    root: &Path,
    snapshot: &TableSnapshot,
    segment: &NativeSegment,
) -> Result<Option<String>> {
    segment_file_cancelable(root, snapshot, segment, &mut || Ok(()))
}

pub(super) fn segment_owners(root: &Path, snapshot: &TableSnapshot) -> Result<()> {
    segment_owners_cancelable(root, snapshot, &mut || Ok(()))
}

pub(super) fn segment_owners_cancelable(
    root: &Path,
    snapshot: &TableSnapshot,
    check_cancelled: &mut dyn FnMut() -> Result<()>,
) -> Result<()> {
    let mut owners = HashSet::new();
    for (version, snapshot_id) in snapshot.segments().iter().flat_map(|segment| {
        std::iter::once((segment.owner_version(), segment.owner_snapshot_id())).chain(
            segment
                .delete_vector()
                .map(|vector| (vector.owner_version(), vector.owner_snapshot_id())),
        )
    }) {
        if !owners.insert((version, snapshot_id)) {
            continue;
        }
        check_cancelled()?;
        let directory = layout::snapshot_directory(root, snapshot.table_id(), version, snapshot_id);
        super::super::io::require_directory(&directory)?;
        let marker = super::persistence::read_marker(&directory)?;
        if marker.database_id != snapshot.database_id()
            || marker.table_id != snapshot.table_id()
            || marker.version != version
            || marker.snapshot_id != snapshot_id
        {
            return Err(Error::native_storage(
                directory,
                "native segment owner marker identity mismatch",
            ));
        }
    }
    Ok(())
}

pub(super) fn segment_file_cancelable(
    root: &Path,
    snapshot: &TableSnapshot,
    segment: &NativeSegment,
    check_cancelled: &mut dyn FnMut() -> Result<()>,
) -> Result<Option<String>> {
    verify_segment_file(root, snapshot, segment, check_cancelled)
}

pub(super) fn segment_fingerprints_cancelable(
    root: &Path,
    snapshot: &TableSnapshot,
    check_cancelled: &mut dyn FnMut() -> Result<()>,
) -> Result<Vec<Option<String>>> {
    segment_owners_cancelable(root, snapshot, check_cancelled)?;
    snapshot
        .segments()
        .iter()
        .map(|segment| {
            check_cancelled()?;
            let (path, metadata) = segment_metadata(root, snapshot, segment)?;
            let sidecar = predicate_sidecar_metadata(root, snapshot, segment)?;
            let delete_vector = delete_vector_metadata(root, snapshot, segment)?;
            Ok(managed_file_fingerprint(
                &path,
                &metadata,
                sidecar.as_ref(),
                delete_vector.as_ref(),
            ))
        })
        .collect()
}

pub(super) fn segment_indices_cancelable(
    root: &Path,
    snapshot: &TableSnapshot,
    indices: &[usize],
    check_cancelled: &mut dyn FnMut() -> Result<()>,
) -> Result<Vec<Option<String>>> {
    indices
        .iter()
        .map(|&index| {
            let segment = snapshot.segments().get(index).ok_or_else(|| {
                Error::Internal("native segment verification index is out of bounds".to_owned())
            })?;
            verify_segment_file(root, snapshot, segment, check_cancelled)
        })
        .collect()
}

fn verify_segment_file(
    root: &Path,
    snapshot: &TableSnapshot,
    segment: &NativeSegment,
    check_cancelled: &mut dyn FnMut() -> Result<()>,
) -> Result<Option<String>> {
    check_cancelled()?;
    let (path, metadata) = segment_metadata(root, snapshot, segment)?;
    let sidecar = predicate_sidecar_metadata(root, snapshot, segment)?;
    let delete_vector = delete_vector_metadata(root, snapshot, segment)?;
    let before_segment = file_fingerprint(&path, &metadata);
    let before =
        managed_file_fingerprint(&path, &metadata, sidecar.as_ref(), delete_vector.as_ref());
    if sha256_file(&path, check_cancelled)? != segment.sha256() {
        return Err(Error::native_storage(
            &path,
            "native segment checksum mismatch",
        ));
    }
    if let Some((sidecar_path, _)) = &sidecar {
        let descriptor = segment
            .predicate_sidecar()
            .expect("predicate sidecar metadata requires a descriptor");
        if sha256_file(sidecar_path, check_cancelled)? != descriptor.sha256() {
            return Err(Error::native_storage(
                sidecar_path,
                "native predicate sidecar checksum mismatch",
            ));
        }
    }
    if let Some((delete_path, _)) = &delete_vector {
        let descriptor = segment
            .delete_vector()
            .expect("delete vector metadata requires a descriptor");
        if sha256_file(delete_path, check_cancelled)? != descriptor.sha256() {
            return Err(Error::native_storage(
                delete_path,
                "native delete vector checksum mismatch",
            ));
        }
        super::DeleteVector::read(delete_path, descriptor)?;
    }

    let file = File::open(&path).map_err(|error| Error::io(Some(path.clone()), error))?;
    let open_metadata = file
        .metadata()
        .map_err(|error| Error::io(Some(path.clone()), error))?;
    if file_fingerprint(&path, &open_metadata) != before_segment {
        return Err(Error::native_storage(
            &path,
            "native segment changed while being verified",
        ));
    }
    let builder = ParquetRecordBatchReaderBuilder::try_new(file).map_err(|error| {
        Error::native_storage(&path, format!("invalid native Parquet segment: {error}"))
    })?;
    let actual_rows = u64::try_from(builder.metadata().file_metadata().num_rows())
        .map_err(|_| Error::native_storage(&path, "negative native segment row count"))?;
    if actual_rows != segment.rows() {
        return Err(Error::native_storage(
            &path,
            "native segment row count mismatch",
        ));
    }
    if let Some(descriptor) = segment.predicate_sidecar() {
        let actual_row_groups =
            u64::try_from(builder.metadata().num_row_groups()).map_err(|_| {
                Error::native_storage(&path, "native segment row-group count does not fit in u64")
            })?;
        if actual_row_groups != descriptor.row_group_count() {
            return Err(Error::native_storage(
                &path,
                "native predicate sidecar row-group binding mismatch",
            ));
        }
    }
    check_cancelled()?;
    let (_, after_metadata) = segment_metadata(root, snapshot, segment)?;
    let after_sidecar = predicate_sidecar_metadata(root, snapshot, segment)?;
    let after_delete_vector = delete_vector_metadata(root, snapshot, segment)?;
    let after = managed_file_fingerprint(
        &path,
        &after_metadata,
        after_sidecar.as_ref(),
        after_delete_vector.as_ref(),
    );
    if before != after {
        return Err(Error::native_storage(
            &path,
            "native segment changed while being verified",
        ));
    }
    Ok(after)
}

fn segment_metadata(
    root: &Path,
    snapshot: &TableSnapshot,
    segment: &NativeSegment,
) -> Result<(std::path::PathBuf, fs::Metadata)> {
    let owner = layout::snapshot_directory(
        root,
        snapshot.table_id(),
        segment.owner_version(),
        segment.owner_snapshot_id(),
    );
    super::super::io::require_directory(&owner)?;
    super::super::io::require_directory(&owner.join("segments"))?;
    let path = layout::segment_path(
        root,
        snapshot.table_id(),
        segment.owner_version(),
        segment.owner_snapshot_id(),
        segment.segment_id(),
    );
    let metadata =
        fs::symlink_metadata(&path).map_err(|error| Error::io(Some(path.clone()), error))?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(Error::native_storage(
            &path,
            "native segment is not a regular file",
        ));
    }
    if metadata.len() != segment.bytes() {
        return Err(Error::native_storage(
            &path,
            "native segment byte size mismatch",
        ));
    }
    Ok((path, metadata))
}

fn predicate_sidecar_metadata(
    root: &Path,
    snapshot: &TableSnapshot,
    segment: &NativeSegment,
) -> Result<Option<(std::path::PathBuf, fs::Metadata)>> {
    let Some(descriptor) = segment.predicate_sidecar() else {
        return Ok(None);
    };
    let path = layout::predicate_sidecar_path(
        root,
        snapshot.table_id(),
        segment.owner_version(),
        segment.owner_snapshot_id(),
        segment.segment_id(),
    );
    let metadata =
        fs::symlink_metadata(&path).map_err(|error| Error::io(Some(path.clone()), error))?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(Error::native_storage(
            &path,
            "native predicate sidecar is not a regular file",
        ));
    }
    if metadata.len() != descriptor.bytes() {
        return Err(Error::native_storage(
            &path,
            "native predicate sidecar byte size mismatch",
        ));
    }
    Ok(Some((path, metadata)))
}

fn delete_vector_metadata(
    root: &Path,
    snapshot: &TableSnapshot,
    segment: &NativeSegment,
) -> Result<Option<(std::path::PathBuf, fs::Metadata)>> {
    let Some(descriptor) = segment.delete_vector() else {
        return Ok(None);
    };
    let path = layout::delete_vector_path(
        root,
        snapshot.table_id(),
        descriptor.owner_version(),
        descriptor.owner_snapshot_id(),
        segment.segment_id(),
    );
    let metadata =
        fs::symlink_metadata(&path).map_err(|error| Error::io(Some(path.clone()), error))?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(Error::native_storage(
            &path,
            "native delete vector is not a regular file",
        ));
    }
    if metadata.len() != descriptor.bytes() {
        return Err(Error::native_storage(
            &path,
            "native delete vector byte size mismatch",
        ));
    }
    Ok(Some((path, metadata)))
}

fn file_fingerprint(_path: &Path, metadata: &fs::Metadata) -> Option<String> {
    LocalFileIdentity::from_metadata(metadata).map(|identity| identity.to_string())
}

fn managed_file_fingerprint(
    segment_path: &Path,
    segment_metadata: &fs::Metadata,
    sidecar: Option<&(std::path::PathBuf, fs::Metadata)>,
    delete_vector: Option<&(std::path::PathBuf, fs::Metadata)>,
) -> Option<String> {
    let segment = file_fingerprint(segment_path, segment_metadata)?;
    let segment = match sidecar {
        Some((path, metadata)) => {
            let sidecar = file_fingerprint(path, metadata)?;
            Some(format!("{segment}|{sidecar}"))
        }
        None => Some(segment),
    }?;
    match delete_vector {
        Some((path, metadata)) => {
            let vector = file_fingerprint(path, metadata)?;
            Some(format!("{segment}|{vector}"))
        }
        None => Some(segment),
    }
}

fn segment_descriptor(
    snapshot: &TableSnapshot,
    segment: &NativeSegment,
    path: &Path,
) -> Result<()> {
    Uuid::parse_str(segment.segment_id())
        .map_err(|error| Error::native_storage(path, format!("invalid segment id: {error}")))?;
    Uuid::parse_str(segment.owner_snapshot_id()).map_err(|error| {
        Error::native_storage(path, format!("invalid segment owner snapshot id: {error}"))
    })?;
    if segment.owner_version() == 0
        || segment.owner_version() > snapshot.version()
        || segment.schema_fingerprint() != snapshot.schema_fingerprint()
        || segment.format_version() != super::super::segment::FORMAT_VERSION
        || !valid_sha256(segment.sha256())
    {
        return Err(Error::native_storage(
            path,
            "invalid native segment descriptor",
        ));
    }
    if let Some(sidecar) = segment.predicate_sidecar() {
        predicate_sidecar_descriptor(snapshot, segment, sidecar, path)?;
    }
    if let Some(vector) = segment.delete_vector() {
        delete_vector_descriptor(snapshot, segment, vector, path)?;
    }
    Ok(())
}

fn delete_vector_descriptor(
    snapshot: &TableSnapshot,
    segment: &NativeSegment,
    vector: &super::delete_vector::DeleteVectorDescriptor,
    path: &Path,
) -> Result<()> {
    let owner_id = Uuid::parse_str(vector.owner_snapshot_id());
    if owner_id.is_err()
        || vector.format_version() != super::delete_vector::FORMAT_VERSION
        || vector.owner_version() == 0
        || vector.owner_version() > snapshot.version()
        || vector.row_count() != segment.rows()
        || vector.deleted_rows() == 0
        || vector.deleted_rows() > vector.row_count()
        || vector.bytes() < 28
        || !valid_sha256(vector.sha256())
    {
        return Err(Error::native_storage(
            path,
            "invalid native delete vector descriptor",
        ));
    }
    Ok(())
}

fn predicate_sidecar_descriptor(
    snapshot: &TableSnapshot,
    segment: &NativeSegment,
    sidecar: &super::PredicateSidecarDescriptor,
    path: &Path,
) -> Result<()> {
    let column_count = u32::try_from(snapshot.schema().fields().len()).map_err(|_| {
        Error::native_storage(path, "native schema column count does not fit in u32")
    })?;
    let ordinals = sidecar.indexed_column_ordinals();
    let ordered_unique = ordinals.windows(2).all(|pair| pair[0] < pair[1]);
    if sidecar.format_version() != super::sidecar::FORMAT_VERSION
        || sidecar.bytes() == 0
        || !valid_sha256(sidecar.sha256())
        || sidecar.row_count() != segment.rows()
        || (sidecar.row_count() > 0 && sidecar.row_group_count() == 0)
        || ordinals.is_empty()
        || !ordered_unique
        || ordinals.iter().any(|ordinal| *ordinal >= column_count)
    {
        return Err(Error::native_storage(
            path,
            "invalid native predicate sidecar descriptor",
        ));
    }
    Ok(())
}

fn checked_sum(
    values_path: &Path,
    mut values: impl Iterator<Item = u64>,
    name: &str,
) -> Result<u64> {
    values.try_fold(0_u64, |total, value| {
        total
            .checked_add(value)
            .ok_or_else(|| Error::native_storage(values_path, format!("snapshot {name} overflow")))
    })
}

fn valid_sha256(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn sha256_file(path: &Path, check_cancelled: &mut dyn FnMut() -> Result<()>) -> Result<String> {
    #[cfg(test)]
    record_full_verification(path);
    let mut file = File::open(path).map_err(|error| Error::io(Some(path.to_path_buf()), error))?;
    let mut digest = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        check_cancelled()?;
        let read = file
            .read(&mut buffer)
            .map_err(|error| Error::io(Some(path.to_path_buf()), error))?;
        if read == 0 {
            break;
        }
        digest.update(&buffer[..read]);
    }
    Ok(format!("{:x}", digest.finalize()))
}

#[cfg(test)]
fn record_full_verification(path: &Path) {
    let counts = FULL_VERIFICATION_COUNTS.get_or_init(Default::default);
    *counts
        .lock()
        .unwrap()
        .entry(path.to_path_buf())
        .or_default() += 1;
}

#[cfg(test)]
pub(super) fn full_verification_count(path: &Path) -> usize {
    FULL_VERIFICATION_COUNTS
        .get_or_init(Default::default)
        .lock()
        .unwrap()
        .get(path)
        .copied()
        .unwrap_or(0)
}

#[cfg(test)]
static FULL_VERIFICATION_COUNTS: OnceLock<Mutex<HashMap<PathBuf, usize>>> = OnceLock::new();
