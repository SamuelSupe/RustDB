use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::{Path, PathBuf},
    sync::Arc,
};

use uuid::Uuid;

use crate::{Error, Result};

use super::{SnapshotLocation, TableSnapshot, layout, persistence::read_marker};
use crate::storage::native::io;

pub(in crate::storage::native) enum SnapshotRemoval {
    Removed,
    RemovedButUnsynced(Error),
}

pub(in crate::storage::native) fn remove_snapshot(
    root: &Path,
    database_id: &str,
    table_id: &str,
    location: &SnapshotLocation,
) -> Result<SnapshotRemoval> {
    let directory =
        layout::snapshot_directory(root, table_id, location.version, &location.snapshot_id);
    io::require_directory(&directory)?;
    let marker = read_marker(&directory)?;
    if marker.database_id != database_id
        || marker.table_id != table_id
        || marker.version != location.version
        || marker.snapshot_id != location.snapshot_id
    {
        return Err(Error::native_storage(
            &directory,
            "snapshot marker identity mismatch during cleanup",
        ));
    }
    fs::remove_dir_all(&directory).map_err(|error| Error::io(Some(directory.clone()), error))?;
    let parent = directory
        .parent()
        .ok_or_else(|| Error::native_storage(&directory, "snapshot has no parent"))?;
    match io::sync_dir(parent) {
        Ok(()) => Ok(SnapshotRemoval::Removed),
        Err(error) => Ok(SnapshotRemoval::RemovedButUnsynced(error)),
    }
}

pub(in crate::storage::native) fn prune_inherited_manifests(
    root: &Path,
    snapshot: &TableSnapshot,
) -> Result<()> {
    let current = snapshot.final_directory(root);
    for directory in snapshot.reachable_directories(root) {
        if directory == current {
            continue;
        }
        let path = layout::manifest_path(&directory);
        match fs::symlink_metadata(&path) {
            Ok(metadata) => {
                if metadata.file_type().is_symlink() || !metadata.is_file() {
                    return Err(Error::native_storage(
                        &path,
                        "inherited snapshot manifest is not a regular file",
                    ));
                }
                io::remove_file(&path)?;
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(Error::io(Some(path), error)),
        }
    }
    Ok(())
}

pub(in crate::storage::native) fn recover_orphans(
    root: &Path,
    database_id: &str,
    tables: &BTreeMap<String, Arc<TableSnapshot>>,
) -> Result<()> {
    let reachable = tables
        .values()
        .flat_map(|snapshot| {
            snapshot.reachable_locations().into_iter().map(|location| {
                layout::snapshot_directory(
                    root,
                    snapshot.table_id(),
                    location.version,
                    &location.snapshot_id,
                )
            })
        })
        .collect::<BTreeSet<_>>();
    let tables_root = root.join("tables");
    for table_entry in
        fs::read_dir(&tables_root).map_err(|error| Error::io(Some(tables_root.clone()), error))?
    {
        let table_entry =
            table_entry.map_err(|error| Error::io(Some(tables_root.clone()), error))?;
        let table_path = table_entry.path();
        let metadata = fs::symlink_metadata(&table_path)
            .map_err(|error| Error::io(Some(table_path.clone()), error))?;
        if metadata.file_type().is_symlink() || !metadata.is_dir() {
            continue;
        }
        let Some(table_id) = table_path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        if Uuid::parse_str(table_id).is_err() {
            continue;
        }
        recover_table_orphans(database_id, table_id, &table_path, &reachable)?;
    }
    Ok(())
}

fn recover_table_orphans(
    database_id: &str,
    table_id: &str,
    table_path: &Path,
    reachable: &BTreeSet<PathBuf>,
) -> Result<()> {
    let snapshots = table_path.join("snapshots");
    let metadata = match fs::symlink_metadata(&snapshots) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(Error::io(Some(snapshots), error)),
    };
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Ok(());
    }
    let mut removed = false;
    for entry in
        fs::read_dir(&snapshots).map_err(|error| Error::io(Some(snapshots.clone()), error))?
    {
        let entry = entry.map_err(|error| Error::io(Some(snapshots.clone()), error))?;
        let path = entry.path();
        if reachable.contains(&path) {
            continue;
        }
        let metadata =
            fs::symlink_metadata(&path).map_err(|error| Error::io(Some(path.clone()), error))?;
        if metadata.file_type().is_symlink() || !metadata.is_dir() {
            continue;
        }
        let Some(location) = snapshot_location_from_name(&path) else {
            continue;
        };
        let marker = match read_marker(&path) {
            Ok(marker) => marker,
            Err(_) => continue,
        };
        if marker.database_id != database_id
            || marker.table_id != table_id
            || marker.version != location.version
            || marker.snapshot_id != location.snapshot_id
        {
            continue;
        }
        fs::remove_dir_all(&path).map_err(|error| Error::io(Some(path), error))?;
        removed = true;
    }
    if removed {
        io::sync_dir(&snapshots)?;
    }
    Ok(())
}

fn snapshot_location_from_name(path: &Path) -> Option<SnapshotLocation> {
    let name = path.file_name()?.to_str()?;
    let (version, snapshot_id) = name.split_once('-')?;
    if version.len() != 20 || !version.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    Uuid::parse_str(snapshot_id).ok()?;
    Some(SnapshotLocation {
        version: version.parse().ok()?,
        snapshot_id: snapshot_id.to_owned(),
    })
}
