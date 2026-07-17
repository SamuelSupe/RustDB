use std::{
    fs::{self, File, OpenOptions},
    io,
    os::unix::fs::OpenOptionsExt,
    path::{Component, Path, PathBuf},
};

use uuid::Uuid;

use crate::{Error, Result};

use super::{MARKER_FILE, NativeDatabase, io as native_io};

mod temp;
#[cfg(test)]
mod tests;

pub(super) fn create(database: &NativeDatabase, destination: &Path) -> Result<()> {
    let resolved = resolve_without_creating(destination)?;
    if resolved.starts_with(database.path()) {
        return Err(Error::InvalidArgument(
            "backup destination must be outside the source database".to_owned(),
        ));
    }
    let parent = destination
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    native_io::create_private_dir_all(parent)?;
    let parent =
        fs::canonicalize(parent).map_err(|error| Error::io(Some(parent.to_path_buf()), error))?;
    let destination = parent.join(
        destination
            .file_name()
            .ok_or_else(|| Error::InvalidArgument("backup destination has no name".to_owned()))?,
    );
    if destination.exists() {
        return Err(Error::native_storage(
            &destination,
            "backup destination already exists",
        ));
    }
    if destination.starts_with(database.path()) {
        return Err(Error::InvalidArgument(
            "backup destination must be outside the source database".to_owned(),
        ));
    }
    temp::recover(&parent)?;

    let backup_id = Uuid::new_v4();
    let temporary = temp::TemporaryBackup::create(&parent, backup_id)?;
    let result = copy_snapshot(database, temporary.path()).and_then(|()| {
        let validation = NativeDatabase::open(temporary.path())?;
        drop(validation);
        publish(
            &temporary,
            &destination,
            &parent,
            backup_id,
            native_io::sync_dir,
        )
    });
    match result {
        Ok(()) => Ok(()),
        Err(error @ Error::CommitOutcomeUnknown { .. }) => Err(error),
        Err(error) => Err(temp::cleanup_failed(temporary.path(), &parent, error)),
    }
}

fn publish(
    temporary: &temp::TemporaryBackup,
    destination: &Path,
    parent: &Path,
    backup_id: Uuid,
    sync_parent: impl FnOnce(&Path) -> Result<()>,
) -> Result<()> {
    rename_no_replace(temporary.path(), destination)?;
    let durable = temporary.remove_owner().and_then(|()| sync_parent(parent));
    durable.map_err(|error| {
        Error::commit_outcome_unknown(
            destination,
            format!("backup-{backup_id}"),
            format!(
                "backup was renamed into place but publication durability could not be confirmed: {error}"
            ),
        )
    })
}

fn rename_no_replace(source: &Path, destination: &Path) -> Result<()> {
    #[cfg(any(target_os = "linux", target_vendor = "apple"))]
    {
        rustix::fs::renameat_with(
            rustix::fs::CWD,
            source,
            rustix::fs::CWD,
            destination,
            rustix::fs::RenameFlags::NOREPLACE,
        )
        .map_err(|error| {
            let error = io::Error::from_raw_os_error(error.raw_os_error());
            if error.kind() == io::ErrorKind::AlreadyExists {
                Error::native_storage(destination, "backup destination already exists")
            } else {
                Error::io(Some(destination.to_path_buf()), error)
            }
        })
    }
    #[cfg(not(any(target_os = "linux", target_vendor = "apple")))]
    {
        let _ = source;
        Err(Error::Unsupported(format!(
            "atomic no-replace backup publication is unsupported on this platform: {}",
            destination.display()
        )))
    }
}

fn resolve_without_creating(path: &Path) -> Result<PathBuf> {
    if path.file_name().is_none() {
        return Err(Error::InvalidArgument(
            "backup destination has no name".to_owned(),
        ));
    }
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()
            .map_err(|error| Error::io(None, error))?
            .join(path)
    };
    let mut ancestor = absolute.as_path();
    let mut missing = Vec::new();
    loop {
        match fs::symlink_metadata(ancestor) {
            Ok(_) => break,
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                let name = ancestor.file_name().ok_or_else(|| {
                    Error::InvalidArgument("backup destination cannot be resolved".to_owned())
                })?;
                missing.push(name.to_os_string());
                ancestor = ancestor.parent().ok_or_else(|| {
                    Error::InvalidArgument("backup destination cannot be resolved".to_owned())
                })?;
            }
            Err(error) => return Err(Error::io(Some(ancestor.to_path_buf()), error)),
        }
    }
    let mut resolved = fs::canonicalize(ancestor)
        .map_err(|error| Error::io(Some(ancestor.to_path_buf()), error))?;
    for component in missing.into_iter().rev() {
        resolved.push(component);
    }
    Ok(normalize_absolute(&resolved))
}

fn normalize_absolute(path: &Path) -> PathBuf {
    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            Component::Prefix(prefix) => normalized.push(prefix.as_os_str()),
            Component::RootDir => normalized.push(component.as_os_str()),
            Component::CurDir => {}
            Component::ParentDir => {
                normalized.pop();
            }
            Component::Normal(part) => normalized.push(part),
        }
    }
    normalized
}

fn copy_snapshot(database: &NativeDatabase, destination: &Path) -> Result<()> {
    native_io::create_private_dir_all(destination)?;
    let state = database.state.lock();
    copy_file(
        &database.path().join(MARKER_FILE),
        &destination.join(MARKER_FILE),
    )?;
    let source_catalog = database.path().join("catalog");
    let target_catalog = destination.join("catalog");
    native_io::create_private_dir_all(&target_catalog.join("generations"))?;
    copy_file(
        &source_catalog.join("CURRENT"),
        &target_catalog.join("CURRENT"),
    )?;
    let generation = format!("{:020}.json", state.catalog.generation());
    copy_file(
        &source_catalog.join("generations").join(&generation),
        &target_catalog.join("generations").join(generation),
    )?;

    native_io::create_private_dir_all(&destination.join("tables"))?;
    native_io::create_private_dir_all(&destination.join("staging"))?;
    for snapshot in state.tables.values() {
        copy_table_snapshot(database.path(), destination, snapshot)?;
    }
    sync_tree(destination)
}

fn copy_table_snapshot(
    source_root: &Path,
    target_root: &Path,
    snapshot: &super::table::TableSnapshot,
) -> Result<()> {
    for source in snapshot.reachable_directories(source_root) {
        let target = translated_path(source_root, target_root, &source)?;
        native_io::create_private_dir_all(&target.join("segments"))?;
        copy_file(
            &source.join(".rustdb-snapshot"),
            &target.join(".rustdb-snapshot"),
        )?;
    }
    let current = snapshot.final_directory(source_root);
    copy_file(
        &current.join("manifest.json"),
        &translated_path(source_root, target_root, &current)?.join("manifest.json"),
    )?;
    for source in snapshot.segment_paths(source_root) {
        let target = translated_path(source_root, target_root, &source)?;
        copy_file(&source, &target)?;
    }
    for source in snapshot.predicate_sidecar_paths(source_root) {
        let target = translated_path(source_root, target_root, &source)?;
        copy_file(&source, &target)?;
    }
    Ok(())
}

fn translated_path(source_root: &Path, target_root: &Path, source: &Path) -> Result<PathBuf> {
    let relative = source.strip_prefix(source_root).map_err(|_| {
        Error::native_storage(source, "backup source escaped the native database root")
    })?;
    Ok(target_root.join(relative))
}

fn copy_file(source: &Path, target: &Path) -> Result<()> {
    native_io::require_regular_file(source)?;
    let mut input =
        File::open(source).map_err(|error| Error::io(Some(source.to_path_buf()), error))?;
    let mut output = OpenOptions::new()
        .create_new(true)
        .write(true)
        .mode(0o600)
        .open(target)
        .map_err(|error| Error::io(Some(target.to_path_buf()), error))?;
    io::copy(&mut input, &mut output)
        .map_err(|error| Error::io(Some(target.to_path_buf()), error))?;
    output
        .sync_all()
        .map_err(|error| Error::io(Some(target.to_path_buf()), error))
}

fn sync_tree(directory: &Path) -> Result<()> {
    for entry in
        fs::read_dir(directory).map_err(|error| Error::io(Some(directory.to_path_buf()), error))?
    {
        let entry = entry.map_err(|error| Error::io(Some(directory.to_path_buf()), error))?;
        let path = entry.path();
        let metadata =
            fs::symlink_metadata(&path).map_err(|error| Error::io(Some(path.clone()), error))?;
        if metadata.file_type().is_symlink() {
            return Err(Error::native_storage(
                path,
                "backup tree must not contain symlinks",
            ));
        }
        if metadata.is_dir() {
            sync_tree(&path)?;
        }
    }
    native_io::sync_dir(directory)
}
