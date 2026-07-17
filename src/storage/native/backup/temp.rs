use std::{
    fs::{self, DirBuilder, File, OpenOptions},
    os::unix::fs::{DirBuilderExt, PermissionsExt},
    path::{Path, PathBuf},
};

use uuid::Uuid;

use crate::{Error, Result};

use super::super::io as native_io;

const OWNER_CONTENT: &[u8] = b"rustdb-backup-owner-v1\n";

pub(super) struct TemporaryBackup {
    path: PathBuf,
    owner_path: PathBuf,
    _owner: File,
}

impl TemporaryBackup {
    pub(super) fn create(parent: &Path, id: Uuid) -> Result<Self> {
        let _parent_lock = lock_parent(parent)?;
        let path = temporary_path(parent, id);
        if fs::symlink_metadata(&path).is_ok() {
            return Err(Error::native_storage(
                &path,
                "backup temporary path already exists",
            ));
        }
        let owner_path = owner_path(parent, id);
        native_io::atomic_create(&owner_path, OWNER_CONTENT)?;
        let mut directory_created = false;
        let result = (|| {
            let owner = OpenOptions::new()
                .read(true)
                .write(true)
                .open(&owner_path)
                .map_err(|error| Error::io(Some(owner_path.clone()), error))?;
            owner.try_lock().map_err(|error| {
                Error::native_storage(&path, format!("could not lock backup temporary: {error}"))
            })?;
            let mut builder = DirBuilder::new();
            builder.mode(0o700).create(&path).map_err(|error| {
                if error.kind() == std::io::ErrorKind::AlreadyExists {
                    Error::native_storage(&path, "backup temporary path already exists")
                } else {
                    Error::io(Some(path.clone()), error)
                }
            })?;
            directory_created = true;
            native_io::sync_dir(&path)?;
            native_io::sync_dir(parent)?;
            Ok(Self {
                path: path.clone(),
                owner_path: owner_path.clone(),
                _owner: owner,
            })
        })();
        result
            .map_err(|error| cleanup_creation(error, &path, &owner_path, parent, directory_created))
    }

    pub(super) fn path(&self) -> &Path {
        &self.path
    }

    pub(super) fn remove_owner(&self) -> Result<()> {
        native_io::remove_file(&self.owner_path)
    }
}

pub(super) fn recover(parent: &Path) -> Result<()> {
    let _parent_lock = lock_parent(parent)?;
    for entry in
        fs::read_dir(parent).map_err(|error| Error::io(Some(parent.to_path_buf()), error))?
    {
        let entry = entry.map_err(|error| Error::io(Some(parent.to_path_buf()), error))?;
        let path = entry.path();
        let Some(id) = owner_id(&path) else {
            continue;
        };
        let Some(owner) = lock_recognized_owner(&path) else {
            continue;
        };
        let temporary = temporary_path(parent, id);
        match fs::symlink_metadata(&temporary) {
            Ok(metadata) if metadata.is_dir() && !metadata.file_type().is_symlink() => {
                remove_directory(&temporary, parent)?;
            }
            Ok(_) => continue,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(Error::io(Some(temporary), error)),
        }
        native_io::remove_file(&path)?;
        drop(owner);
    }
    Ok(())
}

pub(super) fn cleanup_failed(path: &Path, parent: &Path, error: Error) -> Error {
    let Some(id) = temporary_id(path) else {
        return Error::native_storage(
            path,
            format!(
                "{error}; backup temporary directory cleanup failed: path has an invalid owner id"
            ),
        );
    };
    cleanup_creation(error, path, &owner_path(parent, id), parent, true)
}

fn cleanup_creation(
    error: Error,
    path: &Path,
    owner_path: &Path,
    parent: &Path,
    directory_created: bool,
) -> Error {
    if directory_created && let Err(cleanup) = remove_directory(path, parent) {
        return Error::native_storage(
            path,
            format!("{error}; backup temporary directory cleanup failed: {cleanup}"),
        );
    }
    match native_io::remove_file(owner_path) {
        Ok(()) => error,
        Err(cleanup) => Error::native_storage(
            owner_path,
            format!("{error}; backup owner cleanup failed: {cleanup}"),
        ),
    }
}

fn remove_directory(path: &Path, parent: &Path) -> Result<()> {
    match fs::remove_dir_all(path) {
        Ok(()) => native_io::sync_dir(parent),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(Error::io(Some(path.to_path_buf()), error)),
    }
}

fn lock_recognized_owner(path: &Path) -> Option<File> {
    native_io::require_regular_file(path).ok()?;
    let mut owner = OpenOptions::new().read(true).write(true).open(path).ok()?;
    if owner.metadata().ok()?.permissions().mode() & 0o777 != 0o600 {
        return None;
    }
    owner.try_lock().ok()?;
    let contents =
        native_io::read_open_bounded(&mut owner, path, OWNER_CONTENT.len(), "backup owner marker")
            .ok()?;
    if contents != OWNER_CONTENT {
        return None;
    }
    Some(owner)
}

fn lock_parent(parent: &Path) -> Result<File> {
    let file = File::open(parent).map_err(|error| Error::io(Some(parent.to_path_buf()), error))?;
    file.lock().map_err(|error| {
        Error::native_storage(
            parent,
            format!("could not coordinate backup temporary recovery: {error}"),
        )
    })?;
    Ok(file)
}

fn temporary_path(parent: &Path, id: Uuid) -> PathBuf {
    parent.join(format!(".rustdb-backup-{id}.tmp"))
}

fn owner_path(parent: &Path, id: Uuid) -> PathBuf {
    parent.join(format!(".rustdb-backup-{id}.owner"))
}

fn temporary_id(path: &Path) -> Option<Uuid> {
    let name = path.file_name().and_then(|name| name.to_str())?;
    name.strip_prefix(".rustdb-backup-")
        .and_then(|name| name.strip_suffix(".tmp"))
        .and_then(|id| Uuid::parse_str(id).ok())
}

fn owner_id(path: &Path) -> Option<Uuid> {
    let name = path.file_name()?.to_str()?;
    name.strip_prefix(".rustdb-backup-")
        .and_then(|name| name.strip_suffix(".owner"))
        .and_then(|id| Uuid::parse_str(id).ok())
}
