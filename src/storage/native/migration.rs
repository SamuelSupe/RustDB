use std::{
    fs,
    path::{Path, PathBuf},
};

use uuid::Uuid;

use crate::{Error, Result};

use super::{
    DATABASE_FORMAT_VERSION, LEGACY_DATABASE_FORMAT_VERSION, MARKER_FILE, NativeDatabase, io,
    marker,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct NativeMigration {
    pub(crate) from_version: u32,
    pub(crate) to_version: u32,
    pub(crate) backup_path: Option<PathBuf>,
}

pub(super) fn migrate(path: &Path) -> Result<NativeMigration> {
    let database = NativeDatabase::open(path)?;
    if database.format_version == DATABASE_FORMAT_VERSION {
        return Ok(NativeMigration {
            from_version: DATABASE_FORMAT_VERSION,
            to_version: DATABASE_FORMAT_VERSION,
            backup_path: None,
        });
    }
    if database.format_version != LEGACY_DATABASE_FORMAT_VERSION {
        return Err(Error::Unsupported(format!(
            "cannot migrate native database format version {}",
            database.format_version
        )));
    }

    let backup_path = default_backup_path(database.path())?;
    ensure_backup(&database, &backup_path)?;
    ensure_empty_wal(database.path())?;
    let transaction_id = Uuid::new_v4().to_string();
    marker::upgrade_legacy(&database.path().join(MARKER_FILE), &transaction_id)?;
    drop(database);

    let upgraded = NativeDatabase::open(path)?;
    if upgraded.format_version != DATABASE_FORMAT_VERSION || upgraded.wal.is_none() {
        return Err(Error::native_storage(
            upgraded.path(),
            "migration completed without a writable WAL-enabled database",
        ));
    }
    drop(upgraded);
    Ok(NativeMigration {
        from_version: LEGACY_DATABASE_FORMAT_VERSION,
        to_version: DATABASE_FORMAT_VERSION,
        backup_path: Some(backup_path),
    })
}

fn default_backup_path(root: &Path) -> Result<PathBuf> {
    let parent = root
        .parent()
        .ok_or_else(|| Error::InvalidArgument("database path has no parent".to_owned()))?;
    let mut name = root
        .file_name()
        .ok_or_else(|| Error::InvalidArgument("database path has no name".to_owned()))?
        .to_os_string();
    name.push(".v0.7-backup");
    Ok(parent.join(name))
}

fn ensure_backup(database: &NativeDatabase, backup_path: &Path) -> Result<()> {
    if !backup_path.exists() {
        return database.backup_to(backup_path);
    }
    let backup = NativeDatabase::open(backup_path)?;
    let source_catalog = database.state.lock().catalog.clone();
    let backup_catalog = backup.state.lock().catalog.clone();
    if backup.database_id() != database.database_id()
        || backup.format_version != LEGACY_DATABASE_FORMAT_VERSION
        || backup_catalog != source_catalog
    {
        return Err(Error::native_storage(
            backup_path,
            "existing migration backup is not the matching v0.7 catalog snapshot",
        ));
    }
    Ok(())
}

fn ensure_empty_wal(root: &Path) -> Result<()> {
    let path = root.join("wal");
    match fs::symlink_metadata(&path) {
        Ok(_) => {
            io::require_directory(&path)?;
            if fs::read_dir(&path)
                .map_err(|error| Error::io(Some(path.clone()), error))?
                .next()
                .is_some()
            {
                return Err(Error::native_storage(
                    path,
                    "legacy migration WAL directory is not empty",
                ));
            }
            Ok(())
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            io::create_private_dir_all(&path)
        }
        Err(error) => Err(Error::io(Some(path), error)),
    }
}

#[cfg(test)]
#[path = "migration/tests.rs"]
mod tests;
