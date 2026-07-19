use std::{
    fs::{self, File, OpenOptions},
    io::{Seek, Write},
    os::unix::fs::OpenOptionsExt,
    path::{Path, PathBuf},
};

use crate::{Error, Result};

use super::io;

const LOCK_FILE: &str = ".lock";
const LOCK_CONTENT: &[u8] = b"rustdb-lock-v1\n";

#[derive(Debug)]
pub(super) struct DatabaseLock {
    _file: File,
    path: PathBuf,
}

impl DatabaseLock {
    pub(super) fn acquire_existing(root: &Path) -> Result<Self> {
        let path = root.join(LOCK_FILE);
        io::require_regular_file(&path)?;
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .map_err(|error| Error::io(Some(path.clone()), error))?;
        file.try_lock().map_err(|error| {
            Error::native_storage(
                &path,
                format!("database is already open or the lock is unavailable: {error}"),
            )
        })?;
        let contents =
            io::read_open_bounded(&mut file, &path, LOCK_CONTENT.len(), "database lock marker")?;
        if contents != LOCK_CONTENT {
            return Err(Error::native_storage(&path, "invalid database lock marker"));
        }
        Ok(Self { _file: file, path })
    }

    pub(super) fn acquire(root: &Path) -> Result<Self> {
        let path = root.join(LOCK_FILE);
        let (mut file, created) = match OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .mode(0o600)
            .open(&path)
        {
            Ok(file) => (file, true),
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                io::require_regular_file(&path)?;
                (
                    OpenOptions::new()
                        .read(true)
                        .write(true)
                        .open(&path)
                        .map_err(|error| Error::io(Some(path.clone()), error))?,
                    false,
                )
            }
            Err(error) => return Err(Error::io(Some(path), error)),
        };

        file.try_lock().map_err(|error| {
            Error::native_storage(
                &path,
                format!("database is already open or the lock is unavailable: {error}"),
            )
        })?;
        if created {
            write_marker(root, &path, &mut file)?;
        } else {
            let contents = io::read_open_bounded(
                &mut file,
                &path,
                LOCK_CONTENT.len(),
                "database lock marker",
            )?;
            if contents != LOCK_CONTENT {
                if !repairable_initialization(root, &path)? {
                    return Err(Error::native_storage(&path, "invalid database lock marker"));
                }
                write_marker(root, &path, &mut file)?;
            }
        }
        Ok(Self { _file: file, path })
    }

    pub(super) fn path(&self) -> &Path {
        &self.path
    }
}

fn write_marker(root: &Path, path: &Path, file: &mut File) -> Result<()> {
    io::validate_size(
        path,
        LOCK_CONTENT.len(),
        LOCK_CONTENT.len(),
        "database lock marker",
    )?;
    file.set_len(0)
        .and_then(|()| file.rewind())
        .and_then(|()| file.write_all(LOCK_CONTENT))
        .and_then(|()| file.sync_all())
        .map_err(|error| Error::io(Some(path.to_path_buf()), error))?;
    io::sync_dir(root)
}

fn repairable_initialization(root: &Path, lock_path: &Path) -> Result<bool> {
    if root.join(super::MARKER_FILE).exists() || root.join(super::INIT_FILE).exists() {
        return Ok(false);
    }
    for entry in fs::read_dir(root).map_err(|error| Error::io(Some(root.to_path_buf()), error))? {
        let entry = entry.map_err(|error| Error::io(Some(root.to_path_buf()), error))?;
        let path = entry.path();
        if path == lock_path
            || io::is_atomic_create_temporary(&path, super::MARKER_FILE)
            || io::is_atomic_create_temporary(&path, super::INIT_FILE)
        {
            continue;
        }
        return Ok(false);
    }
    Ok(true)
}
