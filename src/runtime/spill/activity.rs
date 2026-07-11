use std::{
    fs::{File, OpenOptions},
    path::Path,
};

use crate::{Error, Result};

pub(super) const ACTIVITY_FILE_NAME: &str = ".rustdb-active";

/// Kernel-backed proof that a query still owns its spill directory.
///
/// The exclusive advisory lock is released automatically when the file is
/// dropped, including when the process exits unexpectedly.
#[derive(Debug)]
pub(super) struct QueryActivityLock {
    _file: File,
}

/// Holds the cleanup-side lock until the candidate directory is removed.
#[derive(Debug)]
pub(super) struct OrphanCleanupLock {
    _file: Option<File>,
}

impl QueryActivityLock {
    pub(super) fn create(directory: &Path) -> Result<Self> {
        let path = directory.join(ACTIVITY_FILE_NAME);
        let mut options = OpenOptions::new();
        options.read(true).write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let file = options
            .open(&path)
            .map_err(|error| Error::io(Some(path.clone()), error))?;
        if let Err(error) = file.lock().and_then(|()| file.sync_all()) {
            drop(file);
            return Err(remove_created_file(
                &path,
                Error::io(Some(path.clone()), error),
            ));
        }
        Ok(Self { _file: file })
    }
}

fn remove_created_file(path: &Path, primary: Error) -> Error {
    match std::fs::remove_file(path) {
        Ok(()) => primary,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => primary,
        Err(error) => Error::Execution(format!(
            "{primary}; additionally failed to remove spill activity file '{}': {error}",
            path.display()
        )),
    }
}

/// Attempts to prove that a marked query directory is not active.
///
/// `Ok(None)` means another process or Engine still holds the query lock and
/// the directory must be preserved. A missing lock file is accepted for spill
/// directories written by older RustDB versions.
pub(super) fn try_lock_for_cleanup(directory: &Path) -> Result<Option<OrphanCleanupLock>> {
    let path = directory.join(ACTIVITY_FILE_NAME);
    let metadata = match std::fs::symlink_metadata(&path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(Some(OrphanCleanupLock { _file: None }));
        }
        Err(error) => return Err(Error::io(Some(path), error)),
    };
    if !metadata.file_type().is_file() {
        return Err(Error::Execution(format!(
            "spill activity lock '{}' is not a regular file",
            path.display()
        )));
    }

    let file = match OpenOptions::new().read(true).write(true).open(&path) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(Some(OrphanCleanupLock { _file: None }));
        }
        Err(error) => return Err(Error::io(Some(path), error)),
    };
    match file.try_lock() {
        Ok(()) => Ok(Some(OrphanCleanupLock { _file: Some(file) })),
        Err(error) => {
            let error: std::io::Error = error.into();
            if error.kind() == std::io::ErrorKind::WouldBlock {
                Ok(None)
            } else {
                Err(Error::io(Some(path), error))
            }
        }
    }
}
