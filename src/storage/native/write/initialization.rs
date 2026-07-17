use std::{
    fs::{self, DirBuilder},
    os::unix::fs::DirBuilderExt,
    path::{Path, PathBuf},
};

use crate::{Error, Result};

use super::super::io;
use super::{INITIALIZING_PREFIX, MAX_TRANSACTION_MARKER_BYTES, Marker, TRANSACTION_MARKER};

pub(super) fn create(database_root: &Path, marker: &Marker) -> Result<(PathBuf, PathBuf)> {
    let staging = database_root.join("staging");
    io::require_directory(&staging)?;
    let initializing = staging.join(format!("{INITIALIZING_PREFIX}{}", marker.transaction_id));
    let root = staging.join(&marker.transaction_id);

    create_directory(&initializing)?;
    if let Err(error) = initialize_and_publish(&staging, &initializing, &root, marker) {
        return match remove_owned_directory(&staging, &initializing) {
            Ok(()) => Err(error),
            Err(cleanup) => Err(Error::native_storage(
                &initializing,
                format!("{error}; staging initialization cleanup failed: {cleanup}"),
            )),
        };
    }
    let snapshot = root.join("snapshot");
    Ok((root, snapshot))
}

fn create_directory(path: &Path) -> Result<()> {
    let parent = path
        .parent()
        .ok_or_else(|| Error::native_storage(path, "staging initialization has no parent"))?;
    let mut builder = DirBuilder::new();
    builder.mode(0o700);
    builder
        .create(path)
        .map_err(|error| Error::io(Some(path.to_path_buf()), error))?;
    if let Err(error) = io::sync_dir(parent) {
        return match remove_owned_directory(parent, path) {
            Ok(()) => Err(error),
            Err(cleanup) => Err(Error::native_storage(
                path,
                format!("{error}; staging initialization cleanup failed: {cleanup}"),
            )),
        };
    }
    Ok(())
}

fn initialize_and_publish(
    staging: &Path,
    initializing: &Path,
    root: &Path,
    marker: &Marker,
) -> Result<()> {
    io::create_private_dir_all(&initializing.join("snapshot").join("segments"))?;
    let marker_path = initializing.join(TRANSACTION_MARKER);
    let bytes = io::encode_json_bounded(
        &marker_path,
        marker,
        MAX_TRANSACTION_MARKER_BYTES,
        "transaction marker",
        true,
        true,
    )?;
    io::atomic_create(&marker_path, &bytes)?;
    io::sync_dir(initializing)?;
    require_missing(root)?;
    fs::rename(initializing, root).map_err(|error| Error::io(Some(root.to_path_buf()), error))?;
    if let Err(error) = io::sync_dir(staging) {
        return match remove_owned_directory(staging, root) {
            Ok(()) => Err(error),
            Err(cleanup) => Err(Error::native_storage(
                root,
                format!("{error}; published staging cleanup failed: {cleanup}"),
            )),
        };
    }
    Ok(())
}

fn require_missing(path: &Path) -> Result<()> {
    match fs::symlink_metadata(path) {
        Ok(_) => Err(Error::native_storage(
            path,
            "staging transaction destination already exists",
        )),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(Error::io(Some(path.to_path_buf()), error)),
    }
}

fn remove_owned_directory(parent: &Path, path: &Path) -> Result<()> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_dir() => Err(
            Error::native_storage(path, "staging initialization is not a directory"),
        ),
        Ok(_) => {
            fs::remove_dir_all(path).map_err(|error| Error::io(Some(path.to_path_buf()), error))?;
            io::sync_dir(parent)
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(Error::io(Some(path.to_path_buf()), error)),
    }
}
