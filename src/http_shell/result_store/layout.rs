use std::{
    fs::{self, File, OpenOptions},
    io::{Read, Seek, Write},
    path::{Path, PathBuf},
};

use crate::{Error, Result};

const ROOT_MARKER: &[u8] = b"rustdb-http-results-v1\n";
pub(super) const QUERY_MARKER: &[u8] = b"rustdb-http-query-result-v1\n";
const ROOT_LOCK_FILE: &str = ".server.lock";
const ROOT_LOCK_MARKER: &[u8] = b"rustdb-http-result-lock-v1\n";

pub(super) fn establish_root_marker(root: &Path) -> Result<()> {
    let marker = root.join("OWNER");
    match fs::symlink_metadata(&marker) {
        Ok(metadata) => {
            if metadata.file_type().is_symlink() || !metadata.is_file() {
                return Err(Error::InvalidArgument(format!(
                    "HTTP result directory owner marker is not a regular file: {}",
                    marker.display()
                )));
            }
            let contents =
                fs::read(&marker).map_err(|error| Error::io(Some(marker.clone()), error))?;
            if contents != ROOT_MARKER {
                return Err(Error::InvalidArgument(format!(
                    "HTTP result directory has an unknown owner marker: {}",
                    root.display()
                )));
            }
            return Ok(());
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(Error::io(Some(marker.clone()), error)),
    }
    if fs::read_dir(root)
        .map_err(|error| Error::io(Some(root.to_owned()), error))?
        .next()
        .is_some()
    {
        return Err(Error::InvalidArgument(format!(
            "refusing to claim non-empty HTTP result directory {}",
            root.display()
        )));
    }
    write_private(&marker, ROOT_MARKER)
}

pub(super) fn acquire_root_lock(root: &Path) -> Result<File> {
    let path = root.join(ROOT_LOCK_FILE);
    match fs::symlink_metadata(&path) {
        Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_file() => {
            return Err(Error::InvalidArgument(format!(
                "HTTP result lock is not a regular file: {}",
                path.display()
            )));
        }
        Ok(_) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(Error::io(Some(path.clone()), error)),
    }
    let mut options = OpenOptions::new();
    options.read(true).write(true).create(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options
        .open(&path)
        .map_err(|error| Error::io(Some(path.clone()), error))?;
    let metadata =
        fs::symlink_metadata(&path).map_err(|error| Error::io(Some(path.clone()), error))?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(Error::InvalidArgument(format!(
            "HTTP result lock is not a regular file: {}",
            path.display()
        )));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if metadata.permissions().mode() & 0o077 != 0 {
            return Err(Error::InvalidArgument(format!(
                "HTTP result lock {} must be private",
                path.display()
            )));
        }
    }
    file.try_lock().map_err(|error| {
        Error::InvalidArgument(format!(
            "HTTP result directory {} is already in use: {error}",
            root.display()
        ))
    })?;
    let mut marker = Vec::new();
    file.rewind()
        .and_then(|_| file.read_to_end(&mut marker))
        .map_err(|error| Error::io(Some(path.clone()), error))?;
    if marker.is_empty() {
        file.rewind()
            .and_then(|_| file.write_all(ROOT_LOCK_MARKER))
            .and_then(|_| file.set_len(ROOT_LOCK_MARKER.len() as u64))
            .and_then(|_| file.sync_all())
            .map_err(|error| Error::io(Some(path.clone()), error))?;
    } else if marker != ROOT_LOCK_MARKER {
        return Err(Error::InvalidArgument(format!(
            "invalid HTTP result lock marker at {}",
            path.display()
        )));
    }
    Ok(file)
}

pub(super) fn owned_query_directories(root: &Path) -> Result<Vec<PathBuf>> {
    let mut directories = Vec::new();
    for entry in fs::read_dir(root).map_err(|error| Error::io(Some(root.to_owned()), error))? {
        let entry = entry.map_err(|error| Error::io(Some(root.to_owned()), error))?;
        if entry.file_name() == "OWNER"
            || !entry
                .file_type()
                .map_err(|error| Error::io(None, error))?
                .is_dir()
        {
            continue;
        }
        let path = entry.path();
        let marker = path.join("OWNER");
        if fs::read(&marker).ok().as_deref() == Some(QUERY_MARKER) {
            directories.push(path);
        }
    }
    directories.sort();
    Ok(directories)
}

pub(super) fn remove_owned_query(path: &Path) -> Result<()> {
    let metadata = path
        .symlink_metadata()
        .map_err(|error| Error::io(Some(path.to_owned()), error))?;
    if !metadata.file_type().is_dir() || metadata.file_type().is_symlink() {
        return Err(Error::InvalidArgument(format!(
            "refusing to remove unsafe HTTP result path {}",
            path.display()
        )));
    }
    let marker = path.join("OWNER");
    if fs::read(&marker).ok().as_deref() != Some(QUERY_MARKER) {
        return Err(Error::InvalidArgument(format!(
            "refusing to remove unowned HTTP result directory {}",
            path.display()
        )));
    }
    fs::remove_dir_all(path).map_err(|error| Error::io(Some(path.to_owned()), error))?;
    let parent = path
        .parent()
        .ok_or_else(|| Error::InvalidArgument("HTTP result path has no parent".into()))?;
    sync_directory(parent)
}

pub(super) fn secure_directory(path: &Path) -> Result<()> {
    let created = match fs::symlink_metadata(path) {
        Ok(_) => false,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            fs::create_dir_all(path).map_err(|error| Error::io(Some(path.to_owned()), error))?;
            true
        }
        Err(error) => return Err(Error::io(Some(path.to_owned()), error)),
    };
    let metadata =
        fs::symlink_metadata(path).map_err(|error| Error::io(Some(path.to_owned()), error))?;
    if !metadata.file_type().is_dir() || metadata.file_type().is_symlink() {
        return Err(Error::InvalidArgument(format!(
            "HTTP state path is not a real directory: {}",
            path.display()
        )));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if created {
            fs::set_permissions(path, fs::Permissions::from_mode(0o700))
                .map_err(|error| Error::io(Some(path.to_owned()), error))?;
        } else if metadata.permissions().mode() & 0o077 != 0 {
            return Err(Error::InvalidArgument(format!(
                "HTTP state directory {} must be private",
                path.display()
            )));
        }
    }
    Ok(())
}

pub(super) fn private_file(path: &Path) -> Result<File> {
    let mut options = OpenOptions::new();
    options.create_new(true).write(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    options
        .open(path)
        .map_err(|error| Error::io(Some(path.to_owned()), error))
}

pub(super) fn write_private(path: &Path, bytes: &[u8]) -> Result<()> {
    let mut file = private_file(path)?;
    file.write_all(bytes)
        .and_then(|_| file.sync_all())
        .map_err(|error| Error::io(Some(path.to_owned()), error))
}

pub(super) fn atomic_write_private(path: &Path, bytes: &[u8]) -> Result<()> {
    let file_name = path
        .file_name()
        .and_then(|value| value.to_str())
        .ok_or_else(|| Error::InvalidArgument("HTTP result path has no file name".into()))?;
    let temporary = path.with_file_name(format!(".{file_name}.partial"));
    if temporary.exists() {
        fs::remove_file(&temporary).map_err(|error| Error::io(Some(temporary.clone()), error))?;
    }
    write_private(&temporary, bytes)?;
    fs::rename(&temporary, path).map_err(|error| Error::io(Some(path.to_owned()), error))?;
    sync_directory(
        path.parent().ok_or_else(|| {
            Error::InvalidArgument("HTTP result path has no parent directory".into())
        })?,
    )
}

pub(super) fn sync_directory(path: &Path) -> Result<()> {
    File::open(path)
        .and_then(|file| file.sync_all())
        .map_err(|error| Error::io(Some(path.to_owned()), error))
}
