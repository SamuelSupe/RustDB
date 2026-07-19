use std::{
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    path::Path,
};

use uuid::Uuid;

use crate::{Error, Result};

pub(super) fn ensure_secure_directory(path: &Path) -> Result<()> {
    let created = match fs::symlink_metadata(path) {
        Ok(metadata) => {
            if metadata.file_type().is_symlink() || !metadata.is_dir() {
                return Err(Error::InvalidArgument(format!(
                    "refusing insecure state directory {}",
                    path.display()
                )));
            }
            false
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            fs::create_dir_all(path).map_err(|error| Error::io(path.to_path_buf(), error))?;
            let metadata =
                fs::symlink_metadata(path).map_err(|error| Error::io(path.to_path_buf(), error))?;
            if metadata.file_type().is_symlink() || !metadata.is_dir() {
                return Err(Error::InvalidArgument(format!(
                    "refusing insecure state directory {}",
                    path.display()
                )));
            }
            true
        }
        Err(error) => return Err(Error::io(path.to_path_buf(), error)),
    };
    if created {
        set_directory_permissions(path)?;
    }
    check_secure_directory(path)
}

pub(super) fn check_secure_directory(path: &Path) -> Result<()> {
    let metadata =
        fs::symlink_metadata(path).map_err(|error| Error::io(path.to_path_buf(), error))?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(Error::InvalidArgument(format!(
            "refusing insecure directory {}",
            path.display()
        )));
    }
    check_private_directory_mode(path, &metadata)
}

pub(super) fn ensure_new_secure_directory(path: &Path) -> Result<()> {
    if fs::symlink_metadata(path).is_ok() {
        return Err(Error::InvalidArgument(format!(
            "destination already exists: {}",
            path.display()
        )));
    }
    let parent = path
        .parent()
        .ok_or_else(|| Error::InvalidArgument(format!("path has no parent: {}", path.display())))?;
    let parent_metadata =
        fs::symlink_metadata(parent).map_err(|error| Error::io(parent.to_path_buf(), error))?;
    if parent_metadata.file_type().is_symlink() || !parent_metadata.is_dir() {
        return Err(Error::InvalidArgument(format!(
            "refusing insecure parent directory {}",
            parent.display()
        )));
    }
    fs::create_dir(path).map_err(|error| Error::io(path.to_path_buf(), error))?;
    set_directory_permissions(path)
}

pub(super) fn read_secure_file(path: &Path, max_bytes: u64) -> Result<Vec<u8>> {
    read_regular_file(path, max_bytes, true)
}

pub(super) fn read_regular_file(
    path: &Path,
    max_bytes: u64,
    require_private_mode: bool,
) -> Result<Vec<u8>> {
    let metadata =
        fs::symlink_metadata(path).map_err(|error| Error::io(path.to_path_buf(), error))?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(Error::InvalidArgument(format!(
            "refusing insecure credential file {}",
            path.display()
        )));
    }
    if metadata.len() > max_bytes {
        return Err(Error::InvalidArgument(format!(
            "credential file {} exceeds {max_bytes} bytes",
            path.display()
        )));
    }
    check_private_file_mode(path, &metadata, require_private_mode)?;
    let file = File::open(path).map_err(|error| Error::io(path.to_path_buf(), error))?;
    let mut bytes = Vec::with_capacity(metadata.len() as usize);
    file.take(max_bytes.saturating_add(1))
        .read_to_end(&mut bytes)
        .map_err(|error| Error::io(path.to_path_buf(), error))?;
    if bytes.len() as u64 > max_bytes {
        return Err(Error::InvalidArgument(format!(
            "credential file {} changed while being read",
            path.display()
        )));
    }
    Ok(bytes)
}

pub(super) fn atomic_write_secure(path: &Path, bytes: &[u8]) -> Result<()> {
    let parent = path
        .parent()
        .ok_or_else(|| Error::InvalidArgument(format!("path has no parent: {}", path.display())))?;
    ensure_secure_directory(parent)?;
    if let Ok(metadata) = fs::symlink_metadata(path)
        && (metadata.file_type().is_symlink() || !metadata.is_file())
    {
        return Err(Error::InvalidArgument(format!(
            "refusing to replace insecure credential file {}",
            path.display()
        )));
    }
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| {
            Error::InvalidArgument(format!("invalid credential path {}", path.display()))
        })?;
    let temporary = parent.join(format!(".{name}.{}.tmp", Uuid::new_v4()));
    let result = (|| {
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        set_new_file_mode(&mut options);
        let mut file = options
            .open(&temporary)
            .map_err(|error| Error::io(temporary.clone(), error))?;
        file.write_all(bytes)
            .map_err(|error| Error::io(temporary.clone(), error))?;
        file.sync_all()
            .map_err(|error| Error::io(temporary.clone(), error))?;
        fs::rename(&temporary, path).map_err(|error| Error::io(path.to_path_buf(), error))?;
        sync_directory(parent)
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    result
}

pub(super) fn secure_rename_directory(source: &Path, destination: &Path) -> Result<()> {
    if fs::symlink_metadata(destination).is_ok() {
        return Err(Error::InvalidArgument(format!(
            "destination already exists: {}",
            destination.display()
        )));
    }
    fs::rename(source, destination).map_err(|error| Error::io(destination.to_path_buf(), error))?;
    let parent = destination.parent().ok_or_else(|| {
        Error::InvalidArgument(format!("path has no parent: {}", destination.display()))
    })?;
    sync_directory(parent)
}

fn sync_directory(path: &Path) -> Result<()> {
    File::open(path)
        .and_then(|file| file.sync_all())
        .map_err(|error| Error::io(path.to_path_buf(), error))
}

#[cfg(unix)]
fn set_directory_permissions(path: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(path, fs::Permissions::from_mode(0o700))
        .map_err(|error| Error::io(path.to_path_buf(), error))
}

#[cfg(not(unix))]
fn set_directory_permissions(_path: &Path) -> Result<()> {
    Ok(())
}

#[cfg(unix)]
fn set_new_file_mode(options: &mut OpenOptions) {
    use std::os::unix::fs::OpenOptionsExt;
    options.mode(0o600);
}

#[cfg(not(unix))]
fn set_new_file_mode(_options: &mut OpenOptions) {}

#[cfg(unix)]
fn check_private_directory_mode(path: &Path, metadata: &fs::Metadata) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    if metadata.permissions().mode() & 0o077 != 0 {
        return Err(Error::InvalidArgument(format!(
            "directory {} must not be accessible by group or other users",
            path.display()
        )));
    }
    Ok(())
}

#[cfg(not(unix))]
fn check_private_directory_mode(_path: &Path, _metadata: &fs::Metadata) -> Result<()> {
    Ok(())
}

#[cfg(unix)]
fn check_private_file_mode(path: &Path, metadata: &fs::Metadata, required: bool) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    if required && metadata.permissions().mode() & 0o077 != 0 {
        return Err(Error::InvalidArgument(format!(
            "credential file {} must not be accessible by group or other users",
            path.display()
        )));
    }
    Ok(())
}

#[cfg(not(unix))]
fn check_private_file_mode(_path: &Path, _metadata: &fs::Metadata, _required: bool) -> Result<()> {
    Ok(())
}
