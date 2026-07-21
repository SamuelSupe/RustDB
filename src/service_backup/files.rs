use std::{
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    path::{Path, PathBuf},
};

use sha2::{Digest, Sha256};

use crate::{Error, Result};

use super::manifest::FileEntry;

pub(super) fn create_private_dir(path: &Path) -> Result<()> {
    let mut builder = fs::DirBuilder::new();
    builder.recursive(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        builder.mode(0o700);
    }
    builder
        .create(path)
        .map_err(|error| Error::io(Some(path.to_path_buf()), error))?;
    check_private(path, true)
}

pub(super) fn create_new_private_dir(path: &Path) -> Result<()> {
    let mut builder = fs::DirBuilder::new();
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        builder.mode(0o700);
    }
    builder
        .create(path)
        .map_err(|error| Error::io(Some(path.to_path_buf()), error))?;
    check_private(path, true)
}

pub(super) fn create_private_file(path: &Path, bytes: &[u8]) -> Result<()> {
    if let Some(parent) = path.parent() {
        create_private_dir(parent)?;
    }
    let mut options = OpenOptions::new();
    options.create_new(true).write(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options
        .open(path)
        .map_err(|error| Error::io(Some(path.to_path_buf()), error))?;
    file.write_all(bytes)
        .and_then(|_| file.sync_all())
        .map_err(|error| Error::io(Some(path.to_path_buf()), error))
}

pub(super) fn copy_private_file(source: &Path, destination: &Path) -> Result<()> {
    check_private(source, false)?;
    let mut input =
        File::open(source).map_err(|error| Error::io(Some(source.to_path_buf()), error))?;
    if let Some(parent) = destination.parent() {
        create_private_dir(parent)?;
    }
    let mut options = OpenOptions::new();
    options.create_new(true).write(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut output = options
        .open(destination)
        .map_err(|error| Error::io(Some(destination.to_path_buf()), error))?;
    std::io::copy(&mut input, &mut output)
        .and_then(|_| output.sync_all())
        .map_err(|error| Error::io(Some(destination.to_path_buf()), error))?;
    Ok(())
}

pub(super) fn walk(root: &Path, skip_manifest: bool) -> Result<(Vec<String>, Vec<FileEntry>)> {
    let mut directories = Vec::new();
    let mut files = Vec::new();
    walk_directory(root, root, skip_manifest, &mut directories, &mut files)?;
    directories.sort();
    files.sort_by(|left, right| left.path.cmp(&right.path));
    Ok((directories, files))
}

fn walk_directory(
    root: &Path,
    directory: &Path,
    skip_manifest: bool,
    directories: &mut Vec<String>,
    files: &mut Vec<FileEntry>,
) -> Result<()> {
    check_private(directory, true)?;
    let mut entries = fs::read_dir(directory)
        .map_err(|error| Error::io(Some(directory.to_path_buf()), error))?
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|error| Error::io(Some(directory.to_path_buf()), error))?;
    entries.sort_by_key(|entry| entry.file_name());
    for entry in entries {
        let path = entry.path();
        let relative = relative_text(root, &path)?;
        if skip_manifest && relative == super::manifest::MANIFEST_FILE {
            continue;
        }
        let metadata =
            fs::symlink_metadata(&path).map_err(|error| Error::io(Some(path.clone()), error))?;
        if metadata.file_type().is_symlink() {
            return Err(Error::Execution(format!(
                "service backup tree contains symlink {}",
                path.display()
            )));
        }
        if metadata.is_dir() {
            directories.push(relative);
            walk_directory(root, &path, skip_manifest, directories, files)?;
        } else if metadata.is_file() {
            check_private(&path, false)?;
            let (bytes, sha256) = hash_file(&path)?;
            files.push(FileEntry {
                path: relative,
                bytes,
                sha256,
            });
        } else {
            return Err(Error::Execution(format!(
                "service backup tree contains non-regular entry {}",
                path.display()
            )));
        }
    }
    Ok(())
}

pub(super) fn hash_file(path: &Path) -> Result<(u64, String)> {
    let mut file = File::open(path).map_err(|error| Error::io(Some(path.to_path_buf()), error))?;
    let mut hasher = Sha256::new();
    let mut bytes = 0_u64;
    let mut buffer = [0_u8; 128 * 1024];
    loop {
        let read = file
            .read(&mut buffer)
            .map_err(|error| Error::io(Some(path.to_path_buf()), error))?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
        bytes = bytes
            .checked_add(read as u64)
            .ok_or_else(|| Error::ResourceExhausted("backup byte count overflow".to_owned()))?;
    }
    Ok((bytes, format!("{:x}", hasher.finalize())))
}

pub(super) fn sync_dir(path: &Path) -> Result<()> {
    File::open(path)
        .and_then(|file| file.sync_all())
        .map_err(|error| Error::io(Some(path.to_path_buf()), error))
}

pub(super) fn sync_directories(root: &Path, directories: &[String]) -> Result<()> {
    for relative in directories.iter().rev() {
        sync_dir(&safe_join(root, relative)?)?;
    }
    sync_dir(root)
}

pub(super) fn cleanup_owned_directory<T>(path: &Path, error: Error) -> Result<T> {
    match fs::remove_dir_all(path) {
        Ok(()) => Err(error),
        Err(cleanup) if cleanup.kind() == std::io::ErrorKind::NotFound => Err(error),
        Err(cleanup) => Err(Error::Execution(format!(
            "{error}; owned temporary directory cleanup also failed for {}: {cleanup}",
            path.display()
        ))),
    }
}

fn relative_text(root: &Path, path: &Path) -> Result<String> {
    let relative = path
        .strip_prefix(root)
        .map_err(|_| Error::Execution(format!("backup entry escaped root: {}", path.display())))?;
    let text = relative.to_str().ok_or_else(|| {
        Error::Execution(format!(
            "backup entry path is not UTF-8: {}",
            path.display()
        ))
    })?;
    Ok(text.replace(std::path::MAIN_SEPARATOR, "/"))
}

pub(super) fn safe_join(root: &Path, relative: &str) -> Result<PathBuf> {
    if !super::manifest::is_safe_relative(relative) {
        return Err(Error::Execution(format!(
            "unsafe service backup path '{relative}'"
        )));
    }
    Ok(root.join(relative))
}

pub(super) fn check_private(path: &Path, directory: bool) -> Result<()> {
    let metadata =
        fs::symlink_metadata(path).map_err(|error| Error::io(Some(path.to_path_buf()), error))?;
    if metadata.file_type().is_symlink()
        || (directory && !metadata.is_dir())
        || (!directory && !metadata.is_file())
    {
        return Err(Error::Execution(format!(
            "backup entry has an unsafe type: {}",
            path.display()
        )));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if metadata.permissions().mode() & 0o077 != 0 {
            return Err(Error::Execution(format!(
                "backup entry is accessible by group or other users: {}",
                path.display()
            )));
        }
    }
    Ok(())
}
