use std::{
    fs::{self, DirBuilder, File, OpenOptions},
    io::{Read, Seek, Write},
    os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt},
    path::Path,
};

use uuid::Uuid;

use crate::{Error, Result};

mod json;

pub(super) use json::{encode_bounded as encode_json_bounded, sha256_bounded as json_sha256};

pub(super) fn read_bounded(path: &Path, max_bytes: usize, kind: &str) -> Result<Vec<u8>> {
    let path_metadata = bounded_regular_metadata(path, max_bytes, kind)?;
    let mut file = File::open(path).map_err(|error| Error::io(Some(path.to_path_buf()), error))?;
    read_open_from_metadata(&mut file, path, max_bytes, kind, &path_metadata)
}

pub(super) fn validate_size(path: &Path, len: usize, max_bytes: usize, kind: &str) -> Result<()> {
    if len > max_bytes {
        return Err(Error::native_storage(
            path,
            format!("{kind} exceeds the {max_bytes}-byte limit (found {len} bytes)"),
        ));
    }
    Ok(())
}

pub(super) fn read_open_bounded(
    file: &mut File,
    path: &Path,
    max_bytes: usize,
    kind: &str,
) -> Result<Vec<u8>> {
    let path_metadata = bounded_regular_metadata(path, max_bytes, kind)?;
    read_open_from_metadata(file, path, max_bytes, kind, &path_metadata)
}

fn read_open_from_metadata(
    file: &mut File,
    path: &Path,
    max_bytes: usize,
    kind: &str,
    path_metadata: &fs::Metadata,
) -> Result<Vec<u8>> {
    let metadata = file
        .metadata()
        .map_err(|error| Error::io(Some(path.to_path_buf()), error))?;
    if !metadata.is_file() {
        return Err(Error::native_storage(
            path,
            format!("{kind} is not a regular file"),
        ));
    }
    if metadata.dev() != path_metadata.dev() || metadata.ino() != path_metadata.ino() {
        return Err(Error::native_storage(
            path,
            format!("{kind} changed while it was being opened"),
        ));
    }
    require_within_limit(path, metadata.len(), max_bytes, kind)?;
    file.rewind()
        .map_err(|error| Error::io(Some(path.to_path_buf()), error))?;
    read_contents_bounded(file, path, max_bytes, kind, metadata.len())
}

fn bounded_regular_metadata(path: &Path, max_bytes: usize, kind: &str) -> Result<fs::Metadata> {
    let metadata =
        fs::symlink_metadata(path).map_err(|error| Error::io(Some(path.to_path_buf()), error))?;
    if metadata.file_type().is_symlink() {
        return Err(Error::native_storage(
            path,
            format!("{kind} must not be a symlink"),
        ));
    }
    if !metadata.is_file() {
        return Err(Error::native_storage(
            path,
            format!("{kind} is not a regular file"),
        ));
    }
    require_within_limit(path, metadata.len(), max_bytes, kind)?;
    Ok(metadata)
}

fn require_within_limit(path: &Path, len: u64, max_bytes: usize, kind: &str) -> Result<()> {
    let max_bytes_u64 = u64::try_from(max_bytes).unwrap_or(u64::MAX);
    if len > max_bytes_u64 {
        return Err(Error::native_storage(
            path,
            format!("{kind} exceeds the {max_bytes}-byte limit (found {len} bytes)"),
        ));
    }
    Ok(())
}

fn read_contents_bounded(
    file: &mut File,
    path: &Path,
    max_bytes: usize,
    kind: &str,
    initial_len: u64,
) -> Result<Vec<u8>> {
    const CHUNK_BYTES: usize = 8 * 1024;

    let initial_capacity = usize::try_from(initial_len)
        .unwrap_or(max_bytes)
        .min(max_bytes);
    let mut contents = Vec::with_capacity(initial_capacity);
    let mut buffer = [0_u8; CHUNK_BYTES];
    loop {
        if contents.len() == max_bytes {
            let mut extra = [0_u8; 1];
            match file.read(&mut extra) {
                Ok(0) => return Ok(contents),
                Ok(_) => {
                    return Err(Error::native_storage(
                        path,
                        format!("{kind} grew beyond the {max_bytes}-byte limit while reading"),
                    ));
                }
                Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
                Err(error) => return Err(Error::io(Some(path.to_path_buf()), error)),
            }
        }

        let remaining = max_bytes - contents.len();
        let read_len = remaining.min(buffer.len());
        match file.read(&mut buffer[..read_len]) {
            Ok(0) => return Ok(contents),
            Ok(read) => contents.extend_from_slice(&buffer[..read]),
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(error) => return Err(Error::io(Some(path.to_path_buf()), error)),
        }
    }
}

pub(super) fn atomic_create(path: &Path, bytes: &[u8]) -> Result<()> {
    let parent = path
        .parent()
        .ok_or_else(|| Error::native_storage(path, "atomic file has no parent directory"))?;
    if path.exists() {
        return Err(Error::native_storage(path, "file already exists"));
    }
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("file");
    let temporary = parent.join(format!(".{name}.{}.tmp", Uuid::new_v4()));

    let result = (|| {
        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .mode(0o600)
            .open(&temporary)
            .map_err(|error| Error::io(Some(temporary.clone()), error))?;
        file.write_all(bytes)
            .map_err(|error| Error::io(Some(temporary.clone()), error))?;
        file.sync_all()
            .map_err(|error| Error::io(Some(temporary.clone()), error))?;
        fs::hard_link(&temporary, path)
            .map_err(|error| Error::io(Some(path.to_path_buf()), error))?;
        fs::remove_file(&temporary).map_err(|error| Error::io(Some(temporary.clone()), error))?;
        sync_dir(parent)
    })();

    if let Err(error) = result {
        return match fs::remove_file(&temporary) {
            Ok(()) => Err(error),
            Err(cleanup) if cleanup.kind() == std::io::ErrorKind::NotFound => Err(error),
            Err(cleanup) => Err(Error::native_storage(
                &temporary,
                format!("{error}; temporary file cleanup failed: {cleanup}"),
            )),
        };
    }
    Ok(())
}

pub(super) fn is_atomic_create_temporary(path: &Path, target_name: &str) -> bool {
    let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
        return false;
    };
    let prefix = format!(".{target_name}.");
    let Some(uuid) = name
        .strip_prefix(&prefix)
        .and_then(|name| name.strip_suffix(".tmp"))
    else {
        return false;
    };
    Uuid::parse_str(uuid).is_ok()
}

pub(super) fn atomic_replace(path: &Path, bytes: &[u8], transaction_id: &str) -> Result<()> {
    require_regular_file(path)?;
    let parent = path
        .parent()
        .ok_or_else(|| Error::native_storage(path, "atomic file has no parent directory"))?;
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("file");
    let temporary = parent.join(format!(".{name}.{transaction_id}.tmp"));

    let before_commit = (|| {
        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .mode(0o600)
            .open(&temporary)
            .map_err(|error| Error::io(Some(temporary.clone()), error))?;
        file.write_all(bytes)
            .map_err(|error| Error::io(Some(temporary.clone()), error))?;
        file.sync_all()
            .map_err(|error| Error::io(Some(temporary.clone()), error))?;
        fs::rename(&temporary, path).map_err(|error| Error::io(Some(path.to_path_buf()), error))
    })();

    if let Err(error) = before_commit {
        return match fs::remove_file(&temporary) {
            Ok(()) => Err(error),
            Err(cleanup) if cleanup.kind() == std::io::ErrorKind::NotFound => Err(error),
            Err(cleanup) => Err(Error::native_storage(
                &temporary,
                format!("{error}; temporary file cleanup failed: {cleanup}"),
            )),
        };
    }

    sync_dir(parent).map_err(|error| {
        Error::commit_outcome_unknown(
            path,
            transaction_id,
            format!("CURRENT was replaced but its parent directory was not synced: {error}"),
        )
    })
}

pub(super) fn create_private_dir_all(path: &Path) -> Result<()> {
    let existed = path.exists();
    let mut builder = DirBuilder::new();
    builder.recursive(true).mode(0o700);
    builder
        .create(path)
        .map_err(|error| Error::io(Some(path.to_path_buf()), error))?;
    if !existed
        && let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
    {
        sync_dir(parent)?;
    }
    Ok(())
}

pub(super) fn remove_file(path: &Path) -> Result<()> {
    match fs::remove_file(path) {
        Ok(()) => {
            if let Some(parent) = path.parent() {
                sync_dir(parent)?;
            }
            Ok(())
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(Error::io(Some(path.to_path_buf()), error)),
    }
}

pub(super) fn sync_dir(path: &Path) -> Result<()> {
    File::open(path)
        .and_then(|directory| directory.sync_all())
        .map_err(|error| Error::io(Some(path.to_path_buf()), error))
}

pub(super) fn require_regular_file(path: &Path) -> Result<()> {
    let metadata =
        fs::symlink_metadata(path).map_err(|error| Error::io(Some(path.to_path_buf()), error))?;
    if metadata.file_type().is_symlink() {
        return Err(Error::native_storage(
            path,
            "managed file must not be a symlink",
        ));
    }
    if !metadata.is_file() {
        return Err(Error::native_storage(
            path,
            "managed path is not a regular file",
        ));
    }
    Ok(())
}

pub(super) fn require_directory(path: &Path) -> Result<()> {
    let metadata =
        fs::symlink_metadata(path).map_err(|error| Error::io(Some(path.to_path_buf()), error))?;
    if metadata.file_type().is_symlink() {
        return Err(Error::native_storage(
            path,
            "managed directory must not be a symlink",
        ));
    }
    if metadata.is_dir() {
        Ok(())
    } else {
        Err(Error::native_storage(
            path,
            "required database directory is missing",
        ))
    }
}

#[cfg(test)]
#[path = "io/tests.rs"]
mod tests;
