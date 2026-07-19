use std::{
    fs::{self, File, OpenOptions},
    io::{Read, Seek, Write},
    path::Path,
};

use crate::{Error, Result};

use super::codec::{MAX_EVENT_BYTES, MAX_SNAPSHOT_BYTES};

const ROOT_MARKER_FILE: &str = "OWNER";
const ROOT_MARKER: &[u8] = b"rustdb-query-journal-v1\n";
const LOCK_FILE: &str = ".lock";
const LOCK_MARKER: &[u8] = b"rustdb-query-journal-lock-v1\n";
pub(super) const SNAPSHOT_FILE: &str = "snapshot.json";
pub(super) const JOURNAL_FILE: &str = "journal.jsonl";
const MAX_JOURNAL_BYTES: usize = 256 * 1024 * 1024;

pub(super) fn open_root(root: &Path) -> Result<File> {
    ensure_private_directory(root)?;
    establish_marker(root)?;
    let lock = acquire_lock(root)?;
    clean_temporary(root)?;
    let _ = private_file_exists(&root.join(SNAPSHOT_FILE))?;
    let _ = private_file_exists(&root.join(JOURNAL_FILE))?;
    Ok(lock)
}

pub(super) fn open_journal(root: &Path) -> Result<File> {
    let path = root.join(JOURNAL_FILE);
    let existed = private_file_exists(&path)?;
    let file = open_private_append(&path, true)?;
    require_private_file(&path)?;
    if !existed {
        sync_directory(root)?;
    }
    Ok(file)
}

pub(super) fn append_event(file: &mut File, bytes: &[u8]) -> Result<()> {
    if bytes.is_empty() || bytes.len() > MAX_EVENT_BYTES || bytes.last() != Some(&b'\n') {
        return Err(Error::InvalidArgument(
            "query journal event encoding is invalid".into(),
        ));
    }
    let original = file
        .metadata()
        .map_err(|error| Error::io(None, error))?
        .len();
    if let Err(error) = file.write_all(bytes).and_then(|_| file.sync_all()) {
        return match file.set_len(original).and_then(|_| file.sync_all()) {
            Ok(()) => Err(Error::io(None, error)),
            Err(rollback) => Err(Error::Execution(format!(
                "failed to append query journal event: {error}; rollback also failed: {rollback}"
            ))),
        };
    }
    Ok(())
}

pub(super) fn read_snapshot(root: &Path) -> Result<Option<Vec<u8>>> {
    let path = root.join(SNAPSHOT_FILE);
    if !path.exists() {
        return Ok(None);
    }
    require_private_file(&path)?;
    read_bounded(&path, MAX_SNAPSHOT_BYTES).map(Some)
}

pub(super) fn read_journal(root: &Path) -> Result<Vec<Vec<u8>>> {
    let path = root.join(JOURNAL_FILE);
    if !path.exists() {
        return Ok(Vec::new());
    }
    require_private_file(&path)?;
    let mut bytes = read_bounded(&path, MAX_JOURNAL_BYTES)?;
    if !bytes.is_empty() && bytes.last() != Some(&b'\n') {
        let valid = bytes
            .iter()
            .rposition(|byte| *byte == b'\n')
            .map_or(0, |index| index + 1);
        truncate_journal(&path, valid)?;
        bytes.truncate(valid);
    }
    let mut records = Vec::new();
    for line in bytes.split(|byte| *byte == b'\n') {
        if line.is_empty() {
            continue;
        }
        if line.len() >= MAX_EVENT_BYTES {
            return Err(Error::InvalidArgument(
                "query journal record exceeds its size limit".into(),
            ));
        }
        records.push(line.to_vec());
    }
    Ok(records)
}

pub(super) fn replace_snapshot(root: &Path, bytes: &[u8]) -> Result<()> {
    if bytes.is_empty() || bytes.len() > MAX_SNAPSHOT_BYTES {
        return Err(Error::InvalidArgument(
            "query journal snapshot encoding is invalid".into(),
        ));
    }
    atomic_replace(&root.join(SNAPSHOT_FILE), bytes)
}

pub(super) fn replace_journal(root: &Path) -> Result<(File, Option<Error>)> {
    let path = root.join(JOURNAL_FILE);
    let temporary = root.join(format!(".{JOURNAL_FILE}.tmp"));
    remove_temporary(&temporary)?;
    let file = open_private_append(&temporary, true)?;
    file.sync_all()
        .map_err(|error| Error::io(Some(temporary.clone()), error))?;
    fs::rename(&temporary, &path).map_err(|error| Error::io(Some(path.clone()), error))?;
    let sync_error = sync_directory(root).err();
    Ok((file, sync_error))
}

fn establish_marker(root: &Path) -> Result<()> {
    let path = root.join(ROOT_MARKER_FILE);
    if path.exists() {
        require_private_file(&path)?;
        let contents = read_bounded(&path, 128)?;
        if contents != ROOT_MARKER {
            return Err(Error::InvalidArgument(format!(
                "query journal root has an unknown format marker: {}",
                root.display()
            )));
        }
        return Ok(());
    }
    if fs::read_dir(root)
        .map_err(|error| Error::io(Some(root.to_owned()), error))?
        .next()
        .is_some()
    {
        return Err(Error::InvalidArgument(format!(
            "refusing to claim non-empty query journal directory {}",
            root.display()
        )));
    }
    write_new_private(&path, ROOT_MARKER)?;
    sync_directory(root)
}

fn acquire_lock(root: &Path) -> Result<File> {
    let path = root.join(LOCK_FILE);
    let existed = private_file_exists(&path)?;
    let mut file = open_private_append(&path, true)?;
    require_private_file(&path)?;
    file.try_lock().map_err(|error| {
        Error::InvalidArgument(format!(
            "query journal directory {} is already in use: {error}",
            root.display()
        ))
    })?;
    let mut marker = Vec::new();
    file.rewind()
        .and_then(|_| file.read_to_end(&mut marker))
        .map_err(|error| Error::io(Some(path.clone()), error))?;
    if marker.is_empty() {
        file.rewind()
            .and_then(|_| file.write_all(LOCK_MARKER))
            .and_then(|_| file.set_len(u64::try_from(LOCK_MARKER.len()).unwrap_or(u64::MAX)))
            .and_then(|_| file.sync_all())
            .map_err(|error| Error::io(Some(path.clone()), error))?;
    } else if marker != LOCK_MARKER {
        return Err(Error::InvalidArgument(format!(
            "query journal lock has an unknown marker: {}",
            path.display()
        )));
    }
    if !existed {
        sync_directory(root)?;
    }
    Ok(file)
}

fn atomic_replace(path: &Path, bytes: &[u8]) -> Result<()> {
    if let Ok(metadata) = path.symlink_metadata()
        && (!metadata.file_type().is_file() || metadata.file_type().is_symlink())
    {
        return Err(Error::InvalidArgument(format!(
            "query journal path is not a regular file: {}",
            path.display()
        )));
    }
    let parent = parent(path)?;
    let name = path
        .file_name()
        .and_then(|value| value.to_str())
        .ok_or_else(|| Error::InvalidArgument("query journal path has no file name".into()))?;
    let temporary = parent.join(format!(".{name}.tmp"));
    remove_temporary(&temporary)?;
    let outcome = write_new_private(&temporary, bytes).and_then(|_| {
        fs::rename(&temporary, path).map_err(|error| Error::io(Some(path.to_owned()), error))
    });
    if outcome.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    outcome.and_then(|_| sync_directory(parent))
}

fn truncate_journal(path: &Path, length: usize) -> Result<()> {
    let file = OpenOptions::new()
        .write(true)
        .open(path)
        .map_err(|error| Error::io(Some(path.to_owned()), error))?;
    file.set_len(u64::try_from(length).unwrap_or(u64::MAX))
        .and_then(|_| file.sync_all())
        .map_err(|error| Error::io(Some(path.to_owned()), error))
}

fn clean_temporary(root: &Path) -> Result<()> {
    let mut changed = false;
    for name in [
        format!(".{SNAPSHOT_FILE}.tmp"),
        format!(".{JOURNAL_FILE}.tmp"),
    ] {
        let path = root.join(name);
        match path.symlink_metadata() {
            Ok(_) => {
                remove_temporary(&path)?;
                changed = true;
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(Error::io(Some(path), error)),
        }
    }
    if changed {
        sync_directory(root)?;
    }
    Ok(())
}

fn remove_temporary(path: &Path) -> Result<()> {
    match path.symlink_metadata() {
        Ok(metadata) if metadata.file_type().is_file() && !metadata.file_type().is_symlink() => {
            fs::remove_file(path).map_err(|error| Error::io(Some(path.to_owned()), error))
        }
        Ok(_) => Err(Error::InvalidArgument(format!(
            "query journal temporary path is unsafe: {}",
            path.display()
        ))),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(Error::io(Some(path.to_owned()), error)),
    }
}

fn read_bounded(path: &Path, maximum: usize) -> Result<Vec<u8>> {
    let metadata = path
        .symlink_metadata()
        .map_err(|error| Error::io(Some(path.to_owned()), error))?;
    if metadata.len() > u64::try_from(maximum).unwrap_or(u64::MAX) {
        return Err(Error::InvalidArgument(format!(
            "query journal file exceeds its size limit: {}",
            path.display()
        )));
    }
    let mut bytes = Vec::with_capacity(usize::try_from(metadata.len()).unwrap_or(maximum));
    File::open(path)
        .and_then(|file| {
            file.take(u64::try_from(maximum).unwrap_or(u64::MAX) + 1)
                .read_to_end(&mut bytes)
        })
        .map_err(|error| Error::io(Some(path.to_owned()), error))?;
    if bytes.len() > maximum {
        return Err(Error::InvalidArgument(format!(
            "query journal file changed while being read: {}",
            path.display()
        )));
    }
    Ok(bytes)
}

fn ensure_private_directory(path: &Path) -> Result<()> {
    let created = match path.symlink_metadata() {
        Ok(_) => false,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            fs::create_dir_all(path).map_err(|error| Error::io(Some(path.to_owned()), error))?;
            true
        }
        Err(error) => return Err(Error::io(Some(path.to_owned()), error)),
    };
    let metadata = path
        .symlink_metadata()
        .map_err(|error| Error::io(Some(path.to_owned()), error))?;
    if !metadata.file_type().is_dir() || metadata.file_type().is_symlink() {
        return Err(Error::InvalidArgument(format!(
            "query journal root is not a real directory: {}",
            path.display()
        )));
    }
    set_private_directory_mode(path, created, &metadata)
}

fn require_private_file(path: &Path) -> Result<()> {
    let metadata = path
        .symlink_metadata()
        .map_err(|error| Error::io(Some(path.to_owned()), error))?;
    if !metadata.file_type().is_file() || metadata.file_type().is_symlink() {
        return Err(Error::InvalidArgument(format!(
            "query journal path is not a regular file: {}",
            path.display()
        )));
    }
    require_private_file_mode(path, &metadata)
}

fn open_private_append(path: &Path, create: bool) -> Result<File> {
    let _ = private_file_exists(path)?;
    let mut options = OpenOptions::new();
    options.read(true).append(true).create(create);
    set_new_file_mode(&mut options);
    options
        .open(path)
        .map_err(|error| Error::io(Some(path.to_owned()), error))
}

fn private_file_exists(path: &Path) -> Result<bool> {
    match path.symlink_metadata() {
        Ok(metadata) => {
            if !metadata.file_type().is_file() || metadata.file_type().is_symlink() {
                return Err(Error::InvalidArgument(format!(
                    "query journal path is not a regular file: {}",
                    path.display()
                )));
            }
            require_private_file_mode(path, &metadata)?;
            Ok(true)
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(Error::io(Some(path.to_owned()), error)),
    }
}

fn write_new_private(path: &Path, bytes: &[u8]) -> Result<()> {
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    set_new_file_mode(&mut options);
    let mut file = options
        .open(path)
        .map_err(|error| Error::io(Some(path.to_owned()), error))?;
    file.write_all(bytes)
        .and_then(|_| file.sync_all())
        .map_err(|error| Error::io(Some(path.to_owned()), error))
}

fn sync_directory(path: &Path) -> Result<()> {
    File::open(path)
        .and_then(|file| file.sync_all())
        .map_err(|error| Error::io(Some(path.to_owned()), error))
}

fn parent(path: &Path) -> Result<&Path> {
    path.parent()
        .ok_or_else(|| Error::InvalidArgument("query journal path has no parent".into()))
}

#[cfg(unix)]
fn set_new_file_mode(options: &mut OpenOptions) {
    use std::os::unix::fs::OpenOptionsExt;
    options.mode(0o600);
}

#[cfg(not(unix))]
fn set_new_file_mode(_options: &mut OpenOptions) {}

#[cfg(unix)]
fn set_private_directory_mode(path: &Path, created: bool, metadata: &fs::Metadata) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    if created {
        fs::set_permissions(path, fs::Permissions::from_mode(0o700))
            .map_err(|error| Error::io(Some(path.to_owned()), error))
    } else if metadata.permissions().mode() & 0o077 != 0 {
        Err(Error::InvalidArgument(format!(
            "query journal directory must be private: {}",
            path.display()
        )))
    } else {
        Ok(())
    }
}

#[cfg(not(unix))]
fn set_private_directory_mode(
    _path: &Path,
    _created: bool,
    _metadata: &fs::Metadata,
) -> Result<()> {
    Ok(())
}

#[cfg(unix)]
fn require_private_file_mode(path: &Path, metadata: &fs::Metadata) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    if metadata.permissions().mode() & 0o077 != 0 {
        Err(Error::InvalidArgument(format!(
            "query journal file must be private: {}",
            path.display()
        )))
    } else {
        Ok(())
    }
}

#[cfg(not(unix))]
fn require_private_file_mode(_path: &Path, _metadata: &fs::Metadata) -> Result<()> {
    Ok(())
}
