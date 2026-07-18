use std::{
    fs::{File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    time::{Duration, SystemTime},
};

use uuid::Uuid;

use crate::{Error, Result};

const OWNER_SUFFIX: &str = ".owner";
const FORMAT_VERSION: u32 = 1;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RemoteTempKind {
    Backup,
    Restore,
}

impl RemoteTempKind {
    fn name(self) -> &'static str {
        match self {
            Self::Backup => "backup",
            Self::Restore => "restore",
        }
    }
}

/// A private temporary directory whose sibling owner marker is also its
/// process-liveness lock. Dropping the guard deliberately leaves both paths
/// recoverable; normal callers must use `finish` or `cleanup`.
pub(crate) struct RemoteTempDir {
    path: PathBuf,
    owner_path: PathBuf,
    _owner: File,
    cleaned: bool,
}

impl RemoteTempDir {
    pub(crate) fn create(root: &Path, kind: RemoteTempKind) -> Result<Self> {
        let id = Uuid::new_v4();
        let path = candidate_path(root, kind, id);
        let owner_path = owner_path(&path);
        let content = owner_content(kind, id);
        let mut owner = create_private_file(&owner_path)?;
        let mut directory_created = false;
        let result = (|| {
            owner
                .write_all(content.as_bytes())
                .and_then(|()| owner.sync_all())
                .map_err(|error| Error::io(Some(owner_path.clone()), error))?;
            owner.lock().map_err(|error| {
                Error::Execution(format!(
                    "could not lock remote {} temporary owner '{}': {error}",
                    kind.name(),
                    owner_path.display()
                ))
            })?;
            create_private_directory(&path)?;
            directory_created = true;
            sync_directory(root)?;
            Ok(Self {
                path: path.clone(),
                owner_path: owner_path.clone(),
                _owner: owner,
                cleaned: false,
            })
        })();
        result.map_err(|error| {
            cleanup_failed_creation(root, &path, &owner_path, directory_created, error)
        })
    }

    pub(crate) fn path(&self) -> &Path {
        &self.path
    }

    pub(crate) fn cleanup(mut self) -> Result<()> {
        remove_directory(&self.path)?;
        sync_directory(parent(&self.path)?)?;
        remove_file(&self.owner_path)?;
        sync_directory(parent(&self.owner_path)?)?;
        self.cleaned = true;
        Ok(())
    }

    pub(crate) fn finish<T>(self, result: Result<T>) -> Result<T> {
        match (result, self.cleanup()) {
            (Ok(value), Ok(())) => Ok(value),
            (Err(error), Ok(())) => Err(error),
            (Ok(_), Err(cleanup)) => Err(Error::Execution(format!(
                "remote operation completed, but temporary directory cleanup failed: {cleanup}"
            ))),
            (Err(error), Err(cleanup)) => Err(append_cleanup_error(error, cleanup)),
        }
    }
}

impl Drop for RemoteTempDir {
    fn drop(&mut self) {
        if !self.cleaned {
            tracing::warn!(
                path = %self.path.display(),
                "remote temporary directory was retained for orphan recovery"
            );
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct RemoteTempScavengeReport {
    pub(crate) removed: u64,
    pub(crate) preserved: u64,
}

pub(crate) fn scavenge(root: &Path, orphan_ttl: Duration) -> Result<RemoteTempScavengeReport> {
    scavenge_at(root, orphan_ttl, SystemTime::now())
}

fn scavenge_at(
    root: &Path,
    orphan_ttl: Duration,
    now: SystemTime,
) -> Result<RemoteTempScavengeReport> {
    let entries = match std::fs::read_dir(root) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(RemoteTempScavengeReport::default());
        }
        Err(error) => return Err(Error::io(Some(root.to_path_buf()), error)),
    };
    let mut report = RemoteTempScavengeReport::default();
    for entry in entries {
        let entry = entry.map_err(|error| Error::io(Some(root.to_path_buf()), error))?;
        let path = entry.path();
        if let Some((kind, id, identity)) = candidate(&entry, &path)? {
            let Some(owner) = lock_old_valid_owner(&path, kind, id, orphan_ttl, now)? else {
                report.preserved += 1;
                continue;
            };
            if !same_directory(&path, identity)? {
                report.preserved += 1;
                continue;
            }
            remove_directory(&path)?;
            remove_file(&owner_path(&path))?;
            drop(owner);
            sync_directory(root)?;
            report.removed += 1;
        } else if let Some((directory, kind, id)) = orphan_owner(&entry, &path)? {
            let missing = match std::fs::symlink_metadata(&directory) {
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => true,
                Ok(_) => false,
                Err(error) => return Err(Error::io(Some(directory), error)),
            };
            let owner = if missing {
                lock_old_valid_owner(&directory, kind, id, orphan_ttl, now)?
            } else {
                None
            };
            let Some(owner) = owner else {
                report.preserved += 1;
                continue;
            };
            remove_file(&path)?;
            drop(owner);
            sync_directory(root)?;
            report.removed += 1;
        } else {
            report.preserved += 1;
        }
    }
    Ok(report)
}

fn candidate(
    entry: &std::fs::DirEntry,
    path: &Path,
) -> Result<Option<(RemoteTempKind, Uuid, FileIdentity)>> {
    let file_type = match entry.file_type() {
        Ok(file_type) => file_type,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(Error::io(Some(path.to_path_buf()), error)),
    };
    if !file_type.is_dir() || file_type.is_symlink() {
        return Ok(None);
    }
    let metadata = match std::fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(Error::io(Some(path.to_path_buf()), error)),
    };
    if !metadata.file_type().is_dir()
        || metadata.file_type().is_symlink()
        || !has_mode_from_metadata(&metadata, 0o700)
    {
        return Ok(None);
    }
    let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
        return Ok(None);
    };
    Ok(parse_candidate_name(name).map(|(kind, id)| (kind, id, identity(&metadata))))
}

fn orphan_owner(
    entry: &std::fs::DirEntry,
    path: &Path,
) -> Result<Option<(PathBuf, RemoteTempKind, Uuid)>> {
    let file_type = match entry.file_type() {
        Ok(file_type) => file_type,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(Error::io(Some(path.to_path_buf()), error)),
    };
    if !file_type.is_file() || file_type.is_symlink() {
        return Ok(None);
    }
    let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
        return Ok(None);
    };
    let Some(candidate_name) = name.strip_suffix(OWNER_SUFFIX) else {
        return Ok(None);
    };
    let Some((kind, id)) = parse_candidate_name(candidate_name) else {
        return Ok(None);
    };
    Ok(Some((path.with_file_name(candidate_name), kind, id)))
}

fn parse_candidate_name(name: &str) -> Option<(RemoteTempKind, Uuid)> {
    for kind in [RemoteTempKind::Backup, RemoteTempKind::Restore] {
        let prefix = format!("rustdb-remote-{}-", kind.name());
        if let Some(suffix) = name.strip_prefix(&prefix)
            && let Ok(id) = Uuid::parse_str(suffix)
        {
            let mut buffer = Uuid::encode_buffer();
            if suffix == id.as_hyphenated().encode_lower(&mut buffer) {
                return Some((kind, id));
            }
        }
    }
    None
}

fn lock_old_valid_owner(
    directory: &Path,
    kind: RemoteTempKind,
    id: Uuid,
    orphan_ttl: Duration,
    now: SystemTime,
) -> Result<Option<File>> {
    let path = owner_path(directory);
    let metadata = match std::fs::symlink_metadata(&path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(Error::io(Some(path), error)),
    };
    let expected = owner_content(kind, id);
    if !metadata.file_type().is_file()
        || metadata.len() != expected.len() as u64
        || !has_mode_from_metadata(&metadata, 0o600)
        || !metadata
            .modified()
            .ok()
            .and_then(|modified| now.duration_since(modified).ok())
            .is_some_and(|age| age >= orphan_ttl)
    {
        return Ok(None);
    }
    let mut owner = match OpenOptions::new().read(true).write(true).open(&path) {
        Ok(owner) => owner,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(Error::io(Some(path), error)),
    };
    match owner.try_lock() {
        Ok(()) => {}
        Err(error) => {
            let error: std::io::Error = error.into();
            if error.kind() == std::io::ErrorKind::WouldBlock {
                return Ok(None);
            }
            return Err(Error::io(Some(path), error));
        }
    }
    let opened = owner
        .metadata()
        .map_err(|error| Error::io(Some(path.clone()), error))?;
    let current = match std::fs::symlink_metadata(&path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(Error::io(Some(path), error)),
    };
    if !current.file_type().is_file()
        || current.file_type().is_symlink()
        || !has_mode_from_metadata(&current, 0o600)
        || identity(&metadata) != identity(&current)
        || identity(&metadata) != identity(&opened)
    {
        return Ok(None);
    }
    let mut content = Vec::with_capacity(expected.len());
    owner
        .seek(SeekFrom::Start(0))
        .map_err(|error| Error::io(Some(path.clone()), error))?;
    Read::take(&mut owner, expected.len() as u64 + 1)
        .read_to_end(&mut content)
        .map_err(|error| Error::io(Some(path.clone()), error))?;
    if content != expected.as_bytes() {
        return Ok(None);
    }
    Ok(Some(owner))
}

fn candidate_path(root: &Path, kind: RemoteTempKind, id: Uuid) -> PathBuf {
    root.join(format!("rustdb-remote-{}-{id}", kind.name()))
}

fn owner_path(directory: &Path) -> PathBuf {
    let mut name = directory
        .file_name()
        .expect("remote temporary directory has a file name")
        .to_os_string();
    name.push(OWNER_SUFFIX);
    directory.with_file_name(name)
}

fn owner_content(kind: RemoteTempKind, id: Uuid) -> String {
    format!(
        "rustdb-remote-temporary\nversion={FORMAT_VERSION}\nkind={}\nid={id}\n",
        kind.name()
    )
}

fn create_private_directory(path: &Path) -> Result<()> {
    let mut builder = std::fs::DirBuilder::new();
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        builder.mode(0o700);
    }
    builder
        .create(path)
        .map_err(|error| Error::io(Some(path.to_path_buf()), error))
}

fn create_private_file(path: &Path) -> Result<File> {
    let mut options = OpenOptions::new();
    options.read(true).write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    options
        .open(path)
        .map_err(|error| Error::io(Some(path.to_path_buf()), error))
}

fn remove_directory(path: &Path) -> Result<()> {
    match std::fs::remove_dir_all(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(Error::io(Some(path.to_path_buf()), error)),
    }
}

fn remove_file(path: &Path) -> Result<()> {
    match std::fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(Error::io(Some(path.to_path_buf()), error)),
    }
}

fn cleanup_failed_creation(
    root: &Path,
    path: &Path,
    owner: &Path,
    directory_created: bool,
    primary: Error,
) -> Error {
    let cleanup = if directory_created {
        remove_directory(path).and_then(|()| remove_file(owner))
    } else {
        remove_file(owner)
    };
    if let Err(cleanup) = cleanup {
        return Error::Execution(format!(
            "{primary}; remote temporary directory creation cleanup also failed: {cleanup}"
        ));
    }
    if let Err(cleanup) = sync_directory(root) {
        return Error::Execution(format!(
            "{primary}; remote temporary directory creation cleanup sync also failed: {cleanup}"
        ));
    }
    primary
}

fn append_cleanup_error(primary: Error, cleanup: Error) -> Error {
    match primary {
        Error::CommitOutcomeUnknown {
            path,
            transaction_id,
            message,
        } => Error::CommitOutcomeUnknown {
            path,
            transaction_id,
            message: format!(
                "{message}; remote temporary directory cleanup also failed: {cleanup}"
            ),
        },
        error => Error::Execution(format!(
            "{error}; remote temporary directory cleanup also failed: {cleanup}"
        )),
    }
}

fn parent(path: &Path) -> Result<&Path> {
    path.parent()
        .ok_or_else(|| Error::Internal("remote temporary path has no parent".to_owned()))
}

fn sync_directory(path: &Path) -> Result<()> {
    let directory = File::open(path).map_err(|error| Error::io(Some(path.to_path_buf()), error))?;
    directory
        .sync_all()
        .map_err(|error| Error::io(Some(path.to_path_buf()), error))
}

fn same_directory(path: &Path, expected: FileIdentity) -> Result<bool> {
    let metadata = match std::fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(Error::io(Some(path.to_path_buf()), error)),
    };
    Ok(metadata.file_type().is_dir()
        && !metadata.file_type().is_symlink()
        && has_mode_from_metadata(&metadata, 0o700)
        && identity(&metadata) == expected)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct FileIdentity {
    #[cfg(unix)]
    device: u64,
    #[cfg(unix)]
    inode: u64,
}

fn identity(metadata: &std::fs::Metadata) -> FileIdentity {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        FileIdentity {
            device: metadata.dev(),
            inode: metadata.ino(),
        }
    }
    #[cfg(not(unix))]
    {
        let _ = metadata;
        FileIdentity {}
    }
}

#[cfg(unix)]
fn has_mode_from_metadata(metadata: &std::fs::Metadata, expected: u32) -> bool {
    use std::os::unix::fs::PermissionsExt;
    metadata.permissions().mode() & 0o777 == expected
}

#[cfg(not(unix))]
fn has_mode_from_metadata(_metadata: &std::fs::Metadata, _expected: u32) -> bool {
    true
}

#[cfg(test)]
mod tests;
