use std::{
    fs::{self, File},
    io::Write,
    path::{Path, PathBuf},
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

#[cfg(not(unix))]
use std::fs::OpenOptions;

use parking_lot::Mutex;
use serde::Serialize;

use crate::{Error, Result};

use super::{files::ensure_secure_directory, state::SecurityState};

const SCHEMA_VERSION: u32 = 1;
const MAX_ACTIVE_BYTES: u64 = 64 * 1024 * 1024;

#[derive(Clone)]
pub(crate) struct AuditLog {
    inner: Arc<Mutex<AuditWriter>>,
}

struct AuditWriter {
    path: PathBuf,
    file: File,
    bytes: u64,
}

#[derive(Clone, Copy, Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum AuditKind {
    ServerStarted,
    ServerStopped,
    AuthenticationFailed,
    AuthorizationFailed,
    QuerySubmitted,
    QueryFinished,
    QueryCancelled,
    QueryDeleted,
    PrincipalChanged,
    TokenChanged,
}

#[derive(Serialize)]
struct AuditRecord<'a> {
    schema_version: u32,
    timestamp_ms: u64,
    kind: AuditKind,
    #[serde(skip_serializing_if = "Option::is_none")]
    principal_id: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    query_id: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    request_id: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    sql_fingerprint: Option<&'a str>,
    outcome: &'a str,
}

impl AuditLog {
    pub(crate) fn open(state: &SecurityState) -> Result<Self> {
        ensure_secure_directory(state.directory())?;
        let path = state.directory().join("audit.jsonl");
        let (file, bytes) = open_append(&path)?;
        Ok(Self {
            inner: Arc::new(Mutex::new(AuditWriter { path, file, bytes })),
        })
    }

    pub(crate) fn record(&self, event: AuditEvent<'_>) -> Result<()> {
        let record = AuditRecord {
            schema_version: SCHEMA_VERSION,
            timestamp_ms: now_ms(),
            kind: event.kind,
            principal_id: event.principal_id,
            query_id: event.query_id,
            request_id: event.request_id,
            sql_fingerprint: event.sql_fingerprint,
            outcome: event.outcome,
        };
        let mut bytes = serde_json::to_vec(&record)
            .map_err(|error| Error::Internal(format!("failed to encode audit record: {error}")))?;
        bytes.push(b'\n');
        let mut writer = self.inner.lock();
        if writer.bytes.saturating_add(bytes.len() as u64) > MAX_ACTIVE_BYTES {
            writer.rotate()?;
        }
        writer
            .file
            .write_all(&bytes)
            .and_then(|_| writer.file.sync_data())
            .map_err(|error| Error::io(Some(writer.path.clone()), error))?;
        writer.bytes = writer.bytes.saturating_add(bytes.len() as u64);
        Ok(())
    }
}

pub(crate) struct AuditEvent<'a> {
    pub(crate) kind: AuditKind,
    pub(crate) principal_id: Option<&'a str>,
    pub(crate) query_id: Option<&'a str>,
    pub(crate) request_id: Option<&'a str>,
    pub(crate) sql_fingerprint: Option<&'a str>,
    pub(crate) outcome: &'a str,
}

impl AuditWriter {
    fn rotate(&mut self) -> Result<()> {
        self.file
            .sync_all()
            .map_err(|error| Error::io(Some(self.path.clone()), error))?;
        let rotated = self.path.with_extension("jsonl.1");
        if rotated.exists() {
            let metadata = rotated
                .symlink_metadata()
                .map_err(|error| Error::io(Some(rotated.clone()), error))?;
            if !metadata.file_type().is_file() || metadata.file_type().is_symlink() {
                return Err(Error::InvalidArgument(format!(
                    "audit rotation target is unsafe: {}",
                    rotated.display()
                )));
            }
            fs::remove_file(&rotated).map_err(|error| Error::io(Some(rotated.clone()), error))?;
        }
        fs::rename(&self.path, &rotated)
            .map_err(|error| Error::io(Some(self.path.clone()), error))?;
        let (file, bytes) = open_append(&self.path)?;
        self.file = file;
        self.bytes = bytes;
        sync_parent(&self.path)
    }
}

fn open_append(path: &Path) -> Result<(File, u64)> {
    preflight_audit_path(path)?;
    let file = open_audit_file(path)?;
    let file_metadata = file
        .metadata()
        .map_err(|error| Error::io(Some(path.to_path_buf()), error))?;
    let metadata = path
        .symlink_metadata()
        .map_err(|error| Error::io(Some(path.to_path_buf()), error))?;
    if !metadata.file_type().is_file() || metadata.file_type().is_symlink() {
        return Err(Error::InvalidArgument(format!(
            "audit log is not a regular file: {}",
            path.display()
        )));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        if metadata.dev() != file_metadata.dev() || metadata.ino() != file_metadata.ino() {
            return Err(Error::InvalidArgument(format!(
                "audit log changed while being opened: {}",
                path.display()
            )));
        }
    }
    check_private_audit_mode(path, &metadata)?;
    Ok((file, metadata.len()))
}

fn preflight_audit_path(path: &Path) -> Result<()> {
    match path.symlink_metadata() {
        Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_file() => {
            Err(Error::InvalidArgument(format!(
                "audit log is not a regular file: {}",
                path.display()
            )))
        }
        Ok(metadata) => check_private_audit_mode(path, &metadata),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(Error::io(Some(path.to_path_buf()), error)),
    }
}

#[cfg(unix)]
fn open_audit_file(path: &Path) -> Result<File> {
    use rustix::fs::{Mode, OFlags, open};

    let flags =
        OFlags::WRONLY | OFlags::APPEND | OFlags::CREATE | OFlags::CLOEXEC | OFlags::NOFOLLOW;
    open(path, flags, Mode::RUSR | Mode::WUSR)
        .map(File::from)
        .map_err(|error| Error::io(Some(path.to_path_buf()), std::io::Error::from(error)))
}

#[cfg(not(unix))]
fn open_audit_file(path: &Path) -> Result<File> {
    let mut options = OpenOptions::new();
    options.append(true).create(true);
    options
        .open(path)
        .map_err(|error| Error::io(Some(path.to_path_buf()), error))
}

#[cfg(unix)]
fn check_private_audit_mode(path: &Path, metadata: &fs::Metadata) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    if metadata.permissions().mode() & 0o077 != 0 {
        return Err(Error::InvalidArgument(format!(
            "audit log must be private: {}",
            path.display()
        )));
    }
    Ok(())
}

#[cfg(not(unix))]
fn check_private_audit_mode(_path: &Path, _metadata: &fs::Metadata) -> Result<()> {
    Ok(())
}

fn sync_parent(path: &Path) -> Result<()> {
    let parent = path
        .parent()
        .ok_or_else(|| Error::InvalidArgument("audit path has no parent".into()))?;
    File::open(parent)
        .and_then(|file| file.sync_all())
        .map_err(|error| Error::io(Some(parent.to_path_buf()), error))
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|value| u64::try_from(value.as_millis()).unwrap_or(u64::MAX))
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use std::fs;

    use super::{AuditEvent, AuditKind, AuditLog};
    use crate::http_shell::security::SecurityState;

    #[test]
    fn writes_private_records_without_secret_fields() {
        let temporary = tempfile::tempdir().unwrap();
        let state =
            SecurityState::open(temporary.path(), "00000000-0000-0000-0000-000000000042").unwrap();
        let log = AuditLog::open(&state).unwrap();
        log.record(AuditEvent {
            kind: AuditKind::QuerySubmitted,
            principal_id: Some("alice"),
            query_id: Some("query-1"),
            request_id: Some("request-1"),
            sql_fingerprint: Some("deadbeef"),
            outcome: "accepted",
        })
        .unwrap();
        let path = state.directory().join("audit.jsonl");
        let contents = fs::read_to_string(&path).unwrap();
        assert!(contents.contains("query_submitted"));
        assert!(!contents.contains("token"));
        assert!(!contents.contains("sql\""));
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(fs::metadata(path).unwrap().permissions().mode() & 0o077, 0);
        }
    }

    #[cfg(unix)]
    #[test]
    fn refuses_dangling_audit_symlink_without_creating_its_target() {
        use std::os::unix::fs::symlink;

        let temporary = tempfile::tempdir().unwrap();
        let state =
            SecurityState::open(temporary.path(), "00000000-0000-0000-0000-000000000042").unwrap();
        let outside = temporary.path().join("outside-audit.jsonl");
        symlink(&outside, state.directory().join("audit.jsonl")).unwrap();

        assert!(AuditLog::open(&state).is_err());
        assert!(!outside.exists());
    }
}
