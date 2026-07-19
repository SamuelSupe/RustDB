use std::{
    fs::{File, OpenOptions},
    io::{Read, Seek, Write},
    path::{Path, PathBuf},
};

use serde::Deserialize;
use uuid::Uuid;

use crate::{Error, Result};

use super::files::{ensure_secure_directory, read_regular_file};

const MAX_DATABASE_MARKER_BYTES: u64 = 4 * 1024;
const SERVER_LOCK_FILE: &str = "server.lock";
const SERVER_LOCK_MARKER: &[u8] = b"rustdb-http-server-lock-v1\n";

/// Per-database state used by the HTTP shell security layer.
#[derive(Clone, Debug)]
pub struct SecurityState {
    database_id: String,
    directory: PathBuf,
}

#[doc(hidden)]
pub struct ServerStateLock {
    _file: File,
}

#[derive(Deserialize)]
struct NativeDatabaseMarker {
    database_id: String,
}

impl SecurityState {
    /// Opens a state directory for an already known native database id.
    pub fn open(state_root: impl AsRef<Path>, database_id: &str) -> Result<Self> {
        validate_database_id(database_id)?;
        let root = state_root.as_ref();
        ensure_secure_directory(root)?;
        let directory = root.join(database_id);
        ensure_secure_directory(&directory)?;
        Ok(Self {
            database_id: database_id.to_owned(),
            directory,
        })
    }

    /// Reads the stable id from a native database marker and opens its state.
    pub fn for_native_database(
        state_root: impl AsRef<Path>,
        database_directory: impl AsRef<Path>,
    ) -> Result<Self> {
        let marker_path = database_directory.as_ref().join(".rustdb");
        let encoded = read_regular_file(&marker_path, MAX_DATABASE_MARKER_BYTES, false)?;
        let marker: NativeDatabaseMarker = serde_json::from_slice(&encoded).map_err(|error| {
            Error::InvalidArgument(format!(
                "invalid native database marker at {}: {error}",
                marker_path.display()
            ))
        })?;
        Self::open(state_root, &marker.database_id)
    }

    pub fn database_id(&self) -> &str {
        &self.database_id
    }

    pub fn directory(&self) -> &Path {
        &self.directory
    }

    pub fn ca_identity_path(&self) -> PathBuf {
        self.directory.join("ca-identity.pem")
    }

    pub fn ca_certificate_path(&self) -> PathBuf {
        self.directory.join("ca.pem")
    }

    pub fn server_identity_path(&self) -> PathBuf {
        self.directory.join("server-identity.pem")
    }

    pub fn tls_metadata_path(&self) -> PathBuf {
        self.directory.join("tls.json")
    }

    pub fn token_path(&self) -> PathBuf {
        self.directory.join("bearer.token")
    }

    #[doc(hidden)]
    pub fn acquire_server_lock(&self) -> Result<ServerStateLock> {
        let path = self.directory.join(SERVER_LOCK_FILE);
        let mut options = OpenOptions::new();
        options.read(true).write(true).create(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let mut file = options
            .open(&path)
            .map_err(|error| Error::io(path.clone(), error))?;
        let metadata =
            std::fs::symlink_metadata(&path).map_err(|error| Error::io(path.clone(), error))?;
        if metadata.file_type().is_symlink() || !metadata.is_file() {
            return Err(Error::InvalidArgument(format!(
                "refusing insecure HTTP server lock {}",
                path.display()
            )));
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            if metadata.permissions().mode() & 0o077 != 0 {
                return Err(Error::InvalidArgument(format!(
                    "HTTP server lock {} must not be accessible by group or other users",
                    path.display()
                )));
            }
        }
        file.try_lock().map_err(|error| {
            Error::InvalidArgument(format!(
                "another HTTP server already owns database id {}: {error}",
                self.database_id
            ))
        })?;
        let mut marker = Vec::new();
        file.rewind()
            .and_then(|_| file.read_to_end(&mut marker))
            .map_err(|error| Error::io(path.clone(), error))?;
        if marker.is_empty() {
            file.rewind()
                .and_then(|_| file.write_all(SERVER_LOCK_MARKER))
                .and_then(|_| file.set_len(SERVER_LOCK_MARKER.len() as u64))
                .and_then(|_| file.sync_all())
                .map_err(|error| Error::io(path.clone(), error))?;
        } else if marker != SERVER_LOCK_MARKER {
            return Err(Error::InvalidArgument(format!(
                "invalid HTTP server lock marker at {}",
                path.display()
            )));
        }
        Ok(ServerStateLock { _file: file })
    }
}

/// Returns the platform user-level root used for server security state.
pub fn default_state_root() -> Result<PathBuf> {
    if let Some(root) = std::env::var_os("RUSTDB_STATE_HOME") {
        return Ok(PathBuf::from(root).join("http-shell"));
    }
    #[cfg(target_os = "macos")]
    {
        return Ok(home_directory()?
            .join("Library")
            .join("Application Support")
            .join("rustdb")
            .join("http-shell")
            .join("state"));
    }
    #[cfg(not(target_os = "macos"))]
    {
        if let Some(root) = std::env::var_os("XDG_STATE_HOME") {
            return Ok(PathBuf::from(root).join("rustdb").join("http-shell"));
        }
        Ok(home_directory()?
            .join(".local")
            .join("state")
            .join("rustdb")
            .join("http-shell"))
    }
}

/// Returns the platform user-level root used for imported client profiles.
pub fn default_profile_root() -> Result<PathBuf> {
    if let Some(root) = std::env::var_os("RUSTDB_CONFIG_HOME") {
        return Ok(PathBuf::from(root).join("http-shell").join("profiles"));
    }
    #[cfg(target_os = "macos")]
    {
        return Ok(home_directory()?
            .join("Library")
            .join("Application Support")
            .join("rustdb")
            .join("http-shell")
            .join("profiles"));
    }
    #[cfg(not(target_os = "macos"))]
    {
        if let Some(root) = std::env::var_os("XDG_CONFIG_HOME") {
            return Ok(PathBuf::from(root)
                .join("rustdb")
                .join("http-shell")
                .join("profiles"));
        }
        Ok(home_directory()?
            .join(".config")
            .join("rustdb")
            .join("http-shell")
            .join("profiles"))
    }
}

fn home_directory() -> Result<PathBuf> {
    std::env::var_os("HOME").map(PathBuf::from).ok_or_else(|| {
        Error::InvalidArgument(
            "HOME is not set; configure an explicit RustDB state path".to_owned(),
        )
    })
}

fn validate_database_id(database_id: &str) -> Result<()> {
    Uuid::parse_str(database_id)
        .map(|_| ())
        .map_err(|_| Error::InvalidArgument("native database id must be a UUID".to_owned()))
}
