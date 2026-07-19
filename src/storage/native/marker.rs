use std::path::Path;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{Error, Result};

use super::{format, io};

pub(super) const MAX_DATABASE_MARKER_BYTES: usize = 4 * 1024;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct DatabaseMarker {
    format: String,
    version: u32,
    database_id: String,
}

impl DatabaseMarker {
    pub(super) fn new() -> Self {
        Self {
            format: format::NAME.to_owned(),
            version: format::CURRENT_DATABASE_VERSION,
            database_id: Uuid::new_v4().to_string(),
        }
    }

    pub(super) fn same_database(&self, other: &Self) -> bool {
        self.database_id == other.database_id
    }

    pub(super) fn database_id(&self) -> &str {
        &self.database_id
    }

    pub(super) fn version(&self) -> u32 {
        self.version
    }
}

#[derive(Deserialize)]
struct DatabaseMarkerHeader {
    format: String,
    version: u32,
}

pub(super) fn preflight_if_present(path: &Path) -> Result<()> {
    match std::fs::symlink_metadata(path) {
        Ok(_) => preflight(path),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(Error::io(Some(path.to_path_buf()), error)),
    }
}

fn preflight(path: &Path) -> Result<()> {
    let bytes = io::read_bounded(path, MAX_DATABASE_MARKER_BYTES, "database marker")?;
    validate_header(path, &bytes)
}

pub(super) fn read(path: &Path) -> Result<DatabaseMarker> {
    let bytes = io::read_bounded(path, MAX_DATABASE_MARKER_BYTES, "database marker")?;
    validate_header(path, &bytes)?;
    let marker: DatabaseMarker = serde_json::from_slice(&bytes).map_err(|error| {
        Error::native_storage(path, format!("invalid database marker: {error}"))
    })?;
    validate(path, &marker)?;
    Ok(marker)
}

pub(super) fn write_new(path: &Path, marker: &DatabaseMarker) -> Result<()> {
    let bytes = encode(path, marker)?;
    io::atomic_create(path, &bytes)
}

fn encode(path: &Path, marker: &DatabaseMarker) -> Result<Vec<u8>> {
    let bytes = io::encode_json_bounded(
        path,
        marker,
        MAX_DATABASE_MARKER_BYTES,
        "database marker",
        true,
        true,
    )?;
    Ok(bytes)
}

fn validate(path: &Path, marker: &DatabaseMarker) -> Result<()> {
    if marker.format != format::NAME {
        return Err(Error::native_storage(
            path,
            format!(
                "expected format '{}', found '{}'",
                format::NAME,
                marker.format
            ),
        ));
    }
    format::require_current_database_version(path, marker.version)?;
    Uuid::parse_str(&marker.database_id)
        .map_err(|error| Error::native_storage(path, format!("invalid database id: {error}")))?;
    Ok(())
}

fn validate_header(path: &Path, bytes: &[u8]) -> Result<()> {
    let header: DatabaseMarkerHeader = serde_json::from_slice(bytes).map_err(|error| {
        Error::native_storage(path, format!("invalid database marker: {error}"))
    })?;
    if header.format != format::NAME {
        return Err(Error::native_storage(
            path,
            format!(
                "expected format '{}', found '{}'",
                format::NAME,
                header.format
            ),
        ));
    }
    format::require_current_database_version(path, header.version)
}
