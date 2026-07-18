use std::path::Path;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{Error, Result};

use super::{DATABASE_FORMAT_VERSION, FORMAT_NAME, LEGACY_DATABASE_FORMAT_VERSION, io};

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
            format: FORMAT_NAME.to_owned(),
            version: DATABASE_FORMAT_VERSION,
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

    pub(super) fn uses_wal(&self) -> bool {
        self.version == DATABASE_FORMAT_VERSION
    }

    pub(super) fn is_legacy(&self) -> bool {
        self.version == LEGACY_DATABASE_FORMAT_VERSION
    }

    fn upgraded(&self) -> Self {
        Self {
            format: self.format.clone(),
            version: DATABASE_FORMAT_VERSION,
            database_id: self.database_id.clone(),
        }
    }
}

pub(super) fn read(path: &Path) -> Result<DatabaseMarker> {
    let bytes = io::read_bounded(path, MAX_DATABASE_MARKER_BYTES, "database marker")?;
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

pub(super) fn upgrade_legacy(path: &Path, transaction_id: &str) -> Result<()> {
    let marker = read(path)?;
    if !marker.is_legacy() {
        return Err(Error::InvalidArgument(
            "database marker is not a legacy v0.7 format".to_owned(),
        ));
    }
    let upgraded = marker.upgraded();
    let bytes = encode(path, &upgraded)?;
    io::atomic_replace(path, &bytes, transaction_id)
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
    if marker.format != FORMAT_NAME {
        return Err(Error::native_storage(
            path,
            format!("expected format '{FORMAT_NAME}', found '{}'", marker.format),
        ));
    }
    if !matches!(
        marker.version,
        LEGACY_DATABASE_FORMAT_VERSION | DATABASE_FORMAT_VERSION
    ) {
        return Err(Error::native_storage(
            path,
            format!(
                "unsupported database format version {}; supported versions are {LEGACY_DATABASE_FORMAT_VERSION} and {DATABASE_FORMAT_VERSION}",
                marker.version,
            ),
        ));
    }
    Uuid::parse_str(&marker.database_id)
        .map_err(|error| Error::native_storage(path, format!("invalid database id: {error}")))?;
    Ok(())
}
