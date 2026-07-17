use std::path::Path;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{Error, Result};

use super::{DATABASE_FORMAT_VERSION, FORMAT_NAME, io};

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
    let bytes = io::encode_json_bounded(
        path,
        marker,
        MAX_DATABASE_MARKER_BYTES,
        "database marker",
        true,
        true,
    )?;
    io::atomic_create(path, &bytes)
}

fn validate(path: &Path, marker: &DatabaseMarker) -> Result<()> {
    if marker.format != FORMAT_NAME {
        return Err(Error::native_storage(
            path,
            format!("expected format '{FORMAT_NAME}', found '{}'", marker.format),
        ));
    }
    if marker.version != DATABASE_FORMAT_VERSION {
        return Err(Error::native_storage(
            path,
            format!(
                "unsupported database format version {}; expected {DATABASE_FORMAT_VERSION}",
                marker.version
            ),
        ));
    }
    Uuid::parse_str(&marker.database_id)
        .map_err(|error| Error::native_storage(path, format!("invalid database id: {error}")))?;
    Ok(())
}
