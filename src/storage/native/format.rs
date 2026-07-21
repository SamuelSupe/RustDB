use std::path::Path;

use crate::{Error, Result};

pub(super) const NAME: &str = "rustdb-native";
pub(super) const CURRENT_DATABASE_VERSION: u32 = 4;

pub(super) const fn is_legacy_database_version(version: u32) -> bool {
    matches!(version, 1..=3)
}

pub(super) fn require_current_database_version(path: &Path, version: u32) -> Result<()> {
    if version == CURRENT_DATABASE_VERSION {
        return Ok(());
    }
    Err(Error::NativeFormatUnsupported {
        path: path.to_path_buf(),
        found_version: version,
        current_version: CURRENT_DATABASE_VERSION,
        legacy: is_legacy_database_version(version),
    })
}
