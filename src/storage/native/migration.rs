use std::path::{Path, PathBuf};

use crate::Result;

use super::{NativeDatabase, format};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct NativeMigration {
    pub(crate) from_version: u32,
    pub(crate) to_version: u32,
    pub(crate) backup_path: Option<PathBuf>,
}

pub(super) fn migrate(path: &Path) -> Result<NativeMigration> {
    let database = NativeDatabase::open(path)?;
    Ok(NativeMigration {
        from_version: database.format_version,
        to_version: format::CURRENT_DATABASE_VERSION,
        backup_path: None,
    })
}

#[cfg(test)]
#[path = "migration/tests.rs"]
mod tests;
