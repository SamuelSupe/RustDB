use std::{collections::BTreeMap, path::Path};

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{Error, Result};

use super::io;

mod generation_io;
mod recovery;

use generation_io::{
    current_path, ensure_generation, generation_path, read_current, read_generation, write_current,
};
pub(super) use recovery::{prune_old_generations, recover_future_generations};

const INITIAL_GENERATION: u64 = 0;
const FORMAT_VERSION: u32 = 1;
pub(super) const MAX_CURRENT_BYTES: usize = 32;
pub(super) const MAX_TABLE_NAME_BYTES: usize = 255;
pub(super) const CATALOG_GENERATION_BUDGET_PER_TABLE_BYTES: usize = 8 * 1024;
// At roughly a few hundred bytes per table this still permits hundreds of
// thousands of table references without allowing a corrupt catalog to grow
// memory without bound.
pub(super) const MAX_CATALOG_MANIFEST_BYTES: usize = 64 * 1024 * 1024;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct CatalogState {
    database_id: String,
    format_version: u32,
    generation: u64,
    tables: BTreeMap<String, TableReference>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct TableReference {
    table_id: String,
    version: u64,
    snapshot_id: String,
    manifest_sha256: String,
}

impl CatalogState {
    pub(super) fn generation(&self) -> u64 {
        self.generation
    }

    pub(super) fn tables(&self) -> &BTreeMap<String, TableReference> {
        &self.tables
    }
}

impl TableReference {
    pub(super) fn new(
        table_id: impl Into<String>,
        version: u64,
        snapshot_id: impl Into<String>,
        manifest_sha256: impl Into<String>,
    ) -> Self {
        Self {
            table_id: table_id.into(),
            version,
            snapshot_id: snapshot_id.into(),
            manifest_sha256: manifest_sha256.into(),
        }
    }

    pub(super) fn table_id(&self) -> &str {
        &self.table_id
    }

    pub(super) fn snapshot_id(&self) -> &str {
        &self.snapshot_id
    }

    pub(super) fn version(&self) -> u64 {
        self.version
    }

    pub(super) fn manifest_sha256(&self) -> &str {
        &self.manifest_sha256
    }

    pub(super) fn validate(&self, path: &Path, name: &str) -> Result<()> {
        Uuid::parse_str(self.table_id()).map_err(|error| {
            Error::native_storage(path, format!("invalid table id for '{name}': {error}"))
        })?;
        Uuid::parse_str(self.snapshot_id()).map_err(|error| {
            Error::native_storage(path, format!("invalid snapshot id for '{name}': {error}"))
        })?;
        if self.version() == 0 {
            return Err(Error::native_storage(
                path,
                format!("invalid zero table version for '{name}'"),
            ));
        }
        if !valid_sha256(self.manifest_sha256()) {
            return Err(Error::native_storage(
                path,
                format!("invalid table manifest checksum for '{name}'"),
            ));
        }
        Ok(())
    }
}

pub(super) fn ensure_initial(root: &Path, database_id: &str) -> Result<()> {
    let state = CatalogState {
        database_id: database_id.to_owned(),
        format_version: FORMAT_VERSION,
        generation: INITIAL_GENERATION,
        tables: BTreeMap::new(),
    };
    ensure_generation(root, &state)?;

    let current_path = current_path(root);
    if current_path.exists() {
        let current = read_current(root)?;
        if current != INITIAL_GENERATION {
            return Err(Error::native_storage(
                current_path,
                format!("initialization expected generation {INITIAL_GENERATION}, found {current}"),
            ));
        }
    } else {
        write_current(root, INITIAL_GENERATION)?;
    }
    Ok(())
}

pub(super) fn validate_current(root: &Path, database_id: &str) -> Result<()> {
    load(root, database_id).map(|_| ())
}

pub(super) fn load(root: &Path, database_id: &str) -> Result<CatalogState> {
    let generation = read_current(root)?;
    let state = read_generation(root, generation)?;
    if state.generation != generation {
        return Err(Error::native_storage(
            generation_path(root, generation),
            format!(
                "manifest generation {} does not match CURRENT generation {generation}",
                state.generation
            ),
        ));
    }
    if state.database_id != database_id {
        return Err(Error::native_storage(
            generation_path(root, generation),
            "catalog manifest belongs to another database",
        ));
    }
    validate_tables(&generation_path(root, generation), &state.tables)?;
    Ok(state)
}

pub(super) fn commit(
    root: &Path,
    database_id: &str,
    expected_generation: u64,
    tables: BTreeMap<String, TableReference>,
) -> Result<CatalogState> {
    let current = load(root, database_id)?;
    if current.generation != expected_generation {
        return Err(Error::Catalog(format!(
            "catalog generation changed: expected {expected_generation}, found {}",
            current.generation
        )));
    }
    let generation = expected_generation.checked_add(1).ok_or_else(|| {
        Error::ResourceExhausted("catalog generation counter is exhausted".to_owned())
    })?;
    let state = CatalogState {
        database_id: database_id.to_owned(),
        format_version: FORMAT_VERSION,
        generation,
        tables,
    };
    let path = generation_path(root, generation);
    validate_tables(&path, &state.tables)?;
    ensure_generation(root, &state)?;

    let transaction_id = Uuid::new_v4().to_string();
    let current_file = current_path(root);
    let current_bytes = format!("{generation}\n");
    io::validate_size(
        &current_file,
        current_bytes.len(),
        MAX_CURRENT_BYTES,
        "catalog CURRENT",
    )?;
    if let Err(error) = io::atomic_replace(&current_file, current_bytes.as_bytes(), &transaction_id)
    {
        if matches!(error, Error::CommitOutcomeUnknown { .. }) {
            return Err(error);
        }
        return match io::remove_file(&path) {
            Ok(()) => Err(error),
            Err(cleanup) => Err(Error::native_storage(
                &path,
                format!(
                    "{error}; unpublished catalog generation cleanup failed and the database must be reopened: {cleanup}"
                ),
            )),
        };
    }

    let committed = load(root, database_id).map_err(|error| {
        Error::native_commit_post_commit_failure(
            current_file.clone(),
            &transaction_id,
            generation,
            format!(
                "CURRENT is durable but catalog verification failed; reopen the engine: {error}"
            ),
        )
    })?;
    if committed != state {
        return Err(Error::native_commit_post_commit_failure(
            current_file,
            transaction_id,
            generation,
            "committed catalog does not match the requested generation; reopen the engine",
        ));
    }
    Ok(committed)
}

pub(super) fn validate_planned_update(
    root: &Path,
    current: &CatalogState,
    name: &str,
    reference: TableReference,
) -> Result<()> {
    let generation = current.generation.checked_add(1).ok_or_else(|| {
        Error::ResourceExhausted("catalog generation counter is exhausted".to_owned())
    })?;
    let mut tables = current.tables.clone();
    tables.insert(name.to_owned(), reference);
    let state = CatalogState {
        database_id: current.database_id.clone(),
        format_version: FORMAT_VERSION,
        generation,
        tables,
    };
    let path = generation_path(root, generation);
    validate_tables(&path, &state.tables)?;
    generation_io::validate_generation_size(root, &state)
}

fn validate_tables(path: &Path, tables: &BTreeMap<String, TableReference>) -> Result<()> {
    for (name, reference) in tables {
        if name.is_empty()
            || name.len() > MAX_TABLE_NAME_BYTES
            || *name != name.to_ascii_lowercase()
            || name.contains('\0')
        {
            return Err(Error::native_storage(
                path,
                format!(
                    "invalid normalized table name '{name}'; names are limited to {MAX_TABLE_NAME_BYTES} UTF-8 bytes"
                ),
            ));
        }
        reference.validate(path, name)?;
    }
    Ok(())
}

fn valid_sha256(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

#[cfg(test)]
#[path = "manifest/tests.rs"]
mod tests;
