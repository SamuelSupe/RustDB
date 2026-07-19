use std::{
    collections::{BTreeMap, BTreeSet},
    path::Path,
};

use arrow::datatypes::{Schema, SchemaRef};

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{Error, Result};

use super::io;

mod generation_io;
mod recovery;

use generation_io::{
    current_path, ensure_generation, read_current, read_generation, write_current,
};
pub(super) use generation_io::{generation_from_name, generation_path};
pub(super) use recovery::{prune_old_generations, recover_future_generations};

const INITIAL_GENERATION: u64 = 0;
const LEGACY_FORMAT_VERSION: u32 = 1;
const PREVIOUS_FORMAT_VERSION: u32 = 2;
const FORMAT_VERSION: u32 = 3;
pub(super) const MAX_CURRENT_BYTES: usize = 32;
pub(super) const MAX_TABLE_NAME_BYTES: usize = 255;
pub(super) const MAX_VIEW_SQL_BYTES: usize = 1024 * 1024;
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    transaction_id: Option<String>,
    #[serde(default, skip_serializing_if = "BTreeSet::is_empty")]
    schemas: BTreeSet<String>,
    tables: BTreeMap<String, TableReference>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    views: BTreeMap<String, ViewReference>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    imports: BTreeMap<String, crate::NativeImportReceipt>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct TableReference {
    table_id: String,
    version: u64,
    snapshot_id: String,
    manifest_sha256: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ViewReference {
    version: u64,
    sql: String,
    schema_ipc_hex: String,
    schema_sha256: String,
}

impl CatalogState {
    pub(super) fn generation(&self) -> u64 {
        self.generation
    }

    pub(super) fn tables(&self) -> &BTreeMap<String, TableReference> {
        &self.tables
    }

    pub(super) fn schemas(&self) -> &BTreeSet<String> {
        &self.schemas
    }

    pub(super) fn views(&self) -> &BTreeMap<String, ViewReference> {
        &self.views
    }

    pub(super) fn imports(&self) -> &BTreeMap<String, crate::NativeImportReceipt> {
        &self.imports
    }

    pub(super) fn transaction_id(&self) -> Option<&str> {
        self.transaction_id.as_deref()
    }
}

impl ViewReference {
    pub(super) fn new(version: u64, sql: impl Into<String>, schema: &Schema) -> Self {
        let bytes = super::schema::encode(schema);
        Self {
            version,
            sql: sql.into(),
            schema_ipc_hex: super::schema::encode_hex(&bytes),
            schema_sha256: super::schema::sha256(&bytes),
        }
    }

    pub(super) fn version(&self) -> u64 {
        self.version
    }

    pub(super) fn sql(&self) -> &str {
        &self.sql
    }

    pub(super) fn schema(&self, path: &Path) -> Result<SchemaRef> {
        super::schema::decode(path, &self.schema_ipc_hex, &self.schema_sha256)
    }

    fn validate(&self, path: &Path, name: &str) -> Result<()> {
        if self.version == 0 {
            return Err(Error::native_storage(
                path,
                format!("persistent view '{name}' has a zero version"),
            ));
        }
        if self.sql.is_empty() || self.sql.len() > MAX_VIEW_SQL_BYTES || self.sql.contains('\0') {
            return Err(Error::native_storage(
                path,
                format!("persistent view '{name}' has invalid SQL text"),
            ));
        }
        self.schema(path)?;
        Ok(())
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
        transaction_id: None,
        schemas: BTreeSet::from([crate::catalog_name::DEFAULT_SCHEMA.to_owned()]),
        tables: BTreeMap::new(),
        views: BTreeMap::new(),
        imports: BTreeMap::new(),
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
    validate_catalog(
        &generation_path(root, generation),
        state.generation,
        &state.schemas,
        &state.tables,
        &state.views,
        &state.imports,
    )?;
    Ok(state)
}

pub(super) fn load_generation(
    root: &Path,
    database_id: &str,
    generation: u64,
) -> Result<CatalogState> {
    let state = read_generation(root, generation)?;
    if state.database_id != database_id {
        return Err(Error::native_storage(
            generation_path(root, generation),
            "catalog manifest belongs to another database",
        ));
    }
    if state.generation != generation {
        return Err(Error::native_storage(
            generation_path(root, generation),
            "catalog generation number does not match its file name",
        ));
    }
    validate_catalog(
        &generation_path(root, generation),
        state.generation,
        &state.schemas,
        &state.tables,
        &state.views,
        &state.imports,
    )?;
    Ok(state)
}

#[cfg(test)]
pub(super) fn commit(
    root: &Path,
    database_id: &str,
    expected_generation: u64,
    tables: BTreeMap<String, TableReference>,
) -> Result<CatalogState> {
    let transaction_id = Uuid::new_v4().to_string();
    let prepared = prepare_commit(
        root,
        database_id,
        expected_generation,
        tables,
        &transaction_id,
    )?;
    publish_prepared(
        root,
        database_id,
        expected_generation,
        prepared,
        &transaction_id,
    )
}

pub(super) fn prepare_commit(
    root: &Path,
    database_id: &str,
    expected_generation: u64,
    tables: BTreeMap<String, TableReference>,
    transaction_id: &str,
) -> Result<CatalogState> {
    let current = load(root, database_id)?;
    prepare_catalog_commit(
        root,
        database_id,
        expected_generation,
        current.schemas.clone(),
        tables,
        current.views.clone(),
        transaction_id,
    )
}

pub(super) fn prepare_catalog_commit(
    root: &Path,
    database_id: &str,
    expected_generation: u64,
    schemas: BTreeSet<String>,
    tables: BTreeMap<String, TableReference>,
    views: BTreeMap<String, ViewReference>,
    transaction_id: &str,
) -> Result<CatalogState> {
    prepare_catalog_commit_with_import(
        root,
        database_id,
        expected_generation,
        schemas,
        tables,
        views,
        None,
        transaction_id,
    )
}

#[allow(clippy::too_many_arguments)]
pub(super) fn prepare_catalog_commit_with_import(
    root: &Path,
    database_id: &str,
    expected_generation: u64,
    schemas: BTreeSet<String>,
    tables: BTreeMap<String, TableReference>,
    views: BTreeMap<String, ViewReference>,
    receipt: Option<crate::NativeImportReceipt>,
    transaction_id: &str,
) -> Result<CatalogState> {
    Uuid::parse_str(transaction_id).map_err(|error| {
        Error::InvalidArgument(format!(
            "invalid transaction id '{transaction_id}': {error}"
        ))
    })?;
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
    let mut imports = current.imports.clone();
    if let Some(receipt) = receipt {
        if imports.contains_key(receipt.import_id()) {
            return Err(Error::native_import_conflict(receipt.import_id()));
        }
        imports.insert(receipt.import_id().to_owned(), receipt);
    }
    let state = CatalogState {
        database_id: database_id.to_owned(),
        format_version: FORMAT_VERSION,
        generation,
        transaction_id: Some(transaction_id.to_owned()),
        schemas,
        tables,
        views,
        imports,
    };
    let path = generation_path(root, generation);
    validate_catalog(
        &path,
        state.generation,
        &state.schemas,
        &state.tables,
        &state.views,
        &state.imports,
    )?;
    ensure_generation(root, &state)?;

    Ok(state)
}

pub(super) fn publish_prepared(
    root: &Path,
    database_id: &str,
    expected_generation: u64,
    state: CatalogState,
    transaction_id: &str,
) -> Result<CatalogState> {
    let current = load(root, database_id)?;
    if current.generation != expected_generation {
        return Err(Error::Catalog(format!(
            "catalog generation changed: expected {expected_generation}, found {}",
            current.generation
        )));
    }
    if state.database_id != database_id
        || state.generation
            != expected_generation.checked_add(1).ok_or_else(|| {
                Error::ResourceExhausted("catalog generation counter is exhausted".to_owned())
            })?
        || state.transaction_id() != Some(transaction_id)
    {
        return Err(Error::native_storage(
            generation_path(root, state.generation),
            "prepared catalog identity does not match the publishing transaction",
        ));
    }
    let generation = state.generation;
    let path = generation_path(root, generation);
    let persisted = load_generation(root, database_id, generation)?;
    if persisted != state {
        return Err(Error::native_storage(
            path,
            "prepared catalog generation changed before publication",
        ));
    }

    let current_file = current_path(root);
    let current_bytes = format!("{generation}\n");
    io::validate_size(
        &current_file,
        current_bytes.len(),
        MAX_CURRENT_BYTES,
        "catalog CURRENT",
    )?;
    io::atomic_replace(&current_file, current_bytes.as_bytes(), transaction_id)?;

    let committed = load(root, database_id).map_err(|error| {
        Error::native_commit_post_commit_failure(
            current_file.clone(),
            transaction_id,
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
    validate_planned_update_with_views(
        root,
        current,
        &current.schemas,
        name,
        reference,
        current.views.clone(),
    )
}

pub(super) fn validate_planned_transaction_update(
    root: &Path,
    current: &CatalogState,
    schemas: &BTreeSet<String>,
    name: &str,
    reference: TableReference,
) -> Result<()> {
    let mut views = current.views.clone();
    views.remove(name);
    validate_planned_update_with_views(root, current, schemas, name, reference, views)
}

pub(super) fn projected_generation_bytes(
    root: &Path,
    current: &CatalogState,
    schemas: BTreeSet<String>,
    tables: BTreeMap<String, TableReference>,
    views: BTreeMap<String, ViewReference>,
) -> Result<u64> {
    let generation = current.generation.checked_add(1).ok_or_else(|| {
        Error::ResourceExhausted("catalog generation counter is exhausted".to_owned())
    })?;
    let state = CatalogState {
        database_id: current.database_id.clone(),
        format_version: FORMAT_VERSION,
        generation,
        transaction_id: Some(Uuid::new_v4().to_string()),
        schemas,
        tables,
        views,
        imports: current.imports.clone(),
    };
    generation_io::encoded_generation_size(root, &state)
}

fn validate_planned_update_with_views(
    root: &Path,
    current: &CatalogState,
    schemas: &BTreeSet<String>,
    name: &str,
    reference: TableReference,
    views: BTreeMap<String, ViewReference>,
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
        transaction_id: Some(Uuid::new_v4().to_string()),
        schemas: schemas.clone(),
        tables,
        views,
        imports: current.imports.clone(),
    };
    let path = generation_path(root, generation);
    validate_catalog(
        &path,
        state.generation,
        &state.schemas,
        &state.tables,
        &state.views,
        &state.imports,
    )?;
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

fn validate_catalog(
    path: &Path,
    generation: u64,
    schemas: &BTreeSet<String>,
    tables: &BTreeMap<String, TableReference>,
    views: &BTreeMap<String, ViewReference>,
    imports: &BTreeMap<String, crate::NativeImportReceipt>,
) -> Result<()> {
    validate_schemas(path, schemas)?;
    validate_tables(path, tables)?;
    for name in tables.keys() {
        validate_object_schema(path, schemas, name)?;
    }
    for (name, view) in views {
        validate_name(path, name, "view")?;
        validate_object_schema(path, schemas, name)?;
        if tables.contains_key(name) {
            return Err(Error::native_storage(
                path,
                format!("catalog name '{name}' is both a table and a view"),
            ));
        }
        view.validate(path, name)?;
    }
    for (import_id, receipt) in imports {
        receipt.validate(import_id).map_err(|error| {
            Error::native_storage(
                path,
                format!("invalid Native import receipt '{import_id}': {error}"),
            )
        })?;
        validate_name(path, receipt.table(), "import receipt table")?;
        if receipt.catalog_generation() > generation {
            return Err(Error::native_storage(
                path,
                format!(
                    "Native import receipt '{import_id}' refers to future catalog generation {}",
                    receipt.catalog_generation()
                ),
            ));
        }
    }
    Ok(())
}

fn validate_schemas(path: &Path, schemas: &BTreeSet<String>) -> Result<()> {
    if !schemas.contains(crate::catalog_name::DEFAULT_SCHEMA) {
        return Err(Error::native_storage(
            path,
            "catalog is missing the default 'main' schema",
        ));
    }
    for schema in schemas {
        validate_name(path, schema, "schema")?;
        if schema.contains('.') {
            return Err(Error::native_storage(
                path,
                format!("invalid schema name '{schema}'"),
            ));
        }
    }
    Ok(())
}

fn validate_object_schema(path: &Path, schemas: &BTreeSet<String>, name: &str) -> Result<()> {
    let schema = crate::catalog_name::schema_of(name);
    if schemas.contains(schema) {
        return Ok(());
    }
    Err(Error::native_storage(
        path,
        format!("catalog object '{name}' refers to missing schema '{schema}'"),
    ))
}

pub(super) fn normalize_legacy(mut state: CatalogState) -> CatalogState {
    if state.schemas.is_empty() {
        state
            .schemas
            .insert(crate::catalog_name::DEFAULT_SCHEMA.to_owned());
    }
    state
}

fn validate_name(path: &Path, name: &str, kind: &str) -> Result<()> {
    if name.is_empty()
        || name.len() > MAX_TABLE_NAME_BYTES
        || name != name.to_ascii_lowercase()
        || name.contains('\0')
    {
        return Err(Error::native_storage(
            path,
            format!(
                "invalid normalized {kind} name '{name}'; names are limited to {MAX_TABLE_NAME_BYTES} UTF-8 bytes"
            ),
        ));
    }
    Ok(())
}

fn valid_sha256(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

#[cfg(test)]
#[path = "manifest/tests.rs"]
mod tests;
