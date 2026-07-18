use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use super::super::Engine;
use crate::{
    Catalog, Error, Result, TableEntry,
    datasource::{NativeSegmentTable, NativeSystemSnapshot, NativeSystemTable, SystemTableKind},
    storage::{
        NativePublishedSnapshot, NativeTableSnapshot, NativeWriteMode, NativeWritePlan,
        PreparedSnapshot,
    },
};
use parking_lot::Mutex;
use uuid::Uuid;

pub(in crate::engine) struct TransactionWorkspace {
    transaction_id: Uuid,
    snapshot_generation: u64,
    base_schemas: BTreeSet<String>,
    base_tables: BTreeMap<String, Arc<NativeTableSnapshot>>,
    base_views: BTreeMap<String, Arc<crate::storage::NativeView>>,
    mutation_active: AtomicBool,
    state: Mutex<State>,
}

struct State {
    expected_schemas: BTreeMap<String, bool>,
    working_schemas: BTreeSet<String>,
    schema_updates: BTreeMap<String, bool>,
    expected: BTreeMap<String, Option<Arc<NativeTableSnapshot>>>,
    working: BTreeMap<String, Arc<NativeTableSnapshot>>,
    working_views: BTreeMap<String, Arc<crate::storage::NativeView>>,
    writes: Vec<NativePublishedSnapshot>,
    drops: std::collections::BTreeSet<String>,
    renames: BTreeMap<String, String>,
    expected_views: BTreeMap<String, Option<Arc<crate::storage::NativeView>>>,
    view_updates: BTreeMap<String, Option<Arc<crate::storage::NativeView>>>,
}

pub(in crate::engine) struct MutationLease {
    workspace: Arc<TransactionWorkspace>,
}

impl TransactionWorkspace {
    pub(in crate::engine) fn new(
        transaction_id: Uuid,
        snapshot_generation: u64,
        schemas: BTreeSet<String>,
        tables: BTreeMap<String, Arc<NativeTableSnapshot>>,
        views: BTreeMap<String, Arc<crate::storage::NativeView>>,
    ) -> Arc<Self> {
        Arc::new(Self {
            transaction_id,
            snapshot_generation,
            base_schemas: schemas.clone(),
            base_tables: tables.clone(),
            base_views: views.clone(),
            mutation_active: AtomicBool::new(false),
            state: Mutex::new(State {
                expected_schemas: BTreeMap::new(),
                working_schemas: schemas,
                schema_updates: BTreeMap::new(),
                expected: BTreeMap::new(),
                working: tables,
                working_views: views,
                writes: Vec::new(),
                drops: Default::default(),
                renames: BTreeMap::new(),
                expected_views: BTreeMap::new(),
                view_updates: BTreeMap::new(),
            }),
        })
    }

    pub(in crate::engine) fn begin_mutation(self: &Arc<Self>) -> Result<MutationLease> {
        if self
            .mutation_active
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return Err(Error::InvalidArgument(
                "a transaction may execute only one native mutation at a time".to_owned(),
            ));
        }
        Ok(MutationLease {
            workspace: Arc::clone(self),
        })
    }

    pub(in crate::engine) fn is_staged(&self, name: &str) -> bool {
        self.state
            .lock()
            .expected
            .contains_key(&name.to_ascii_lowercase())
    }

    pub(in crate::engine) fn working_snapshot(
        &self,
        name: &str,
    ) -> Option<Arc<NativeTableSnapshot>> {
        self.state
            .lock()
            .working
            .get(&name.to_ascii_lowercase())
            .cloned()
    }

    pub(in crate::engine) fn working_view(
        &self,
        name: &str,
    ) -> Option<Arc<crate::storage::NativeView>> {
        self.state
            .lock()
            .working_views
            .get(&name.to_ascii_lowercase())
            .cloned()
    }

    pub(in crate::engine) fn schema_names(&self) -> Vec<String> {
        self.state.lock().working_schemas.iter().cloned().collect()
    }

    pub(in crate::engine) fn stage_schema_create(
        &self,
        name: &str,
        if_not_exists: bool,
    ) -> Result<bool> {
        let name = name.to_ascii_lowercase();
        let mut state = self.state.lock();
        if state.working_schemas.contains(&name) {
            return if if_not_exists {
                Ok(false)
            } else {
                Err(Error::Catalog(format!("schema '{name}' already exists")))
            };
        }
        state.working_schemas.insert(name.clone());
        if self.base_schemas.contains(&name) {
            state.expected_schemas.remove(&name);
            state.schema_updates.remove(&name);
        } else {
            state.expected_schemas.insert(name.clone(), false);
            state.schema_updates.insert(name, true);
        }
        Ok(true)
    }

    pub(in crate::engine) fn stage_schema_drop(&self, name: &str, if_exists: bool) -> Result<bool> {
        let name = name.to_ascii_lowercase();
        if name == crate::catalog_name::DEFAULT_SCHEMA {
            return Err(Error::Unsupported(
                "the default 'main' schema cannot be dropped".to_owned(),
            ));
        }
        let mut state = self.state.lock();
        if !state.working_schemas.contains(&name) {
            return if if_exists {
                Ok(false)
            } else {
                Err(Error::Catalog(format!("schema '{name}' does not exist")))
            };
        }
        if state
            .working
            .keys()
            .chain(state.working_views.keys())
            .any(|object| crate::catalog_name::schema_of(object) == name)
        {
            return Err(Error::Catalog(format!("schema '{name}' is not empty")));
        }
        state.working_schemas.remove(&name);
        if self.base_schemas.contains(&name) {
            state.expected_schemas.insert(name.clone(), true);
            state.schema_updates.insert(name, false);
        } else {
            state.expected_schemas.remove(&name);
            state.schema_updates.remove(&name);
        }
        Ok(true)
    }

    pub(in crate::engine) fn pin_catalog(&self, catalog: &Catalog) -> Result<Catalog> {
        let state = self.state.lock();
        let snapshot = NativeSystemSnapshot::new(
            state.working_schemas.clone(),
            state.working.clone(),
            state.working_views.clone(),
        );
        for (name, kind) in [
            (
                "information_schema.schemata",
                SystemTableKind::InformationSchemata,
            ),
            (
                "information_schema.tables",
                SystemTableKind::InformationTables,
            ),
            (
                "information_schema.columns",
                SystemTableKind::InformationColumns,
            ),
            ("rustdb_system.tables", SystemTableKind::NativeTables),
        ] {
            catalog.register(TableEntry::new(
                name,
                Arc::new(NativeSystemTable::for_transaction(snapshot.clone(), kind)),
            ))?;
        }
        Ok(catalog.pin())
    }

    pub(in crate::engine) fn stage_view(
        &self,
        catalog: &Catalog,
        name: &str,
        sql: String,
        schema: arrow::datatypes::SchemaRef,
        entry: TableEntry,
        replace: bool,
    ) -> Result<()> {
        let name = name.to_ascii_lowercase();
        let mut state = self.state.lock();
        let schema_name = crate::catalog_name::schema_of(&name);
        if !state.working_schemas.contains(schema_name) {
            return Err(Error::Catalog(format!(
                "schema '{schema_name}' does not exist"
            )));
        }
        state
            .expected_schemas
            .entry(schema_name.to_owned())
            .or_insert_with(|| self.base_schemas.contains(schema_name));
        if state.working.contains_key(&name) {
            return Err(Error::Catalog(format!(
                "cannot replace table '{name}' with a view"
            )));
        }
        let previous = state.working_views.get(&name).cloned();
        if previous.is_some() && !replace {
            return Err(Error::Catalog(format!("view '{name}' already exists")));
        }
        let version = match previous.as_ref() {
            Some(view) => view
                .version()
                .checked_add(1)
                .ok_or_else(|| Error::ResourceExhausted("view version overflowed".to_owned()))?,
            None => 1,
        };
        let view = Arc::new(crate::storage::NativeView::new(
            version,
            sql.clone(),
            schema,
        )?);
        catalog.register_view(entry, sql, true)?;
        state
            .expected_views
            .entry(name.clone())
            .or_insert_with(|| self.base_views.get(&name).cloned());
        state
            .expected
            .entry(name.clone())
            .or_insert_with(|| self.base_tables.get(&name).cloned());
        state.working_views.insert(name.clone(), Arc::clone(&view));
        state.view_updates.insert(name, Some(view));
        Ok(())
    }

    pub(in crate::engine) fn stage_view_drop(
        &self,
        catalog: &Catalog,
        name: &str,
        if_exists: bool,
    ) -> Result<bool> {
        let name = name.to_ascii_lowercase();
        let mut state = self.state.lock();
        if !state.working_views.contains_key(&name) {
            return if if_exists {
                Ok(false)
            } else {
                Err(Error::Catalog(format!("view '{name}' does not exist")))
            };
        }
        catalog.hide_persistent(&name);
        state.working_views.remove(&name);
        state
            .expected_views
            .entry(name.clone())
            .or_insert_with(|| self.base_views.get(&name).cloned());
        state.view_updates.insert(name.clone(), None);
        Ok(true)
    }

    pub(in crate::engine) fn plan_write(
        &self,
        engine: &Engine,
        name: &str,
        mode: NativeWriteMode,
        schema: arrow::datatypes::SchemaRef,
        source_bytes: u64,
    ) -> Result<NativeWritePlan> {
        let normalized = name.to_ascii_lowercase();
        let mut state = self.state.lock();
        let schema_name = crate::catalog_name::schema_of(&normalized);
        if !state.working_schemas.contains(schema_name) {
            return Err(Error::Catalog(format!(
                "schema '{schema_name}' does not exist"
            )));
        }
        state
            .expected_schemas
            .entry(schema_name.to_owned())
            .or_insert_with(|| self.base_schemas.contains(schema_name));
        if state.working_views.contains_key(&normalized) {
            return Err(Error::Catalog(format!(
                "cannot replace view '{normalized}' with a table"
            )));
        }
        let existing = state.working.get(&normalized).cloned();
        let schemas = state.working_schemas.clone();
        drop(state);
        let database = engine.inner.database.as_ref().ok_or_else(|| {
            Error::Unsupported("transactional native DML requires a persistent database".to_owned())
        })?;
        database.plan_transaction_write(
            &normalized,
            mode,
            self.snapshot_generation,
            existing,
            &schemas,
            schema,
            source_bytes,
        )
    }

    pub(in crate::engine) fn stage_prepared(
        &self,
        engine: &Engine,
        catalog: &Catalog,
        prepared: PreparedSnapshot,
    ) -> Result<()> {
        let _gate = engine.inner.native_commit.lock();
        if let Err(error) = engine.ensure_native_healthy() {
            return match prepared.abort() {
                Ok(()) => Err(error),
                Err(cleanup) => Err(Error::native_storage(
                    engine
                        .database_path()
                        .unwrap_or_else(|| std::path::Path::new("native database")),
                    format!("{error}; transaction staging cleanup failed: {cleanup}"),
                )),
            };
        }
        let database = engine.inner.database.as_ref().ok_or_else(|| {
            Error::Internal("persistent engine lost its native database".to_owned())
        })?;
        let published = match database.publish_transaction_write(prepared) {
            Ok(published) => published,
            Err(error) => {
                super::super::native_write::poison_on_native_write_error(engine, Some(&error));
                return Err(error);
            }
        };
        let name = published.name().to_owned();
        let snapshot = published.snapshot();
        let provider = NativeSegmentTable::new(
            database.path(),
            Arc::clone(&snapshot),
            &engine.inner.config,
            engine.inner.metadata_cache.clone(),
        );
        {
            let mut state = self.state.lock();
            if let Err(error) = catalog.register(TableEntry::new(name.clone(), Arc::new(provider)))
            {
                drop(state);
                return match database.abort_transaction_writes(vec![published]) {
                    Ok(()) => Err(error),
                    Err(cleanup) => {
                        engine.inner.native_poisoned.store(true, Ordering::Release);
                        Err(Error::native_storage(
                            database.path(),
                            format!(
                                "{error}; published transaction write cleanup failed: {cleanup}"
                            ),
                        ))
                    }
                };
            }
            state
                .expected
                .entry(name.clone())
                .or_insert_with(|| self.base_tables.get(&name).cloned());
            state
                .expected_views
                .entry(name.clone())
                .or_insert_with(|| self.base_views.get(&name).cloned());
            state.working.insert(name.clone(), snapshot);
            state.drops.remove(&name);
            state.writes.push(published);
        }
        Ok(())
    }

    pub(in crate::engine) fn stage_drop(
        &self,
        catalog: &Catalog,
        name: &str,
        if_exists: bool,
    ) -> Result<bool> {
        let name = name.to_ascii_lowercase();
        let mut state = self.state.lock();
        if !state.working.contains_key(&name) {
            return if if_exists {
                Ok(false)
            } else {
                Err(Error::Catalog(format!(
                    "native table '{name}' does not exist"
                )))
            };
        }
        catalog.hide_persistent(&name);
        state.working.remove(&name);
        state
            .expected
            .entry(name.clone())
            .or_insert_with(|| self.base_tables.get(&name).cloned());
        state.drops.insert(name.clone());
        Ok(true)
    }

    pub(in crate::engine) fn stage_rename(
        &self,
        engine: &Engine,
        catalog: &Catalog,
        old_name: &str,
        new_name: &str,
    ) -> Result<()> {
        let old_name = old_name.to_ascii_lowercase();
        let new_name = new_name.to_ascii_lowercase();
        if old_name == new_name {
            return Err(Error::InvalidArgument(
                "ALTER TABLE RENAME requires a different target name".to_owned(),
            ));
        }
        let mut state = self.state.lock();
        let target_schema = crate::catalog_name::schema_of(&new_name);
        if !state.working_schemas.contains(target_schema) {
            return Err(Error::Catalog(format!(
                "schema '{target_schema}' does not exist"
            )));
        }
        state
            .expected_schemas
            .entry(target_schema.to_owned())
            .or_insert_with(|| self.base_schemas.contains(target_schema));
        let snapshot = state
            .working
            .remove(&old_name)
            .ok_or_else(|| Error::Catalog(format!("native table '{old_name}' does not exist")))?;
        if state.working.contains_key(&new_name) {
            state.working.insert(old_name, snapshot);
            return Err(Error::Catalog(format!("table '{new_name}' already exists")));
        }
        if state.working_views.contains_key(&new_name) {
            state.working.insert(old_name, snapshot);
            return Err(Error::Catalog(format!(
                "table or view '{new_name}' already exists"
            )));
        }
        state
            .expected
            .entry(old_name.clone())
            .or_insert_with(|| self.base_tables.get(&old_name).cloned());
        state
            .expected
            .entry(new_name.clone())
            .or_insert_with(|| self.base_tables.get(&new_name).cloned());
        state
            .expected_views
            .entry(new_name.clone())
            .or_insert_with(|| self.base_views.get(&new_name).cloned());
        for write in &mut state.writes {
            if write.name() == old_name {
                write.rename_to(new_name.clone());
                if let Some(database) = engine.inner.database.as_ref() {
                    database.rename_transaction_write(write.transaction_id(), &new_name);
                }
            }
        }
        let mut changed_chain = false;
        for target in state.renames.values_mut() {
            if *target == old_name {
                *target = new_name.clone();
                changed_chain = true;
            }
        }
        if !changed_chain && self.base_tables.contains_key(&old_name) {
            state.renames.insert(old_name.clone(), new_name.clone());
        }
        state.renames.retain(|source, target| source != target);
        state.drops.remove(&new_name);
        state
            .working
            .insert(new_name.clone(), Arc::clone(&snapshot));

        let database = engine.inner.database.as_ref().ok_or_else(|| {
            Error::Internal("persistent engine lost its native database".to_owned())
        })?;
        let provider = NativeSegmentTable::new(
            database.path(),
            snapshot,
            &engine.inner.config,
            engine.inner.metadata_cache.clone(),
        );
        catalog.hide_persistent(&old_name);
        catalog.register(TableEntry::new(new_name, Arc::new(provider)))?;
        Ok(())
    }

    pub(in crate::engine) fn commit(&self, engine: &Engine) -> Result<Option<u64>> {
        let has_changes = {
            let state = self.state.lock();
            !state.writes.is_empty()
                || !state.drops.is_empty()
                || !state.renames.is_empty()
                || !state.view_updates.is_empty()
                || !state.schema_updates.is_empty()
        };
        if !has_changes {
            return Ok(None);
        }
        if let Err(error) = engine.ensure_native_healthy() {
            return self.rollback_after_preflight_error(engine, error);
        }
        let gate = engine.inner.native_commit.lock();
        if let Err(error) = engine.ensure_native_healthy() {
            drop(gate);
            return self.rollback_after_preflight_error(engine, error);
        }
        let Some(database) = engine.inner.database.as_ref() else {
            drop(gate);
            return self.rollback_after_preflight_error(
                engine,
                Error::Internal("persistent engine lost its native database".to_owned()),
            );
        };
        let (
            expected_schemas,
            schema_updates,
            expected,
            writes,
            drops,
            renames,
            expected_views,
            view_updates,
        ) = {
            let mut state = self.state.lock();
            (
                std::mem::take(&mut state.expected_schemas),
                std::mem::take(&mut state.schema_updates),
                std::mem::take(&mut state.expected),
                std::mem::take(&mut state.writes),
                std::mem::take(&mut state.drops),
                std::mem::take(&mut state.renames),
                std::mem::take(&mut state.expected_views),
                std::mem::take(&mut state.view_updates),
            )
        };
        let commit =
            match database.commit_transaction_writes(crate::storage::NativeTransactionChanges {
                snapshot_generation: self.snapshot_generation,
                expected_schemas,
                schema_updates,
                expected,
                writes,
                drops,
                renames,
                expected_views,
                view_updates,
            }) {
                Ok(commit) => commit,
                Err(Error::TransactionConflict { message, .. }) => {
                    return Err(Error::TransactionConflict {
                        transaction_id: self.transaction_id.to_string(),
                        message,
                    });
                }
                Err(error) => {
                    if super::super::native_write::native_commit_error_requires_reopen(&error) {
                        engine.inner.native_poisoned.store(true, Ordering::Release);
                    }
                    return Err(error);
                }
            };
        let generation = commit.generation();
        super::super::native_write::install_native_commit(engine, None, commit)?;
        Ok(Some(generation))
    }

    fn rollback_after_preflight_error<T>(&self, engine: &Engine, error: Error) -> Result<T> {
        match self.rollback(engine) {
            Ok(()) => Err(error),
            Err(cleanup) => Err(Error::native_storage(
                engine
                    .database_path()
                    .unwrap_or_else(|| std::path::Path::new("native database")),
                format!("{error}; transaction preflight cleanup failed: {cleanup}"),
            )),
        }
    }

    pub(in crate::engine) fn rollback(&self, engine: &Engine) -> Result<()> {
        let writes = {
            let mut state = self.state.lock();
            state.expected_schemas.clear();
            state.schema_updates.clear();
            state.expected.clear();
            state.drops.clear();
            state.renames.clear();
            state.expected_views.clear();
            state.view_updates.clear();
            std::mem::take(&mut state.writes)
        };
        if writes.is_empty() {
            return Ok(());
        }
        let _gate = engine.inner.native_commit.lock();
        let database = engine.inner.database.as_ref().ok_or_else(|| {
            Error::Internal("persistent engine lost its native database".to_owned())
        })?;
        database.abort_transaction_writes(writes).inspect_err(|_| {
            engine.inner.native_poisoned.store(true, Ordering::Release);
        })
    }
}

impl Drop for MutationLease {
    fn drop(&mut self) {
        self.workspace
            .mutation_active
            .store(false, Ordering::Release);
    }
}
