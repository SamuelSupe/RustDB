use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::{Path, PathBuf},
    sync::{Arc, Weak},
};

use parking_lot::Mutex;

use crate::{Error, Result};

mod active_wal;
mod backup;
mod commit;
mod database_open;
mod delete_writer;
mod disk_budget;
mod io;
mod lock;
mod manifest;
mod marker;
mod migration;
mod quota;
mod rebase;
mod schema;
mod segment;
mod table;
mod table_writer;
mod transaction_commit;
mod view;
mod wal;
mod write;
mod write_plan;

#[cfg(test)]
mod quota_publication_test_hook {
    use std::{
        cell::RefCell,
        sync::mpsc::{Receiver, SyncSender},
    };

    thread_local! {
        static HOOK: RefCell<Option<(SyncSender<()>, Receiver<()>)>> = const { RefCell::new(None) };
    }

    pub(super) fn arm(entered: SyncSender<()>, release: Receiver<()>) {
        HOOK.with(|hook| *hook.borrow_mut() = Some((entered, release)));
    }

    pub(super) fn hit() {
        HOOK.with(|hook| {
            let Some((entered, release)) = hook.borrow_mut().take() else {
                return;
            };
            entered.send(()).expect("quota test observer is alive");
            release.recv().expect("quota test releases publication");
        });
    }
}

pub(crate) use commit::NativeCommit;
#[cfg(test)]
pub(crate) use commit::test_failpoint::{
    Boundary as NativeCommitTestBoundary, arm as arm_native_commit_test_failpoint,
};
#[cfg(test)]
pub(crate) fn arm_native_wal_ambiguous_reconciliation() {
    wal::test_failpoint::arm_ambiguous_reconciliation();
}
pub(crate) use delete_writer::NativeDeleteWriter;
pub(crate) use migration::NativeMigration;
pub(crate) use segment::encoding::{
    decode as decode_native_segment_batch, physical_schema as native_segment_physical_schema,
    required as native_segment_encoding_required,
};
pub(crate) use segment::predicate_sidecar::{
    ComparisonOp as NativePredicateComparisonOp, EncodedPredicateBlock as NativePredicateBlock,
    Predicate as NativePredicate, PredicateSidecarIndex as NativePredicateSidecarIndex,
};
pub(crate) use table::{
    DeleteVector, NativeDeleteSegment, NativePredicateSidecarBinding, TableSnapshot,
};
pub(crate) use table_writer::{NativeTableWriter, PreparedSnapshot};
pub(crate) use transaction_commit::NativePublishedSnapshot;
pub(crate) use view::NativeView;
pub(crate) use write::StagedSnapshot;
pub(crate) use write_plan::{NativeWriteMode, NativeWritePlan};

use database_open::{
    directory_contains_only, ensure_root, finish_initialization, load_tables, load_views,
    open_existing, prepare_root_before_lock, recover_initialization_temps,
};

#[cfg(test)]
mod durability_tests;
#[cfg(test)]
mod retired_budget_tests;
#[cfg(test)]
mod startup_tests;
#[cfg(test)]
mod tests;

const FORMAT_NAME: &str = "rustdb-native";
const DATABASE_FORMAT_VERSION: u32 = 2;
const LEGACY_DATABASE_FORMAT_VERSION: u32 = 1;
const MARKER_FILE: &str = ".rustdb";
const INIT_FILE: &str = ".rustdb-init";

#[derive(Debug)]
pub(crate) struct NativeDatabase {
    root: PathBuf,
    database_id: String,
    format_version: u32,
    wal: Option<Arc<wal::Wal>>,
    quota: crate::NativeStorageConfig,
    active_quota_writes: Mutex<BTreeMap<String, quota::ActiveWrite>>,
    quota_publication_gate: Mutex<()>,
    state: Mutex<NativeState>,
    _lock: lock::DatabaseLock,
}

#[derive(Debug)]
struct NativeState {
    catalog: manifest::CatalogState,
    tables: BTreeMap<String, Arc<table::TableSnapshot>>,
    views: BTreeMap<String, Arc<view::NativeView>>,
    retired: Vec<RetiredSnapshot>,
}

pub(crate) type NativeTransactionSnapshot = (
    u64,
    BTreeSet<String>,
    BTreeMap<String, Arc<table::TableSnapshot>>,
    BTreeMap<String, Arc<view::NativeView>>,
);

pub(crate) struct NativeTransactionChanges {
    pub(crate) snapshot_generation: u64,
    pub(crate) expected_schemas: BTreeMap<String, bool>,
    pub(crate) schema_updates: BTreeMap<String, bool>,
    pub(crate) expected: BTreeMap<String, Option<Arc<table::TableSnapshot>>>,
    pub(crate) writes: Vec<NativePublishedSnapshot>,
    pub(crate) drops: BTreeSet<String>,
    pub(crate) renames: BTreeMap<String, String>,
    pub(crate) expected_views: BTreeMap<String, Option<Arc<view::NativeView>>>,
    pub(crate) view_updates: BTreeMap<String, Option<Arc<view::NativeView>>>,
}

#[derive(Debug)]
struct RetiredSnapshot {
    name: String,
    lease: Weak<()>,
    table_id: String,
    location: table::SnapshotLocation,
    source_bytes: u64,
}

#[derive(Clone)]
pub(crate) struct NativeCatalogObjectInfo {
    pub(crate) name: String,
    pub(crate) object_type: &'static str,
    pub(crate) schema: arrow::datatypes::SchemaRef,
}

#[derive(Clone, Debug)]
pub(crate) struct NativeTableInfo {
    pub(crate) name: String,
    pub(crate) table_id: String,
    pub(crate) version: u64,
    pub(crate) snapshot_id: String,
    pub(crate) rows: u64,
    pub(crate) physical_rows: u64,
    pub(crate) deleted_rows: u64,
    pub(crate) segments: u64,
    pub(crate) source_bytes: u64,
    pub(crate) storage_bytes: u64,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct NativeWalInfo {
    pub(crate) next_lsn: u64,
    pub(crate) tracked_transactions: u64,
    pub(crate) active_writes: u64,
}

impl NativeDatabase {
    pub(crate) fn migrate(path: impl AsRef<Path>) -> Result<NativeMigration> {
        migration::migrate(path.as_ref())
    }

    pub(crate) fn open(path: impl AsRef<Path>) -> Result<Self> {
        let requested = path.as_ref();
        if requested.as_os_str().is_empty() {
            return Err(Error::InvalidArgument(
                "database path must not be empty".to_owned(),
            ));
        }

        ensure_root(requested)?;
        let root = fs::canonicalize(requested)
            .map_err(|error| Error::io(Some(requested.to_path_buf()), error))?;
        if root.to_str().is_none() {
            return Err(Error::native_storage(
                &root,
                "native database path must be valid UTF-8",
            ));
        }
        prepare_root_before_lock(&root)?;
        let database_lock = lock::DatabaseLock::acquire(&root)?;
        let marker_path = root.join(MARKER_FILE);
        let init_path = root.join(INIT_FILE);
        recover_initialization_temps(&root, database_lock.path(), &marker_path, &init_path)?;

        if marker_path.exists() {
            open_existing(&root, &marker_path, &init_path)?;
        } else if init_path.exists() {
            let marker = marker::read(&init_path)?;
            finish_initialization(&root, &marker)?;
        } else if directory_contains_only(&root, database_lock.path())? {
            let marker = marker::DatabaseMarker::new();
            marker::write_new(&init_path, &marker)?;
            finish_initialization(&root, &marker)?;
        } else {
            return Err(Error::native_storage(
                &root,
                "directory is not empty and has no RustDB marker",
            ));
        }

        let marker = marker::read(&marker_path)?;
        let wal = if marker.uses_wal() {
            let wal = Arc::new(wal::Wal::open(&root, marker.database_id())?);
            wal.recover_catalog(&root, marker.database_id())?;
            Some(wal)
        } else {
            debug_assert!(marker.is_legacy());
            None
        };
        manifest::recover_future_generations(&root, marker.database_id())?;
        write::recover_staging(&root, marker.database_id())?;
        let catalog = manifest::load(&root, marker.database_id())?;
        let tables = load_tables(&root, marker.database_id(), &catalog)?;
        let views = load_views(&root, &catalog)?;
        table::recover_orphans(&root, marker.database_id(), &tables)?;
        if let Some(wal) = &wal {
            wal.abort_recovered_transactions()?;
        }
        Ok(Self {
            root,
            database_id: marker.database_id().to_owned(),
            format_version: marker.version(),
            wal,
            quota: crate::NativeStorageConfig::default(),
            active_quota_writes: Mutex::new(BTreeMap::new()),
            quota_publication_gate: Mutex::new(()),
            state: Mutex::new(NativeState {
                catalog,
                tables,
                views,
                retired: Vec::new(),
            }),
            _lock: database_lock,
        })
    }

    pub(crate) fn open_with_storage(
        path: impl AsRef<Path>,
        quota: crate::NativeStorageConfig,
    ) -> Result<Self> {
        let mut database = Self::open(path)?;
        database.quota = quota;
        Ok(database)
    }

    pub(crate) fn path(&self) -> &Path {
        &self.root
    }

    pub(crate) fn catalog_generation(&self) -> u64 {
        self.state.lock().catalog.generation()
    }

    pub(crate) fn schema_names(&self) -> Vec<String> {
        self.state
            .lock()
            .catalog
            .schemas()
            .iter()
            .cloned()
            .collect()
    }

    pub(crate) fn table_snapshots(&self) -> Vec<(String, Arc<table::TableSnapshot>)> {
        self.state
            .lock()
            .tables
            .iter()
            .map(|(name, snapshot)| (name.clone(), Arc::clone(snapshot)))
            .collect()
    }

    pub(crate) fn view_definitions(&self) -> Vec<(String, Arc<view::NativeView>)> {
        self.state
            .lock()
            .views
            .iter()
            .map(|(name, view)| (name.clone(), Arc::clone(view)))
            .collect()
    }

    pub(crate) fn catalog_object_infos(&self) -> Vec<NativeCatalogObjectInfo> {
        let state = self.state.lock();
        state
            .tables
            .iter()
            .map(|(name, snapshot)| NativeCatalogObjectInfo {
                name: name.clone(),
                object_type: "BASE TABLE",
                schema: snapshot.schema(),
            })
            .chain(
                state
                    .views
                    .iter()
                    .map(|(name, view)| NativeCatalogObjectInfo {
                        name: name.clone(),
                        object_type: "VIEW",
                        schema: view.schema(),
                    }),
            )
            .collect()
    }

    pub(crate) fn table_infos(&self) -> Vec<NativeTableInfo> {
        self.state
            .lock()
            .tables
            .iter()
            .map(|(name, snapshot)| NativeTableInfo {
                name: name.clone(),
                table_id: snapshot.table_id().to_owned(),
                version: snapshot.version(),
                snapshot_id: snapshot.snapshot_id().to_owned(),
                rows: snapshot.row_count(),
                physical_rows: snapshot.physical_row_count(),
                deleted_rows: snapshot.deleted_row_count(),
                segments: u64::try_from(snapshot.segment_count()).unwrap_or(u64::MAX),
                source_bytes: snapshot.source_bytes(),
                storage_bytes: snapshot.storage_bytes(),
            })
            .collect()
    }

    pub(crate) fn wal_info(&self) -> Result<NativeWalInfo> {
        let (next_lsn, tracked, active) = self.wal()?.stats();
        Ok(NativeWalInfo {
            next_lsn,
            tracked_transactions: u64::try_from(tracked).unwrap_or(u64::MAX),
            active_writes: u64::try_from(active).unwrap_or(u64::MAX),
        })
    }

    pub(crate) fn transaction_snapshot(&self) -> NativeTransactionSnapshot {
        let state = self.state.lock();
        (
            state.catalog.generation(),
            state.catalog.schemas().clone(),
            state.tables.clone(),
            state.views.clone(),
        )
    }

    pub(crate) fn table_snapshot(&self, name: &str) -> Result<Arc<table::TableSnapshot>> {
        self.state
            .lock()
            .tables
            .get(&name.to_ascii_lowercase())
            .cloned()
            .ok_or_else(|| Error::Catalog(format!("native table '{name}' does not exist")))
    }

    pub(crate) fn schema_exists(&self, name: &str) -> bool {
        self.state
            .lock()
            .catalog
            .schemas()
            .contains(&name.to_ascii_lowercase())
    }

    pub(crate) fn database_id(&self) -> &str {
        &self.database_id
    }

    pub(in crate::storage::native) fn wal(&self) -> Result<Arc<wal::Wal>> {
        if self.format_version == LEGACY_DATABASE_FORMAT_VERSION {
            return Err(Error::Unsupported(
                "native database format v1 is read-only; run 'rustdb migrate' before writing"
                    .to_owned(),
            ));
        }
        self.wal.clone().ok_or_else(|| {
            Error::Unsupported("native database WAL is unavailable; reopen the database".to_owned())
        })
    }

    pub(crate) fn plan_write(
        &self,
        name: &str,
        mode: NativeWriteMode,
        expected_generation: u64,
        source_schema: arrow::datatypes::SchemaRef,
        source_bytes: u64,
    ) -> Result<NativeWritePlan> {
        self.wal()?;
        let schema = crate::catalog_name::schema_of(name);
        if !self.schema_exists(schema) {
            return Err(Error::Catalog(format!("schema '{schema}' does not exist")));
        }
        write_plan::plan(
            self,
            name,
            mode,
            expected_generation,
            source_schema,
            source_bytes,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn plan_transaction_write(
        &self,
        name: &str,
        mode: NativeWriteMode,
        snapshot_generation: u64,
        existing: Option<Arc<table::TableSnapshot>>,
        schemas: &BTreeSet<String>,
        source_schema: arrow::datatypes::SchemaRef,
        source_bytes: u64,
    ) -> Result<NativeWritePlan> {
        self.wal()?;
        write_plan::plan_from_snapshot(
            self,
            name,
            mode,
            snapshot_generation,
            existing,
            schemas,
            source_schema,
            source_bytes,
        )
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn start_write(&self, plan: NativeWritePlan) -> Result<NativeTableWriter> {
        NativeTableWriter::begin(self, plan)
    }

    pub(crate) fn start_write_with_memory(
        &self,
        plan: NativeWritePlan,
        _memory: crate::runtime::MemoryPool,
    ) -> Result<NativeTableWriter> {
        NativeTableWriter::begin(self, plan)
    }

    pub(crate) fn plan_delete(
        &self,
        name: &str,
        expected_generation: u64,
    ) -> Result<(NativeWritePlan, Arc<table::TableSnapshot>)> {
        let snapshot = self.table_snapshot(name)?;
        let plan = self.plan_write(
            name,
            NativeWriteMode::Delete,
            expected_generation,
            snapshot.schema(),
            0,
        )?;
        Ok((plan, snapshot))
    }

    pub(crate) fn plan_update(
        &self,
        name: &str,
        expected_generation: u64,
    ) -> Result<(NativeWritePlan, Arc<table::TableSnapshot>)> {
        let snapshot = self.table_snapshot(name)?;
        let plan = self.plan_write(
            name,
            NativeWriteMode::Update,
            expected_generation,
            snapshot.schema(),
            0,
        )?;
        Ok((plan, snapshot))
    }

    pub(crate) fn start_delete(&self, plan: NativeWritePlan) -> Result<NativeDeleteWriter> {
        NativeDeleteWriter::begin(self, plan)
    }

    pub(crate) fn backup_to(&self, destination: &Path) -> Result<()> {
        backup::create(self, destination)
    }

    pub(crate) fn checkpoint(&self) -> Result<u64> {
        let generation = self.catalog_generation();
        io::sync_dir(&self.root.join("catalog/generations"))?;
        io::sync_dir(&self.root.join("catalog"))?;
        io::sync_dir(&self.root.join("tables"))?;
        io::sync_dir(&self.root)?;
        self.wal()?.checkpoint(generation)
    }

    pub(crate) fn commit_write(&self, prepared: PreparedSnapshot) -> Result<NativeCommit> {
        let _publication = self.quota_publication_gate.lock();
        if let Err(error) = quota::check_prepared(self, &prepared) {
            return match prepared.abort() {
                Ok(()) => Err(error),
                Err(cleanup) => Err(Error::native_storage(
                    self.path(),
                    format!("{error}; quota rejection cleanup failed: {cleanup}"),
                )),
            };
        }
        #[cfg(test)]
        quota_publication_test_hook::hit();
        commit::commit(self, prepared)
    }

    pub(crate) fn publish_transaction_write(
        &self,
        prepared: PreparedSnapshot,
    ) -> Result<NativePublishedSnapshot> {
        let _publication = self.quota_publication_gate.lock();
        self.publish_transaction_write_under_publication_gate(prepared)
    }

    fn publish_transaction_write_under_publication_gate(
        &self,
        prepared: PreparedSnapshot,
    ) -> Result<NativePublishedSnapshot> {
        if let Err(error) = quota::check_prepared(self, &prepared) {
            return match prepared.abort() {
                Ok(()) => Err(error),
                Err(cleanup) => Err(Error::native_storage(
                    self.path(),
                    format!("{error}; quota rejection cleanup failed: {cleanup}"),
                )),
            };
        }
        #[cfg(test)]
        quota_publication_test_hook::hit();
        let published = transaction_commit::publish(self, prepared)?;
        quota::register_published(self, &published);
        Ok(published)
    }

    pub(crate) fn commit_transaction_writes(
        &self,
        changes: NativeTransactionChanges,
    ) -> Result<NativeCommit> {
        let _publication = self.quota_publication_gate.lock();
        if let Err(error) = quota::check_transaction(self, &changes) {
            let result = match transaction_commit::abort(self, changes.writes) {
                Ok(()) => Err(error),
                Err(cleanup) => Err(Error::native_storage(
                    self.path(),
                    format!("{error}; quota rejection cleanup failed: {cleanup}"),
                )),
            };
            quota::prune_published(self);
            return result;
        }
        let result = transaction_commit::commit(self, changes);
        quota::prune_published(self);
        result
    }

    pub(crate) fn commit_table_drop(
        &self,
        expected_generation: u64,
        name: &str,
        if_exists: bool,
    ) -> Result<Option<NativeCommit>> {
        let normalized = name.to_ascii_lowercase();
        let expected = self.state.lock().tables.get(&normalized).cloned();
        let Some(snapshot) = expected else {
            return if if_exists {
                Ok(None)
            } else {
                Err(Error::Catalog(format!(
                    "native table '{normalized}' does not exist"
                )))
            };
        };
        let mut expected_tables = BTreeMap::new();
        expected_tables.insert(normalized.clone(), Some(snapshot));
        self.commit_transaction_writes(NativeTransactionChanges {
            snapshot_generation: expected_generation,
            expected_schemas: BTreeMap::new(),
            schema_updates: BTreeMap::new(),
            expected: expected_tables,
            writes: Vec::new(),
            drops: std::iter::once(normalized).collect(),
            renames: BTreeMap::new(),
            expected_views: BTreeMap::new(),
            view_updates: BTreeMap::new(),
        })
        .map(Some)
    }

    pub(crate) fn commit_table_rename(
        &self,
        expected_generation: u64,
        old_name: &str,
        new_name: &str,
    ) -> Result<NativeCommit> {
        let old_name = old_name.to_ascii_lowercase();
        let new_name = new_name.to_ascii_lowercase();
        if old_name == new_name {
            return Err(Error::InvalidArgument(
                "ALTER TABLE RENAME requires a different target name".to_owned(),
            ));
        }
        let state = self.state.lock();
        let snapshot =
            state.tables.get(&old_name).cloned().ok_or_else(|| {
                Error::Catalog(format!("native table '{old_name}' does not exist"))
            })?;
        let target_schema = crate::catalog_name::schema_of(&new_name);
        if !state.catalog.schemas().contains(target_schema) {
            return Err(Error::Catalog(format!(
                "schema '{target_schema}' does not exist"
            )));
        }
        if state.tables.contains_key(&new_name) || state.views.contains_key(&new_name) {
            return Err(Error::Catalog(format!(
                "table or view '{new_name}' already exists"
            )));
        }
        drop(state);
        let expected =
            BTreeMap::from([(old_name.clone(), Some(snapshot)), (new_name.clone(), None)]);
        self.commit_transaction_writes(NativeTransactionChanges {
            snapshot_generation: expected_generation,
            expected_schemas: BTreeMap::new(),
            schema_updates: BTreeMap::new(),
            expected,
            writes: Vec::new(),
            drops: BTreeSet::new(),
            renames: BTreeMap::from([(old_name, new_name)]),
            expected_views: BTreeMap::new(),
            view_updates: BTreeMap::new(),
        })
    }

    pub(crate) fn commit_view_create(
        &self,
        expected_generation: u64,
        name: &str,
        sql: String,
        schema: arrow::datatypes::SchemaRef,
        replace: bool,
    ) -> Result<NativeCommit> {
        let name = name.to_ascii_lowercase();
        let state = self.state.lock();
        let schema_name = crate::catalog_name::schema_of(&name);
        if !state.catalog.schemas().contains(schema_name) {
            return Err(Error::Catalog(format!(
                "schema '{schema_name}' does not exist"
            )));
        }
        if state.tables.contains_key(&name) {
            return Err(Error::Catalog(format!(
                "cannot replace table '{name}' with a view"
            )));
        }
        let previous = state.views.get(&name).cloned();
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
        drop(state);
        let view = Arc::new(view::NativeView::new(version, sql, schema)?);
        self.commit_transaction_writes(NativeTransactionChanges {
            snapshot_generation: expected_generation,
            expected_schemas: BTreeMap::new(),
            schema_updates: BTreeMap::new(),
            expected: BTreeMap::new(),
            writes: Vec::new(),
            drops: BTreeSet::new(),
            renames: BTreeMap::new(),
            expected_views: BTreeMap::from([(name.clone(), previous)]),
            view_updates: BTreeMap::from([(name, Some(view))]),
        })
    }

    pub(crate) fn commit_view_drop(
        &self,
        expected_generation: u64,
        name: &str,
        if_exists: bool,
    ) -> Result<Option<NativeCommit>> {
        let name = name.to_ascii_lowercase();
        let previous = self.state.lock().views.get(&name).cloned();
        let Some(previous) = previous else {
            return if if_exists {
                Ok(None)
            } else {
                Err(Error::Catalog(format!("view '{name}' does not exist")))
            };
        };
        self.commit_transaction_writes(NativeTransactionChanges {
            snapshot_generation: expected_generation,
            expected_schemas: BTreeMap::new(),
            schema_updates: BTreeMap::new(),
            expected: BTreeMap::new(),
            writes: Vec::new(),
            drops: BTreeSet::new(),
            renames: BTreeMap::new(),
            expected_views: BTreeMap::from([(name.clone(), Some(previous))]),
            view_updates: BTreeMap::from([(name, None)]),
        })
        .map(Some)
    }

    pub(crate) fn commit_schema_create(
        &self,
        expected_generation: u64,
        name: &str,
        if_not_exists: bool,
    ) -> Result<Option<NativeCommit>> {
        let name = name.to_ascii_lowercase();
        let exists = self.schema_exists(&name);
        if exists {
            return if if_not_exists {
                Ok(None)
            } else {
                Err(Error::Catalog(format!("schema '{name}' already exists")))
            };
        }
        self.commit_transaction_writes(NativeTransactionChanges {
            snapshot_generation: expected_generation,
            expected_schemas: BTreeMap::from([(name.clone(), false)]),
            schema_updates: BTreeMap::from([(name, true)]),
            expected: BTreeMap::new(),
            writes: Vec::new(),
            drops: BTreeSet::new(),
            renames: BTreeMap::new(),
            expected_views: BTreeMap::new(),
            view_updates: BTreeMap::new(),
        })
        .map(Some)
    }

    pub(crate) fn commit_schema_drop(
        &self,
        expected_generation: u64,
        name: &str,
        if_exists: bool,
    ) -> Result<Option<NativeCommit>> {
        let name = name.to_ascii_lowercase();
        if name == crate::catalog_name::DEFAULT_SCHEMA {
            return Err(Error::Unsupported(
                "the default 'main' schema cannot be dropped".to_owned(),
            ));
        }
        let state = self.state.lock();
        if !state.catalog.schemas().contains(&name) {
            return if if_exists {
                Ok(None)
            } else {
                Err(Error::Catalog(format!("schema '{name}' does not exist")))
            };
        }
        if state
            .tables
            .keys()
            .chain(state.views.keys())
            .any(|object| crate::catalog_name::schema_of(object) == name)
        {
            return Err(Error::Catalog(format!("schema '{name}' is not empty")));
        }
        drop(state);
        self.commit_transaction_writes(NativeTransactionChanges {
            snapshot_generation: expected_generation,
            expected_schemas: BTreeMap::from([(name.clone(), true)]),
            schema_updates: BTreeMap::from([(name, false)]),
            expected: BTreeMap::new(),
            writes: Vec::new(),
            drops: BTreeSet::new(),
            renames: BTreeMap::new(),
            expected_views: BTreeMap::new(),
            view_updates: BTreeMap::new(),
        })
        .map(Some)
    }

    pub(crate) fn abort_transaction_writes(
        &self,
        writes: Vec<NativePublishedSnapshot>,
    ) -> Result<()> {
        let result = transaction_commit::abort(self, writes);
        quota::prune_published(self);
        result
    }

    pub(crate) fn rename_transaction_write(&self, transaction_id: &str, name: &str) {
        if let Some(write) = self.active_quota_writes.lock().get_mut(transaction_id) {
            write.name = name.to_owned();
        }
    }

    pub(crate) fn drain_retired(&self) -> Result<()> {
        self.vacuum(None).map(|_| ())
    }

    pub(crate) fn vacuum(&self, table_name: Option<&str>) -> Result<u64> {
        let mut state = self.state.lock();
        let table_name = table_name.map(str::to_ascii_lowercase);
        if let Some(name) = table_name.as_deref()
            && !state.tables.contains_key(name)
            && !state.retired.iter().any(|retired| retired.name == name)
        {
            return Err(Error::Catalog(format!(
                "native table '{name}' does not exist"
            )));
        }
        let mut failure = None;
        let mut removed = 0_u64;
        let mut index = 0;
        while index < state.retired.len() {
            if table_name
                .as_ref()
                .is_some_and(|name| state.retired[index].name != *name)
            {
                index += 1;
                continue;
            }
            if state.retired[index].lease.upgrade().is_some() {
                index += 1;
                continue;
            }
            let retired = &state.retired[index];
            match table::remove_snapshot(
                &self.root,
                &self.database_id,
                &retired.table_id,
                &retired.location,
            ) {
                Ok(table::SnapshotRemoval::Removed) => {
                    state.retired.remove(index);
                    removed = removed.saturating_add(1);
                }
                Ok(table::SnapshotRemoval::RemovedButUnsynced(error)) => {
                    state.retired.remove(index);
                    removed = removed.saturating_add(1);
                    failure = Some(error);
                }
                Err(error) => {
                    failure = Some(error);
                    index += 1;
                }
            }
        }
        match failure {
            Some(error) => Err(error),
            None => Ok(removed),
        }
    }
}

fn retired_storage_bytes(root: &Path, state: &NativeState, table_id: &str) -> Result<u64> {
    disk_budget::directories_storage_bytes(
        state
            .retired
            .iter()
            .filter(|retired| retired.table_id == table_id)
            .map(|retired| table::location_directory(root, table_id, &retired.location)),
    )
}

fn retired_source_bytes(state: &NativeState, table_id: &str) -> Result<u64> {
    state
        .retired
        .iter()
        .filter(|retired| retired.table_id == table_id)
        .try_fold(0_u64, |total, retired| {
            total.checked_add(retired.source_bytes).ok_or_else(|| {
                Error::ResourceExhausted("retired native source byte count overflow".to_owned())
            })
        })
}
