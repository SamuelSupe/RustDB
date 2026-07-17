use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
    sync::{Arc, Weak},
};

use parking_lot::Mutex;

use crate::{Error, Result};

mod backup;
mod commit;
mod database_open;
mod disk_budget;
mod io;
mod lock;
mod manifest;
mod marker;
mod schema;
mod segment;
mod table;
mod table_writer;
mod write;
mod write_plan;

pub(crate) use commit::NativeCommit;
pub(crate) use segment::predicate_sidecar::{
    ComparisonOp as NativePredicateComparisonOp, EncodedPredicateBlock as NativePredicateBlock,
    Predicate as NativePredicate, PredicateSidecarIndex as NativePredicateSidecarIndex,
};
pub(crate) use table::{NativePredicateSidecarBinding, TableSnapshot};
pub(crate) use table_writer::{NativeTableWriter, PreparedSnapshot};
pub(crate) use write::StagedSnapshot;
pub(crate) use write_plan::{NativeWriteMode, NativeWritePlan};

use database_open::{
    directory_contains_only, ensure_root, finish_initialization, load_tables, open_existing,
    prepare_root_before_lock, recover_initialization_temps,
};

#[cfg(test)]
mod retired_budget_tests;
#[cfg(test)]
mod startup_tests;
#[cfg(test)]
mod tests;

const FORMAT_NAME: &str = "rustdb-native";
const DATABASE_FORMAT_VERSION: u32 = 1;
const MARKER_FILE: &str = ".rustdb";
const INIT_FILE: &str = ".rustdb-init";

#[derive(Debug)]
pub(crate) struct NativeDatabase {
    root: PathBuf,
    database_id: String,
    state: Mutex<NativeState>,
    _lock: lock::DatabaseLock,
}

#[derive(Debug)]
struct NativeState {
    catalog: manifest::CatalogState,
    tables: BTreeMap<String, Arc<table::TableSnapshot>>,
    retired: Vec<RetiredSnapshot>,
}

#[derive(Debug)]
struct RetiredSnapshot {
    lease: Weak<()>,
    table_id: String,
    location: table::SnapshotLocation,
    source_bytes: u64,
}

impl NativeDatabase {
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
        manifest::recover_future_generations(&root, marker.database_id())?;
        write::recover_staging(&root, marker.database_id())?;
        let catalog = manifest::load(&root, marker.database_id())?;
        let tables = load_tables(&root, marker.database_id(), &catalog)?;
        table::recover_orphans(&root, marker.database_id(), &tables)?;
        Ok(Self {
            root,
            database_id: marker.database_id().to_owned(),
            state: Mutex::new(NativeState {
                catalog,
                tables,
                retired: Vec::new(),
            }),
            _lock: database_lock,
        })
    }

    pub(crate) fn path(&self) -> &Path {
        &self.root
    }

    pub(crate) fn catalog_generation(&self) -> u64 {
        self.state.lock().catalog.generation()
    }

    pub(crate) fn table_snapshots(&self) -> Vec<(String, Arc<table::TableSnapshot>)> {
        self.state
            .lock()
            .tables
            .iter()
            .map(|(name, snapshot)| (name.clone(), Arc::clone(snapshot)))
            .collect()
    }

    pub(crate) fn database_id(&self) -> &str {
        &self.database_id
    }

    pub(crate) fn plan_write(
        &self,
        name: &str,
        mode: NativeWriteMode,
        expected_generation: u64,
        source_schema: arrow::datatypes::SchemaRef,
        source_bytes: u64,
    ) -> Result<NativeWritePlan> {
        write_plan::plan(
            self,
            name,
            mode,
            expected_generation,
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

    pub(crate) fn backup_to(&self, destination: &Path) -> Result<()> {
        backup::create(self, destination)
    }

    pub(crate) fn commit_write(&self, prepared: PreparedSnapshot) -> Result<NativeCommit> {
        commit::commit(self, prepared)
    }

    pub(crate) fn drain_retired(&self) -> Result<()> {
        let mut state = self.state.lock();
        let mut failure = None;
        let mut index = 0;
        while index < state.retired.len() {
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
                }
                Ok(table::SnapshotRemoval::RemovedButUnsynced(error)) => {
                    state.retired.remove(index);
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
            None => Ok(()),
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
