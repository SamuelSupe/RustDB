use std::{collections::BTreeSet, sync::Arc};

use crate::{Error, Result};

use super::{
    NativeDatabase, PreparedSnapshot, manifest,
    table::{self, SnapshotOperation},
};

pub(crate) struct NativeCommit {
    generation: u64,
    transaction_id: String,
}

impl NativeCommit {
    pub(crate) fn generation(&self) -> u64 {
        self.generation
    }

    pub(crate) fn transaction_id(&self) -> &str {
        &self.transaction_id
    }
}

pub(super) fn commit(
    database: &NativeDatabase,
    prepared: PreparedSnapshot,
) -> Result<NativeCommit> {
    let PreparedSnapshot {
        staging,
        snapshot,
        name,
        expected_generation,
    } = prepared;
    let mut state = database.state.lock();
    if state.catalog.generation() != expected_generation {
        let found = state.catalog.generation();
        return abort_with(
            staging,
            Error::Catalog(format!(
                "catalog generation changed: expected {expected_generation}, found {found}"
            )),
        );
    }
    let current = state.catalog.tables().get(&name);
    let parent_matches = match (snapshot.operation(), snapshot.parent(), current) {
        (SnapshotOperation::Import, None, None) => true,
        (SnapshotOperation::Append | SnapshotOperation::Replace, Some(parent), Some(current)) => {
            parent == current
        }
        _ => false,
    };
    if !parent_matches {
        return abort_with(
            staging,
            Error::Catalog(format!(
                "native table '{name}' changed while the write was running"
            )),
        );
    }

    let transaction_id = staging.transaction_id().to_owned();
    let reference = snapshot.table_reference();
    let destination = snapshot.final_directory(database.path());
    staging.publish(&destination)?;
    let mut loaded = table::load(database.path(), database.database_id(), &reference)?;
    let mut tables = state.catalog.tables().clone();
    tables.insert(name.clone(), reference);
    let committed = manifest::commit(
        database.path(),
        database.database_id(),
        expected_generation,
        tables,
    )?;
    let generation = committed.generation();
    if let Err(error) =
        manifest::prune_old_generations(database.path(), database.database_id(), generation)
    {
        return Err(post_commit_failure(
            database,
            &transaction_id,
            generation,
            "old catalog generation cleanup failed",
            error,
        ));
    }
    if let Err(error) = table::prune_inherited_manifests(database.path(), &loaded) {
        return Err(post_commit_failure(
            database,
            &transaction_id,
            generation,
            "inherited snapshot manifest cleanup failed",
            error,
        ));
    }
    let storage_bytes = match super::disk_budget::snapshot_storage_bytes(database.path(), &loaded) {
        Ok(bytes) => bytes,
        Err(error) => {
            return Err(post_commit_failure(
                database,
                &transaction_id,
                generation,
                "committed snapshot accounting failed",
                error,
            ));
        }
    };
    loaded.set_storage_bytes(storage_bytes);
    if let Some(old) = state.tables.get(&name) {
        loaded.inherit_location_leases(old);
    }
    let loaded = Arc::new(loaded);
    state.catalog = committed;
    let reachable = loaded
        .reachable_locations()
        .into_iter()
        .collect::<BTreeSet<_>>();
    let old = state.tables.insert(name, Arc::clone(&loaded));
    if let Some(old) = old {
        let locations = old
            .reachable_locations()
            .into_iter()
            .filter(|location| !reachable.contains(location))
            .collect::<Vec<_>>();
        let mut retired_source_bytes = old.source_bytes();
        for location in locations {
            state.retired.push(super::RetiredSnapshot {
                lease: old.location_lease(&location),
                table_id: old.table_id().to_owned(),
                location,
                source_bytes: std::mem::take(&mut retired_source_bytes),
            });
        }
    }
    Ok(NativeCommit {
        generation,
        transaction_id,
    })
}

fn post_commit_failure(
    database: &NativeDatabase,
    transaction_id: &str,
    generation: u64,
    action: &str,
    error: Error,
) -> Error {
    Error::native_commit_post_commit_failure(
        database.path(),
        transaction_id,
        generation,
        format!("{action}; reopen the engine: {error}"),
    )
}

fn abort_with<T>(staging: super::StagedSnapshot, error: Error) -> Result<T> {
    let path = staging.path().to_owned();
    match staging.abort() {
        Ok(()) => Err(error),
        Err(cleanup) => Err(Error::native_storage(
            path,
            format!("{error}; staging cleanup failed: {cleanup}"),
        )),
    }
}
