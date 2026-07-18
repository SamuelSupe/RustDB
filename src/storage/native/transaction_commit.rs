use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};
use uuid::Uuid;

use crate::{Error, Result};

use super::{
    NativeCommit, NativeDatabase, NativeTransactionChanges, PreparedSnapshot, manifest,
    table::{self, SnapshotLocation, TableSnapshot},
};

pub(crate) struct NativePublishedSnapshot {
    wal: Arc<super::wal::Wal>,
    wal_owner: super::active_wal::ActiveWal,
    snapshot: Arc<TableSnapshot>,
    name: String,
    transaction_id: String,
}

impl NativePublishedSnapshot {
    pub(crate) fn name(&self) -> &str {
        &self.name
    }

    pub(crate) fn snapshot(&self) -> Arc<TableSnapshot> {
        Arc::clone(&self.snapshot)
    }

    pub(crate) fn transaction_id(&self) -> &str {
        &self.transaction_id
    }

    pub(crate) fn rename_to(&mut self, name: String) {
        self.name = name;
    }

    fn abort_wal(&mut self) -> Result<()> {
        self.wal_owner.abort()
    }

    fn release_wal_to_recovery(&mut self) {
        self.wal_owner.release_to_recovery();
    }
}

pub(super) fn publish(
    database: &NativeDatabase,
    prepared: PreparedSnapshot,
) -> Result<NativePublishedSnapshot> {
    let PreparedSnapshot {
        wal,
        mut wal_owner,
        staging,
        snapshot,
        name,
    } = prepared;
    let transaction_id = staging.transaction_id().to_owned();
    let reference = snapshot.table_reference();
    let destination = snapshot.final_directory(database.path());
    staging.publish(&destination)?;
    #[cfg(test)]
    super::commit::test_failpoint::hit(
        super::commit::test_failpoint::Boundary::SnapshotPublished,
        database.path(),
        &transaction_id,
        None,
    )?;
    let loaded = table::load(database.path(), database.database_id(), &reference);
    let mut loaded = match loaded {
        Ok(loaded) => loaded,
        Err(error) => {
            return cleanup_published(database, &snapshot, &mut wal_owner, error);
        }
    };
    let storage_bytes = match super::disk_budget::snapshot_storage_bytes(database.path(), &loaded) {
        Ok(bytes) => bytes,
        Err(error) => {
            return cleanup_published(database, &snapshot, &mut wal_owner, error);
        }
    };
    loaded.set_storage_bytes(storage_bytes);
    Ok(NativePublishedSnapshot {
        wal,
        wal_owner,
        snapshot: Arc::new(loaded),
        name,
        transaction_id,
    })
}

pub(super) fn abort(
    database: &NativeDatabase,
    mut writes: Vec<NativePublishedSnapshot>,
) -> Result<()> {
    let mut failure = None;
    while let Some(write) = writes.pop() {
        if let Err(error) = remove_and_abort(database, write) {
            append_failure(&mut failure, error);
        }
    }
    match failure {
        Some(error) => Err(error),
        None => Ok(()),
    }
}

pub(super) fn commit(
    database: &NativeDatabase,
    changes: NativeTransactionChanges,
) -> Result<NativeCommit> {
    let NativeTransactionChanges {
        snapshot_generation,
        expected_schemas,
        schema_updates,
        mut expected,
        mut writes,
        drops,
        renames,
        expected_views,
        view_updates,
    } = changes;
    if writes.is_empty()
        && drops.is_empty()
        && renames.is_empty()
        && view_updates.is_empty()
        && schema_updates.is_empty()
    {
        return Err(Error::InvalidArgument(
            "cannot commit an empty native catalog mutation".to_owned(),
        ));
    }
    let standalone_coordinator = writes.is_empty();
    let coordinator = writes
        .first()
        .map(|write| write.transaction_id.clone())
        .unwrap_or_else(|| Uuid::new_v4().to_string());
    if standalone_coordinator {
        database.wal()?.begin(&coordinator, snapshot_generation)?;
    }
    let (current_schemas, current_tables, current_views) = {
        let state = database.state.lock();
        (
            state.catalog.schemas().clone(),
            state.tables.clone(),
            state.views.clone(),
        )
    };
    let mut did_rebase = false;
    let mut conflict = expected_schemas.iter().find_map(|(name, expected)| {
        (current_schemas.contains(name) != *expected).then(|| name.clone())
    });
    for (name, base) in expected.clone() {
        let current = current_tables.get(&name).cloned();
        let matches = match (&base, &current) {
            (None, None) => true,
            (Some(base), Some(current)) => base.table_reference() == current.table_reference(),
            _ => false,
        };
        if matches {
            continue;
        }
        let final_write = writes
            .iter()
            .rposition(|write| write.name == name)
            .filter(|_| !drops.contains(&name) && !renames.contains_key(&name));
        let rebase_result = match (base.as_ref(), current.as_ref(), final_write) {
            (Some(base), Some(current), Some(index)) => {
                super::rebase::write(database, &name, base, current, &writes[index].snapshot)
            }
            _ => Ok(None),
        };
        match rebase_result {
            Ok(Some(write)) => {
                expected.insert(name, current);
                writes.push(write);
                did_rebase = true;
            }
            Ok(None) => {
                conflict = Some(name);
                break;
            }
            Err(error) => {
                return abort_commit(
                    database,
                    writes,
                    standalone_coordinator,
                    &coordinator,
                    error,
                );
            }
        }
    }
    if conflict.is_none() {
        conflict = expected_views.iter().find_map(|(name, expected)| {
            let current = current_views.get(name);
            let matches = match (expected, current) {
                (None, None) => true,
                (Some(expected), Some(current)) => expected.reference() == current.reference(),
                _ => false,
            };
            (!matches).then(|| name.clone())
        });
    }
    if conflict.is_none() {
        conflict = schema_updates.iter().find_map(|(schema, create)| {
            (!*create
                && (current_tables.keys().any(|name| {
                    crate::catalog_name::schema_of(name) == schema
                        && !drops.contains(name)
                        && !renames.contains_key(name)
                }) || current_views.keys().any(|name| {
                    crate::catalog_name::schema_of(name) == schema
                        && !matches!(view_updates.get(name), Some(None))
                })))
            .then(|| schema.clone())
        });
    }
    if let Some(name) = conflict {
        let error = Error::TransactionConflict {
            transaction_id: coordinator.clone(),
            message: format!("catalog object '{name}' changed after the transaction snapshot"),
        };
        return abort_commit(
            database,
            writes,
            standalone_coordinator,
            &coordinator,
            error,
        );
    }

    if did_rebase
        && let Err(error) = super::quota::check_final_transaction(
            database,
            &writes,
            &drops,
            &renames,
            &schema_updates,
            &view_updates,
        )
    {
        return abort_commit(
            database,
            writes,
            standalone_coordinator,
            &coordinator,
            error,
        );
    }

    let mut state = database.state.lock();
    let previous_generation = state.catalog.generation();
    let mut schemas = state.catalog.schemas().clone();
    for (name, create) in &schema_updates {
        if *create {
            schemas.insert(name.clone());
        } else {
            schemas.remove(name);
        }
    }
    let mut final_indices = BTreeMap::new();
    for (index, write) in writes.iter().enumerate() {
        if !drops.contains(&write.name) {
            final_indices.insert(write.name.clone(), index);
        }
    }
    let mut tables = state.catalog.tables().clone();
    if let Err(error) = apply_renames(&mut tables, &renames, "catalog") {
        drop(state);
        return abort_commit(
            database,
            writes,
            standalone_coordinator,
            &coordinator,
            error,
        );
    }
    let mut renamed_snapshots = state.tables.clone();
    let overwritten_snapshots = match apply_renames(&mut renamed_snapshots, &renames, "snapshot") {
        Ok(overwritten) => overwritten,
        Err(error) => {
            drop(state);
            return abort_commit(
                database,
                writes,
                standalone_coordinator,
                &coordinator,
                error,
            );
        }
    };
    for name in &drops {
        tables.remove(name);
    }
    for (name, index) in &final_indices {
        tables.insert(name.clone(), writes[*index].snapshot.table_reference());
    }
    let mut views = state.catalog.views().clone();
    for (name, update) in &view_updates {
        match update {
            Some(view) => {
                views.insert(name.clone(), view.reference());
            }
            None => {
                views.remove(name);
            }
        }
    }
    let prepared_catalog = match manifest::prepare_catalog_commit(
        database.path(),
        database.database_id(),
        previous_generation,
        schemas,
        tables,
        views,
        &coordinator,
    ) {
        Ok(prepared) => prepared,
        Err(error) => {
            drop(state);
            return abort_commit(
                database,
                writes,
                standalone_coordinator,
                &coordinator,
                error,
            );
        }
    };
    let generation = prepared_catalog.generation();
    #[cfg(test)]
    super::commit::test_failpoint::hit(
        super::commit::test_failpoint::Boundary::CatalogPrepared,
        database.path(),
        &coordinator,
        Some(generation),
    )?;
    let coordinator_wal = match writes.first() {
        Some(write) => Arc::clone(&write.wal),
        None => database.wal()?,
    };
    if let Some(write) = writes.first_mut() {
        write.release_wal_to_recovery();
    }
    if let Err(error) =
        coordinator_wal.commit_catalog(&coordinator, previous_generation, generation)
    {
        return Err(super::commit::wal_commit_error(
            database,
            &coordinator,
            error,
        ));
    }
    for write in &mut writes {
        write.release_wal_to_recovery();
    }
    #[cfg(test)]
    super::commit::test_failpoint::hit(
        super::commit::test_failpoint::Boundary::WalCommitted,
        database.path(),
        &coordinator,
        Some(generation),
    )?;
    let committed = manifest::publish_prepared(
        database.path(),
        database.database_id(),
        previous_generation,
        prepared_catalog,
        &coordinator,
    )
    .map_err(|error| super::commit::catalog_publication_error(database, &coordinator, error))?;
    #[cfg(test)]
    super::commit::test_failpoint::hit(
        super::commit::test_failpoint::Boundary::CatalogPublished,
        database.path(),
        &coordinator,
        Some(generation),
    )?;

    for index in final_indices.values() {
        table::prune_inherited_manifests(database.path(), &writes[*index].snapshot).map_err(
            |error| {
                post_commit(
                    database,
                    &coordinator,
                    generation,
                    "manifest cleanup",
                    error,
                )
            },
        )?;
    }
    manifest::prune_old_generations(database.path(), database.database_id(), generation).map_err(
        |error| post_commit(database, &coordinator, generation, "catalog cleanup", error),
    )?;

    state.catalog = committed;
    for retired in &mut state.retired {
        if let Some(new_name) = renames.get(&retired.name) {
            retired.name = new_name.clone();
        }
    }
    state.tables = renamed_snapshots;
    for (name, snapshot) in overwritten_snapshots {
        retire_snapshot(&mut state, &name, snapshot);
    }
    for (name, index) in &final_indices {
        install_snapshot(
            &mut state,
            name.clone(),
            Arc::clone(&writes[*index].snapshot),
        );
    }
    for name in &drops {
        retire_dropped_snapshot(&mut state, name);
    }
    for (name, update) in view_updates {
        match update {
            Some(view) => {
                state.views.insert(name, view);
            }
            None => {
                state.views.remove(&name);
            }
        }
    }
    drop(state);

    cleanup_nonfinal(
        database,
        &mut writes,
        &final_indices,
        &coordinator,
        generation,
    )?;
    Ok(NativeCommit {
        previous_generation,
        generation,
        transaction_id: coordinator,
    })
}

fn apply_renames<T>(
    entries: &mut BTreeMap<String, T>,
    renames: &BTreeMap<String, String>,
    object: &str,
) -> Result<Vec<(String, T)>> {
    let mut targets = BTreeSet::new();
    let mut moved = Vec::with_capacity(renames.len());
    for (old, new) in renames {
        if !targets.insert(new) {
            return Err(Error::Internal(format!(
                "multiple native table renames target '{new}'"
            )));
        }
        let value = entries.remove(old).ok_or_else(|| {
            Error::Internal(format!(
                "renamed native table {object} '{old}' disappeared at commit"
            ))
        })?;
        moved.push((new.clone(), value));
    }
    Ok(moved
        .into_iter()
        .filter_map(|(new, value)| {
            entries
                .insert(new.clone(), value)
                .map(|overwritten| (new, overwritten))
        })
        .collect())
}

fn retire_dropped_snapshot(state: &mut super::NativeState, name: &str) {
    let Some(old) = state.tables.remove(name) else {
        return;
    };
    retire_snapshot(state, name, old);
}

fn retire_snapshot(state: &mut super::NativeState, name: &str, old: Arc<TableSnapshot>) {
    let mut source_bytes = old.source_bytes();
    for location in old.reachable_locations() {
        state.retired.push(super::RetiredSnapshot {
            name: name.to_owned(),
            lease: old.location_lease(&location),
            table_id: old.table_id().to_owned(),
            location,
            source_bytes: std::mem::take(&mut source_bytes),
        });
    }
}

fn install_snapshot(state: &mut super::NativeState, name: String, loaded: Arc<TableSnapshot>) {
    let reachable = loaded
        .reachable_locations()
        .into_iter()
        .collect::<BTreeSet<_>>();
    let old = state.tables.insert(name.clone(), Arc::clone(&loaded));
    if let Some(old) = old {
        let locations = old
            .reachable_locations()
            .into_iter()
            .filter(|location| !reachable.contains(location))
            .collect::<Vec<_>>();
        let mut retired_source_bytes = old.source_bytes();
        for location in locations {
            state.retired.push(super::RetiredSnapshot {
                name: name.clone(),
                lease: old.location_lease(&location),
                table_id: old.table_id().to_owned(),
                location,
                source_bytes: std::mem::take(&mut retired_source_bytes),
            });
        }
    }
}

fn cleanup_nonfinal(
    database: &NativeDatabase,
    writes: &mut [NativePublishedSnapshot],
    final_indices: &BTreeMap<String, usize>,
    coordinator: &str,
    generation: u64,
) -> Result<()> {
    let reachable = final_indices
        .values()
        .flat_map(|index| writes[*index].snapshot.reachable_locations())
        .collect::<BTreeSet<SnapshotLocation>>();
    for (index, write) in writes.iter_mut().enumerate().rev() {
        if final_indices.get(&write.name) != Some(&index)
            && !reachable.contains(&write.snapshot.own_location())
        {
            write
                .snapshot
                .remove_own_snapshot(database.path())
                .map_err(|error| {
                    post_commit(
                        database,
                        coordinator,
                        generation,
                        "superseded snapshot cleanup",
                        error,
                    )
                })?;
        }
        if write.transaction_id != coordinator {
            write.wal.abort(&write.transaction_id).map_err(|error| {
                post_commit(
                    database,
                    coordinator,
                    generation,
                    "subordinate WAL cleanup",
                    error,
                )
            })?;
        }
    }
    Ok(())
}

fn cleanup_published<T>(
    database: &NativeDatabase,
    snapshot: &TableSnapshot,
    wal_owner: &mut super::active_wal::ActiveWal,
    error: Error,
) -> Result<T> {
    let snapshot_cleanup = snapshot.remove_own_snapshot(database.path());
    let wal_cleanup = wal_owner.abort();
    match (snapshot_cleanup, wal_cleanup) {
        (Ok(()), Ok(())) => Err(error),
        (snapshot, wal) => Err(Error::native_storage(
            database.path(),
            format!(
                "{error}; published transaction snapshot cleanup failed: snapshot={:?}, WAL={:?}",
                snapshot.err(),
                wal.err()
            ),
        )),
    }
}

fn remove_and_abort(database: &NativeDatabase, mut write: NativePublishedSnapshot) -> Result<()> {
    let mut failure = None;
    if let Err(error) = write.snapshot.remove_own_snapshot(database.path()) {
        append_failure(&mut failure, error);
    }
    if let Err(error) = write.abort_wal() {
        append_failure(&mut failure, error);
    }
    match failure {
        Some(error) => Err(error),
        None => Ok(()),
    }
}

fn abort_commit<T>(
    database: &NativeDatabase,
    writes: Vec<NativePublishedSnapshot>,
    standalone_coordinator: bool,
    coordinator: &str,
    error: Error,
) -> Result<T> {
    let writes_cleanup = abort(database, writes);
    let coordinator_cleanup = if standalone_coordinator {
        database.wal()?.abort(coordinator)
    } else {
        Ok(())
    };
    match (writes_cleanup, coordinator_cleanup) {
        (Ok(()), Ok(())) => Err(error),
        (writes, coordinator) => Err(Error::native_storage(
            database.path(),
            format!(
                "{error}; transaction mutation cleanup failed: writes={:?}, coordinator={:?}",
                writes.err(),
                coordinator.err()
            ),
        )),
    }
}

fn append_failure(failure: &mut Option<Error>, error: Error) {
    *failure = Some(match failure.take() {
        Some(previous) => Error::native_storage(
            "native transaction",
            format!("{previous}; additional cleanup failure: {error}"),
        ),
        None => error,
    });
}

fn post_commit(
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
        format!("{action} failed; reopen the engine: {error}"),
    )
}
