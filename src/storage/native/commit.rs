use std::{collections::BTreeSet, sync::Arc};

use crate::{Error, Result};

use super::{
    NativeDatabase, PreparedSnapshot, manifest,
    table::{self, SnapshotOperation},
};

#[cfg(test)]
pub(super) mod test_failpoint {
    use std::cell::Cell;

    use crate::{Error, Result};

    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    pub(crate) enum Boundary {
        SnapshotPublished,
        CatalogPrepared,
        WalCommitted,
        CatalogPublished,
    }

    thread_local! {
        static ARMED: Cell<Option<Boundary>> = const { Cell::new(None) };
    }

    pub(crate) fn arm(boundary: Boundary) {
        ARMED.with(|armed| armed.set(Some(boundary)));
    }

    pub(in crate::storage::native) fn hit(
        boundary: Boundary,
        path: &std::path::Path,
        transaction_id: &str,
        generation: Option<u64>,
    ) -> Result<()> {
        let inject = ARMED.with(|armed| {
            if armed.get() == Some(boundary) {
                armed.set(None);
                true
            } else {
                false
            }
        });
        if !inject {
            return Ok(());
        }
        let message = format!("injected restart at durable boundary {boundary:?}");
        Err(match boundary {
            Boundary::SnapshotPublished | Boundary::CatalogPrepared => {
                Error::native_storage(path, message)
            }
            Boundary::WalCommitted => Error::commit_outcome_unknown(path, transaction_id, message),
            Boundary::CatalogPublished => Error::native_commit_post_commit_failure(
                path,
                transaction_id,
                generation.expect("published catalog has a generation"),
                message,
            ),
        })
    }
}

#[derive(Debug)]
pub(crate) struct NativeCommit {
    pub(super) previous_generation: u64,
    pub(super) generation: u64,
    pub(super) transaction_id: String,
}

impl NativeCommit {
    pub(crate) fn previous_generation(&self) -> u64 {
        self.previous_generation
    }

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
        wal,
        mut wal_owner,
        staging,
        snapshot,
        name,
    } = prepared;
    let mut state = database.state.lock();
    let commit_base_generation = state.catalog.generation();
    let transaction_id = staging.transaction_id().to_owned();
    let current = state.catalog.tables().get(&name);
    let parent_matches = match (snapshot.operation(), snapshot.parent(), current) {
        (SnapshotOperation::Import, None, None) => true,
        (
            SnapshotOperation::Append
            | SnapshotOperation::Replace
            | SnapshotOperation::Delete
            | SnapshotOperation::Update
            | SnapshotOperation::Truncate,
            Some(parent),
            Some(current),
        ) => parent == current,
        _ => false,
    };
    if !parent_matches {
        return abort_with(
            staging,
            &mut wal_owner,
            Error::TransactionConflict {
                transaction_id: transaction_id.clone(),
                message: format!("native table '{name}' changed while the write was running"),
            },
        );
    }

    let reference = snapshot.table_reference();
    let destination = snapshot.final_directory(database.path());
    staging.publish(&destination)?;
    #[cfg(test)]
    test_failpoint::hit(
        test_failpoint::Boundary::SnapshotPublished,
        database.path(),
        &transaction_id,
        None,
    )?;
    let mut loaded = match table::load(database.path(), database.database_id(), &reference) {
        Ok(loaded) => loaded,
        Err(error) => {
            return abort_published(database, &snapshot, &mut wal_owner, error);
        }
    };
    let mut tables = state.catalog.tables().clone();
    tables.insert(name.clone(), reference);
    let prepared_catalog = match manifest::prepare_commit(
        database.path(),
        database.database_id(),
        commit_base_generation,
        tables,
        &transaction_id,
    ) {
        Ok(prepared) => prepared,
        Err(error) => {
            return abort_published(database, &snapshot, &mut wal_owner, error);
        }
    };
    let generation = prepared_catalog.generation();
    #[cfg(test)]
    test_failpoint::hit(
        test_failpoint::Boundary::CatalogPrepared,
        database.path(),
        &transaction_id,
        Some(generation),
    )?;
    wal_owner.release_to_recovery();
    if let Err(error) = wal.commit_catalog(&transaction_id, commit_base_generation, generation) {
        return Err(wal_commit_error(database, &transaction_id, error));
    }
    #[cfg(test)]
    test_failpoint::hit(
        test_failpoint::Boundary::WalCommitted,
        database.path(),
        &transaction_id,
        Some(generation),
    )?;
    let committed = match manifest::publish_prepared(
        database.path(),
        database.database_id(),
        commit_base_generation,
        prepared_catalog,
        &transaction_id,
    ) {
        Ok(committed) => committed,
        Err(error) => {
            return Err(catalog_publication_error(database, &transaction_id, error));
        }
    };
    #[cfg(test)]
    test_failpoint::hit(
        test_failpoint::Boundary::CatalogPublished,
        database.path(),
        &transaction_id,
        Some(generation),
    )?;
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
    Ok(NativeCommit {
        previous_generation: commit_base_generation,
        generation,
        transaction_id,
    })
}

pub(super) fn wal_commit_error(
    database: &NativeDatabase,
    transaction_id: &str,
    error: Error,
) -> Error {
    match error {
        error @ (Error::CommitOutcomeUnknown { .. }
        | Error::NativeCommitPostCommitFailure { .. }) => error,
        error => Error::native_storage(
            database.path(),
            format!(
                "failed to persist WAL commit for transaction {transaction_id}; reopen the engine: {error}"
            ),
        ),
    }
}

pub(super) fn catalog_publication_error(
    database: &NativeDatabase,
    transaction_id: &str,
    error: Error,
) -> Error {
    match error {
        error @ (Error::CommitOutcomeUnknown { .. }
        | Error::NativeCommitPostCommitFailure { .. }) => error,
        error => Error::commit_outcome_unknown(
            database.path(),
            transaction_id,
            format!(
                "WAL commit is durable but catalog publication did not complete; reopen the engine to recover: {error}"
            ),
        ),
    }
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

fn abort_with<T>(
    staging: super::StagedSnapshot,
    wal_owner: &mut super::active_wal::ActiveWal,
    error: Error,
) -> Result<T> {
    let path = staging.path().to_owned();
    match super::table_writer::abort_owned_transaction(staging, wal_owner) {
        Ok(()) => Err(error),
        Err(cleanup) => Err(Error::native_storage(
            path,
            format!("{error}; staging cleanup failed: {cleanup}"),
        )),
    }
}

fn abort_published<T>(
    database: &NativeDatabase,
    snapshot: &super::table::TableSnapshot,
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
                "{error}; published snapshot cleanup failed: snapshot={:?}, WAL={:?}; reopen the engine",
                snapshot.err(),
                wal.err()
            ),
        )),
    }
}
