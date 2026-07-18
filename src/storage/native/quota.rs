use std::{
    collections::{BTreeMap, BTreeSet},
    path::PathBuf,
    sync::Arc,
};

use crate::{Error, Result};

use super::{
    NativeDatabase, NativePublishedSnapshot, NativeState, NativeTransactionChanges, NativeView,
    PreparedSnapshot, disk_budget::directories_storage_bytes, manifest, table, wal,
};

struct TransactionQuotaChanges<'a> {
    writes: &'a [NativePublishedSnapshot],
    drops: &'a BTreeSet<String>,
    renames: &'a BTreeMap<String, String>,
    schema_updates: &'a BTreeMap<String, bool>,
    view_updates: &'a BTreeMap<String, Option<Arc<NativeView>>>,
}

#[derive(Debug)]
pub(super) struct ActiveWrite {
    pub(super) name: String,
    directory: PathBuf,
}

pub(super) fn register_published(database: &NativeDatabase, published: &NativePublishedSnapshot) {
    database.active_quota_writes.lock().insert(
        published.transaction_id().to_owned(),
        ActiveWrite {
            name: published.name().to_owned(),
            directory: published.snapshot().final_directory(database.path()),
        },
    );
}

pub(super) fn prune_published(database: &NativeDatabase) {
    let Some(wal) = database.wal.as_ref() else {
        database.active_quota_writes.lock().clear();
        return;
    };
    database
        .active_quota_writes
        .lock()
        .retain(|transaction_id, _| wal.transaction_is_active(transaction_id));
}

pub(super) fn check_prepared(database: &NativeDatabase, prepared: &PreparedSnapshot) -> Result<()> {
    prune_published(database);
    if database.quota.engine_limit_bytes.is_none()
        && database.quota.table_limit(&prepared.name).is_none()
    {
        return Ok(());
    }
    let state = database.state.lock();
    if let Some(limit) = database.quota.engine_limit_bytes {
        let peak = root_bytes(database)?;
        let staged = directories_storage_bytes([prepared.staging.path().to_path_buf()])?;
        let current = peak.checked_sub(staged).ok_or_else(|| {
            Error::Internal("Native staged bytes exceed the engine root usage".to_owned())
        })?;
        check_limit(
            database,
            None,
            current,
            peak,
            engine_commit_headroom(
                projected_table_count(&state, &prepared.name)?,
                projected_prepared_catalog_bytes(database, &state, prepared)?,
            )?,
            limit,
        )?;
    }
    if let Some(limit) = database.quota.table_limit(&prepared.name) {
        let current_directories = current_table_directories(database, &state, &prepared.name);
        let current = directories_storage_bytes(current_directories.clone())?;
        let mut peak_directories = current_directories;
        peak_directories.extend(active_table_directories(database, &prepared.name));
        peak_directories.extend(table_directories(database, [prepared.snapshot.table_id()]));
        peak_directories.extend(prepared.quota_directories(database.path()));
        check_limit(
            database,
            Some(prepared.name.clone()),
            current,
            directories_storage_bytes(peak_directories)?,
            0,
            limit,
        )?;
    }
    Ok(())
}

pub(super) fn check_transaction(
    database: &NativeDatabase,
    changes: &NativeTransactionChanges,
) -> Result<()> {
    prune_published(database);
    let state = database.state.lock();
    check_transaction_with_state(
        database,
        &state,
        TransactionQuotaChanges {
            writes: &changes.writes,
            drops: &changes.drops,
            renames: &changes.renames,
            schema_updates: &changes.schema_updates,
            view_updates: &changes.view_updates,
        },
    )
}

pub(super) fn check_final_transaction(
    database: &NativeDatabase,
    writes: &[NativePublishedSnapshot],
    drops: &BTreeSet<String>,
    renames: &BTreeMap<String, String>,
    schema_updates: &BTreeMap<String, bool>,
    view_updates: &BTreeMap<String, Option<Arc<NativeView>>>,
) -> Result<()> {
    prune_published(database);
    let state = database.state.lock();
    check_transaction_with_state(
        database,
        &state,
        TransactionQuotaChanges {
            writes,
            drops,
            renames,
            schema_updates,
            view_updates,
        },
    )
}

fn check_transaction_with_state(
    database: &NativeDatabase,
    state: &NativeState,
    changes: TransactionQuotaChanges<'_>,
) -> Result<()> {
    if let Some(limit) = database.quota.engine_limit_bytes {
        let peak = root_bytes(database)?;
        let write_bytes = directories_storage_bytes(
            changes
                .writes
                .iter()
                .map(|write| write.snapshot().final_directory(database.path())),
        )?;
        let current = peak.checked_sub(write_bytes).ok_or_else(|| {
            Error::Internal("Native transaction bytes exceed the engine root usage".to_owned())
        })?;
        check_limit(
            database,
            None,
            current,
            peak,
            engine_commit_headroom(
                projected_transaction_table_count(state, &changes)?,
                projected_transaction_catalog_bytes(database, state, &changes)?,
            )?,
            limit,
        )?;
    }

    let mut tables = BTreeMap::<String, BTreeSet<String>>::new();
    for write in changes.writes {
        tables
            .entry(write.name().to_owned())
            .or_default()
            .insert(write.snapshot().table_id().to_owned());
    }
    for (old, new) in changes.renames {
        tables.entry(new.clone()).or_default();
        if old == new {
            return Err(Error::Internal(
                "Native transaction contains a no-op rename".to_owned(),
            ));
        }
    }
    for (name, table_ids) in tables {
        let Some(limit) = database.quota.table_limit(&name) else {
            continue;
        };
        let source_name = changes
            .renames
            .iter()
            .find_map(|(old, new)| (new == &name).then_some(old.as_str()))
            .unwrap_or(&name);
        let current_directories = current_table_directories(database, state, source_name);
        let current = directories_storage_bytes(current_directories.clone())?;
        let mut peak_directories = current_directories;
        peak_directories.extend(active_table_directories(database, &name));
        peak_directories.extend(table_directories(
            database,
            table_ids.iter().map(String::as_str),
        ));
        check_limit(
            database,
            Some(name),
            current,
            directories_storage_bytes(peak_directories)?,
            0,
            limit,
        )?;
    }
    Ok(())
}

fn active_table_directories(database: &NativeDatabase, name: &str) -> Vec<PathBuf> {
    database
        .active_quota_writes
        .lock()
        .values()
        .filter(|write| write.name == name && write.directory.exists())
        .map(|write| write.directory.clone())
        .collect()
}

fn check_limit(
    database: &NativeDatabase,
    table: Option<String>,
    current_bytes: u64,
    physical_peak_bytes: u64,
    headroom_bytes: u64,
    limit_bytes: u64,
) -> Result<()> {
    let peak_bytes = physical_peak_bytes
        .checked_add(headroom_bytes)
        .ok_or_else(|| Error::ResourceExhausted("Native quota peak overflowed".to_owned()))?;
    if peak_bytes <= limit_bytes {
        return Ok(());
    }
    let added_bytes = peak_bytes
        .checked_sub(current_bytes)
        .ok_or_else(|| Error::Internal("Native quota peak is below current storage".to_owned()))?;
    Err(Error::native_disk_quota_exceeded(
        database.path(),
        table,
        current_bytes,
        added_bytes,
        peak_bytes,
        limit_bytes,
    ))
}

fn root_bytes(database: &NativeDatabase) -> Result<u64> {
    directories_storage_bytes([database.path().to_path_buf()])
}

fn current_table_directories(
    database: &NativeDatabase,
    state: &NativeState,
    name: &str,
) -> Vec<PathBuf> {
    let current = state
        .tables
        .get(name)
        .into_iter()
        .flat_map(|snapshot| snapshot.reachable_directories(database.path()));
    let retired = state
        .retired
        .iter()
        .filter(|retired| retired.name == name)
        .map(|retired| {
            table::location_directory(database.path(), &retired.table_id, &retired.location)
        });
    current.chain(retired).collect()
}

fn table_directories<'a>(
    database: &NativeDatabase,
    table_ids: impl IntoIterator<Item = &'a str>,
) -> Vec<PathBuf> {
    table_ids
        .into_iter()
        .map(|table_id| database.path().join("tables").join(table_id))
        .filter(|directory| directory.exists())
        .collect()
}

fn projected_table_count(state: &NativeState, name: &str) -> Result<usize> {
    state
        .tables
        .len()
        .checked_add(usize::from(!state.tables.contains_key(name)))
        .ok_or_else(|| Error::ResourceExhausted("Native table count overflowed".to_owned()))
}

fn projected_transaction_table_count(
    state: &NativeState,
    changes: &TransactionQuotaChanges<'_>,
) -> Result<usize> {
    let mut names = state.tables.keys().cloned().collect::<BTreeSet<_>>();
    for (old, new) in changes.renames {
        names.remove(old);
        names.insert(new.clone());
    }
    for name in changes.drops {
        names.remove(name);
    }
    for write in changes.writes {
        if !changes.drops.contains(write.name()) {
            names.insert(write.name().to_owned());
        }
    }
    Ok(names.len())
}

fn projected_prepared_catalog_bytes(
    database: &NativeDatabase,
    state: &NativeState,
    prepared: &PreparedSnapshot,
) -> Result<u64> {
    let mut tables = state.catalog.tables().clone();
    tables.insert(prepared.name.clone(), prepared.snapshot.table_reference());
    manifest::projected_generation_bytes(
        database.path(),
        &state.catalog,
        state.catalog.schemas().clone(),
        tables,
        state.catalog.views().clone(),
    )
}

fn projected_transaction_catalog_bytes(
    database: &NativeDatabase,
    state: &NativeState,
    changes: &TransactionQuotaChanges<'_>,
) -> Result<u64> {
    let mut schemas = state.catalog.schemas().clone();
    for (name, create) in changes.schema_updates {
        if *create {
            schemas.insert(name.clone());
        } else {
            schemas.remove(name);
        }
    }
    let mut tables = state.catalog.tables().clone();
    for (old, new) in changes.renames {
        if let Some(reference) = tables.remove(old) {
            tables.insert(new.clone(), reference);
        }
    }
    for name in changes.drops {
        tables.remove(name);
    }
    for write in changes.writes {
        if !changes.drops.contains(write.name()) {
            tables.insert(write.name().to_owned(), write.snapshot().table_reference());
        }
    }
    let mut views = state.catalog.views().clone();
    for (name, update) in changes.view_updates {
        match update {
            Some(view) => {
                views.insert(name.clone(), view.reference());
            }
            None => {
                views.remove(name);
            }
        }
    }
    manifest::projected_generation_bytes(database.path(), &state.catalog, schemas, tables, views)
}

#[cfg(test)]
pub(super) fn projected_transaction_engine_peak(
    database: &NativeDatabase,
    writes: &[NativePublishedSnapshot],
    drops: &BTreeSet<String>,
    renames: &BTreeMap<String, String>,
    schema_updates: &BTreeMap<String, bool>,
    view_updates: &BTreeMap<String, Option<Arc<NativeView>>>,
) -> Result<u64> {
    let state = database.state.lock();
    let changes = TransactionQuotaChanges {
        writes,
        drops,
        renames,
        schema_updates,
        view_updates,
    };
    root_bytes(database)?
        .checked_add(engine_commit_headroom(
            projected_transaction_table_count(&state, &changes)?,
            projected_transaction_catalog_bytes(database, &state, &changes)?,
        )?)
        .ok_or_else(|| Error::ResourceExhausted("Native quota peak overflowed".to_owned()))
}

fn engine_commit_headroom(table_count: usize, encoded_catalog_bytes: u64) -> Result<u64> {
    let conservative_catalog = table_count
        .max(1)
        .checked_mul(manifest::CATALOG_GENERATION_BUDGET_PER_TABLE_BYTES)
        .map(|bytes| bytes.min(manifest::MAX_CATALOG_MANIFEST_BYTES))
        .ok_or_else(|| Error::ResourceExhausted("Native Catalog headroom overflowed".to_owned()))?;
    u64::try_from(conservative_catalog)
        .ok()
        .map(|bytes| bytes.max(encoded_catalog_bytes))
        .and_then(|bytes| bytes.checked_add(wal::COMMIT_RECORD_HEADROOM_BYTES))
        .and_then(|bytes| bytes.checked_add(manifest::MAX_CURRENT_BYTES as u64))
        .ok_or_else(|| Error::ResourceExhausted("Native commit headroom overflowed".to_owned()))
}

#[cfg(test)]
mod tests;
