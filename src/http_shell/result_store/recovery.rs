use std::{
    collections::HashSet,
    fs,
    path::Path,
    sync::Arc,
    time::{Duration, SystemTime},
};

use crate::{Error, Result};

use super::{
    AccessRegistry, RecoveredResult, StoredResult, access_for,
    layout::{owned_query_directories, remove_owned_query, secure_directory, sync_directory},
    manifest::{self, MANIFEST_FILE, ManifestState, PRODUCER_VERSION},
    quota::{QuotaLease, QuotaPool},
    reader::{self, chunk_path},
    writer::clear_batch_files,
};

pub(super) fn recover(
    root: &Path,
    ttl: Duration,
    quota: &Arc<QuotaPool>,
    accesses: &AccessRegistry,
) -> Result<Vec<RecoveredResult>> {
    let now = SystemTime::now();
    let mut recovered = Vec::new();
    for directory in owned_query_directories(root)? {
        clean_temporary_files(&directory)?;
        let Some(query_id) = query_id(&directory) else {
            remove_if_expired_orphan(&directory, ttl, now)?;
            continue;
        };
        let path = directory.join(MANIFEST_FILE);
        let mut value = match manifest::load(&path) {
            Ok(value) => value,
            Err(error) => {
                tracing::warn!(%error, path = %directory.display(), "ignoring unreadable HTTP result manifest");
                remove_if_expired_orphan(&directory, ttl, now)?;
                continue;
            }
        };
        if let Err(error) = value.validate(&query_id) {
            tracing::warn!(%error, query_id, "ignoring invalid HTTP result manifest");
            remove_if_expired_orphan(&directory, ttl, now)?;
            continue;
        }
        if value.expired(ttl, now) {
            remove_owned_query(&directory)?;
            continue;
        }
        secure_directory(&directory.join("batches"))?;
        if value.producer_version != PRODUCER_VERSION {
            clear_batch_files(&directory, None)?;
            if value.state != ManifestState::Invalidated {
                value.invalidate(format!(
                    "result producer {} differs from server {}",
                    value.producer_version, PRODUCER_VERSION
                ));
                manifest::persist(&directory, &value)?;
            }
            recovered.push(RecoveredResult::terminal(
                query_id,
                ManifestState::Invalidated,
                value.error.clone(),
                access_for(accesses, &value.query_id),
            ));
            continue;
        }
        if value.state == ManifestState::Running {
            clear_batch_files(&directory, None)?;
            value.fail("server_restarted");
            manifest::persist(&directory, &value)?;
            recovered.push(RecoveredResult::terminal(
                query_id,
                ManifestState::Failed,
                value.error.clone(),
                access_for(accesses, &value.query_id),
            ));
            continue;
        }
        if let Err(error) = clean_and_validate_chunks(&directory, &value.batches) {
            tracing::warn!(%error, query_id, "invalidating corrupt HTTP result chunks");
            clear_batch_files(&directory, None)?;
            value.fail(format!("result_corrupt: {error}"));
            manifest::persist(&directory, &value)?;
            recovered.push(RecoveredResult::terminal(
                query_id,
                ManifestState::Failed,
                value.error.clone(),
                access_for(accesses, &value.query_id),
            ));
            continue;
        }
        match value.state {
            ManifestState::Completed => {
                let access = access_for(accesses, &query_id);
                let lease = Arc::new(QuotaLease::new(Arc::clone(quota)));
                lease.reserve_existing(value.bytes).map_err(|error| {
                    Error::ResourceExhausted(format!(
                        "recovered HTTP result {query_id} exceeds configured quota: {error}"
                    ))
                })?;
                let schema = value.schema()?;
                let result = Arc::new(StoredResult::completed(
                    directory,
                    schema,
                    &value,
                    lease,
                    Arc::clone(&access),
                ));
                recovered.push(RecoveredResult::completed(query_id, result, access));
            }
            ManifestState::Failed | ManifestState::Invalidated => {
                let access = access_for(accesses, &query_id);
                recovered.push(RecoveredResult::terminal(
                    query_id,
                    value.state,
                    value.error.clone(),
                    access,
                ));
            }
            ManifestState::Running => unreachable!("running state handled above"),
        }
    }
    Ok(recovered)
}

fn clean_temporary_files(directory: &Path) -> Result<()> {
    let temporary = directory.join(format!(".{MANIFEST_FILE}.partial"));
    let mut changed = false;
    if temporary.exists() {
        fs::remove_file(&temporary).map_err(|error| Error::io(Some(temporary.clone()), error))?;
        changed = true;
    }
    let batches = directory.join("batches");
    if batches.exists() {
        secure_directory(&batches)?;
        let mut batch_changed = false;
        for entry in
            fs::read_dir(&batches).map_err(|error| Error::io(Some(batches.clone()), error))?
        {
            let entry = entry.map_err(|error| Error::io(Some(batches.clone()), error))?;
            let path = entry.path();
            if path.extension().and_then(|value| value.to_str()) != Some("partial") {
                continue;
            }
            let metadata = path
                .symlink_metadata()
                .map_err(|error| Error::io(Some(path.clone()), error))?;
            if !metadata.file_type().is_file() || metadata.file_type().is_symlink() {
                return Err(Error::InvalidArgument(format!(
                    "invalid HTTP result temporary path: {}",
                    path.display()
                )));
            }
            fs::remove_file(&path).map_err(|error| Error::io(Some(path.clone()), error))?;
            batch_changed = true;
        }
        if batch_changed {
            sync_directory(&batches)?;
        }
    }
    if changed {
        sync_directory(directory)?;
    }
    Ok(())
}

fn clean_and_validate_chunks(
    directory: &Path,
    batches: &[super::manifest::BatchEntry],
) -> Result<()> {
    let batch_directory = directory.join("batches");
    let expected = batches
        .iter()
        .map(|entry| chunk_path(directory, entry.seq))
        .collect::<HashSet<_>>();
    let mut changed = false;
    for entry in fs::read_dir(&batch_directory)
        .map_err(|error| Error::io(Some(batch_directory.clone()), error))?
    {
        let entry = entry.map_err(|error| Error::io(Some(batch_directory.clone()), error))?;
        let path = entry.path();
        let metadata = path
            .symlink_metadata()
            .map_err(|error| Error::io(Some(path.clone()), error))?;
        if !metadata.file_type().is_file() || metadata.file_type().is_symlink() {
            return Err(Error::InvalidArgument(format!(
                "unexpected entry in HTTP result batch directory: {}",
                path.display()
            )));
        }
        if !expected.contains(&path) {
            fs::remove_file(&path).map_err(|error| Error::io(Some(path.clone()), error))?;
            changed = true;
        }
    }
    if changed {
        sync_directory(&batch_directory)?;
    }
    for entry in batches {
        let path = chunk_path(directory, entry.seq);
        let metadata = path
            .symlink_metadata()
            .map_err(|error| Error::io(Some(path.clone()), error))?;
        if !metadata.file_type().is_file()
            || metadata.file_type().is_symlink()
            || metadata.len() != entry.bytes
        {
            return Err(Error::Execution(format!(
                "HTTP result batch {} does not match its manifest",
                entry.seq
            )));
        }
        let _ = reader::read_chunk_bytes(directory, entry)?;
    }
    Ok(())
}

fn query_id(directory: &Path) -> Option<String> {
    directory
        .file_name()?
        .to_str()?
        .strip_prefix("q-")
        .filter(|value| super::valid_query_id(value))
        .map(ToOwned::to_owned)
}

fn remove_if_expired_orphan(directory: &Path, ttl: Duration, now: SystemTime) -> Result<()> {
    let modified = directory
        .metadata()
        .and_then(|value| value.modified())
        .unwrap_or(now);
    if now.duration_since(modified).is_ok_and(|age| age >= ttl) {
        remove_owned_query(directory)?;
    }
    Ok(())
}
