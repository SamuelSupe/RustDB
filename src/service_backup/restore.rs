use std::{
    fs,
    path::{Component, Path, PathBuf},
};

use uuid::Uuid;

use crate::{Engine, EngineConfig, Error, Result};

use super::{
    NATIVE_DIRECTORY, SERVICE_DIRECTORY,
    files::{
        cleanup_owned_directory, copy_private_file, create_new_private_dir, create_private_dir,
        safe_join, sync_directories, walk,
    },
    location::{publish, refuse_existing},
};

pub(super) fn local(
    backup: &Path,
    database: &Path,
    state_root: &Path,
    config: EngineConfig,
) -> Result<Engine> {
    let report = super::validate::local(backup, &backup.display().to_string())?;
    refuse_existing(database, "restore database")?;
    let state_target = state_root.join(report.database_id());
    ensure_distinct_targets(database, &state_target)?;
    refuse_existing(&state_target, "restore HTTP state")?;
    let state_staging = state_root.join(format!(
        ".{}.rustdb-service-restore-{}",
        report.database_id(),
        Uuid::new_v4()
    ));
    let state_staged = if report.service_state_included() {
        create_private_dir(state_root)?;
        copy_tree(&backup.join(SERVICE_DIRECTORY), &state_staging)?;
        true
    } else {
        false
    };
    let engine = match Engine::restore_from(backup.join(NATIVE_DIRECTORY), database, config) {
        Ok(engine) => engine,
        Err(error) if state_staged => {
            return cleanup_owned_directory(&state_staging, error);
        }
        Err(error) => return Err(error),
    };
    if state_staged && let Err(error) = publish(&state_staging, &state_target) {
        return rollback_publication(engine, database, &state_staging, &state_target, error);
    }
    Ok(engine)
}

fn copy_tree(source: &Path, target: &Path) -> Result<()> {
    create_new_private_dir(target)?;
    let result = (|| {
        let (directories, entries) = walk(source, false)?;
        for relative in &directories {
            create_private_dir(&safe_join(target, relative)?)?;
        }
        for entry in entries {
            copy_private_file(
                &safe_join(source, &entry.path)?,
                &safe_join(target, &entry.path)?,
            )?;
        }
        sync_directories(target, &directories)
    })();
    match result {
        Ok(()) => Ok(()),
        Err(error) => cleanup_owned_directory(target, error),
    }
}

fn rollback_publication(
    engine: Engine,
    database: &Path,
    state_staging: &Path,
    state_target: &Path,
    error: Error,
) -> Result<Engine> {
    drop(engine);
    let database_cleanup = fs::remove_dir_all(database);
    let state_cleanup = cleanup_service_publication(state_staging, state_target);
    if database_cleanup.is_ok() && state_cleanup.is_ok() {
        return Err(error);
    }
    let mut message = error.to_string();
    if let Err(cleanup) = database_cleanup {
        message.push_str(&format!("; fresh database cleanup also failed: {cleanup}"));
    }
    if let Err(cleanup) = state_cleanup {
        message.push_str(&format!("; service-state cleanup also failed: {cleanup}"));
    }
    Err(Error::Execution(message))
}

fn cleanup_service_publication(staging: &Path, target: &Path) -> Result<()> {
    let owned = match fs::symlink_metadata(staging) {
        Ok(_) => staging,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => target,
        Err(error) => return Err(Error::io(Some(staging.to_path_buf()), error)),
    };
    match fs::remove_dir_all(owned) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(Error::io(Some(owned.to_path_buf()), error)),
    }
}

fn ensure_distinct_targets(database: &Path, state: &Path) -> Result<()> {
    let database = absolute_lexical(database)?;
    let state = absolute_lexical(state)?;
    if database.starts_with(&state) || state.starts_with(&database) {
        return Err(Error::InvalidArgument(
            "restore database and HTTP state targets must not contain each other".to_owned(),
        ));
    }
    Ok(())
}

fn absolute_lexical(path: &Path) -> Result<PathBuf> {
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()
            .map_err(|error| Error::io(None, error))?
            .join(path)
    };
    let mut normalized = PathBuf::new();
    for component in absolute.components() {
        match component {
            Component::Prefix(prefix) => normalized.push(prefix.as_os_str()),
            Component::RootDir => normalized.push(component.as_os_str()),
            Component::CurDir => {}
            Component::ParentDir => {
                normalized.pop();
            }
            Component::Normal(value) => normalized.push(value),
        }
    }
    Ok(normalized)
}

#[cfg(test)]
mod tests;
