use std::{fs, path::Path};

use uuid::Uuid;

use crate::{Engine, Error, Result};

use super::{
    EXCLUDED_STATE, NATIVE_DIRECTORY, ServiceBackupReport,
    files::{
        cleanup_owned_directory, create_new_private_dir, create_private_file, sync_directories,
        walk,
    },
    location::publish,
    manifest::{self, FORMAT, FORMAT_VERSION, MANIFEST_FILE, Manifest},
};

pub(super) fn local(
    engine: &Engine,
    state_root: &Path,
    destination: &Path,
) -> Result<ServiceBackupReport> {
    if destination.exists() {
        return Err(Error::InvalidArgument(format!(
            "service backup destination already exists: {}",
            destination.display()
        )));
    }
    let parent = destination
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    fs::create_dir_all(parent).map_err(|error| Error::io(Some(parent.to_path_buf()), error))?;
    let staging = parent.join(format!(".rustdb-service-backup-{}.tmp", Uuid::new_v4()));
    create_new_private_dir(&staging)?;
    let result = create_staged(engine, state_root, destination, &staging);
    match result {
        Ok(report) => Ok(report),
        Err(error) => cleanup_owned_directory(&staging, error),
    }
}

fn create_staged(
    engine: &Engine,
    state_root: &Path,
    destination: &Path,
    staging: &Path,
) -> Result<ServiceBackupReport> {
    engine.backup_to(staging.join(NATIVE_DIRECTORY))?;
    let database_id = super::validate::database_id(&staging.join(NATIVE_DIRECTORY))?;
    let service_state_included = super::state::copy_control(state_root, &database_id, staging)?;
    let (directories, files) = walk(staging, true)?;
    sync_directories(staging, &directories)?;
    let manifest = Manifest {
        format: FORMAT.to_owned(),
        format_version: FORMAT_VERSION,
        producer_version: env!("CARGO_PKG_VERSION").to_owned(),
        database_id,
        service_state_included,
        excluded: EXCLUDED_STATE
            .iter()
            .map(|value| (*value).to_owned())
            .collect(),
        directories,
        files,
    };
    create_private_file(&staging.join(MANIFEST_FILE), &manifest::encode(&manifest)?)?;
    super::files::sync_dir(staging)?;
    let mut report = super::validate::local(staging, &destination.display().to_string())?;
    publish(staging, destination)?;
    report.location = destination.display().to_string();
    Ok(report)
}
