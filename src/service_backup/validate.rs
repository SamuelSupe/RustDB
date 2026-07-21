use std::{collections::HashMap, fs, path::Path};

use serde::Deserialize;
use uuid::Uuid;

use crate::{Engine, Error, Result};

use super::{
    EXCLUDED_STATE, NATIVE_DIRECTORY, SERVICE_DIRECTORY, STATE_FILES, ServiceBackupReport,
    files::walk,
    manifest::{self, MANIFEST_FILE, Manifest},
};

pub(super) fn local(root: &Path, location: &str) -> Result<ServiceBackupReport> {
    let manifest_path = root.join(MANIFEST_FILE);
    super::files::check_private(&manifest_path, false)?;
    let metadata = fs::symlink_metadata(&manifest_path)
        .map_err(|error| Error::io(Some(manifest_path.clone()), error))?;
    if metadata.len() > manifest::MAX_MANIFEST_BYTES {
        return Err(Error::ResourceExhausted(format!(
            "service backup manifest exceeds {} bytes",
            manifest::MAX_MANIFEST_BYTES
        )));
    }
    let encoded =
        fs::read(&manifest_path).map_err(|error| Error::io(Some(manifest_path), error))?;
    let manifest = manifest::decode(&encoded)?;
    inventory_shape(&manifest)?;
    let (actual_directories, actual_files) = walk(root, true)?;
    if actual_directories != manifest.directories {
        return Err(Error::Execution(
            "service backup directory inventory mismatch".to_owned(),
        ));
    }
    let actual = actual_files
        .into_iter()
        .map(|entry| (entry.path.clone(), entry))
        .collect::<HashMap<_, _>>();
    if actual.len() != manifest.files.len() {
        return Err(Error::Execution(
            "service backup file inventory mismatch".to_owned(),
        ));
    }
    let mut bytes = 0_u64;
    for expected in &manifest.files {
        let Some(found) = actual.get(&expected.path) else {
            return Err(Error::Execution(format!(
                "service backup file is missing: {}",
                expected.path
            )));
        };
        if found.bytes != expected.bytes || found.sha256 != expected.sha256 {
            return Err(Error::Execution(format!(
                "service backup checksum mismatch: {}",
                expected.path
            )));
        }
        bytes = bytes.checked_add(found.bytes).ok_or_else(|| {
            Error::ResourceExhausted("service backup byte count overflow".to_owned())
        })?;
    }
    if database_id(&root.join(NATIVE_DIRECTORY))? != manifest.database_id {
        return Err(Error::Execution(
            "service backup Native database id does not match its manifest".to_owned(),
        ));
    }
    let native = Engine::check_native(root.join(NATIVE_DIRECTORY))?;
    if !native.is_ok() {
        return Err(Error::Execution(format!(
            "service backup Native integrity check found {} error(s)",
            native.errors().len()
        )));
    }
    Ok(ServiceBackupReport {
        location: location.to_owned(),
        database_id: manifest.database_id,
        service_state_included: manifest.service_state_included,
        files: manifest.files.len() as u64,
        bytes,
    })
}

pub(super) fn database_id(database: &Path) -> Result<String> {
    #[derive(Deserialize)]
    struct Marker {
        database_id: String,
    }
    let path = database.join(".rustdb");
    let metadata =
        fs::symlink_metadata(&path).map_err(|error| Error::io(Some(path.clone()), error))?;
    if metadata.file_type().is_symlink() || !metadata.is_file() || metadata.len() > 4 * 1024 {
        return Err(Error::Execution(
            "Native database marker in backup is unsafe or too large".to_owned(),
        ));
    }
    let bytes = fs::read(&path).map_err(|error| Error::io(Some(path.clone()), error))?;
    let marker: Marker = serde_json::from_slice(&bytes).map_err(|error| {
        Error::Execution(format!("invalid Native database marker in backup: {error}"))
    })?;
    Uuid::parse_str(&marker.database_id)
        .map_err(|_| Error::Execution("Native database id in backup is invalid".to_owned()))?;
    Ok(marker.database_id)
}

fn inventory_shape(manifest: &Manifest) -> Result<()> {
    let native_prefix = format!("{NATIVE_DIRECTORY}/");
    let service_prefix = format!("{SERVICE_DIRECTORY}/");
    if !manifest
        .excluded
        .iter()
        .map(String::as_str)
        .eq(EXCLUDED_STATE.iter().copied())
        || !manifest
            .directories
            .iter()
            .any(|path| path == NATIVE_DIRECTORY)
        || manifest.files.iter().any(|entry| {
            !entry.path.starts_with(&native_prefix) && !allowed_service_file(&entry.path)
        })
        || manifest.directories.iter().any(|path| {
            path != NATIVE_DIRECTORY
                && !path.starts_with(&native_prefix)
                && !allowed_service_directory(path)
        })
    {
        return Err(Error::Execution(
            "service backup inventory contains an unsupported path".to_owned(),
        ));
    }
    let has_service = manifest
        .directories
        .iter()
        .any(|path| path == SERVICE_DIRECTORY)
        || manifest
            .files
            .iter()
            .any(|entry| entry.path.starts_with(&service_prefix));
    if has_service != manifest.service_state_included {
        return Err(Error::Execution(
            "service backup control-state marker does not match its inventory".to_owned(),
        ));
    }
    Ok(())
}

fn allowed_service_file(path: &str) -> bool {
    let Some(relative) = path.strip_prefix("service/") else {
        return false;
    };
    if STATE_FILES.contains(&relative) {
        return true;
    }
    if let Some(name) = relative.strip_prefix("profile-tokens/") {
        return !name.contains('/') && name.ends_with(".token");
    }
    matches!(
        relative,
        "connection.rustdb-profile/profile.json"
            | "connection.rustdb-profile/ca.pem"
            | "connection.rustdb-profile/bearer.token"
    )
}

fn allowed_service_directory(path: &str) -> bool {
    matches!(
        path,
        SERVICE_DIRECTORY | "service/profile-tokens" | "service/connection.rustdb-profile"
    )
}
