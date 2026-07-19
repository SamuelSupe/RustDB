use std::{fs, path::Path};

use uuid::Uuid;

use crate::{Error, Result};

use super::super::io;
use super::{
    INITIALIZING_PREFIX, MAX_TRANSACTION_MARKER_BYTES, Marker, OwnedStaging, TRANSACTION_MARKER,
};

enum StagingEntry {
    Initializing(String),
    Transaction(String),
}

pub(in crate::storage::native) fn recover_staging(
    database_root: &Path,
    database_id: &str,
) -> Result<()> {
    let staging = database_root.join("staging");
    let mut removable = Vec::new();
    for entry in fs::read_dir(&staging).map_err(|error| Error::io(Some(staging.clone()), error))? {
        let entry = entry.map_err(|error| Error::io(Some(staging.clone()), error))?;
        let path = entry.path();
        let Some(kind) = classify(&path) else {
            continue;
        };
        require_managed_directory(&path)?;
        match kind {
            StagingEntry::Initializing(_) => removable.push(path),
            StagingEntry::Transaction(transaction_id) => {
                if has_matching_marker(&path, database_id, &transaction_id)? {
                    removable.push(path);
                }
            }
        }
    }
    for path in &removable {
        require_managed_directory(path)?;
        remove(path)?;
    }
    Ok(())
}

pub(in crate::storage::native) fn inspect_owned_staging(
    database_root: &Path,
    database_id: &str,
) -> Result<Vec<OwnedStaging>> {
    let staging = database_root.join("staging");
    let mut owned = Vec::new();
    for entry in fs::read_dir(&staging).map_err(|error| Error::io(Some(staging.clone()), error))? {
        let entry = entry.map_err(|error| Error::io(Some(staging.clone()), error))?;
        let path = entry.path();
        let Some(kind) = classify(&path) else {
            continue;
        };
        require_managed_directory(&path)?;
        let transaction_id = match kind {
            StagingEntry::Initializing(transaction_id)
            | StagingEntry::Transaction(transaction_id) => transaction_id,
        };
        if has_matching_marker(&path, database_id, &transaction_id)? {
            owned.push(OwnedStaging {
                path,
                transaction_id,
            });
        }
    }
    owned.sort_by(|left, right| left.path.cmp(&right.path));
    Ok(owned)
}

fn classify(path: &Path) -> Option<StagingEntry> {
    let name = path.file_name()?.to_str()?;
    if let Some(transaction_id) = name.strip_prefix(INITIALIZING_PREFIX) {
        return canonical_uuid(transaction_id)
            .then(|| StagingEntry::Initializing(transaction_id.to_owned()));
    }
    canonical_uuid(name).then(|| StagingEntry::Transaction(name.to_owned()))
}

fn canonical_uuid(value: &str) -> bool {
    Uuid::parse_str(value)
        .map(|uuid| uuid.to_string() == value)
        .unwrap_or(false)
}

fn require_managed_directory(path: &Path) -> Result<()> {
    let metadata =
        fs::symlink_metadata(path).map_err(|error| Error::io(Some(path.to_path_buf()), error))?;
    if metadata.file_type().is_symlink() {
        return Err(Error::native_storage(
            path,
            "managed staging entry must not be a symlink",
        ));
    }
    if !metadata.is_dir() {
        return Err(Error::native_storage(
            path,
            "managed staging entry is not a directory",
        ));
    }
    Ok(())
}

fn has_matching_marker(path: &Path, database_id: &str, transaction_id: &str) -> Result<bool> {
    let marker_path = path.join(TRANSACTION_MARKER);
    let metadata = match fs::symlink_metadata(&marker_path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(Error::io(Some(marker_path), error)),
    };
    if metadata.file_type().is_symlink() {
        return Err(Error::native_storage(
            &marker_path,
            "transaction marker must not be a symlink",
        ));
    }
    if !metadata.is_file() {
        return Ok(false);
    }
    let bytes = io::read_bounded(
        &marker_path,
        MAX_TRANSACTION_MARKER_BYTES,
        "transaction marker",
    )?;
    let marker: Marker = match serde_json::from_slice(&bytes) {
        Ok(marker) => marker,
        Err(_) => return Ok(false),
    };
    Ok(marker.database_id == database_id && marker.transaction_id == transaction_id)
}

fn remove(path: &Path) -> Result<()> {
    let parent = path
        .parent()
        .ok_or_else(|| Error::native_storage(path, "staging entry has no parent"))?;
    fs::remove_dir_all(path).map_err(|error| Error::io(Some(path.to_path_buf()), error))?;
    io::sync_dir(parent)
}

#[cfg(test)]
mod tests {
    use std::{fs, os::unix::fs::symlink};

    use super::*;

    #[test]
    fn removes_reserved_initialization_and_preserves_unknown_entries() {
        let directory = tempfile::tempdir().unwrap();
        let staging = directory.path().join("staging");
        fs::create_dir(&staging).unwrap();
        let transaction_id = Uuid::new_v4().to_string();
        let initializing = staging.join(format!("{INITIALIZING_PREFIX}{transaction_id}"));
        let unknown = staging.join("keep-me");
        let lookalike = staging.join(format!("{INITIALIZING_PREFIX}NOT-A-UUID"));
        let noncanonical = staging.join(format!(
            "{INITIALIZING_PREFIX}{}",
            "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa".to_uppercase()
        ));
        let outside = directory.path().join("outside");
        let unknown_symlink = staging.join("keep-link");
        fs::create_dir(&initializing).unwrap();
        fs::create_dir(&unknown).unwrap();
        fs::create_dir(&lookalike).unwrap();
        fs::create_dir(&noncanonical).unwrap();
        fs::create_dir(&outside).unwrap();
        symlink(&outside, &unknown_symlink).unwrap();

        recover_staging(directory.path(), "database").unwrap();

        assert!(!initializing.exists());
        assert!(unknown.is_dir());
        assert!(lookalike.is_dir());
        assert!(noncanonical.is_dir());
        assert!(unknown_symlink.is_symlink());
        assert!(outside.is_dir());
    }

    #[test]
    fn rejects_a_reserved_initialization_symlink() {
        let directory = tempfile::tempdir().unwrap();
        let staging = directory.path().join("staging");
        let outside = directory.path().join("outside");
        fs::create_dir(&staging).unwrap();
        fs::create_dir(&outside).unwrap();
        let transaction_id = Uuid::new_v4().to_string();
        let initializing = staging.join(format!("{INITIALIZING_PREFIX}{transaction_id}"));
        symlink(&outside, &initializing).unwrap();

        assert!(matches!(
            recover_staging(directory.path(), "database").unwrap_err(),
            Error::NativeStorage { .. }
        ));
        assert!(outside.is_dir());
        assert!(initializing.is_symlink());
    }
}
