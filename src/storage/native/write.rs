use std::{
    fs,
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{Error, Result};

use super::io;

mod initialization;
mod recovery;

pub(super) use recovery::recover_staging;

const INITIALIZING_PREFIX: &str = ".rustdb-transaction-init-";
const TRANSACTION_MARKER: &str = ".rustdb-transaction";
const MAX_TRANSACTION_MARKER_BYTES: usize = 4 * 1024;

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Marker {
    database_id: String,
    transaction_id: String,
}

pub(crate) struct StagedSnapshot {
    root: PathBuf,
    snapshot: PathBuf,
    marker: Marker,
    active: bool,
}

impl StagedSnapshot {
    pub(crate) fn begin(database_root: &Path, database_id: &str) -> Result<Self> {
        let transaction_id = Uuid::new_v4().to_string();
        let marker = Marker {
            database_id: database_id.to_owned(),
            transaction_id,
        };
        let (root, snapshot) = initialization::create(database_root, &marker)?;

        Ok(Self {
            root,
            snapshot,
            marker,
            active: true,
        })
    }

    pub(crate) fn snapshot_directory(&self) -> &Path {
        &self.snapshot
    }

    pub(crate) fn path(&self) -> &Path {
        &self.root
    }

    pub(crate) fn database_id(&self) -> &str {
        &self.marker.database_id
    }

    pub(crate) fn transaction_id(&self) -> &str {
        &self.marker.transaction_id
    }

    pub(crate) fn segment_path(&self, segment_id: &str) -> PathBuf {
        self.snapshot
            .join("segments")
            .join(format!("{segment_id}.rdbseg"))
    }

    #[cfg(test)]
    pub(crate) fn predicate_sidecar_path(&self, segment_id: &str) -> PathBuf {
        self.snapshot
            .join("segments")
            .join(format!("{segment_id}.rdbpred"))
    }

    pub(crate) fn publish(mut self, destination: &Path) -> Result<()> {
        let parent = destination
            .parent()
            .ok_or_else(|| Error::native_storage(destination, "snapshot has no parent"))?;
        prepare_destination_parent(parent)?;
        if destination.exists() {
            return Err(Error::native_storage(
                destination,
                "snapshot destination already exists",
            ));
        }
        fs::rename(&self.snapshot, destination)
            .map_err(|error| Error::io(Some(destination.to_path_buf()), error))?;
        io::sync_dir(parent)?;
        self.remove_transaction_root()?;
        self.active = false;
        Ok(())
    }

    pub(crate) fn abort(mut self) -> Result<()> {
        self.remove_transaction_root()?;
        self.active = false;
        Ok(())
    }

    fn remove_transaction_root(&self) -> Result<()> {
        verify_marker(&self.root, &self.marker)?;
        fs::remove_dir_all(&self.root)
            .map_err(|error| Error::io(Some(self.root.clone()), error))?;
        io::sync_dir(
            self.root
                .parent()
                .ok_or_else(|| Error::native_storage(&self.root, "staging path has no parent"))?,
        )
    }
}

impl Drop for StagedSnapshot {
    fn drop(&mut self) {
        if self.active
            && let Err(error) = verify_marker(&self.root, &self.marker).and_then(|()| {
                fs::remove_dir_all(&self.root)
                    .map_err(|error| Error::io(Some(self.root.clone()), error))
            })
        {
            tracing::error!(%error, path = %self.root.display(), "failed to clean native staging transaction");
        }
    }
}

fn prepare_destination_parent(parent: &Path) -> Result<()> {
    let snapshots = parent;
    let table = snapshots
        .parent()
        .ok_or_else(|| Error::native_storage(parent, "snapshot parent has no table directory"))?;
    match fs::symlink_metadata(table) {
        Ok(_) => io::require_directory(table)?,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            io::create_private_dir_all(table)?;
        }
        Err(error) => return Err(Error::io(Some(table.to_path_buf()), error)),
    }
    match fs::symlink_metadata(snapshots) {
        Ok(_) => io::require_directory(snapshots),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            io::create_private_dir_all(snapshots)
        }
        Err(error) => Err(Error::io(Some(snapshots.to_path_buf()), error)),
    }
}

fn verify_marker(root: &Path, expected: &Marker) -> Result<()> {
    let path = root.join(TRANSACTION_MARKER);
    let bytes = io::read_bounded(&path, MAX_TRANSACTION_MARKER_BYTES, "transaction marker")?;
    let actual: Marker = serde_json::from_slice(&bytes).map_err(|error| {
        Error::native_storage(&path, format!("invalid transaction marker: {error}"))
    })?;
    if actual.database_id != expected.database_id
        || actual.transaction_id != expected.transaction_id
    {
        return Err(Error::native_storage(
            &path,
            "transaction marker identity mismatch",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::os::unix::fs::PermissionsExt;

    use super::*;

    #[test]
    fn begin_publishes_only_a_marker_ready_transaction_directory() {
        let directory = tempfile::tempdir().unwrap();
        let staging = directory.path().join("staging");
        fs::create_dir(&staging).unwrap();

        let staged = StagedSnapshot::begin(directory.path(), "database").unwrap();

        assert_eq!(
            staged.path().file_name().unwrap().to_str().unwrap(),
            staged.transaction_id()
        );
        verify_marker(staged.path(), &staged.marker).unwrap();
        assert_eq!(
            fs::metadata(staged.path()).unwrap().permissions().mode() & 0o777,
            0o700
        );
        assert!(fs::read_dir(&staging).unwrap().all(|entry| {
            !entry
                .unwrap()
                .file_name()
                .to_string_lossy()
                .starts_with(INITIALIZING_PREFIX)
        }));
        staged.abort().unwrap();
    }
}
