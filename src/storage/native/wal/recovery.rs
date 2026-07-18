use crate::{Error, Result};

use super::{CatalogCommit, Wal};
use std::path::Path;

use crate::storage::native::manifest;

pub(super) fn replay_catalog(root: &Path, database_id: &str, wal: &Wal) -> Result<()> {
    let mut current = manifest::load(root, database_id)?;
    let mut commits = wal.catalog_commits();
    commits.sort_unstable_by_key(|commit| commit.generation);

    for commit in commits {
        if commit.generation <= current.generation() {
            continue;
        }
        if commit.expected_generation != current.generation() {
            return Err(gap_error(root, &commit, current.generation()));
        }
        let prepared = manifest::load_generation(root, database_id, commit.generation)?;
        if prepared.transaction_id() != Some(commit.transaction_id.as_str()) {
            return Err(Error::native_storage(
                root,
                format!(
                    "WAL transaction {} does not own catalog generation {}",
                    commit.transaction_id, commit.generation
                ),
            ));
        }
        current = manifest::publish_prepared(
            root,
            database_id,
            commit.expected_generation,
            prepared,
            &commit.transaction_id,
        )?;
    }
    Ok(())
}

fn gap_error(root: &Path, commit: &CatalogCommit, current: u64) -> Error {
    Error::native_storage(
        root,
        format!(
            "WAL catalog generation gap for transaction {}: expected current {}, found {current}",
            commit.transaction_id, commit.expected_generation
        ),
    )
}
