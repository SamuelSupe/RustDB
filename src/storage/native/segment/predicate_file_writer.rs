use std::{fs, io::Write, path::Path};

use crate::{Error, Result};

use super::{
    predicate_sidecar::PredicateSidecarArtifact,
    staging_file::{Sha256Writer, open_private, sync_parent},
};
use crate::storage::native::disk_budget::{DiskBudget, QuotaFile};

pub(in crate::storage::native) fn write(
    path: &Path,
    artifact: &PredicateSidecarArtifact,
    expected_sha256: &str,
    budget: &DiskBudget,
) -> Result<()> {
    let budget_before = budget.used();
    match write_inner(path, artifact, expected_sha256, budget) {
        Ok(()) => Ok(()),
        Err(error) => cleanup(path, budget, budget_before, error),
    }
}

fn write_inner(
    path: &Path,
    artifact: &PredicateSidecarArtifact,
    expected_sha256: &str,
    budget: &DiskBudget,
) -> Result<()> {
    let mut writer = Sha256Writer::new(QuotaFile::new(open_private(path)?, budget.clone()));
    writer
        .write_all(&artifact.bytes)
        .map_err(|error| Error::io(Some(path.to_path_buf()), error))?;
    writer
        .inner()
        .sync_all()
        .map_err(|error| Error::io(Some(path.to_path_buf()), error))?;
    let actual_bytes = writer
        .inner()
        .metadata()
        .map_err(|error| Error::io(Some(path.to_path_buf()), error))?
        .len();
    let actual_sha256 = writer.sha256();
    drop(writer);
    let expected_bytes = u64::try_from(artifact.bytes.len())
        .map_err(|_| Error::ResourceExhausted("predicate sidecar size overflow".to_owned()))?;
    if actual_bytes != expected_bytes || actual_sha256 != expected_sha256 {
        return Err(Error::native_storage(
            path,
            "native predicate sidecar write verification failed",
        ));
    }
    sync_parent(path)
}

fn cleanup(path: &Path, budget: &DiskBudget, budget_before: u64, original: Error) -> Result<()> {
    let charged = budget.used().saturating_sub(budget_before);
    match fs::remove_file(path) {
        Ok(()) => {
            budget.release_deleted_file(charged);
            sync_parent(path).map_err(|cleanup| {
                Error::native_storage(path, format!("{original}; cleanup failed: {cleanup}"))
            })?;
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            budget.release_deleted_file(charged);
        }
        Err(cleanup) => {
            return Err(Error::native_storage(
                path,
                format!("{original}; cleanup failed: {cleanup}"),
            ));
        }
    }
    Err(original)
}
