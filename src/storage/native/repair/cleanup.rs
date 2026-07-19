use std::{
    collections::BTreeSet,
    fs::{self, File},
    io::Read,
    path::{Path, PathBuf},
    time::{Duration, SystemTime},
};

use sha2::{Digest, Sha256};

use super::NativeRepairAction;
use crate::{Error, Result};

const MIN_CLEANUP_AGE: Duration = Duration::from_secs(24 * 60 * 60);

pub(super) fn metadata_targets(files: &[PathBuf]) -> BTreeSet<PathBuf> {
    files
        .iter()
        .filter(|path| {
            matches!(
                path.extension().and_then(|extension| extension.to_str()),
                Some("json" | "wal")
            ) || matches!(
                path.file_name().and_then(|name| name.to_str()),
                Some(".rustdb" | ".rustdb-init" | ".rustdb-snapshot" | "CURRENT" | "CHECKPOINT")
            )
        })
        .cloned()
        .collect()
}

pub(super) fn atomic_temporaries(
    root: &Path,
    targets: &BTreeSet<PathBuf>,
    removed_roots: &[PathBuf],
) -> Result<Vec<NativeRepairAction>> {
    let mut actions = Vec::new();
    walk(root, &mut |path, metadata| {
        if removed_roots
            .iter()
            .any(|removed| path.starts_with(removed))
        {
            return Ok(());
        }
        let Some(target) = atomic_target(path) else {
            return Ok(());
        };
        if !targets.contains(&target) {
            return Ok(());
        }
        if metadata.file_type().is_symlink() || !metadata.is_file() {
            return Err(Error::native_repair_refused(
                path,
                "recognized atomic temporary is not a regular file",
            ));
        }
        if old_enough(metadata, SystemTime::now()) && same_contents(path, &target)? {
            actions.push(NativeRepairAction::RemoveAtomicTemporary {
                path: path.to_path_buf(),
                target,
            });
        }
        Ok(())
    })?;
    Ok(actions)
}

pub(super) fn old_enough_tree(path: &Path, now: SystemTime) -> Result<bool> {
    let metadata =
        fs::symlink_metadata(path).map_err(|error| Error::io(Some(path.to_path_buf()), error))?;
    if !old_enough(&metadata, now) {
        return Ok(false);
    }
    if metadata.is_dir() && !metadata.file_type().is_symlink() {
        for entry in
            fs::read_dir(path).map_err(|error| Error::io(Some(path.to_path_buf()), error))?
        {
            let entry = entry.map_err(|error| Error::io(Some(path.to_path_buf()), error))?;
            if !old_enough_tree(&entry.path(), now)? {
                return Ok(false);
            }
        }
    }
    Ok(true)
}

fn old_enough(metadata: &fs::Metadata, now: SystemTime) -> bool {
    metadata
        .modified()
        .ok()
        .and_then(|modified| now.duration_since(modified).ok())
        .is_some_and(|age| age >= MIN_CLEANUP_AGE)
}

fn walk(path: &Path, visitor: &mut dyn FnMut(&Path, &fs::Metadata) -> Result<()>) -> Result<()> {
    let metadata =
        fs::symlink_metadata(path).map_err(|error| Error::io(Some(path.to_path_buf()), error))?;
    visitor(path, &metadata)?;
    if metadata.is_dir() && !metadata.file_type().is_symlink() {
        for entry in
            fs::read_dir(path).map_err(|error| Error::io(Some(path.to_path_buf()), error))?
        {
            let entry = entry.map_err(|error| Error::io(Some(path.to_path_buf()), error))?;
            walk(&entry.path(), visitor)?;
        }
    }
    Ok(())
}

pub(super) fn atomic_target(path: &Path) -> Option<PathBuf> {
    let name = path.file_name()?.to_str()?;
    let inner = name.strip_prefix('.')?.strip_suffix(".tmp")?;
    let (target, owner) = inner.rsplit_once('.')?;
    uuid::Uuid::parse_str(owner).ok()?;
    Some(path.parent()?.join(target))
}

pub(super) fn same_contents(left: &Path, right: &Path) -> Result<bool> {
    let left_metadata =
        fs::symlink_metadata(left).map_err(|error| Error::io(Some(left.to_path_buf()), error))?;
    let right_metadata =
        fs::symlink_metadata(right).map_err(|error| Error::io(Some(right.to_path_buf()), error))?;
    if left_metadata.file_type().is_symlink()
        || right_metadata.file_type().is_symlink()
        || !left_metadata.is_file()
        || !right_metadata.is_file()
        || left_metadata.len() != right_metadata.len()
    {
        return Ok(false);
    }
    Ok(file_sha256(left)? == file_sha256(right)?)
}

fn file_sha256(path: &Path) -> Result<[u8; 32]> {
    let mut file = File::open(path).map_err(|error| Error::io(Some(path.to_path_buf()), error))?;
    let mut digest = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = file
            .read(&mut buffer)
            .map_err(|error| Error::io(Some(path.to_path_buf()), error))?;
        if read == 0 {
            break;
        }
        digest.update(&buffer[..read]);
    }
    Ok(digest.finalize().into())
}
