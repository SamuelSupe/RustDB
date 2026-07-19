use std::{
    fs::{self, File},
    os::unix::fs::PermissionsExt,
    path::Path,
};

use super::NativeRepairAction;
use crate::{Error, Result};

pub(super) fn execute(root: &Path, actions: &[NativeRepairAction]) -> Result<u64> {
    let mut applied = 0_u64;
    for action in actions {
        match action {
            NativeRepairAction::SetPermissions { path, mode } => {
                require_planned(root, action)?;
                set_permissions(path, *mode)?;
            }
            NativeRepairAction::RemoveOwnedStaging {
                path,
                transaction_id,
            } => remove_owned_staging(root, path, transaction_id, action)?,
            NativeRepairAction::RemoveAtomicTemporary { path, target } => {
                remove_atomic(path, target, action, root)?;
            }
            NativeRepairAction::RestoreCurrent { path, generation } => {
                require_planned(root, action)?;
                restore_current(path, *generation)?;
            }
        }
        applied = applied.saturating_add(1);
    }
    Ok(applied)
}

fn set_permissions(path: &Path, mode: u32) -> Result<()> {
    let metadata =
        fs::symlink_metadata(path).map_err(|error| Error::io(Some(path.to_path_buf()), error))?;
    if metadata.file_type().is_symlink() || (!metadata.is_file() && !metadata.is_dir()) {
        return Err(Error::native_repair_refused(
            path,
            "permission target changed type after planning",
        ));
    }
    fs::set_permissions(path, fs::Permissions::from_mode(mode))
        .map_err(|error| Error::io(Some(path.to_path_buf()), error))?;
    if metadata.is_dir() {
        super::super::io::sync_dir(path)?;
    } else {
        File::open(path)
            .and_then(|file| file.sync_all())
            .map_err(|error| Error::io(Some(path.to_path_buf()), error))?;
    }
    sync_parent(path)
}

fn remove_owned_staging(
    root: &Path,
    path: &Path,
    transaction_id: &str,
    action: &NativeRepairAction,
) -> Result<()> {
    require_planned(root, action)?;
    let marker_still_matches = super::super::write::inspect_owned_staging(
        root,
        super::super::marker::read(&root.join(super::super::MARKER_FILE))?.database_id(),
    )?
    .into_iter()
    .any(|owned| owned.path == path && owned.transaction_id == transaction_id);
    if !marker_still_matches {
        return Err(Error::native_repair_refused(
            path,
            "staging owner marker changed after planning",
        ));
    }
    let metadata =
        fs::symlink_metadata(path).map_err(|error| Error::io(Some(path.to_path_buf()), error))?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(Error::native_repair_refused(
            path,
            "owned staging path is no longer a directory",
        ));
    }
    fs::remove_dir_all(path).map_err(|error| Error::io(Some(path.to_path_buf()), error))?;
    sync_parent(path)
}

fn remove_atomic(
    path: &Path,
    target: &Path,
    action: &NativeRepairAction,
    root: &Path,
) -> Result<()> {
    require_planned(root, action)?;
    if super::cleanup::atomic_target(path).as_deref() != Some(target)
        || !super::cleanup::same_contents(path, target)?
    {
        return Err(Error::native_repair_refused(
            path,
            "atomic temporary or its target changed after planning",
        ));
    }
    fs::remove_file(path).map_err(|error| Error::io(Some(path.to_path_buf()), error))?;
    sync_parent(path)
}

fn restore_current(path: &Path, generation: u64) -> Result<()> {
    let bytes = format!("{generation}\n");
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_file() => Err(
            Error::native_repair_refused(path, "catalog CURRENT is not a regular file"),
        ),
        Ok(_) => super::super::io::atomic_replace(
            path,
            bytes.as_bytes(),
            &uuid::Uuid::new_v4().to_string(),
        ),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            super::super::io::atomic_create(path, bytes.as_bytes())
        }
        Err(error) => Err(Error::io(Some(path.to_path_buf()), error)),
    }
}

fn require_planned(root: &Path, expected: &NativeRepairAction) -> Result<()> {
    let plan = super::plan::build(root)?;
    if !plan.blockers().is_empty() || !plan.actions().contains(expected) {
        return Err(Error::native_repair_refused(
            expected.path(),
            "repair action is no longer safe after revalidation",
        ));
    }
    Ok(())
}

fn sync_parent(path: &Path) -> Result<()> {
    let parent = path
        .parent()
        .ok_or_else(|| Error::native_repair_refused(path, "repair target has no parent"))?;
    super::super::io::sync_dir(parent)
}
