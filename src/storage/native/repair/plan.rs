use std::{
    collections::BTreeSet,
    fs,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    time::SystemTime,
};

use super::{NativeRepairAction, NativeRepairPlan};
use crate::{Error, Result};

pub(super) fn build(path: &Path) -> Result<NativeRepairPlan> {
    let before = super::super::check::database(path)?;
    let mut blockers = Vec::new();
    let current_path = path.join("catalog").join("CURRENT");
    let current_only_failure = !before.errors().is_empty()
        && before
            .errors()
            .iter()
            .all(|issue| issue.path() == Some(current_path.as_path()));

    let marker = match super::super::marker::read(&path.join(super::super::MARKER_FILE)) {
        Ok(marker) => marker,
        Err(error) => {
            blockers.extend(before.errors().iter().cloned());
            if blockers.is_empty() {
                blockers.push(super::super::check::NativeCheckIssue::from_error(
                    &error,
                    Some(path.join(super::super::MARKER_FILE)),
                ));
            }
            return Ok(NativeRepairPlan {
                path: path.to_path_buf(),
                before,
                actions: Vec::new(),
                blockers,
            });
        }
    };
    let wal = match super::super::wal::inspect_read_only(path, marker.database_id()) {
        Ok(wal) => wal,
        Err(error) => {
            blockers.push(super::super::check::NativeCheckIssue::from_error(
                &error,
                Some(path.join("wal")),
            ));
            return Ok(NativeRepairPlan {
                path: path.to_path_buf(),
                before,
                actions: Vec::new(),
                blockers,
            });
        }
    };

    let generations = match scan_generations(path, marker.database_id()) {
        Ok(generations) => generations,
        Err(error) => {
            blockers.push(super::super::check::NativeCheckIssue::from_error(
                &error,
                Some(path.join("catalog").join("generations")),
            ));
            return Ok(NativeRepairPlan {
                path: path.to_path_buf(),
                before,
                actions: Vec::new(),
                blockers,
            });
        }
    };
    let mut catalog_transactions = generations
        .iter()
        .filter_map(|(_, state)| state.transaction_id().map(str::to_owned))
        .collect::<BTreeSet<_>>();
    catalog_transactions.extend(wal.referenced_transactions.iter().cloned());

    let mut verified_files = before.verified_files().to_vec();
    verified_files.extend(wal.files.iter().cloned());
    let mut actions = Vec::new();
    if current_only_failure {
        match recovery_candidate(path, marker.database_id(), &wal, &generations) {
            Ok((generation, files)) => {
                verified_files.extend(files);
                actions.push(NativeRepairAction::RestoreCurrent {
                    path: current_path.clone(),
                    generation,
                });
            }
            Err(error) => blockers.push(super::super::check::NativeCheckIssue::from_error(
                &error,
                Some(current_path.clone()),
            )),
        }
    } else if !before.errors().is_empty() {
        blockers.extend(before.errors().iter().cloned());
    }

    if blockers.is_empty() {
        plan_permissions(path, &verified_files, &mut actions, &mut blockers);
        let staging = match super::super::write::inspect_owned_staging(path, marker.database_id()) {
            Ok(staging) => staging,
            Err(error) => {
                blockers.push(super::super::check::NativeCheckIssue::from_error(
                    &error,
                    Some(path.join("staging")),
                ));
                Vec::new()
            }
        };
        let mut removed_roots = Vec::new();
        for owned in staging {
            let old_enough = match super::cleanup::old_enough_tree(&owned.path, SystemTime::now()) {
                Ok(old_enough) => old_enough,
                Err(error) => {
                    blockers.push(super::super::check::NativeCheckIssue::from_error(
                        &error,
                        Some(owned.path.clone()),
                    ));
                    continue;
                }
            };
            if catalog_transactions.contains(&owned.transaction_id) || !old_enough {
                continue;
            }
            removed_roots.push(owned.path.clone());
            actions.push(NativeRepairAction::RemoveOwnedStaging {
                path: owned.path,
                transaction_id: owned.transaction_id,
            });
        }
        let targets = super::cleanup::metadata_targets(&verified_files);
        match super::cleanup::atomic_temporaries(path, &targets, &removed_roots) {
            Ok(temporaries) => actions.extend(temporaries),
            Err(error) => blockers.push(super::super::check::NativeCheckIssue::from_error(
                &error,
                Some(path.to_path_buf()),
            )),
        }
    }

    actions.sort_by(|left, right| {
        action_rank(left)
            .cmp(&action_rank(right))
            .then_with(|| left.path().cmp(right.path()))
    });
    actions.dedup();
    Ok(NativeRepairPlan {
        path: path.to_path_buf(),
        before,
        actions,
        blockers,
    })
}

fn scan_generations(
    root: &Path,
    database_id: &str,
) -> Result<Vec<(u64, super::super::manifest::CatalogState)>> {
    let directory = root.join("catalog").join("generations");
    let mut states = Vec::new();
    for entry in
        fs::read_dir(&directory).map_err(|error| Error::io(Some(directory.clone()), error))?
    {
        let entry = entry.map_err(|error| Error::io(Some(directory.clone()), error))?;
        let path = entry.path();
        let Some(generation) = super::super::manifest::generation_from_name(&path) else {
            continue;
        };
        let state = super::super::manifest::load_generation(root, database_id, generation)?;
        states.push((generation, state));
    }
    states.sort_by_key(|(generation, _)| *generation);
    Ok(states)
}

fn recovery_candidate(
    root: &Path,
    database_id: &str,
    wal: &super::super::wal::WalInspection,
    generations: &[(u64, super::super::manifest::CatalogState)],
) -> Result<(u64, Vec<PathBuf>)> {
    let required = wal
        .committed_generations
        .keys()
        .copied()
        .max()
        .unwrap_or(0)
        .max(wal.checkpoint_catalog_generation);
    let (highest, state) = generations.last().ok_or_else(|| {
        Error::native_repair_refused(
            root.join("catalog").join("generations"),
            "no valid catalog generation is available",
        )
    })?;
    if *highest != required {
        return Err(if *highest < required {
            Error::native_repair_refused(
                root.join("catalog").join("generations"),
                format!("committed catalog generation {required} is missing or invalid"),
            )
        } else {
            Error::native_repair_refused(
                super::super::manifest::generation_path(root, *highest),
                format!(
                    "highest catalog generation {highest} lacks conservative WAL or checkpoint proof"
                ),
            )
        });
    }
    if required > wal.checkpoint_catalog_generation {
        let wal_owner = wal.committed_generations.get(&required).ok_or_else(|| {
            Error::native_repair_refused(root, "highest catalog generation lacks WAL commit proof")
        })?;
        if state.transaction_id() != Some(wal_owner.as_str()) {
            return Err(Error::native_repair_refused(
                super::super::manifest::generation_path(root, required),
                "catalog generation owner does not match its WAL commit",
            ));
        }
    }

    let mut files = vec![
        root.join(super::super::MARKER_FILE),
        super::super::manifest::generation_path(root, required),
    ];
    for reference in state.tables().values() {
        let snapshot = super::super::table::load(root, database_id, reference)?;
        collect_snapshot_files(root, &snapshot, &mut files);
    }
    files.extend(wal.files.iter().cloned());
    Ok((required, files))
}

fn collect_snapshot_files(
    root: &Path,
    snapshot: &super::super::table::TableSnapshot,
    files: &mut Vec<PathBuf>,
) {
    files.push(snapshot.final_directory(root).join("manifest.json"));
    files.extend(
        snapshot
            .reachable_directories(root)
            .into_iter()
            .map(|directory| directory.join(".rustdb-snapshot")),
    );
    files.extend(snapshot.segment_paths(root));
    files.extend(snapshot.predicate_sidecar_paths(root));
    files.extend(snapshot.delete_vector_paths(root));
}

fn plan_permissions(
    root: &Path,
    verified_files: &[PathBuf],
    actions: &mut Vec<NativeRepairAction>,
    blockers: &mut Vec<super::super::check::NativeCheckIssue>,
) {
    let mut directories = BTreeSet::from([root.to_path_buf()]);
    for name in ["catalog", "tables", "staging", "wal"] {
        let path = root.join(name);
        if path.is_dir() {
            directories.insert(path);
        }
    }
    for file in verified_files {
        let mut parent = file.parent();
        while let Some(directory) = parent {
            if !directory.starts_with(root) {
                break;
            }
            directories.insert(directory.to_path_buf());
            if directory == root {
                break;
            }
            parent = directory.parent();
        }
    }
    for directory in directories {
        permission_action(&directory, 0o700, true, actions, blockers);
    }
    let mut files = verified_files.iter().cloned().collect::<BTreeSet<_>>();
    files.insert(root.join(".lock"));
    for file in files {
        permission_action(&file, 0o600, false, actions, blockers);
    }
}

fn permission_action(
    path: &Path,
    mode: u32,
    directory: bool,
    actions: &mut Vec<NativeRepairAction>,
    blockers: &mut Vec<super::super::check::NativeCheckIssue>,
) {
    match fs::symlink_metadata(path) {
        Ok(metadata)
            if !metadata.file_type().is_symlink()
                && ((directory && metadata.is_dir()) || (!directory && metadata.is_file())) =>
        {
            if metadata.permissions().mode() & 0o777 != mode {
                actions.push(NativeRepairAction::SetPermissions {
                    path: path.to_path_buf(),
                    mode,
                });
            }
        }
        Ok(_) => blockers.push(super::super::check::NativeCheckIssue::new(
            "native.repair_refused",
            Some(path.to_path_buf()),
            "managed permission target has an unexpected type or is a symlink",
        )),
        Err(error) => blockers.push(super::super::check::NativeCheckIssue::from_error(
            &Error::io(Some(path.to_path_buf()), error),
            Some(path.to_path_buf()),
        )),
    }
}

fn action_rank(action: &NativeRepairAction) -> u8 {
    match action {
        NativeRepairAction::RestoreCurrent { .. } => 0,
        NativeRepairAction::SetPermissions { .. } => 1,
        NativeRepairAction::RemoveAtomicTemporary { .. } => 2,
        NativeRepairAction::RemoveOwnedStaging { .. } => 3,
    }
}
