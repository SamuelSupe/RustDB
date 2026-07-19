use std::{collections::BTreeMap, fs, os::unix::fs::PermissionsExt, path::Path, sync::Arc};

use crate::{Error, Result};

use super::{INIT_FILE, MARKER_FILE, io, manifest, marker, table, view::NativeView};

pub(super) fn recover_initialization_temps(
    root: &Path,
    lock_path: &Path,
    marker_path: &Path,
    init_path: &Path,
) -> Result<()> {
    let entries = fs::read_dir(root)
        .map_err(|error| Error::io(Some(root.to_path_buf()), error))?
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|error| Error::io(Some(root.to_path_buf()), error))?;
    let recognized = marker_path.exists() || init_path.exists();
    if !recognized
        && entries.iter().any(|entry| {
            let path = entry.path();
            path != lock_path
                && !io::is_atomic_create_temporary(&path, MARKER_FILE)
                && !io::is_atomic_create_temporary(&path, INIT_FILE)
        })
    {
        return Ok(());
    }

    for entry in entries {
        let path = entry.path();
        if !io::is_atomic_create_temporary(&path, MARKER_FILE)
            && !io::is_atomic_create_temporary(&path, INIT_FILE)
        {
            continue;
        }
        let metadata =
            fs::symlink_metadata(&path).map_err(|error| Error::io(Some(path.clone()), error))?;
        if metadata.file_type().is_symlink() || !metadata.is_file() {
            return Err(Error::native_storage(
                path,
                "initialization temporary path is not a regular file",
            ));
        }
        io::remove_file(&path)?;
    }
    Ok(())
}

pub(super) fn load_tables(
    root: &Path,
    database_id: &str,
    catalog: &manifest::CatalogState,
) -> Result<BTreeMap<String, Arc<table::TableSnapshot>>> {
    catalog
        .tables()
        .iter()
        .map(|(name, reference)| {
            table::load(root, database_id, reference)
                .map(Arc::new)
                .map(|snapshot| (name.clone(), snapshot))
        })
        .collect()
}

pub(super) fn load_views(
    root: &Path,
    catalog: &manifest::CatalogState,
) -> Result<BTreeMap<String, Arc<NativeView>>> {
    catalog
        .views()
        .iter()
        .map(|(name, reference)| {
            NativeView::load(root, reference)
                .map(Arc::new)
                .map(|view| (name.clone(), view))
        })
        .collect()
}

pub(super) fn ensure_root(path: &Path) -> Result<()> {
    match fs::symlink_metadata(path) {
        Ok(metadata) => {
            if metadata.file_type().is_symlink() {
                return Err(Error::native_storage(
                    path,
                    "database path must not be a symlink",
                ));
            }
            if !metadata.is_dir() {
                return Err(Error::native_storage(
                    path,
                    "database path is not a directory",
                ));
            }
            Ok(())
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            io::create_private_dir_all(path)
        }
        Err(error) => Err(Error::io(Some(path.to_path_buf()), error)),
    }
}

pub(super) fn prepare_root_before_lock(root: &Path) -> Result<()> {
    let marker = root.join(MARKER_FILE);
    let initializing = root.join(INIT_FILE);
    let marker_present = regular_marker_if_present(&marker)?;
    let initializing_present = regular_marker_if_present(&initializing)?;
    let recognized = marker_present || initializing_present;
    if !recognized {
        let lock = root.join(".lock");
        for entry in
            fs::read_dir(root).map_err(|error| Error::io(Some(root.to_path_buf()), error))?
        {
            let entry = entry.map_err(|error| Error::io(Some(root.to_path_buf()), error))?;
            let path = entry.path();
            if path == lock {
                io::require_regular_file(&path)?;
                continue;
            }
            if io::is_atomic_create_temporary(&path, MARKER_FILE)
                || io::is_atomic_create_temporary(&path, INIT_FILE)
            {
                io::require_regular_file(&path)?;
                continue;
            }
            return Err(Error::native_storage(
                root,
                "directory is not empty and has no RustDB marker",
            ));
        }
    }

    let permissions = fs::metadata(root)
        .map_err(|error| Error::io(Some(root.to_path_buf()), error))?
        .permissions();
    if permissions.mode() & 0o777 != 0o700 {
        fs::set_permissions(root, fs::Permissions::from_mode(0o700))
            .map_err(|error| Error::io(Some(root.to_path_buf()), error))?;
        io::sync_dir(root)?;
    }
    Ok(())
}

fn regular_marker_if_present(path: &Path) -> Result<bool> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_file() => Err(
            Error::native_storage(path, "database marker path is not a regular file"),
        ),
        Ok(_) => Ok(true),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(Error::io(Some(path.to_path_buf()), error)),
    }
}

pub(super) fn open_existing(root: &Path, marker_path: &Path, init_path: &Path) -> Result<()> {
    let marker = marker::read(marker_path)?;
    io::require_directory(&root.join("catalog"))?;
    io::require_directory(&root.join("catalog").join("generations"))?;
    io::require_directory(&root.join("tables"))?;
    io::require_directory(&root.join("staging"))?;
    io::require_directory(&root.join("wal"))?;
    manifest::validate_current(root, marker.database_id())?;

    if init_path.exists() {
        let initial = marker::read(init_path)?;
        if !marker.same_database(&initial) {
            return Err(Error::native_storage(
                init_path,
                "initialization marker belongs to another database",
            ));
        }
        io::remove_file(init_path)?;
    }
    Ok(())
}

pub(super) fn finish_initialization(root: &Path, marker: &marker::DatabaseMarker) -> Result<()> {
    io::create_private_dir_all(&root.join("catalog").join("generations"))?;
    io::create_private_dir_all(&root.join("tables"))?;
    io::create_private_dir_all(&root.join("staging"))?;
    io::create_private_dir_all(&root.join("wal"))?;
    io::sync_dir(root)?;

    manifest::ensure_initial(root, marker.database_id())?;
    if !root.join(MARKER_FILE).exists() {
        marker::write_new(&root.join(MARKER_FILE), marker)?;
    }
    io::remove_file(&root.join(INIT_FILE))?;
    manifest::validate_current(root, marker.database_id())
}

pub(super) fn directory_contains_only(path: &Path, allowed: &Path) -> Result<bool> {
    for entry in fs::read_dir(path).map_err(|error| Error::io(Some(path.to_path_buf()), error))? {
        let entry = entry.map_err(|error| Error::io(Some(path.to_path_buf()), error))?;
        if entry.path() != allowed {
            return Ok(false);
        }
    }
    Ok(true)
}
