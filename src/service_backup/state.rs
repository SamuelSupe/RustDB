use std::{fs, path::Path};

use crate::{Error, Result, http_shell::security::SecurityState};

use super::{
    SERVICE_DIRECTORY, STATE_DIRECTORIES, STATE_FILES,
    files::{copy_private_file, create_private_dir},
};

pub(super) fn copy_control(state_root: &Path, database_id: &str, bundle: &Path) -> Result<bool> {
    let source = state_root.join(database_id);
    match fs::symlink_metadata(&source) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(Error::io(Some(source), error)),
        Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_dir() => {
            return Err(Error::InvalidArgument(format!(
                "HTTP state path is unsafe: {}",
                source.display()
            )));
        }
        Ok(_) => {}
    }
    let state = SecurityState::open(state_root, database_id)?;
    let _lock = state.acquire_server_lock()?;
    let target = bundle.join(SERVICE_DIRECTORY);
    create_private_dir(&target)?;
    for name in STATE_FILES {
        copy_if_present(&source.join(name), &target.join(name))?;
    }
    for (directory, allowed_files) in STATE_DIRECTORIES {
        copy_directory(
            &source.join(directory),
            &target.join(directory),
            allowed_files,
        )?;
    }
    Ok(true)
}

fn copy_directory(source: &Path, target: &Path, allowed_files: &[&str]) -> Result<()> {
    let metadata = match fs::symlink_metadata(source) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(Error::io(Some(source.to_path_buf()), error)),
        Ok(metadata) => metadata,
    };
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(Error::InvalidArgument(format!(
            "HTTP control state path is unsafe: {}",
            source.display()
        )));
    }
    create_private_dir(target)?;
    let mut entries = fs::read_dir(source)
        .map_err(|error| Error::io(Some(source.to_path_buf()), error))?
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|error| Error::io(Some(source.to_path_buf()), error))?;
    entries.sort_by_key(|entry| entry.file_name());
    for entry in entries {
        let name = entry.file_name();
        let name = name.to_str().ok_or_else(|| {
            Error::InvalidArgument(format!(
                "HTTP control state has a non-UTF-8 entry in {}",
                source.display()
            ))
        })?;
        let allowed = if allowed_files.is_empty() {
            name.ends_with(".token")
        } else {
            allowed_files.contains(&name)
        };
        if allowed {
            copy_private_file(&entry.path(), &target.join(name))?;
        }
    }
    Ok(())
}

fn copy_if_present(source: &Path, target: &Path) -> Result<()> {
    match fs::symlink_metadata(source) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(Error::io(Some(source.to_path_buf()), error)),
        Ok(_) => copy_private_file(source, target),
    }
}
