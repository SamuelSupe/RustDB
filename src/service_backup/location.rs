use std::{
    fs,
    path::{Path, PathBuf},
};

use crate::{Error, Result};

pub(super) fn local_path(location: &str) -> Result<PathBuf> {
    if !location.starts_with("file://") {
        return Ok(PathBuf::from(location));
    }
    let url = url::Url::parse(location)
        .map_err(|error| Error::InvalidArgument(format!("invalid file URI: {error}")))?;
    if !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err(Error::InvalidArgument(
            "file URI must not contain credentials, query, or fragment".to_owned(),
        ));
    }
    url.to_file_path()
        .map_err(|()| Error::InvalidArgument("file URI is not a local path".to_owned()))
}

pub(super) fn refuse_existing(path: &Path, kind: &str) -> Result<()> {
    match fs::symlink_metadata(path) {
        Ok(_) => Err(Error::InvalidArgument(format!(
            "{kind} target already exists: {}",
            path.display()
        ))),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(Error::io(Some(path.to_path_buf()), error)),
    }
}

pub(super) fn publish(source: &Path, destination: &Path) -> Result<()> {
    #[cfg(any(target_os = "linux", target_vendor = "apple"))]
    {
        rustix::fs::renameat_with(
            rustix::fs::CWD,
            source,
            rustix::fs::CWD,
            destination,
            rustix::fs::RenameFlags::NOREPLACE,
        )
        .map_err(|error| {
            let error = std::io::Error::from(error);
            if error.kind() == std::io::ErrorKind::AlreadyExists {
                Error::InvalidArgument(format!(
                    "publication target already exists: {}",
                    destination.display()
                ))
            } else {
                Error::io(Some(destination.to_path_buf()), error)
            }
        })?;
    }
    #[cfg(not(any(target_os = "linux", target_vendor = "apple")))]
    {
        refuse_existing(destination, "publication")?;
        fs::rename(source, destination)
            .map_err(|error| Error::io(Some(destination.to_path_buf()), error))?;
    }
    if let Some(parent) = destination.parent() {
        super::files::sync_dir(parent).map_err(|error| {
            Error::commit_outcome_unknown(
                destination,
                "service-backup-publication",
                format!(
                    "service backup was renamed into place but publication durability could not be confirmed: {error}"
                ),
            )
        })?;
    }
    Ok(())
}
