use std::{
    ffi::OsStr,
    fs::{self, File, OpenOptions, Permissions},
    io::{self, Write},
    os::unix::fs::{OpenOptionsExt, PermissionsExt},
    path::Path,
};

use sha2::{Digest, Sha256};

use crate::{Error, Result};

pub(super) struct Sha256Writer<W> {
    inner: W,
    digest: Sha256,
}

impl<W> Sha256Writer<W> {
    pub(super) fn new(inner: W) -> Self {
        Self {
            inner,
            digest: Sha256::new(),
        }
    }

    pub(super) fn inner(&self) -> &W {
        &self.inner
    }

    pub(super) fn sha256(&self) -> String {
        format!("{:x}", self.digest.clone().finalize())
    }
}

impl<W: Write> Write for Sha256Writer<W> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let written = self.inner.write(bytes)?;
        self.digest.update(&bytes[..written]);
        Ok(written)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

pub(super) fn require_segment_path(path: &Path) -> Result<()> {
    if path.extension() != Some(OsStr::new("rdbseg")) {
        return Err(Error::native_storage(
            path,
            "native segment path must end in .rdbseg",
        ));
    }
    if path.file_stem().is_none_or(OsStr::is_empty) {
        return Err(Error::native_storage(path, "native segment name is empty"));
    }
    Ok(())
}

pub(super) fn open_private(path: &Path) -> Result<File> {
    let file = OpenOptions::new()
        .create_new(true)
        .read(true)
        .write(true)
        .mode(0o600)
        .open(path)
        .map_err(|error| Error::io(Some(path.to_path_buf()), error))?;
    if let Err(error) = file.set_permissions(Permissions::from_mode(0o600)) {
        drop(file);
        return Err(cleanup_error(
            path,
            Error::io(Some(path.to_path_buf()), error),
        ));
    }
    Ok(file)
}

pub(super) fn sync_parent(path: &Path) -> Result<()> {
    let parent = path
        .parent()
        .ok_or_else(|| Error::native_storage(path, "native segment has no parent directory"))?;
    File::open(parent)
        .and_then(|directory| directory.sync_all())
        .map_err(|error| Error::io(Some(parent.to_path_buf()), error))
}

pub(super) fn cleanup_error(path: &Path, original: Error) -> Error {
    match fs::remove_file(path) {
        Ok(()) => original,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => original,
        Err(error) => Error::native_storage(
            path,
            format!("{original}; partial segment cleanup failed: {error}"),
        ),
    }
}
