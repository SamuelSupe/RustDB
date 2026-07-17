use std::{
    fs::{File, Metadata},
    io::{self, Write},
    os::unix::fs::MetadataExt,
    path::Path,
    sync::Arc,
};

use parking_lot::Mutex;

use crate::{Error, Result};

use super::table::TableSnapshot;

#[derive(Clone)]
pub(super) struct DiskBudget {
    inner: Arc<Mutex<State>>,
}

struct State {
    limit: u64,
    used: u64,
}

pub(super) struct QuotaFile {
    file: File,
    budget: DiskBudget,
}

impl DiskBudget {
    pub(super) fn new(limit: u64) -> Self {
        Self {
            inner: Arc::new(Mutex::new(State { limit, used: 0 })),
        }
    }

    #[cfg(test)]
    pub(super) fn unlimited() -> Self {
        Self::new(u64::MAX)
    }

    pub(super) fn reserve_metadata(&self, bytes: u64, path: &Path) -> Result<()> {
        self.reserve(bytes).map_err(|error| {
            Error::ResourceExhausted(format!(
                "native snapshot metadata at {} exceeds its disk budget: {error}",
                path.display()
            ))
        })
    }

    pub(super) fn used(&self) -> u64 {
        self.inner.lock().used
    }

    #[cfg(test)]
    pub(super) fn remaining(&self) -> u64 {
        let state = self.inner.lock();
        state.limit.saturating_sub(state.used)
    }

    #[cfg(test)]
    pub(super) fn release_deleted_file(&self, bytes: u64) {
        self.release(bytes);
    }

    pub(super) fn raise_limit(&self, limit: u64) {
        let mut state = self.inner.lock();
        state.limit = state.limit.max(limit);
    }

    fn reserve(&self, bytes: u64) -> io::Result<()> {
        let mut state = self.inner.lock();
        let projected = state
            .used
            .checked_add(bytes)
            .ok_or_else(|| io::Error::other("native snapshot disk byte count overflow"))?;
        if projected > state.limit {
            return Err(io::Error::other(format!(
                "write requires {bytes} bytes with {} already used, exceeding the {} byte limit",
                state.used, state.limit
            )));
        }
        state.used = projected;
        Ok(())
    }

    fn release(&self, bytes: u64) {
        let mut state = self.inner.lock();
        state.used = state.used.saturating_sub(bytes);
    }
}

impl QuotaFile {
    pub(super) fn new(file: File, budget: DiskBudget) -> Self {
        Self { file, budget }
    }

    pub(super) fn sync_all(&self) -> io::Result<()> {
        self.file.sync_all()
    }

    pub(super) fn metadata(&self) -> io::Result<Metadata> {
        self.file.metadata()
    }
}

impl Write for QuotaFile {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let requested = u64::try_from(bytes.len())
            .map_err(|_| io::Error::other("native write size does not fit in u64"))?;
        self.budget.reserve(requested)?;
        match self.file.write(bytes) {
            Ok(written) => {
                self.budget
                    .release(requested.saturating_sub(written as u64));
                Ok(written)
            }
            Err(error) => {
                self.budget.release(requested);
                Err(error)
            }
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        self.file.flush()
    }
}

pub(super) fn snapshot_storage_bytes(root: &Path, snapshot: &TableSnapshot) -> Result<u64> {
    directories_storage_bytes(snapshot.reachable_directories(root))
}

pub(super) fn directories_storage_bytes(
    directories: impl IntoIterator<Item = std::path::PathBuf>,
) -> Result<u64> {
    let mut inodes = std::collections::HashSet::new();
    let mut total = 0_u64;
    for directory in directories {
        total = total
            .checked_add(directory_bytes(&directory, &mut inodes)?)
            .ok_or_else(|| {
                Error::ResourceExhausted("native storage byte count overflow".to_owned())
            })?;
    }
    Ok(total)
}

fn directory_bytes(
    directory: &Path,
    inodes: &mut std::collections::HashSet<(u64, u64)>,
) -> Result<u64> {
    let mut total = 0_u64;
    for entry in std::fs::read_dir(directory)
        .map_err(|error| Error::io(Some(directory.to_path_buf()), error))?
    {
        let entry = entry.map_err(|error| Error::io(Some(directory.to_path_buf()), error))?;
        let path = entry.path();
        let metadata = std::fs::symlink_metadata(&path)
            .map_err(|error| Error::io(Some(path.clone()), error))?;
        if metadata.file_type().is_symlink() {
            return Err(Error::native_storage(
                path,
                "managed native storage must not contain symlinks",
            ));
        }
        if metadata.is_dir() {
            total = total
                .checked_add(directory_bytes(&path, inodes)?)
                .ok_or_else(|| {
                    Error::ResourceExhausted("native storage byte count overflow".to_owned())
                })?;
        } else if metadata.is_file() && inodes.insert((metadata.dev(), metadata.ino())) {
            total = total.checked_add(metadata.len()).ok_or_else(|| {
                Error::ResourceExhausted("native storage byte count overflow".to_owned())
            })?;
        } else if !metadata.is_file() {
            return Err(Error::native_storage(
                path,
                "managed native storage contains an unsupported file type",
            ));
        }
    }
    Ok(total)
}

#[cfg(test)]
mod tests;
