use std::{
    collections::{HashMap, hash_map::Entry},
    path::{Path, PathBuf},
    sync::atomic::{AtomicBool, Ordering},
};

use parking_lot::Mutex;

use crate::{Error, Result};

use super::super::{MemoryPool, MemoryReservation};
use super::SpillCharge;

/// Fixed portion of the reservation retained for every active spill file.
///
/// 1 KiB covers the hash-map bucket/key object, `MemoryReservation`, task/vector
/// slots and allocator slack. Path storage is charged separately for four
/// concurrent owned copies: the registry, returned `SpillFile`, partition file
/// vector, and a pending/repartition task clone.
pub(super) const ACTIVE_FILE_METADATA_BASE_BYTES: usize = 1_024;
const ACTIVE_FILE_PATH_COPIES: usize = 4;

#[derive(Debug)]
pub(super) struct ActiveFiles {
    memory: MemoryPool,
    entries: Mutex<HashMap<PathBuf, ActiveFile>>,
    retain_charges_on_drop: AtomicBool,
}

#[derive(Debug)]
struct ActiveFile {
    _memory: MemoryReservation,
    charge: Option<SpillCharge>,
}

impl ActiveFiles {
    pub(super) fn new(memory: MemoryPool) -> Self {
        Self {
            memory,
            entries: Mutex::new(HashMap::new()),
            retain_charges_on_drop: AtomicBool::new(false),
        }
    }

    pub(super) fn insert(&self, path: PathBuf, cleaned: &AtomicBool) -> Result<()> {
        let active = self.entries.lock().len();
        let required = active_file_metadata_bytes(&path);
        let reservation = self.memory.try_reserve(required).map_err(|error| {
            Error::ResourceExhausted(format!(
                "spill active-file metadata cannot reserve \
                     {required} bytes (active files {active}, used {}, limit \
                     {}): {error}",
                self.memory.used(),
                self.memory.limit(),
            ))
        })?;

        let mut entries = self.entries.lock();
        if cleaned.load(Ordering::Acquire) {
            return Err(Error::Cancelled);
        }
        match entries.entry(path) {
            Entry::Vacant(entry) => {
                entry.insert(ActiveFile {
                    _memory: reservation,
                    charge: None,
                });
                Ok(())
            }
            Entry::Occupied(entry) => {
                debug_assert!(
                    false,
                    "duplicate active spill path: {}",
                    entry.key().display()
                );
                Err(Error::Internal(format!(
                    "duplicate active spill path allocation: {}",
                    entry.key().display()
                )))
            }
        }
    }

    pub(super) fn contains(&self, path: &Path) -> bool {
        self.entries.lock().contains_key(path)
    }

    pub(super) fn add_charge(&self, path: &Path, charge: SpillCharge) -> Result<()> {
        let mut entries = self.entries.lock();
        let entry = entries.get_mut(path).ok_or(Error::Cancelled)?;
        if let Some(existing) = &mut entry.charge {
            existing.merge(charge)?;
        } else {
            entry.charge = Some(charge);
        }
        Ok(())
    }

    pub(super) fn remove(&self, path: &Path) {
        let entry = self.entries.lock().remove(path);
        drop(entry);
    }

    pub(super) fn clear(&self) {
        let entries = std::mem::take(&mut *self.entries.lock());
        drop(entries);
    }

    pub(super) fn retain_charges_on_drop(&self) {
        self.retain_charges_on_drop.store(true, Ordering::Release);
    }
}

impl Drop for ActiveFiles {
    fn drop(&mut self) {
        if !self.retain_charges_on_drop.load(Ordering::Acquire) {
            return;
        }
        for active in self.entries.get_mut().values_mut() {
            if let Some(charge) = active.charge.take() {
                charge.retain_engine();
            }
        }
    }
}

pub(super) fn active_file_metadata_bytes(path: &Path) -> usize {
    ACTIVE_FILE_METADATA_BASE_BYTES
        .saturating_add(path_storage_bytes(path).saturating_mul(ACTIVE_FILE_PATH_COPIES))
}

#[cfg(unix)]
fn path_storage_bytes(path: &Path) -> usize {
    use std::os::unix::ffi::OsStrExt;
    path.as_os_str().as_bytes().len()
}

#[cfg(not(unix))]
fn path_storage_bytes(path: &Path) -> usize {
    // UTF-16 paths can consume two bytes per lossy UTF-8 character even when
    // the replacement representation is shorter than the original OS string.
    path.to_string_lossy().len().saturating_mul(2)
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::{
        ACTIVE_FILE_METADATA_BASE_BYTES, ACTIVE_FILE_PATH_COPIES, active_file_metadata_bytes,
        path_storage_bytes,
    };

    #[test]
    fn active_file_charge_grows_with_the_full_path() {
        let short = Path::new("/spill/a.arrow");
        let long = Path::new("/spill/a-very-long-query-and-partition-name.arrow");
        let short_charge = active_file_metadata_bytes(short);
        let long_charge = active_file_metadata_bytes(long);

        assert!(short_charge >= ACTIVE_FILE_METADATA_BASE_BYTES);
        assert_eq!(
            long_charge - short_charge,
            (path_storage_bytes(long) - path_storage_bytes(short)) * ACTIVE_FILE_PATH_COPIES
        );
    }
}
