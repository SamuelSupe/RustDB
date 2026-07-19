use std::{
    io,
    path::{Path, PathBuf},
    sync::Arc,
};

use parking_lot::Mutex;
use sysinfo::Disks;

use crate::{Error, NativeStorageConfig, Result};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct DiskSpace {
    available_bytes: u64,
    total_bytes: u64,
}

#[derive(Debug)]
struct State {
    pending_bytes: u64,
}

#[derive(Debug)]
struct Inner {
    directory: PathBuf,
    min_free_ratio: f64,
    min_free_bytes: u64,
    state: Mutex<State>,
}

/// Shared admission for all Native writers in one engine.
#[derive(Clone, Debug)]
pub(super) struct DiskAdmission {
    inner: Arc<Inner>,
}

#[derive(Clone, Debug)]
pub(super) struct DiskReservation {
    charge: Arc<ReservationCharge>,
}

#[derive(Debug)]
struct ReservationCharge {
    admission: DiskAdmission,
    bytes: Mutex<u64>,
}

impl DiskAdmission {
    pub(super) fn new(directory: &Path, config: &NativeStorageConfig) -> Self {
        Self {
            inner: Arc::new(Inner {
                directory: directory.to_path_buf(),
                min_free_ratio: config.min_free_ratio,
                min_free_bytes: config.min_free_bytes,
                state: Mutex::new(State { pending_bytes: 0 }),
            }),
        }
    }

    pub(super) fn reserve(&self, bytes: u64) -> Result<DiskReservation> {
        let mut state = self.inner.state.lock();
        self.check(&state, bytes)?;
        state.pending_bytes = state
            .pending_bytes
            .checked_add(bytes)
            .ok_or_else(|| Error::ResourceExhausted("Native disk admission overflowed".into()))?;
        Ok(DiskReservation {
            charge: Arc::new(ReservationCharge {
                admission: self.clone(),
                bytes: Mutex::new(bytes),
            }),
        })
    }

    fn check(&self, state: &State, added: u64) -> Result<()> {
        let disk = system_disk_space(&self.inner.directory)
            .map_err(|error| Error::io(Some(self.inner.directory.clone()), error))?;
        check_space(
            disk,
            state.pending_bytes,
            added,
            self.inner.min_free_ratio,
            self.inner.min_free_bytes,
            &self.inner.directory,
        )
    }
}

impl DiskReservation {
    pub(super) fn raise(&self, total_bytes: u64) -> Result<()> {
        let mut current = self.charge.bytes.lock();
        if total_bytes <= *current {
            return Ok(());
        }
        let added = total_bytes - *current;
        let mut state = self.charge.admission.inner.state.lock();
        self.charge.admission.check(&state, added)?;
        state.pending_bytes = state
            .pending_bytes
            .checked_add(added)
            .ok_or_else(|| Error::ResourceExhausted("Native disk admission overflowed".into()))?;
        *current = total_bytes;
        Ok(())
    }
}

impl Drop for ReservationCharge {
    fn drop(&mut self) {
        let bytes = *self.bytes.lock();
        let mut state = self.admission.inner.state.lock();
        state.pending_bytes = state.pending_bytes.saturating_sub(bytes);
    }
}

fn check_space(
    disk: DiskSpace,
    pending: u64,
    added: u64,
    min_free_ratio: f64,
    min_free_bytes: u64,
    directory: &Path,
) -> Result<()> {
    if disk.available_bytes > disk.total_bytes {
        return Err(Error::Internal(
            "Native disk probe returned more available than total bytes".into(),
        ));
    }
    let ratio_reserve = (disk.total_bytes as f64 * min_free_ratio).ceil();
    let ratio_reserve = if ratio_reserve >= u64::MAX as f64 {
        u64::MAX
    } else {
        ratio_reserve as u64
    };
    let reserve = min_free_bytes.max(ratio_reserve);
    let required = reserve
        .checked_add(pending)
        .and_then(|value| value.checked_add(added))
        .ok_or_else(|| Error::ResourceExhausted("Native disk reserve overflowed".into()))?;
    if disk.available_bytes < required {
        return Err(Error::ResourceExhausted(format!(
            "Native write admission on '{}' requires {added} bytes with {pending} bytes already pending and {reserve} bytes reserved, but only {} bytes are available",
            directory.display(),
            disk.available_bytes
        )));
    }
    Ok(())
}

fn system_disk_space(path: &Path) -> io::Result<DiskSpace> {
    let resolved = path.canonicalize()?;
    let disks = Disks::new_with_refreshed_list();
    disks
        .iter()
        .filter(|disk| resolved.starts_with(disk.mount_point()))
        .max_by_key(|disk| disk.mount_point().components().count())
        .map(|disk| DiskSpace {
            available_bytes: disk.available_space(),
            total_bytes: disk.total_space(),
        })
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                format!(
                    "could not determine the filesystem containing '{}'",
                    path.display()
                ),
            )
        })
}

#[cfg(test)]
mod tests {
    use super::{DiskSpace, check_space};

    #[test]
    fn enforces_the_larger_ratio_or_absolute_reserve() {
        let disk = DiskSpace {
            available_bytes: 250,
            total_bytes: 1_000,
        };
        assert!(check_space(disk, 40, 10, 0.10, 100, "db".as_ref()).is_ok());
        assert!(check_space(disk, 40, 111, 0.10, 100, "db".as_ref()).is_err());
        assert!(check_space(disk, 0, 1, 0.30, 100, "db".as_ref()).is_err());
    }
}
