use std::{
    io,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use parking_lot::Mutex;
use sysinfo::Disks;

use crate::{Error, Result};

const DEFAULT_GLOBAL_LIMIT: u64 = 10 * 1024 * 1024 * 1024;
const MIN_FREE_BYTES: u64 = 1024 * 1024 * 1024;
const SPACE_CHECK_INTERVAL: u64 = 64 * 1024 * 1024;

pub(super) struct QuotaPool {
    used: Mutex<u64>,
    global_limit: u64,
    query_limit: u64,
    directory: PathBuf,
    free_reserve: u64,
}

impl QuotaPool {
    pub(super) fn configured(
        directory: &Path,
        global_limit_bytes: Option<u64>,
        query_limit_bytes: Option<u64>,
    ) -> Result<Self> {
        let space = filesystem_space(directory);
        let capacity_limit = space
            .as_ref()
            .map(|space| space.total / 10)
            .unwrap_or(DEFAULT_GLOBAL_LIMIT);
        let free_reserve = space
            .as_ref()
            .map(|space| MIN_FREE_BYTES.max(space.total / 10))
            .unwrap_or(0);
        let writable = space
            .as_ref()
            .map(|space| space.available.saturating_sub(free_reserve))
            .unwrap_or(DEFAULT_GLOBAL_LIMIT);
        let global_limit = global_limit_bytes
            .unwrap_or(DEFAULT_GLOBAL_LIMIT.min(capacity_limit))
            .min(writable);
        if global_limit == 0 {
            return Err(Error::ResourceExhausted(
                "HTTP result store cannot preserve its filesystem free-space reserve".into(),
            ));
        }
        let query_limit = query_limit_bytes.unwrap_or(global_limit / 4).max(1);
        if query_limit > global_limit {
            return Err(Error::InvalidArgument(
                "HTTP per-query result limit cannot exceed the global limit".into(),
            ));
        }
        Ok(Self {
            used: Mutex::new(0),
            global_limit,
            query_limit,
            directory: directory.to_owned(),
            free_reserve,
        })
    }

    pub(super) fn ensure_free_space(&self, additional: u64) -> io::Result<()> {
        let Some(space) = filesystem_space(&self.directory) else {
            return Ok(());
        };
        if space.available.saturating_sub(additional) < self.free_reserve {
            Err(storage_full())
        } else {
            Ok(())
        }
    }
}

pub(super) struct QuotaLease {
    pool: Arc<QuotaPool>,
    bytes: AtomicU64,
    next_space_check: AtomicU64,
}

impl QuotaLease {
    pub(super) fn new(pool: Arc<QuotaPool>) -> Self {
        Self {
            pool,
            bytes: AtomicU64::new(0),
            next_space_check: AtomicU64::new(0),
        }
    }

    pub(super) fn reserve(&self, bytes: u64) -> io::Result<()> {
        self.reserve_inner(bytes, true)
    }

    pub(super) fn reserve_existing(&self, bytes: u64) -> io::Result<()> {
        self.reserve_inner(bytes, false)
    }

    fn reserve_inner(&self, bytes: u64, check_space: bool) -> io::Result<()> {
        if bytes == 0 {
            return Ok(());
        }
        let current = self.bytes.load(Ordering::Acquire);
        let query_next = current.checked_add(bytes).ok_or_else(storage_full)?;
        let mut global = self.pool.used.lock();
        let global_next = global.checked_add(bytes).ok_or_else(storage_full)?;
        if query_next > self.pool.query_limit || global_next > self.pool.global_limit {
            return Err(storage_full());
        }
        if check_space && current >= self.next_space_check.load(Ordering::Acquire) {
            self.pool.ensure_free_space(bytes)?;
            self.next_space_check.store(
                query_next.saturating_add(SPACE_CHECK_INTERVAL),
                Ordering::Release,
            );
        }
        *global = global_next;
        self.bytes.store(query_next, Ordering::Release);
        Ok(())
    }

    pub(super) fn release(&self, bytes: u64) {
        if bytes == 0 {
            return;
        }
        let released = self
            .bytes
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                Some(current.saturating_sub(bytes))
            })
            .unwrap_or(0)
            .min(bytes);
        let mut global = self.pool.used.lock();
        *global = global.saturating_sub(released);
    }
}

impl Drop for QuotaLease {
    fn drop(&mut self) {
        let bytes = self.bytes.swap(0, Ordering::AcqRel);
        let mut global = self.pool.used.lock();
        *global = global.saturating_sub(bytes);
    }
}

fn storage_full() -> io::Error {
    io::Error::new(io::ErrorKind::StorageFull, "HTTP result quota exceeded")
}

struct FilesystemSpace {
    total: u64,
    available: u64,
}

fn filesystem_space(path: &Path) -> Option<FilesystemSpace> {
    let canonical = path.canonicalize().ok()?;
    Disks::new_with_refreshed_list()
        .list()
        .iter()
        .filter(|disk| canonical.starts_with(disk.mount_point()))
        .max_by_key(|disk| disk.mount_point().components().count())
        .map(|disk| FilesystemSpace {
            total: disk.total_space(),
            available: disk.available_space(),
        })
}
