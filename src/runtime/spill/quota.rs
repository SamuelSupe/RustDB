use std::{
    fmt::Debug,
    io,
    path::{Path, PathBuf},
    sync::Arc,
};

use parking_lot::Mutex;
use sysinfo::Disks;

use crate::{Error, Result, config::SpillConfig};

const DISK_SPACE_CHECK_BYTES: u64 = 64 * 1024 * 1024;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct DiskSpace {
    pub(crate) available_bytes: u64,
    pub(crate) total_bytes: u64,
}

pub(crate) trait DiskSpaceProbe: Debug + Send + Sync {
    fn probe(&self, path: &Path) -> io::Result<DiskSpace>;
}

#[derive(Debug, Default)]
pub(crate) struct SystemDiskSpaceProbe;

impl DiskSpaceProbe for SystemDiskSpaceProbe {
    fn probe(&self, path: &Path) -> io::Result<DiskSpace> {
        let resolved = nearest_existing_path(path)?.canonicalize()?;
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
                        resolved.display()
                    ),
                )
            })
    }
}

#[derive(Debug, Default)]
struct Usage {
    committed: u64,
    pending: u64,
    bytes_since_probe: u64,
}

#[derive(Clone, Debug)]
pub(crate) struct SpillQuotaPool {
    config: SpillConfig,
    probe: Arc<dyn DiskSpaceProbe>,
    usage: Arc<Mutex<Usage>>,
    retained: Arc<Mutex<Vec<RetainedEngineCharge>>>,
}

impl SpillQuotaPool {
    pub(crate) fn new(config: SpillConfig) -> Result<Self> {
        Self::with_probe(config, Arc::new(SystemDiskSpaceProbe))
    }

    pub(crate) fn with_probe(config: SpillConfig, probe: Arc<dyn DiskSpaceProbe>) -> Result<Self> {
        config.validate()?;
        Ok(Self {
            config,
            probe,
            usage: Arc::new(Mutex::new(Usage::default())),
            retained: Arc::new(Mutex::new(Vec::new())),
        })
    }

    pub(crate) fn start_query(&self) -> QuerySpillQuota {
        QuerySpillQuota {
            pool: self.clone(),
            usage: Arc::new(Mutex::new(Usage::default())),
        }
    }

    pub(crate) fn committed_bytes(&self) -> u64 {
        self.usage.lock().committed
    }

    pub(crate) fn pending_bytes(&self) -> u64 {
        self.usage.lock().pending
    }
}

#[derive(Clone, Debug)]
pub(crate) struct QuerySpillQuota {
    pool: SpillQuotaPool,
    usage: Arc<Mutex<Usage>>,
}

impl QuerySpillQuota {
    /// Revalidates configured byte limits and disk headroom without retaining
    /// a byte charge. Spill files call this immediately before creation.
    pub(crate) fn check_available(&self) -> Result<()> {
        let reservation = self.try_reserve_inner(0, true, true)?;
        drop(reservation);
        Ok(())
    }

    pub(crate) fn try_reserve(&self, bytes: u64) -> Result<SpillReservation> {
        self.try_reserve_inner(bytes, bytes != 0, false)
    }

    fn try_reserve_inner(
        &self,
        bytes: u64,
        retain: bool,
        force_disk_check: bool,
    ) -> Result<SpillReservation> {
        let mut engine = self.pool.usage.lock();
        let mut query = self.usage.lock();
        let engine_projected = projected_usage(&engine, bytes, "engine")?;
        let query_projected = projected_usage(&query, bytes, "query")?;
        enforce_limit(
            "engine",
            self.pool.config.engine_limit_bytes,
            engine_projected,
        )?;
        enforce_limit("query", self.pool.config.query_limit_bytes, query_projected)?;

        let bytes_since_probe = engine.bytes_since_probe.saturating_add(bytes);
        let check_disk = force_disk_check || bytes_since_probe >= DISK_SPACE_CHECK_BYTES;
        if check_disk {
            check_disk_space(&self.pool, &engine, bytes)?;
        }

        if retain {
            engine.pending += bytes;
            query.pending += bytes;
        }
        engine.bytes_since_probe = if check_disk { 0 } else { bytes_since_probe };
        Ok(SpillReservation {
            quota: self.clone(),
            bytes,
            active: retain,
        })
    }

    pub(crate) fn committed_bytes(&self) -> u64 {
        self.usage.lock().committed
    }

    pub(crate) fn pending_bytes(&self) -> u64 {
        self.usage.lock().pending
    }

    fn release_pending(&self, bytes: u64) {
        let mut engine = self.pool.usage.lock();
        let mut query = self.usage.lock();
        debug_assert!(engine.pending >= bytes);
        debug_assert!(query.pending >= bytes);
        engine.pending = engine.pending.saturating_sub(bytes);
        query.pending = query.pending.saturating_sub(bytes);
    }

    fn commit(&self, reserved: u64, actual: u64) {
        let mut engine = self.pool.usage.lock();
        let mut query = self.usage.lock();
        debug_assert!(engine.pending >= reserved);
        debug_assert!(query.pending >= reserved);
        engine.pending = engine.pending.saturating_sub(reserved);
        query.pending = query.pending.saturating_sub(reserved);
        engine.committed = engine.committed.saturating_add(actual);
        query.committed = query.committed.saturating_add(actual);
    }

    fn release_committed(&self, bytes: u64) {
        let mut engine = self.pool.usage.lock();
        let mut query = self.usage.lock();
        debug_assert!(engine.committed >= bytes);
        debug_assert!(query.committed >= bytes);
        engine.committed = engine.committed.saturating_sub(bytes);
        query.committed = query.committed.saturating_sub(bytes);
    }

    fn release_query_committed(&self, bytes: u64) {
        let mut query = self.usage.lock();
        debug_assert!(query.committed >= bytes);
        query.committed = query.committed.saturating_sub(bytes);
    }
}

fn check_disk_space(pool: &SpillQuotaPool, engine: &Usage, bytes: u64) -> Result<()> {
    let disk = pool
        .probe
        .probe(&pool.config.directory)
        .map_err(|error| Error::io(Some(pool.config.directory.clone()), error))?;
    if disk.available_bytes > disk.total_bytes {
        return Err(Error::Internal(format!(
            "disk-space probe returned {} available bytes but only {} total bytes for '{}'",
            disk.available_bytes,
            disk.total_bytes,
            pool.config.directory.display()
        )));
    }
    let required_free = pool
        .config
        .min_free_bytes
        .max(ratio_bytes(disk.total_bytes, pool.config.min_free_ratio));
    let pending_after = engine.pending.checked_add(bytes).ok_or_else(|| {
        Error::ResourceExhausted("spill pending-byte accounting overflowed".to_owned())
    })?;
    let required_available = required_free.checked_add(pending_after).ok_or_else(|| {
        Error::ResourceExhausted(
            "spill free-space requirement exceeds the supported byte range".to_owned(),
        )
    })?;
    if disk.available_bytes < required_available {
        return Err(Error::ResourceExhausted(format!(
            "spill write of {bytes} bytes would leave less than the configured free-space reserve on '{}': available {}, pending {}, required free {}",
            pool.config.directory.display(),
            disk.available_bytes,
            engine.pending,
            required_free,
        )));
    }
    Ok(())
}

#[derive(Debug)]
pub(crate) struct SpillReservation {
    quota: QuerySpillQuota,
    bytes: u64,
    active: bool,
}

impl SpillReservation {
    pub(crate) fn reserved_bytes(&self) -> u64 {
        self.bytes
    }

    pub(crate) fn commit(mut self, actual_bytes: u64) -> Result<SpillCharge> {
        if actual_bytes > self.bytes {
            return Err(Error::InvalidArgument(format!(
                "spill write committed {actual_bytes} bytes after reserving only {} bytes",
                self.bytes
            )));
        }
        if self.active {
            self.quota.commit(self.bytes, actual_bytes);
            self.active = false;
        }
        Ok(SpillCharge {
            quota: self.quota.clone(),
            bytes: actual_bytes,
            active: actual_bytes != 0,
        })
    }
}

impl Drop for SpillReservation {
    fn drop(&mut self) {
        if self.active {
            self.quota.release_pending(self.bytes);
            self.active = false;
        }
    }
}

#[derive(Debug)]
pub(crate) struct SpillCharge {
    quota: QuerySpillQuota,
    bytes: u64,
    active: bool,
}

impl SpillCharge {
    pub(crate) fn bytes(&self) -> u64 {
        self.bytes
    }

    pub(crate) fn release(mut self) {
        self.release_inner();
    }

    pub(crate) fn merge(&mut self, mut other: Self) -> Result<()> {
        if !other.active {
            return Ok(());
        }
        self.bytes = self.bytes.checked_add(other.bytes).ok_or_else(|| {
            Error::ResourceExhausted("spill committed-byte accounting overflowed".to_owned())
        })?;
        other.active = false;
        Ok(())
    }

    /// Ends the query portion of this charge while retaining the engine charge
    /// until the owning Engine is dropped. Used only when physical cleanup has
    /// failed and the orphan must continue to count against the hard budget.
    pub(crate) fn retain_engine(mut self) {
        if !self.active {
            return;
        }
        self.quota.release_query_committed(self.bytes);
        self.quota.pool.retained.lock().push(RetainedEngineCharge {
            usage: Arc::clone(&self.quota.pool.usage),
            bytes: self.bytes,
        });
        self.active = false;
    }

    fn release_inner(&mut self) {
        if self.active {
            self.quota.release_committed(self.bytes);
            self.active = false;
        }
    }
}

#[derive(Debug)]
struct RetainedEngineCharge {
    usage: Arc<Mutex<Usage>>,
    bytes: u64,
}

impl Drop for RetainedEngineCharge {
    fn drop(&mut self) {
        let mut usage = self.usage.lock();
        debug_assert!(usage.committed >= self.bytes);
        usage.committed = usage.committed.saturating_sub(self.bytes);
    }
}

impl Drop for SpillCharge {
    fn drop(&mut self) {
        self.release_inner();
    }
}

fn projected_usage(usage: &Usage, bytes: u64, scope: &str) -> Result<u64> {
    usage
        .committed
        .checked_add(usage.pending)
        .and_then(|used| used.checked_add(bytes))
        .ok_or_else(|| {
            Error::ResourceExhausted(format!("spill {scope} byte accounting overflowed"))
        })
}

fn enforce_limit(scope: &str, limit: Option<u64>, projected: u64) -> Result<()> {
    if let Some(limit) = limit
        && projected > limit
    {
        return Err(Error::ResourceExhausted(format!(
            "spill {scope} quota exceeded: projected {projected} bytes, limit {limit} bytes"
        )));
    }
    Ok(())
}

fn ratio_bytes(total_bytes: u64, ratio: f64) -> u64 {
    let bytes = (total_bytes as f64 * ratio).ceil();
    if bytes >= u64::MAX as f64 {
        u64::MAX
    } else {
        bytes as u64
    }
}

fn nearest_existing_path(path: &Path) -> io::Result<PathBuf> {
    let mut candidate = path;
    loop {
        if candidate.exists() {
            return Ok(candidate.to_path_buf());
        }
        candidate = candidate.parent().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                format!("no existing ancestor for '{}'", path.display()),
            )
        })?;
    }
}

#[cfg(test)]
#[path = "quota_tests.rs"]
mod tests;
