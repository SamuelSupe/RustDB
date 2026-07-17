use std::{
    fs,
    io::ErrorKind,
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    thread,
    time::Duration,
};

pub(crate) struct StorageReport {
    pub(crate) baseline_bytes: u64,
    pub(crate) peak_bytes: u64,
    pub(crate) final_bytes: u64,
}

pub(crate) struct StorageSampler {
    root: PathBuf,
    max_bytes: u64,
    baseline: u64,
    peak: Arc<AtomicU64>,
    exceeded: Arc<AtomicBool>,
    error: Arc<Mutex<Option<String>>>,
    stop: Arc<AtomicBool>,
    worker: Option<thread::JoinHandle<()>>,
}

impl StorageSampler {
    pub(crate) fn start(root: PathBuf, max_bytes: u64) -> Result<Self, String> {
        let baseline = directory_bytes(&root)?;
        if baseline > max_bytes {
            return Err("native setup storage baseline exceeds its limit".to_owned());
        }
        let peak = Arc::new(AtomicU64::new(baseline));
        let exceeded = Arc::new(AtomicBool::new(false));
        let error = Arc::new(Mutex::new(None));
        let stop = Arc::new(AtomicBool::new(false));
        let worker_root = root.clone();
        let worker_peak = Arc::clone(&peak);
        let worker_exceeded = Arc::clone(&exceeded);
        let worker_error = Arc::clone(&error);
        let worker_stop = Arc::clone(&stop);
        let worker = thread::spawn(move || {
            while !worker_stop.load(Ordering::Acquire) {
                sample(
                    &worker_root,
                    max_bytes,
                    &worker_peak,
                    &worker_exceeded,
                    &worker_error,
                );
                thread::sleep(Duration::from_millis(2));
            }
            sample(
                &worker_root,
                max_bytes,
                &worker_peak,
                &worker_exceeded,
                &worker_error,
            );
        });
        Ok(Self {
            root,
            max_bytes,
            baseline,
            peak,
            exceeded,
            error,
            stop,
            worker: Some(worker),
        })
    }

    pub(crate) fn check(&self) -> Result<(), String> {
        if self
            .error
            .lock()
            .map_err(|_| "native setup storage sampler failed".to_owned())?
            .is_some()
        {
            return Err("native setup storage sampling failed".to_owned());
        }
        if self.exceeded.load(Ordering::Acquire) {
            return Err("native setup exceeded its storage limit".to_owned());
        }
        Ok(())
    }

    pub(crate) fn stop(mut self) -> Result<StorageReport, String> {
        self.finish()
    }

    fn finish(&mut self) -> Result<StorageReport, String> {
        self.stop.store(true, Ordering::Release);
        if let Some(worker) = self.worker.take() {
            worker
                .join()
                .map_err(|_| "native setup storage sampler panicked".to_owned())?;
        }
        self.check()?;
        let final_bytes = directory_bytes(&self.root)?;
        let peak_bytes = self
            .peak
            .fetch_max(final_bytes, Ordering::AcqRel)
            .max(final_bytes);
        if final_bytes > self.max_bytes || peak_bytes > self.max_bytes {
            return Err("native setup exceeded its storage limit".to_owned());
        }
        Ok(StorageReport {
            baseline_bytes: self.baseline,
            peak_bytes,
            final_bytes,
        })
    }
}

impl Drop for StorageSampler {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

pub(crate) fn common_storage_root(paths: &[&Path]) -> Result<PathBuf, String> {
    let first = paths
        .first()
        .ok_or_else(|| "native setup has no storage paths".to_owned())?;
    let mut root = (*first).to_path_buf();
    while !paths.iter().all(|path| path.starts_with(&root)) {
        if !root.pop() {
            return Err("native database and spill directory have no common root".to_owned());
        }
    }
    Ok(root)
}

fn sample(
    root: &Path,
    max_bytes: u64,
    peak: &AtomicU64,
    exceeded: &AtomicBool,
    error: &Mutex<Option<String>>,
) {
    match directory_bytes(root) {
        Ok(bytes) => {
            peak.fetch_max(bytes, Ordering::AcqRel);
            if bytes > max_bytes {
                exceeded.store(true, Ordering::Release);
            }
        }
        Err(message) => {
            if let Ok(mut slot) = error.lock()
                && slot.is_none()
            {
                *slot = Some(message);
            }
        }
    }
}

fn directory_bytes(path: &Path) -> Result<u64, String> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(0),
        Err(_) => return Err("cannot inspect native setup storage".to_owned()),
    };
    if !metadata.is_dir() {
        return Ok(metadata.len());
    }
    let entries = match fs::read_dir(path) {
        Ok(entries) => entries,
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(0),
        Err(_) => return Err("cannot enumerate native setup storage".to_owned()),
    };
    let mut bytes = 0_u64;
    for entry in entries {
        let entry = match entry {
            Ok(entry) => entry,
            Err(error) if error.kind() == ErrorKind::NotFound => continue,
            Err(_) => return Err("cannot enumerate native setup storage".to_owned()),
        };
        bytes = bytes
            .checked_add(directory_bytes(&entry.path())?)
            .ok_or_else(|| "native setup storage size overflowed".to_owned())?;
    }
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use super::{StorageSampler, common_storage_root};

    #[test]
    fn accounts_for_files_and_finds_common_root() {
        let temp = tempfile::tempdir().unwrap();
        let database = temp.path().join("database");
        let spill = temp.path().join("spill");
        std::fs::create_dir_all(&database).unwrap();
        std::fs::create_dir_all(&spill).unwrap();
        std::fs::write(database.join("segment"), [0_u8; 32]).unwrap();
        assert_eq!(
            common_storage_root(&[&database, &spill]).unwrap(),
            temp.path()
        );
        let sampler = StorageSampler::start(temp.path().to_path_buf(), 64).unwrap();
        let report = sampler.stop().unwrap();
        assert_eq!(report.baseline_bytes, 32);
        assert_eq!(report.final_bytes, 32);
        assert!(report.peak_bytes >= report.final_bytes);
    }
}
