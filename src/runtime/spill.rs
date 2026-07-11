use std::{
    path::{Path, PathBuf},
    sync::{
        Arc, Weak,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
};

use arrow::{
    datatypes::{Schema, SchemaRef},
    record_batch::RecordBatch,
};
use parking_lot::Mutex;
use uuid::Uuid;

#[cfg(test)]
use crate::config::SpillConfig;
use crate::{Error, Result};

use super::{MemoryPool, QueryControl, QueryMetrics};
#[cfg(test)]
use super::{RecordBatchStream, boxed_record_batch_stream};

mod activity;
mod io;
mod io_pool;
mod metadata;
#[allow(dead_code)]
mod quota;
#[allow(dead_code)]
mod scavenger;

pub(crate) use io::SpillWriter;
#[cfg(test)]
use io::writer_memory_bytes;
use io::{SpillReader, copy_memory_bytes};
pub(crate) use io_pool::SpillIoPool;
use metadata::ActiveFiles;
#[allow(unused_imports)]
pub(crate) use quota::{
    DiskSpace, DiskSpaceProbe, QuerySpillQuota, SpillCharge, SpillQuotaPool, SpillReservation,
    SystemDiskSpaceProbe,
};
#[allow(unused_imports)]
pub(crate) use scavenger::{ScavengeReport, scavenge_orphans};

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct SpillFile {
    path: PathBuf,
}

impl SpillFile {
    pub fn path(&self) -> &Path {
        &self.path
    }
}

#[derive(Clone, Debug)]
pub struct SpillManager {
    state: Arc<State>,
}

#[derive(Debug)]
struct State {
    directory: PathBuf,
    activity_lock: Mutex<Option<activity::QueryActivityLock>>,
    control: QueryControl,
    memory: MemoryPool,
    files: Arc<ActiveFiles>,
    quota: QuerySpillQuota,
    io_pool: SpillIoPool,
    next_file: AtomicU64,
    cleaned: AtomicBool,
    cleanup_lock: Mutex<()>,
    metrics: Option<QueryMetrics>,
}

impl SpillManager {
    #[cfg(test)]
    pub fn new(spill_root: impl AsRef<Path>, memory: MemoryPool) -> Result<Self> {
        let root = spill_root.as_ref().to_path_buf();
        let (quota, io_pool) = compatibility_resources(&root)?;
        Self::create(
            root,
            Uuid::new_v4(),
            QueryControl::new(),
            memory,
            None,
            quota,
            io_pool,
        )
    }

    #[cfg(test)]
    pub fn for_query(
        spill_root: impl AsRef<Path>,
        query_id: Uuid,
        control: &QueryControl,
        memory: MemoryPool,
        metrics: Option<QueryMetrics>,
    ) -> Result<Self> {
        let root = spill_root.as_ref().to_path_buf();
        let (quota, io_pool) = compatibility_resources(&root)?;
        Self::for_query_with_resources(root, query_id, control, memory, metrics, quota, io_pool)
    }

    /// Production constructor. Engine owns one `SpillQuotaPool` and one
    /// `SpillIoPool`, then starts a query quota and passes both resources here.
    pub(crate) fn for_query_with_resources(
        spill_root: impl AsRef<Path>,
        query_id: Uuid,
        control: &QueryControl,
        memory: MemoryPool,
        metrics: Option<QueryMetrics>,
        quota: QuerySpillQuota,
        io_pool: SpillIoPool,
    ) -> Result<Self> {
        let manager = Self::create(
            spill_root,
            query_id,
            control.clone(),
            memory,
            metrics,
            quota,
            io_pool,
        )?;
        let state = Arc::downgrade(&manager.state);
        control.register_cleanup(move || cleanup_weak(state));
        control.check_cancelled()?;
        Ok(manager)
    }

    fn create(
        spill_root: impl AsRef<Path>,
        query_id: Uuid,
        control: QueryControl,
        memory: MemoryPool,
        metrics: Option<QueryMetrics>,
        quota: QuerySpillQuota,
        io_pool: SpillIoPool,
    ) -> Result<Self> {
        let root = spill_root.as_ref().to_path_buf();
        let create_root = root.clone();
        io_pool.run(move || {
            std::fs::create_dir_all(&create_root)
                .map_err(|error| Error::io(Some(create_root), error))
        })?;
        let directory = root.join(format!("query-{query_id}"));
        let create_directory = directory.clone();
        let activity_lock = io_pool.run(move || {
            io::create_query_directory(&create_directory)?;
            if let Err(error) = io::set_directory_permissions(&create_directory) {
                let cleanup = std::fs::remove_dir_all(&create_directory);
                return match cleanup {
                    Ok(()) => Err(error),
                    Err(cleanup) => Err(Error::Execution(format!(
                        "{error}; additionally failed to remove spill directory '{}': {cleanup}",
                        create_directory.display()
                    ))),
                };
            }
            let activity_lock = match activity::QueryActivityLock::create(&create_directory) {
                Ok(activity_lock) => activity_lock,
                Err(error) => {
                    let cleanup = std::fs::remove_dir_all(&create_directory);
                    return match cleanup {
                        Ok(()) => Err(error),
                        Err(cleanup) => Err(Error::Execution(format!(
                            "{error}; additionally failed to remove spill directory '{}': {cleanup}",
                            create_directory.display()
                        ))),
                    };
                }
            };
            if let Err(error) = scavenger::write_query_marker(&create_directory) {
                drop(activity_lock);
                let cleanup = std::fs::remove_dir_all(&create_directory);
                return match cleanup {
                    Ok(()) => Err(error),
                    Err(cleanup) => Err(Error::Execution(format!(
                        "{error}; additionally failed to remove spill directory '{}': {cleanup}",
                        create_directory.display()
                    ))),
                };
            }
            Ok(activity_lock)
        })?;

        Ok(Self {
            state: Arc::new(State {
                directory,
                activity_lock: Mutex::new(Some(activity_lock)),
                control,
                memory: memory.clone(),
                files: Arc::new(ActiveFiles::new(memory)),
                quota,
                io_pool,
                next_file: AtomicU64::new(0),
                cleaned: AtomicBool::new(false),
                cleanup_lock: Mutex::new(()),
                metrics,
            }),
        })
    }

    pub fn directory(&self) -> &Path {
        &self.state.directory
    }

    pub fn write_batches<I>(&self, label: &str, schema: SchemaRef, batches: I) -> Result<SpillFile>
    where
        I: IntoIterator<Item = Result<RecordBatch>>,
    {
        let mut writer = self.writer(label, schema)?;
        for batch in batches {
            writer.write_batch(&batch?)?;
        }
        writer.finish(1)
    }

    #[cfg(test)]
    pub fn write_record_batches<I>(
        &self,
        label: &str,
        schema: SchemaRef,
        batches: I,
    ) -> Result<SpillFile>
    where
        I: IntoIterator<Item = RecordBatch>,
    {
        self.write_batches(label, schema, batches.into_iter().map(Ok))
    }

    pub(crate) fn writer(&self, label: &str, schema: SchemaRef) -> Result<SpillWriter> {
        self.ensure_active()?;
        let writer_memory = io::reserve_writer_memory(&self.state.memory, schema.as_ref())?;
        if let Some(metrics) = &self.state.metrics {
            metrics.observe_memory(self.state.memory.used());
        }
        let spill_file = self.allocate_file(label)?;
        SpillWriter::create(Arc::clone(&self.state), spill_file, schema, writer_memory)
    }

    pub(crate) fn writer_headroom_bytes(&self, label: &str, schema: &Schema) -> usize {
        io::writer_memory_bytes(schema)
            .saturating_add(self.write_copy_headroom_bytes())
            .saturating_add(metadata::active_file_metadata_bytes(
                &self.spill_path(u64::MAX, label),
            ))
    }

    pub(crate) fn write_copy_headroom_bytes(&self) -> usize {
        copy_memory_bytes(self.state.memory.limit())
    }

    #[cfg(test)]
    pub fn read_batches(&self, spill_file: &SpillFile) -> Result<Vec<RecordBatch>> {
        self.read_file(spill_file)?.collect()
    }

    pub(crate) fn read_file(
        &self,
        spill_file: &SpillFile,
    ) -> Result<impl Iterator<Item = Result<RecordBatch>> + use<>> {
        self.ensure_active()?;
        self.validate_file(spill_file)?;
        SpillReader::open(Arc::clone(&self.state), spill_file)
    }

    #[cfg(test)]
    pub fn read_stream(&self, spill_file: &SpillFile) -> Result<RecordBatchStream> {
        let reader = self.read_file(spill_file)?;
        Ok(boxed_record_batch_stream(async_stream::try_stream! {
            for batch in reader {
                yield batch?;
            }
        }))
    }

    pub fn remove_file(&self, spill_file: &SpillFile) -> Result<()> {
        self.state.remove_file(spill_file)
    }

    pub fn cleanup(&self) -> Result<()> {
        self.state.cleanup()
    }

    #[cfg(test)]
    fn activity_lock_held(&self) -> bool {
        self.state.activity_lock.lock().is_some()
    }

    fn allocate_file(&self, label: &str) -> Result<SpillFile> {
        self.ensure_active()?;
        let sequence = self.state.next_file.fetch_add(1, Ordering::Relaxed);
        let path = self.spill_path(sequence, label);
        self.state.files.insert(path.clone(), &self.state.cleaned)?;
        Ok(SpillFile { path })
    }

    fn spill_path(&self, sequence: u64, label: &str) -> PathBuf {
        let label = io::safe_label(label);
        self.state
            .directory
            .join(format!("{sequence:08}-{label}.arrow"))
    }

    fn validate_file(&self, spill_file: &SpillFile) -> Result<()> {
        if self.state.files.contains(spill_file.path())
            && spill_file.path().parent() == Some(self.directory())
        {
            Ok(())
        } else {
            Err(Error::InvalidArgument(format!(
                "spill file '{}' does not belong to this query",
                spill_file.path().display()
            )))
        }
    }

    fn ensure_active(&self) -> Result<()> {
        if self.state.cleaned.load(Ordering::Acquire) {
            Err(Error::Cancelled)
        } else {
            Ok(())
        }
    }
}

impl State {
    fn ensure_active(&self) -> Result<()> {
        if self.cleaned.load(Ordering::Acquire) {
            Err(Error::Cancelled)
        } else {
            Ok(())
        }
    }

    fn record_spill_read(&self, bytes: u64) {
        if let Some(metrics) = &self.metrics {
            metrics.add_spill_read_bytes(bytes);
        }
    }

    fn record_spill_file(&self) {
        if let Some(metrics) = &self.metrics {
            metrics.add_spill_file();
        }
    }

    fn remove_file(&self, spill_file: &SpillFile) -> Result<()> {
        let path = spill_file.path().to_path_buf();
        let remove_path = path.clone();
        self.io_pool.run_cancelable(&self.control, move || {
            match std::fs::remove_file(&remove_path) {
                Ok(()) => Ok(()),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
                Err(error) => Err(Error::io(Some(remove_path), error)),
            }
        })?;
        // Retained quota charges are released only after physical deletion has
        // succeeded (or the path was already absent).
        self.files.remove(&path);
        Ok(())
    }

    fn cleanup(&self) -> Result<()> {
        let _cleanup = self.cleanup_lock.lock();
        self.cleaned.store(true, Ordering::Release);
        // Do not unlink a directory while its advisory-lock file is still
        // open. That is legal on local Unix filesystems, but shared macOS/Linux
        // mounts can expose an empty ghost directory when the handle closes
        // later. Cleanup owns the lifecycle lock, so release it before removal.
        let activity_lock = self.activity_lock.lock().take();
        let directory = self.directory.clone();
        let verify = directory.clone();
        let sync_path = directory.clone();
        self.io_pool.run_cleanup(move || {
            drop(activity_lock);
            match std::fs::remove_dir_all(&directory) {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(Error::io(Some(directory), error)),
            }
            // Some shared filesystems can expose an empty directory briefly
            // after remove_dir_all reports success.
            match std::fs::remove_dir(&verify) {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(Error::io(Some(verify), error)),
            }
            match std::fs::symlink_metadata(&verify) {
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
                Err(error) => Err(Error::io(Some(verify), error)),
                Ok(_) => Err(Error::Execution(format!(
                    "spill directory '{}' remained after cleanup",
                    verify.display()
                ))),
            }
        })?;
        self.files.clear();
        self.io_pool
            .run_cleanup(move || io::sync_parent_directory(&sync_path))
    }
}

impl Drop for State {
    fn drop(&mut self) {
        if let Err(error) = self.cleanup() {
            self.files.retain_charges_on_drop();
            tracing::error!(%error, directory = %self.directory.display(), "spill cleanup failed");
        }
    }
}

fn cleanup_weak(state: Weak<State>) {
    if let Some(state) = state.upgrade()
        && let Err(error) = state.cleanup()
    {
        tracing::error!(%error, directory = %state.directory.display(), "spill cleanup failed");
    }
}

#[cfg(test)]
fn compatibility_resources(root: &Path) -> Result<(QuerySpillQuota, SpillIoPool)> {
    let config = SpillConfig {
        directory: root.to_path_buf(),
        min_free_ratio: 0.0,
        min_free_bytes: 0,
        ..SpillConfig::default()
    };
    let io_pool = SpillIoPool::new(config.io_threads)?;
    let quota = SpillQuotaPool::new(config)?.start_query();
    Ok((quota, io_pool))
}

#[cfg(test)]
#[path = "spill_tests.rs"]
mod tests;
