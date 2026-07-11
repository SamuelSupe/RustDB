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
use uuid::Uuid;

use crate::{Error, Result};

use super::{MemoryPool, QueryControl, QueryMetrics};
#[cfg(test)]
use super::{RecordBatchStream, boxed_record_batch_stream};

mod io;
mod metadata;

use io::SpillReader;
pub(crate) use io::SpillWriter;
#[cfg(test)]
use io::writer_memory_bytes;
use metadata::ActiveFiles;

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
    memory: MemoryPool,
    files: ActiveFiles,
    next_file: AtomicU64,
    cleaned: AtomicBool,
    metrics: Option<QueryMetrics>,
}

impl SpillManager {
    #[cfg(test)]
    pub fn new(spill_root: impl AsRef<Path>, memory: MemoryPool) -> Result<Self> {
        Self::create(spill_root, Uuid::new_v4(), memory, None)
    }

    pub fn for_query(
        spill_root: impl AsRef<Path>,
        query_id: Uuid,
        control: &QueryControl,
        memory: MemoryPool,
        metrics: Option<QueryMetrics>,
    ) -> Result<Self> {
        let manager = Self::create(spill_root, query_id, memory, metrics)?;
        let state = Arc::downgrade(&manager.state);
        control.register_cleanup(move || cleanup_weak(state));
        control.check_cancelled()?;
        Ok(manager)
    }

    fn create(
        spill_root: impl AsRef<Path>,
        query_id: Uuid,
        memory: MemoryPool,
        metrics: Option<QueryMetrics>,
    ) -> Result<Self> {
        let root = spill_root.as_ref();
        std::fs::create_dir_all(root)
            .map_err(|error| Error::io(Some(root.to_path_buf()), error))?;
        let directory = root.join(format!("query-{query_id}"));
        io::create_query_directory(&directory)?;
        if let Err(error) = io::set_directory_permissions(&directory) {
            let _ = std::fs::remove_dir_all(&directory);
            return Err(error);
        }

        Ok(Self {
            state: Arc::new(State {
                directory,
                memory: memory.clone(),
                files: ActiveFiles::new(memory),
                next_file: AtomicU64::new(0),
                cleaned: AtomicBool::new(false),
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
        let spill_file = self.allocate_file(label)?;
        SpillWriter::create(Arc::clone(&self.state), spill_file, schema, writer_memory)
    }

    pub(crate) fn writer_headroom_bytes(&self, label: &str, schema: &Schema) -> usize {
        io::writer_memory_bytes(schema).saturating_add(metadata::active_file_metadata_bytes(
            &self.spill_path(u64::MAX, label),
        ))
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

    pub fn remove_file(&self, spill_file: &SpillFile) {
        self.state.remove_file(spill_file);
    }

    pub fn cleanup(&self) -> Result<()> {
        self.state.cleanup()
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

    fn remove_file(&self, spill_file: &SpillFile) {
        self.files.remove(spill_file.path());
        match std::fs::remove_file(spill_file.path()) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(_) => {}
        }
    }

    fn cleanup(&self) -> Result<()> {
        self.cleaned.store(true, Ordering::Release);
        self.files.clear();
        match std::fs::remove_dir_all(&self.directory) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(Error::io(Some(self.directory.clone()), error)),
        }
        // Some shared filesystems can expose an empty directory briefly after
        // remove_dir_all reports success. A second root removal plus a parent
        // directory sync makes query completion a durable cleanup boundary.
        match std::fs::remove_dir(&self.directory) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(Error::io(Some(self.directory.clone()), error)),
        }
        io::sync_parent_directory(&self.directory)?;
        match std::fs::symlink_metadata(&self.directory) {
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(Error::io(Some(self.directory.clone()), error)),
            Ok(_) => Err(Error::Execution(format!(
                "spill directory '{}' remained after cleanup",
                self.directory.display()
            ))),
        }
    }
}

impl Drop for State {
    fn drop(&mut self) {
        let _ = self.cleanup();
    }
}

fn cleanup_weak(state: Weak<State>) {
    if let Some(state) = state.upgrade() {
        let _ = state.cleanup();
    }
}

#[cfg(test)]
#[path = "spill_tests.rs"]
mod tests;
