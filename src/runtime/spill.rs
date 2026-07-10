use std::{
    collections::HashSet,
    fs::{File, OpenOptions},
    io::BufWriter,
    path::{Path, PathBuf},
    sync::{
        Arc, Weak,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
};

use arrow::{
    datatypes::SchemaRef,
    ipc::{
        CompressionType,
        reader::FileReader,
        writer::{FileWriter, IpcWriteOptions},
    },
    record_batch::RecordBatch,
};
use parking_lot::Mutex;
use uuid::Uuid;

use crate::{Error, Result};

use super::{QueryControl, QueryMetrics};
#[cfg(test)]
use super::{RecordBatchStream, boxed_record_batch_stream};

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
    files: Mutex<HashSet<PathBuf>>,
    next_file: AtomicU64,
    cleaned: AtomicBool,
    metrics: Option<QueryMetrics>,
}

impl SpillManager {
    #[cfg(test)]
    pub fn new(spill_root: impl AsRef<Path>) -> Result<Self> {
        Self::create(spill_root, Uuid::new_v4(), None)
    }

    pub fn for_query(
        spill_root: impl AsRef<Path>,
        query_id: Uuid,
        control: &QueryControl,
        metrics: Option<QueryMetrics>,
    ) -> Result<Self> {
        let manager = Self::create(spill_root, query_id, metrics)?;
        let state = Arc::downgrade(&manager.state);
        control.register_cleanup(move || cleanup_weak(state));
        control.check_cancelled()?;
        Ok(manager)
    }

    fn create(
        spill_root: impl AsRef<Path>,
        query_id: Uuid,
        metrics: Option<QueryMetrics>,
    ) -> Result<Self> {
        let root = spill_root.as_ref();
        std::fs::create_dir_all(root)
            .map_err(|error| Error::io(Some(root.to_path_buf()), error))?;
        let directory = root.join(format!("query-{query_id}"));
        create_query_directory(&directory)?;
        if let Err(error) = set_directory_permissions(&directory) {
            let _ = std::fs::remove_dir_all(&directory);
            return Err(error);
        }

        Ok(Self {
            state: Arc::new(State {
                directory,
                files: Mutex::new(HashSet::new()),
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
        self.ensure_active()?;
        let spill_file = self.allocate_file(label)?;
        let result = self.write_batches_to(&spill_file, schema, batches);
        if let Err(error) = result {
            self.remove_file(&spill_file);
            return Err(error);
        }

        let bytes = match std::fs::metadata(spill_file.path()) {
            Ok(metadata) => metadata.len(),
            Err(error) => {
                self.remove_file(&spill_file);
                return Err(Error::io(Some(spill_file.path.clone()), error));
            }
        };
        if let Some(metrics) = &self.state.metrics {
            metrics.record_spill(bytes, 1);
        }
        Ok(spill_file)
    }

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

    fn write_batches_to<I>(
        &self,
        spill_file: &SpillFile,
        schema: SchemaRef,
        batches: I,
    ) -> Result<()>
    where
        I: IntoIterator<Item = Result<RecordBatch>>,
    {
        let file = secure_create(spill_file.path())?;
        let options =
            IpcWriteOptions::default().try_with_compression(Some(CompressionType::LZ4_FRAME))?;
        let mut writer =
            FileWriter::try_new_with_options(BufWriter::new(file), schema.as_ref(), options)?;
        for batch in batches {
            self.ensure_active()?;
            writer.write(&batch?)?;
        }
        writer.finish()?;
        Ok(())
    }

    pub fn read_batches(&self, spill_file: &SpillFile) -> Result<Vec<RecordBatch>> {
        self.ensure_active()?;
        self.validate_file(spill_file)?;
        let file = File::open(spill_file.path())
            .map_err(|error| Error::io(Some(spill_file.path.clone()), error))?;
        let reader = FileReader::try_new_buffered(file, None)?;
        let mut batches = Vec::new();
        for batch in reader {
            self.ensure_active()?;
            batches.push(batch?);
        }
        Ok(batches)
    }

    #[cfg(test)]
    pub fn read_stream(&self, spill_file: &SpillFile) -> Result<RecordBatchStream> {
        self.ensure_active()?;
        self.validate_file(spill_file)?;
        let file = File::open(spill_file.path())
            .map_err(|error| Error::io(Some(spill_file.path.clone()), error))?;
        let reader = FileReader::try_new_buffered(file, None)?;
        let state = Arc::downgrade(&self.state);
        Ok(boxed_record_batch_stream(async_stream::try_stream! {
            for batch in reader {
                match state.upgrade() {
                    Some(state) if !state.cleaned.load(Ordering::Acquire) => {}
                    _ => Err(Error::Cancelled)?,
                }
                yield batch?;
            }
        }))
    }

    pub fn remove_file(&self, spill_file: &SpillFile) {
        self.state.files.lock().remove(spill_file.path());
        match std::fs::remove_file(spill_file.path()) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(_) => {}
        }
    }

    pub fn cleanup(&self) -> Result<()> {
        self.state.cleanup()
    }

    fn allocate_file(&self, label: &str) -> Result<SpillFile> {
        self.ensure_active()?;
        let sequence = self.state.next_file.fetch_add(1, Ordering::Relaxed);
        let label = safe_label(label);
        let path = self
            .state
            .directory
            .join(format!("{sequence:08}-{label}.arrow"));
        self.state.files.lock().insert(path.clone());
        Ok(SpillFile { path })
    }

    fn validate_file(&self, spill_file: &SpillFile) -> Result<()> {
        if self.state.files.lock().contains(spill_file.path())
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
    fn cleanup(&self) -> Result<()> {
        self.cleaned.store(true, Ordering::Release);
        self.files.lock().clear();
        match std::fs::remove_dir_all(&self.directory) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(Error::io(Some(self.directory.clone()), error)),
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

fn safe_label(label: &str) -> String {
    let label: String = label
        .chars()
        .filter(|character| character.is_ascii_alphanumeric() || matches!(character, '-' | '_'))
        .take(48)
        .collect();
    if label.is_empty() {
        "spill".to_owned()
    } else {
        label
    }
}

fn secure_create(path: &Path) -> Result<File> {
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    options
        .open(path)
        .map_err(|error| Error::io(Some(path.to_path_buf()), error))
}

fn create_query_directory(path: &Path) -> Result<()> {
    let mut builder = std::fs::DirBuilder::new();
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        builder.mode(0o700);
    }
    builder
        .create(path)
        .map_err(|error| Error::io(Some(path.to_path_buf()), error))
}

fn set_directory_permissions(path: &Path) -> Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let permissions = std::fs::Permissions::from_mode(0o700);
        std::fs::set_permissions(path, permissions)
            .map_err(|error| Error::io(Some(path.to_path_buf()), error))?;
    }
    Ok(())
}

#[cfg(test)]
#[path = "spill_tests.rs"]
mod tests;
