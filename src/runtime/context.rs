use std::{
    collections::HashMap,
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
};

use parking_lot::RwLock;
use uuid::Uuid;

use crate::{Error, Result, sql::LogicalPlan, storage::ObjectSnapshot};

#[cfg(test)]
use super::MemoryReservation;
use super::{MemoryPool, QueryControl, QueryMetrics, SpillManager};

pub struct QueryContext {
    pub query_id: Uuid,
    pub batch_size: usize,
    pub control: QueryControl,
    pub metrics: QueryMetrics,
    pub memory: MemoryPool,
    pub spill: SpillManager,
    view_depth: Arc<AtomicUsize>,
    object_snapshots: RwLock<HashMap<String, ObjectSnapshot>>,
    object_snapshots_sealed: AtomicBool,
    view_plans: RwLock<HashMap<String, LogicalPlan>>,
}

impl QueryContext {
    #[cfg(test)]
    const DEFAULT_BATCH_SIZE: usize = 8_192;

    /// Creates a context over a query-level memory pool and a shared spill root.
    #[cfg(test)]
    pub fn new(memory: MemoryPool, spill_root: impl AsRef<Path>) -> Result<Self> {
        Self::with_query_id(Uuid::new_v4(), memory, spill_root)
    }

    #[cfg(test)]
    pub fn with_query_id(
        query_id: Uuid,
        memory: MemoryPool,
        spill_root: impl AsRef<Path>,
    ) -> Result<Self> {
        Self::with_query_id_and_batch_size(query_id, memory, spill_root, Self::DEFAULT_BATCH_SIZE)
    }

    pub fn with_query_id_and_batch_size(
        query_id: Uuid,
        memory: MemoryPool,
        spill_root: impl AsRef<Path>,
        batch_size: usize,
    ) -> Result<Self> {
        let control = QueryControl::new();
        let metrics = QueryMetrics::with_memory_pool(memory.clone());
        let spill = SpillManager::for_query(spill_root, query_id, &control, Some(metrics.clone()))?;
        Ok(Self {
            query_id,
            batch_size,
            control,
            metrics,
            memory,
            spill,
            view_depth: Arc::new(AtomicUsize::new(0)),
            object_snapshots: RwLock::new(HashMap::new()),
            object_snapshots_sealed: AtomicBool::new(false),
            view_plans: RwLock::new(HashMap::new()),
        })
    }

    #[cfg(test)]
    pub fn shared(memory: MemoryPool, spill_root: impl AsRef<Path>) -> Result<Arc<Self>> {
        Ok(Arc::new(Self::new(memory, spill_root)?))
    }

    pub fn check_cancelled(&self) -> Result<()> {
        self.control.check_cancelled()
    }

    #[cfg(test)]
    pub fn try_reserve(&self, bytes: usize) -> Result<MemoryReservation> {
        let reservation = self.memory.try_reserve(bytes)?;
        self.metrics.observe_memory(self.memory.used());
        Ok(reservation)
    }

    pub fn cancel(&self) {
        self.control.cancel();
        self.metrics.finish();
    }

    pub(crate) fn enter_view(&self, name: &str) -> Result<ViewExpansion> {
        const MAX_VIEW_DEPTH: usize = 64;
        let depth = self.view_depth.fetch_add(1, Ordering::AcqRel) + 1;
        if depth > MAX_VIEW_DEPTH {
            self.view_depth.fetch_sub(1, Ordering::AcqRel);
            return Err(crate::Error::ResourceExhausted(format!(
                "temporary view expansion exceeded {MAX_VIEW_DEPTH} levels while entering '{name}'; check for a view cycle"
            )));
        }
        Ok(ViewExpansion {
            depth: Arc::clone(&self.view_depth),
        })
    }

    pub(crate) fn register_object_snapshot(
        &self,
        uri: &str,
        snapshot: ObjectSnapshot,
    ) -> Result<()> {
        let mut snapshots = self.object_snapshots.write();
        if let Some(existing) = snapshots.get(uri) {
            if existing == &snapshot {
                return Ok(());
            }
            return Err(Error::Execution(format!(
                "object identity changed while preparing query: {uri}"
            )));
        }
        if self.object_snapshots_sealed.load(Ordering::Acquire) {
            return Err(Error::Execution(format!(
                "object was not present when the query snapshot was fixed: {uri}"
            )));
        }
        snapshots.insert(uri.to_owned(), snapshot);
        Ok(())
    }

    pub(crate) fn seal_object_snapshots(&self) {
        self.object_snapshots_sealed.store(true, Ordering::Release);
    }

    pub(crate) fn object_snapshots_sealed(&self) -> bool {
        self.object_snapshots_sealed.load(Ordering::Acquire)
    }

    pub(crate) fn object_snapshot(&self, uri: &str) -> Result<ObjectSnapshot> {
        if !self.object_snapshots_sealed() {
            return Err(Error::Internal(
                "object scan started before query snapshots were fixed".to_owned(),
            ));
        }
        self.object_snapshots
            .read()
            .get(uri)
            .cloned()
            .ok_or_else(|| {
                Error::Execution(format!(
                    "object was not present when the query snapshot was fixed: {uri}"
                ))
            })
    }

    pub(crate) fn cache_view_plan(&self, name: &str, plan: LogicalPlan) -> Result<()> {
        if self.object_snapshots_sealed() {
            return Err(Error::Internal(format!(
                "temporary view '{name}' was planned after query snapshots were fixed"
            )));
        }
        self.view_plans
            .write()
            .entry(name.to_ascii_lowercase())
            .or_insert(plan);
        Ok(())
    }

    pub(crate) fn view_plan(&self, name: &str) -> Option<LogicalPlan> {
        self.view_plans
            .read()
            .get(&name.to_ascii_lowercase())
            .cloned()
    }
}

pub(crate) struct ViewExpansion {
    depth: Arc<AtomicUsize>,
}

impl Drop for ViewExpansion {
    fn drop(&mut self) {
        self.depth.fetch_sub(1, Ordering::AcqRel);
    }
}

impl Drop for QueryContext {
    fn drop(&mut self) {
        self.metrics.finish();
        let _ = self.spill.cleanup();
    }
}

#[cfg(test)]
mod tests {
    use super::QueryContext;
    use crate::runtime::MemoryPool;
    use crate::storage::ObjectSnapshot;

    #[test]
    fn context_tracks_memory_and_cleans_spill_on_drop() {
        let root = tempfile::tempdir().expect("tempdir");
        let context = QueryContext::new(MemoryPool::new(128), root.path()).expect("context");
        let directory = context.spill.directory().to_owned();
        let metrics = context.metrics.clone();
        let reservation = context.try_reserve(64).expect("reservation");

        assert_eq!(metrics.snapshot().peak_memory_bytes, 64);
        drop(reservation);
        drop(context);

        assert!(!directory.exists());
        assert!(!metrics.snapshot().elapsed.is_zero());
    }

    #[test]
    fn query_snapshot_rejects_identity_changes_and_late_objects() {
        let root = tempfile::tempdir().expect("tempdir");
        let context = QueryContext::new(MemoryPool::new(128), root.path()).expect("context");
        let first = ObjectSnapshot {
            size: 10,
            e_tag: Some("v1".to_owned()),
            version: None,
        };
        context
            .register_object_snapshot("s3://bucket/data.csv", first.clone())
            .unwrap();
        context
            .register_object_snapshot("s3://bucket/data.csv", first.clone())
            .unwrap();
        let changed = ObjectSnapshot {
            size: 11,
            e_tag: Some("v2".to_owned()),
            version: None,
        };
        assert!(
            context
                .register_object_snapshot("s3://bucket/data.csv", changed)
                .unwrap_err()
                .to_string()
                .contains("identity changed")
        );

        context.seal_object_snapshots();
        assert_eq!(
            context.object_snapshot("s3://bucket/data.csv").unwrap(),
            first
        );
        assert!(
            context
                .register_object_snapshot(
                    "s3://bucket/late.csv",
                    ObjectSnapshot {
                        size: 1,
                        e_tag: None,
                        version: None,
                    },
                )
                .unwrap_err()
                .to_string()
                .contains("not present")
        );
    }
}
