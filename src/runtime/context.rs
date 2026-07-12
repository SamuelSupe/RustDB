use std::{
    collections::HashMap,
    mem::size_of,
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
};

use parking_lot::{Mutex, RwLock};
use uuid::Uuid;

use crate::{Error, Result, datasource::TableProvider, sql::LogicalPlan, storage::ObjectSnapshot};

use super::{
    MemoryPool, MemoryReservation, QueryControl, QueryMetrics, QueryScheduler, QuerySpillQuota,
    SpillIoPool, SpillManager, TaskGroup,
};

pub struct QueryContext {
    pub query_id: Uuid,
    pub batch_size: usize,
    pub control: QueryControl,
    pub metrics: QueryMetrics,
    pub memory: MemoryPool,
    pub spill: SpillManager,
    pub(crate) scheduler: QueryScheduler,
    pub(crate) tasks: TaskGroup,
    cleanup: Arc<QueryCleanup>,
    view_depth: Arc<AtomicUsize>,
    object_snapshots: RwLock<ObjectSnapshots>,
    object_snapshots_sealed: AtomicBool,
    view_plans: RwLock<HashMap<String, LogicalPlan>>,
    prepared_providers: RwLock<HashMap<u64, Arc<dyn TableProvider>>>,
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

    #[cfg(test)]
    pub fn with_query_id_and_batch_size(
        query_id: Uuid,
        memory: MemoryPool,
        spill_root: impl AsRef<Path>,
        batch_size: usize,
    ) -> Result<Self> {
        let control = QueryControl::new();
        let metrics = QueryMetrics::with_memory_pool(memory.clone());
        let spill = SpillManager::for_task_group_query(
            spill_root,
            query_id,
            &control,
            memory.clone(),
            Some(metrics.clone()),
        )?;
        Ok(Self::from_runtime_parts(
            query_id, memory, batch_size, control, metrics, spill,
        ))
    }

    /// Creates a query over engine-shared spill quota and blocking-I/O
    /// resources. Engine should use this constructor for production queries.
    pub(crate) fn with_spill_resources(
        query_id: Uuid,
        memory: MemoryPool,
        spill_root: impl AsRef<Path>,
        batch_size: usize,
        spill_quota: QuerySpillQuota,
        spill_io: SpillIoPool,
    ) -> Result<Self> {
        let control = QueryControl::new();
        let metrics = QueryMetrics::with_memory_pool(memory.clone());
        let spill = SpillManager::for_task_group_query_with_resources(
            spill_root,
            query_id,
            &control,
            memory.clone(),
            Some(metrics.clone()),
            spill_quota,
            spill_io,
        )?;
        Ok(Self::from_runtime_parts(
            query_id, memory, batch_size, control, metrics, spill,
        ))
    }

    fn from_runtime_parts(
        query_id: Uuid,
        memory: MemoryPool,
        batch_size: usize,
        control: QueryControl,
        metrics: QueryMetrics,
        spill: SpillManager,
    ) -> Self {
        let scheduler = QueryScheduler::new(metrics.clone());
        let tasks = TaskGroup::new(control.clone());
        let cleanup = Arc::new(QueryCleanup::new(spill.clone()));
        let weak_tasks = tasks.downgrade();
        let weak_cleanup = Arc::downgrade(&cleanup);
        let cleanup_directory = spill.directory().to_owned();
        let cancel_metrics = metrics.clone();
        control.register_cleanup(move || {
            cancel_metrics.finish();
            let Some(cleanup) = weak_cleanup.upgrade() else {
                return;
            };
            weak_tasks.reap(move || {
                if let Err(error) = cleanup.run() {
                    tracing::error!(
                        %error,
                        %query_id,
                        directory = %cleanup_directory.display(),
                        "failed to clean spill resources after query cancellation"
                    );
                }
            });
        });
        let snapshot_memory = memory.reservation();
        Self {
            query_id,
            batch_size,
            control,
            metrics,
            memory,
            spill,
            scheduler,
            tasks,
            cleanup,
            view_depth: Arc::new(AtomicUsize::new(0)),
            object_snapshots: RwLock::new(ObjectSnapshots {
                entries: HashMap::new(),
                memory: snapshot_memory,
            }),
            object_snapshots_sealed: AtomicBool::new(false),
            view_plans: RwLock::new(HashMap::new()),
            prepared_providers: RwLock::new(HashMap::new()),
        }
    }

    #[cfg(test)]
    pub fn shared(memory: MemoryPool, spill_root: impl AsRef<Path>) -> Result<Arc<Self>> {
        Ok(Arc::new(Self::new(memory, spill_root)?))
    }

    pub fn check_cancelled(&self) -> Result<()> {
        if self.control.is_cancelled() {
            Err(self.tasks.first_failure().unwrap_or(Error::Cancelled))
        } else {
            Ok(())
        }
    }

    pub(crate) async fn reserve_memory(
        &self,
        bytes: usize,
        owner: &'static str,
    ) -> Result<MemoryReservation> {
        self.reserve_memory_while_holding(bytes, 0, owner).await
    }

    /// Waits only for releasable pipeline pressure. If one kernel would need
    /// more than the query limit while its input lease remains live, fail
    /// immediately instead of waiting for memory that this operation itself
    /// prevents from being released.
    pub(crate) async fn reserve_memory_while_holding(
        &self,
        bytes: usize,
        held_bytes: usize,
        owner: &'static str,
    ) -> Result<MemoryReservation> {
        // Object snapshots live for the whole query. They cannot release memory
        // to satisfy a blocked kernel, so include them in the impossible
        // single-operation check without treating releasable queue pressure as
        // permanent.
        let retained_bytes = held_bytes.saturating_add(self.object_snapshots.read().memory.size());
        self.memory
            .reserve_wait(bytes, retained_bytes, &self.control)
            .await
            .map_err(|error| match error {
                Error::Cancelled => Error::Cancelled,
                error => Error::ResourceExhausted(format!(
                    "{owner} requires {bytes} bytes while retaining {retained_bytes} bytes (query limit {}, available {}): {error}",
                    self.memory.limit(),
                    self.memory.available(),
                )),
            })
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

    pub(crate) fn record_task_failure(&self, error: &Error) {
        self.tasks.record_failure(error);
    }

    /// Removes this query's spill resources exactly once through the query
    /// lifecycle. The first caller receives any deletion failure; later
    /// teardown paths are no-ops so they cannot mask or duplicate that error.
    pub(crate) fn cleanup_spill(&self) -> Result<()> {
        // Closing first makes the active-count check a cleanup barrier: no
        // worker can register between observing zero and deleting the query
        // directory.
        self.tasks.close();
        if self.tasks.active_tasks() != 0 {
            self.schedule_cleanup();
            return Ok(());
        }
        self.cleanup.run()
    }

    pub(crate) async fn cleanup_spill_after_tasks(&self) -> Result<()> {
        self.tasks.quiesce().await;
        self.cleanup.run()
    }

    pub(crate) fn schedule_cleanup(&self) {
        if self.cleanup.was_attempted() {
            return;
        }
        let cleanup = Arc::clone(&self.cleanup);
        let query_id = self.query_id;
        let directory = self.spill.directory().to_owned();
        self.tasks.reap(move || {
            if let Err(error) = cleanup.run() {
                tracing::error!(
                    %error,
                    %query_id,
                    directory = %directory.display(),
                    "failed to clean spill resources in query reaper"
                );
            }
        });
    }

    /// Preserves an execution/cancellation failure while making a concurrent
    /// spill deletion failure visible to the caller as one terminal error.
    pub(crate) fn error_with_cleanup(&self, error: Error) -> Error {
        match self.cleanup_spill() {
            Ok(()) => error,
            Err(cleanup_error) => Error::Execution(format!(
                "{error}; additionally failed to clean spill resources: {cleanup_error}"
            )),
        }
    }

    pub(crate) async fn error_with_cleanup_after_tasks(&self, error: Error) -> Error {
        match self.cleanup_spill_after_tasks().await {
            Ok(()) => error,
            Err(cleanup_error) => Error::Execution(format!(
                "{error}; additionally failed to clean spill resources: {cleanup_error}"
            )),
        }
    }

    #[cfg(test)]
    pub(crate) fn set_spill_cleanup_hook(
        &self,
        cleanup: impl Fn() -> Result<()> + Send + Sync + 'static,
    ) {
        *self.cleanup.hook.write() = Some(Arc::new(cleanup));
    }

    pub(crate) fn configure_compute_lanes(&self, lanes: usize) {
        self.scheduler.configure(lanes, self.memory.limit());
    }

    /// Some operator unit tests deliberately exercise parallel spill paths
    /// with unrealistically tiny pools. Production query setup always uses
    /// `configure_compute_lanes`, which applies the forward-progress cap.
    #[cfg(test)]
    pub(crate) fn configure_compute_lanes_unbounded_for_test(&self, lanes: usize) {
        self.scheduler.configure_unbounded(lanes);
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
        if let Some(existing) = snapshots.entries.get(uri) {
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
        let bytes = snapshot_entry_bytes(uri, &snapshot);
        snapshots.memory.try_grow(bytes).map_err(|_| {
            Error::ResourceExhausted(format!(
                "object snapshot metadata for '{uri}' requires {bytes} bytes, but the query \
                 memory limit is {} bytes with {} bytes currently available; narrow the file \
                 pattern or increase the memory limit",
                self.memory.limit(),
                self.memory.available()
            ))
        })?;
        snapshots.entries.insert(uri.to_owned(), snapshot);
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
            .entries
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

    pub(crate) fn cache_prepared_provider(
        &self,
        id: u64,
        provider: Arc<dyn TableProvider>,
    ) -> Result<()> {
        if self.object_snapshots_sealed() {
            return Err(Error::Internal(
                "external table was prepared after query snapshots were fixed".to_owned(),
            ));
        }
        self.prepared_providers
            .write()
            .entry(id)
            .or_insert(provider);
        Ok(())
    }

    pub(crate) fn prepared_provider(&self, id: u64) -> Option<Arc<dyn TableProvider>> {
        self.prepared_providers.read().get(&id).cloned()
    }
}

struct ObjectSnapshots {
    entries: HashMap<String, ObjectSnapshot>,
    memory: MemoryReservation,
}

fn snapshot_entry_bytes(uri: &str, snapshot: &ObjectSnapshot) -> usize {
    size_of::<(String, ObjectSnapshot)>()
        .saturating_mul(3)
        .saturating_add(uri.len())
        .saturating_add(snapshot.e_tag.as_ref().map_or(0, String::capacity))
        .saturating_add(snapshot.version.as_ref().map_or(0, String::capacity))
        // Covers the HashMap bucket plus per-file provider/Hive/query-plan
        // handles that remain live alongside the snapshot.
        .saturating_add(512)
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
        self.schedule_cleanup();
    }
}

struct QueryCleanup {
    spill: SpillManager,
    outcome: Mutex<Option<std::result::Result<(), String>>>,
    #[cfg(test)]
    hook: RwLock<Option<SpillCleanupHook>>,
}

impl QueryCleanup {
    fn new(spill: SpillManager) -> Self {
        Self {
            spill,
            outcome: Mutex::new(None),
            #[cfg(test)]
            hook: RwLock::new(None),
        }
    }

    fn run(&self) -> Result<()> {
        let mut outcome = self.outcome.lock();
        if let Some(outcome) = outcome.as_ref() {
            return replay_cleanup_outcome(outcome);
        }

        #[cfg(test)]
        let result = if let Some(cleanup) = self.hook.read().as_ref() {
            cleanup()
        } else {
            self.spill.cleanup()
        };

        #[cfg(not(test))]
        let result = self.spill.cleanup();

        *outcome = Some(result.as_ref().map(|_| ()).map_err(ToString::to_string));
        result
    }

    fn was_attempted(&self) -> bool {
        self.outcome.lock().is_some()
    }
}

fn replay_cleanup_outcome(outcome: &std::result::Result<(), String>) -> Result<()> {
    match outcome {
        Ok(()) => Ok(()),
        Err(message) => Err(Error::Execution(format!(
            "spill cleanup previously failed: {message}"
        ))),
    }
}

#[cfg(test)]
type SpillCleanupHook = Arc<dyn Fn() -> Result<()> + Send + Sync>;

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    use std::time::{Duration, Instant};

    use tokio::sync::Notify;

    use super::QueryContext;
    use crate::storage::ObjectSnapshot;
    use crate::{Error, runtime::MemoryPool};

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

        let deadline = Instant::now() + Duration::from_secs(2);
        while directory.exists() && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(5));
        }
        assert!(!directory.exists());
        assert!(!metrics.snapshot().elapsed.is_zero());
    }

    #[test]
    fn cleanup_failure_is_cached_across_teardown_paths() {
        let root = tempfile::tempdir().expect("tempdir");
        let context = QueryContext::new(MemoryPool::new(128), root.path()).expect("context");
        let attempts = Arc::new(AtomicUsize::new(0));
        let observed = Arc::clone(&attempts);
        context.set_spill_cleanup_hook(move || {
            observed.fetch_add(1, Ordering::Relaxed);
            Err(Error::ResourceExhausted(
                "injected cleanup rejection".to_owned(),
            ))
        });

        let error = context.cleanup_spill().unwrap_err();
        assert!(error.to_string().contains("injected cleanup rejection"));
        let replay = context.cleanup_spill().unwrap_err();
        assert!(replay.to_string().contains("injected cleanup rejection"));
        drop(context);
        assert_eq!(attempts.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn synchronous_cleanup_closes_task_registration_barrier() {
        let root = tempfile::tempdir().expect("tempdir");
        let context = QueryContext::new(MemoryPool::new(128), root.path()).expect("context");

        context.cleanup_spill().unwrap();

        assert!(matches!(
            context.tasks.spawn("too-late", async { Ok(()) }),
            Err(Error::Cancelled)
        ));
        assert_eq!(context.tasks.active_tasks(), 0);
    }

    #[tokio::test]
    async fn cancellation_reaper_waits_for_registered_tasks_before_cleanup() {
        let root = tempfile::tempdir().expect("tempdir");
        let context =
            Arc::new(QueryContext::new(MemoryPool::new(1 << 20), root.path()).expect("context"));
        let directory = context.spill.directory().to_owned();
        let release = Arc::new(Notify::new());
        let worker_release = Arc::clone(&release);
        context
            .tasks
            .spawn("cleanup-barrier", async move {
                worker_release.notified().await;
                Ok(())
            })
            .unwrap();
        let cleanup_attempts = Arc::new(AtomicUsize::new(0));
        let observed = Arc::clone(&cleanup_attempts);
        let cleanup_spill = context.spill.clone();
        context.set_spill_cleanup_hook(move || {
            observed.fetch_add(1, Ordering::Release);
            cleanup_spill.cleanup()
        });

        context.cancel();
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert_eq!(cleanup_attempts.load(Ordering::Acquire), 0);
        assert!(directory.exists());
        assert_eq!(context.tasks.active_tasks(), 1);

        release.notify_one();
        tokio::time::timeout(Duration::from_secs(2), async {
            while cleanup_attempts.load(Ordering::Acquire) == 0 || directory.exists() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("cleanup must run after registered tasks quiesce");
        assert_eq!(context.tasks.active_tasks(), 0);
        assert!(!directory.exists());
    }

    #[tokio::test]
    async fn background_cleanup_failure_is_replayed_to_the_public_error() {
        let root = tempfile::tempdir().expect("tempdir");
        let context =
            Arc::new(QueryContext::new(MemoryPool::new(1 << 20), root.path()).expect("context"));
        let attempts = Arc::new(AtomicUsize::new(0));
        let observed = Arc::clone(&attempts);
        context.set_spill_cleanup_hook(move || {
            observed.fetch_add(1, Ordering::Release);
            Err(Error::ResourceExhausted(
                "injected background deletion failure".into(),
            ))
        });

        context.cancel();
        tokio::time::timeout(Duration::from_secs(2), async {
            while !context.cleanup.was_attempted() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("automatic reaper must attempt cleanup");

        let error = context
            .error_with_cleanup_after_tasks(Error::Execution("injected query failure".into()))
            .await;
        let message = error.to_string();
        assert!(message.contains("injected query failure"), "{message}");
        assert!(
            message.contains("injected background deletion failure"),
            "{message}"
        );
        assert_eq!(attempts.load(Ordering::Acquire), 1);
    }

    #[test]
    fn query_snapshot_rejects_identity_changes_and_late_objects() {
        let root = tempfile::tempdir().expect("tempdir");
        let context = QueryContext::new(MemoryPool::new(4_096), root.path()).expect("context");
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

    #[test]
    fn snapshot_metadata_is_reserved_once_and_released_with_the_query() {
        let root = tempfile::tempdir().expect("tempdir");
        let memory = MemoryPool::new(1_200);
        let context = QueryContext::new(memory.clone(), root.path()).expect("context");
        let first = ObjectSnapshot {
            size: 10,
            e_tag: Some("etag-1".to_owned()),
            version: Some("version-1".to_owned()),
        };
        context
            .register_object_snapshot("s3://bucket/first.parquet", first.clone())
            .unwrap();
        let charged = memory.used();
        assert!(charged > 0);
        assert!(charged <= memory.limit());

        context
            .register_object_snapshot("s3://bucket/first.parquet", first)
            .unwrap();
        assert_eq!(memory.used(), charged);

        let failed_uri = "s3://bucket/second-object-with-a-long-name.parquet";
        let error = context
            .register_object_snapshot(
                failed_uri,
                ObjectSnapshot {
                    size: 20,
                    e_tag: Some("x".repeat(128)),
                    version: Some("y".repeat(128)),
                },
            )
            .unwrap_err();
        assert!(matches!(error, crate::Error::ResourceExhausted(_)));
        assert_eq!(memory.used(), charged);
        assert!(memory.peak() <= memory.limit());

        context.seal_object_snapshots();
        assert!(context.object_snapshot(failed_uri).is_err());
        drop(context);
        assert_eq!(memory.used(), 0);
    }
}
