use std::{
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

use arrow::{datatypes::SchemaRef, record_batch::RecordBatch};
use async_stream::stream;
use futures::StreamExt;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use uuid::Uuid;

use crate::{
    Catalog, CsvOptions, EngineConfig, Error, ParquetOptions, PreparedStatement, QueryMetrics,
    Result, TableEntry,
    catalog::PersistentCatalog,
    command::{ParsedStatement, SessionCommand, ViewTable},
    datasource::{MetadataCache, NativeSegmentTable, RegisteredCsvTable, RegisteredParquetTable},
    runtime::{
        ComputeRuntime, GlobalComputeScheduler, MemoryPool, QueryContext, QueryControl,
        RecordBatchStream, SpillIoPool, SpillManager, SpillQuotaPool, boxed_record_batch_stream,
        scavenge_orphans,
    },
    sql::{LogicalPlan, StatementPlan},
    storage::NativeDatabase,
};

pub use memory_snapshot::EngineMemorySnapshot;

#[derive(Clone)]
pub struct Engine {
    inner: Arc<EngineInner>,
}

struct EngineInner {
    config: EngineConfig,
    memory: MemoryPool,
    admission: Arc<Semaphore>,
    compute: ComputeRuntime,
    compute_scheduler: GlobalComputeScheduler,
    metadata_cache: MetadataCache,
    spill_quota: SpillQuotaPool,
    spill_io: SpillIoPool,
    database: Option<Arc<NativeDatabase>>,
    persistent_catalog: PersistentCatalog,
    native_commit: parking_lot::Mutex<()>,
    native_write_admission: Arc<Semaphore>,
    native_poisoned: AtomicBool,
}

impl Engine {
    pub fn new(config: EngineConfig) -> Result<Self> {
        validate_config(&config)?;
        Self::from_validated_config(config, None)
    }

    pub fn open(path: impl AsRef<Path>, config: EngineConfig) -> Result<Self> {
        validate_config(&config)?;
        let database = NativeDatabase::open(path)?;
        Self::from_validated_config(config, Some(database))
    }

    pub fn restore_from(
        backup: impl AsRef<Path>,
        destination: impl AsRef<Path>,
        config: EngineConfig,
    ) -> Result<Self> {
        validate_config(&config)?;
        let source = NativeDatabase::open(backup)?;
        source.backup_to(destination.as_ref())?;
        drop(source);
        Self::open(destination, config)
    }

    fn from_validated_config(
        config: EngineConfig,
        database: Option<NativeDatabase>,
    ) -> Result<Self> {
        let database = database.map(Arc::new);
        std::fs::create_dir_all(&config.spill.directory)
            .map_err(|error| Error::io(Some(config.spill.directory.clone()), error))?;
        scavenge_orphans(&config.spill.directory, config.spill.orphan_ttl)?;
        let memory = MemoryPool::named_root("engine", config.memory_limit);
        SpillManager::protect_io_headroom(&memory, config.spill.io_threads)?;
        let admission = Arc::new(Semaphore::new(config.max_concurrent_queries));
        let compute = ComputeRuntime::new(config.compute_threads)?;
        let compute_scheduler = GlobalComputeScheduler::new(config.compute_threads)?;
        let metadata_cache = MetadataCache::new(config.metadata_cache_bytes);
        let persistent_catalog = match database.as_ref() {
            Some(database) => {
                let entries = database
                    .table_snapshots()
                    .into_iter()
                    .map(|(name, snapshot)| {
                        let provider = NativeSegmentTable::new(
                            database.path(),
                            snapshot,
                            &config,
                            metadata_cache.clone(),
                        );
                        TableEntry::new(name, Arc::new(provider))
                    })
                    .collect::<Vec<_>>();
                PersistentCatalog::new(database.catalog_generation(), entries)?
            }
            None => PersistentCatalog::default(),
        };
        let spill_quota = SpillQuotaPool::new(config.spill.clone())?;
        let spill_io = SpillIoPool::new(config.spill.io_threads)?;
        Ok(Self {
            inner: Arc::new(EngineInner {
                config,
                memory,
                admission,
                compute,
                compute_scheduler,
                metadata_cache,
                spill_quota,
                spill_io,
                database,
                persistent_catalog,
                native_commit: parking_lot::Mutex::new(()),
                native_write_admission: Arc::new(Semaphore::new(1)),
                native_poisoned: AtomicBool::new(false),
            }),
        })
    }

    pub fn config(&self) -> &EngineConfig {
        &self.inner.config
    }

    pub fn memory_snapshot(&self) -> EngineMemorySnapshot {
        EngineMemorySnapshot::from_pool(&self.inner.memory)
    }

    pub fn database_path(&self) -> Option<&Path> {
        self.inner.database.as_ref().map(|database| database.path())
    }

    pub fn backup_to(&self, destination: impl AsRef<Path>) -> Result<()> {
        let database = self.inner.database.as_ref().ok_or_else(|| {
            Error::Unsupported("backup requires Engine::open(path, config)".to_owned())
        })?;
        self.ensure_native_healthy()?;
        let _commit = self.inner.native_commit.lock();
        self.ensure_native_healthy()?;
        database.backup_to(destination.as_ref())
    }

    pub fn session(&self) -> Session {
        Session {
            engine: self.clone(),
            catalog: Catalog::with_persistent(self.inner.persistent_catalog.clone()),
        }
    }

    #[cfg(test)]
    pub(crate) fn query_context_for_test(&self) -> Result<Arc<QueryContext>> {
        self.session().query_context()
    }

    #[cfg(test)]
    pub(crate) fn compute_scheduler_counts_for_test(&self) -> (usize, usize, usize, usize) {
        let snapshot = self.inner.compute_scheduler.snapshot();
        (
            snapshot.active_slots,
            snapshot.peak_active_slots,
            snapshot.queued_waiters,
            snapshot.waiting_queries,
        )
    }

    fn drain_native_retired(&self) -> Result<()> {
        match self.inner.database.as_ref() {
            Some(database) => database.drain_retired(),
            None => Ok(()),
        }
    }

    fn ensure_native_healthy(&self) -> Result<()> {
        if !self.inner.native_poisoned.load(Ordering::Acquire) {
            return Ok(());
        }
        Err(Error::native_storage(
            self.database_path()
                .unwrap_or_else(|| Path::new("native database")),
            "engine state requires reopen after a native commit failure",
        ))
    }
}

#[derive(Clone)]
pub struct Session {
    engine: Engine,
    catalog: Catalog,
}

impl Session {
    #[cfg(test)]
    fn catalog(&self) -> &Catalog {
        &self.catalog
    }

    pub fn table_names(&self) -> Vec<String> {
        self.catalog
            .table_names()
            .into_iter()
            .filter(|name| !name.starts_with("__rustdb_file_"))
            .collect()
    }

    pub fn prepare(&self, sql: &str) -> Result<PreparedStatement> {
        PreparedStatement::new(self.clone(), sql)
    }

    pub async fn register_parquet<I, S>(
        &self,
        name: impl Into<String>,
        locations: I,
        options: ParquetOptions,
    ) -> Result<()>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let locations = normalize_locations(locations);
        let provider = RegisteredParquetTable::try_new(
            locations,
            options,
            &self.engine.inner.config,
            self.engine.inner.metadata_cache.clone(),
        )
        .await?;
        self.catalog
            .register(TableEntry::new(name, Arc::new(provider)))
    }

    pub async fn register_csv<I, S>(
        &self,
        name: impl Into<String>,
        locations: I,
        options: CsvOptions,
    ) -> Result<()>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let locations = normalize_locations(locations);
        let provider =
            RegisteredCsvTable::try_new(locations, options, &self.engine.inner.config).await?;
        self.catalog
            .register(TableEntry::new(name, Arc::new(provider)))
    }

    pub async fn refresh_table(&self, name: &str) -> Result<SchemaRef> {
        let entry = self
            .catalog
            .table(name)
            .ok_or_else(|| Error::Catalog(format!("table '{name}' does not exist")))?;
        let replacement = entry.provider().refreshed().await?.ok_or_else(|| {
            Error::Catalog(format!(
                "table '{name}' is not a refreshable external table"
            ))
        })?;
        let schema = replacement.schema();
        self.catalog
            .replace_provider(name, entry.provider(), replacement)?;
        Ok(schema)
    }

    pub async fn execute(&self, sql: &str) -> Result<QueryResult> {
        let admission_started = Instant::now();
        let permit = self.acquire_query_permit().await?;
        let admission_wait = admission_started.elapsed();
        let parse_started = Instant::now();
        let parsed = crate::command::parse(sql);
        let parse_time = parse_started.elapsed();

        match parsed? {
            ParsedStatement::Command(command) => {
                self.execute_command(command, permit, admission_wait, parse_time)
                    .await
            }
            ParsedStatement::Query(statement) => {
                self.execute_query(*statement, permit, admission_wait, parse_time)
                    .await
            }
        }
    }

    async fn execute_query(
        &self,
        statement: sqlparser::ast::Statement,
        permit: OwnedSemaphorePermit,
        admission_wait: Duration,
        parse_time: Duration,
    ) -> Result<QueryResult> {
        // Start query accounting before file-function schema discovery so the
        // reported elapsed time includes planning and metadata preparation.
        let context = self.query_context()?;
        context.metrics.record_query_admission_wait(admission_wait);
        context.metrics.record_sql_parse_time(parse_time);
        let plan = match self
            .prepare_ast_for_query(statement, Some(Arc::clone(&context)))
            .await
        {
            Ok(plan) => plan,
            Err(error) => return Err(context.error_with_cleanup(error)),
        };
        let schema = plan.schema();
        let stream = match crate::execution::execute_internal(plan, Arc::clone(&context)).await {
            Ok(stream) => stream,
            Err(error) => return Err(context.error_with_cleanup(error)),
        };
        let stream = self.engine.inner.compute.pipe(stream, Arc::clone(&context));
        Ok(query_result(
            schema,
            stream,
            context,
            permit,
            self.engine.clone(),
        ))
    }

    pub(crate) async fn execute_prepared(
        &self,
        statement: sqlparser::ast::Statement,
    ) -> Result<QueryResult> {
        let admission_started = Instant::now();
        let permit = self.acquire_query_permit().await?;
        let admission_wait = admission_started.elapsed();
        let context = self.query_context()?;
        context.metrics.record_query_admission_wait(admission_wait);
        let plan = match self
            .prepare_ast_for_query(statement, Some(Arc::clone(&context)))
            .await
        {
            Ok(plan) => plan,
            Err(error) => return Err(context.error_with_cleanup(error)),
        };
        let schema = plan.schema();
        let stream = match crate::execution::execute_internal(plan, Arc::clone(&context)).await {
            Ok(stream) => stream,
            Err(error) => return Err(context.error_with_cleanup(error)),
        };
        let stream = self.engine.inner.compute.pipe(stream, Arc::clone(&context));
        Ok(query_result(
            schema,
            stream,
            context,
            permit,
            self.engine.clone(),
        ))
    }

    async fn acquire_query_permit(&self) -> Result<OwnedSemaphorePermit> {
        self.engine.ensure_native_healthy()?;
        let permit = Arc::clone(&self.engine.inner.admission)
            .acquire_owned()
            .await
            .map_err(|_| Error::Internal("query admission controller closed".to_owned()))?;
        self.engine.ensure_native_healthy()?;
        Ok(permit)
    }

    async fn execute_command(
        &self,
        command: SessionCommand,
        permit: OwnedSemaphorePermit,
        admission_wait: Duration,
        parse_time: Duration,
    ) -> Result<QueryResult> {
        if let SessionCommand::NativeWrite(command) = command {
            return self
                .execute_native_write(command, permit, admission_wait, parse_time)
                .await;
        }
        let context = self.query_context()?;
        context.metrics.record_query_admission_wait(admission_wait);
        context.metrics.record_sql_parse_time(parse_time);
        let batch = async {
            match command {
                SessionCommand::ShowTables => crate::command::show_tables(&self.catalog),
                SessionCommand::Describe { name } => crate::command::describe(&self.catalog, &name),
                SessionCommand::RefreshTable { name } => {
                    self.refresh_table(&name).await?;
                    crate::command::status("REFRESH TABLE")
                }
                SessionCommand::CreateTempView {
                    name,
                    query,
                    replace,
                } => {
                    let plan = self
                        .prepare_view_plan(&query, Some(Arc::clone(&context)))
                        .await?;
                    let provider = Arc::new(ViewTable::new(
                        name.clone(),
                        query.clone(),
                        plan,
                        self.catalog.clone(),
                        self.engine.inner.config.clone(),
                        self.engine.inner.metadata_cache.clone(),
                    ));
                    self.catalog
                        .register_view(TableEntry::new(name, provider), query, replace)?;
                    crate::command::status("CREATE VIEW")
                }
                SessionCommand::DropView { name, if_exists } => {
                    if !self.catalog.drop_view(&name) && !if_exists {
                        return Err(Error::Catalog(format!("view '{name}' does not exist")));
                    }
                    crate::command::status("DROP VIEW")
                }
                SessionCommand::NativeWrite(_) => unreachable!("handled before command dispatch"),
            }
        }
        .await;
        let batch = match batch {
            Ok(batch) => batch,
            Err(error) => return Err(context.error_with_cleanup(error)),
        };
        self.batch_result(batch, permit, context)
    }

    async fn prepare_view_plan(
        &self,
        query: &str,
        context: Option<Arc<QueryContext>>,
    ) -> Result<LogicalPlan> {
        match self.prepare_statement_for_query(query, context).await? {
            StatementPlan::Query(plan) => Ok(plan),
            StatementPlan::Explain(_) | StatementPlan::ExplainAnalyze(_) => Err(Error::Internal(
                "a view query unexpectedly produced an EXPLAIN plan".into(),
            )),
        }
    }

    #[cfg(test)]
    async fn prepare_statement(&self, sql: &str) -> Result<StatementPlan> {
        self.prepare_statement_for_query(sql, None).await
    }

    async fn prepare_statement_for_query(
        &self,
        sql: &str,
        context: Option<Arc<QueryContext>>,
    ) -> Result<StatementPlan> {
        let catalog = self.catalog.pin();
        if let Some(context) = context.as_ref() {
            context.set_catalog_snapshot(catalog.clone())?;
        }
        // CREATE TEMP VIEW retains the original query source for diagnostics,
        // so its inner query is parsed once more here. Keep that parse in the
        // SQL phase rather than folding it into table-function preparation.
        let parse_started = Instant::now();
        let statements = crate::sql::parse_statements(sql);
        if let Some(context) = context.as_ref() {
            context
                .metrics
                .record_sql_parse_time(parse_started.elapsed());
        }
        let mut statements = statements?;
        if statements.len() != 1 {
            return Err(Error::InvalidArgument(
                "exactly one SQL statement is required".to_owned(),
            ));
        }
        let statement = statements.remove(0);
        let table_function_started = Instant::now();
        let prepared = crate::table_function::prepare_statement_with_cache_for_query(
            &catalog,
            &self.engine.inner.config,
            &self.engine.inner.metadata_cache,
            statement,
            context.clone(),
        )
        .await;
        if let Some(context) = context.as_ref() {
            context
                .metrics
                .record_table_function_prepare_time(table_function_started.elapsed());
        }
        let prepared = prepared?;
        let crate::table_function::PreparedSql {
            statement,
            generated_tables,
        } = prepared;
        let _generated_tables =
            crate::table_function::GeneratedTablesGuard::new(&catalog, generated_tables);
        async {
            let bind_started = Instant::now();
            let bound = crate::sql::bind_statement(&catalog, statement);
            if let Some(context) = context.as_ref() {
                context.metrics.record_bind_time(bind_started.elapsed());
            }
            let bound = bound?;
            if let Some(context) = context.as_ref() {
                crate::execution::prepare_plan(bound.logical_plan(), Arc::clone(context)).await?;
                context.seal_object_snapshots();
            }
            let optimize_started = Instant::now();
            let optimized = crate::sql::optimize_statement(bound, context.as_deref());
            if let Some(context) = context.as_ref() {
                context
                    .metrics
                    .record_optimize_time(optimize_started.elapsed());
            }
            optimized
        }
        .await
    }

    async fn prepare_ast_for_query(
        &self,
        statement: sqlparser::ast::Statement,
        context: Option<Arc<QueryContext>>,
    ) -> Result<StatementPlan> {
        let catalog = self.catalog.pin();
        if let Some(context) = context.as_ref() {
            context.set_catalog_snapshot(catalog.clone())?;
        }
        let table_function_started = Instant::now();
        let prepared = crate::table_function::prepare_statement_with_cache_for_query(
            &catalog,
            &self.engine.inner.config,
            &self.engine.inner.metadata_cache,
            statement,
            context.clone(),
        )
        .await;
        if let Some(context) = context.as_ref() {
            context
                .metrics
                .record_table_function_prepare_time(table_function_started.elapsed());
        }
        let prepared = prepared?;
        let crate::table_function::PreparedSql {
            statement,
            generated_tables,
        } = prepared;
        let _generated_tables =
            crate::table_function::GeneratedTablesGuard::new(&catalog, generated_tables);
        async {
            let bind_started = Instant::now();
            let bound = crate::sql::bind_statement(&catalog, statement);
            if let Some(context) = context.as_ref() {
                context.metrics.record_bind_time(bind_started.elapsed());
            }
            let bound = bound?;
            if let Some(context) = context.as_ref() {
                crate::execution::prepare_plan(bound.logical_plan(), Arc::clone(context)).await?;
                context.seal_object_snapshots();
            }
            let optimize_started = Instant::now();
            let optimized = crate::sql::optimize_statement(bound, context.as_deref());
            if let Some(context) = context.as_ref() {
                context
                    .metrics
                    .record_optimize_time(optimize_started.elapsed());
            }
            optimized
        }
        .await
    }

    fn batch_result(
        &self,
        batch: RecordBatch,
        permit: OwnedSemaphorePermit,
        context: Arc<QueryContext>,
    ) -> Result<QueryResult> {
        let schema = batch.schema();
        let stream = boxed_record_batch_stream(futures::stream::once(async move { Ok(batch) }));
        Ok(query_result(
            schema,
            stream,
            context,
            permit,
            self.engine.clone(),
        ))
    }

    fn query_context(&self) -> Result<Arc<QueryContext>> {
        let query_id = Uuid::new_v4();
        let query_memory = self.engine.inner.memory.child(
            format!("query-{query_id}"),
            self.engine.inner.config.memory_limit,
        );
        let context = Arc::new(QueryContext::with_spill_resources(
            query_id,
            query_memory,
            &self.engine.inner.config.spill.directory,
            self.engine.inner.config.batch_size,
            self.engine.inner.spill_quota.start_query(),
            self.engine.inner.spill_io.clone(),
            self.engine.inner.compute_scheduler.clone(),
            self.engine.inner.config.execution.clone(),
            self.engine.inner.config.max_concurrent_queries,
        )?);
        if let Some(database) = self.engine.inner.database.as_ref() {
            context.set_native_database_cleanup(Arc::downgrade(database));
        }
        context.configure_compute_lanes(self.engine.inner.config.compute_threads);
        Ok(context)
    }
}

fn query_result(
    schema: SchemaRef,
    stream: RecordBatchStream,
    context: Arc<QueryContext>,
    permit: OwnedSemaphorePermit,
    engine: Engine,
) -> QueryResult {
    let stream = instrument_output(stream, Arc::clone(&context), permit, engine);
    QueryResult {
        schema,
        stream,
        context,
    }
}

pub struct QueryResult {
    schema: SchemaRef,
    stream: RecordBatchStream,
    context: Arc<QueryContext>,
}

#[derive(Clone, Debug)]
pub struct QueryCancellation {
    control: QueryControl,
}

impl QueryCancellation {
    pub fn cancel(&self) {
        self.control.cancel();
    }
}

impl QueryResult {
    pub fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    pub fn stream(&mut self) -> &mut RecordBatchStream {
        &mut self.stream
    }

    pub fn into_stream(self) -> RecordBatchStream {
        self.stream
    }

    pub fn cancel(&self) {
        self.context.cancel();
    }

    pub fn cancellation_handle(&self) -> QueryCancellation {
        QueryCancellation {
            control: self.context.control.clone(),
        }
    }

    pub fn metrics(&self) -> QueryMetrics {
        self.context.metrics.clone()
    }

    pub fn query_id(&self) -> Uuid {
        self.context.query_id
    }
}

fn instrument_output(
    mut input: RecordBatchStream,
    context: Arc<QueryContext>,
    permit: OwnedSemaphorePermit,
    engine: Engine,
) -> RecordBatchStream {
    boxed_record_batch_stream(stream! {
        // A QueryResult has no lifetime tied to Session. Keep the compute
        // runtime alive in the returned stream, including after into_stream().
        let _engine_keepalive = engine;
        let _permit = permit;
        while let Some(item) = input.next().await {
            match item {
                Ok(batch) => {
                    if !context.native_commit_is_durable()
                        && let Err(error) = context.check_cancelled()
                    {
                        context.metrics.finish();
                        let error = context.tasks.first_failure().unwrap_or(error);
                        let mut error = context.error_with_cleanup_after_tasks(error).await;
                        context.release_catalog_snapshot();
                        if let Err(cleanup) = _engine_keepalive.drain_native_retired() {
                            error = Error::Execution(format!(
                                "{error}; additionally failed to clean retired native snapshots: {cleanup}"
                            ));
                        }
                        yield Err(error);
                        return;
                    }
                    context.metrics.record_output(
                        u64::try_from(batch.num_rows()).unwrap_or(u64::MAX),
                        1,
                        u64::try_from(batch.get_array_memory_size()).unwrap_or(u64::MAX),
                    );
                    yield Ok(batch);
                }
                Err(error) => {
                    context.metrics.finish();
                    let error = context.tasks.first_failure().unwrap_or(error);
                    // A QueryResult may remain alive after the consumer sees
                    // an execution error.  Clean this query's files now rather
                    // than waiting for QueryContext::drop().
                    let mut error = context.error_with_cleanup_after_tasks(error).await;
                    context.release_catalog_snapshot();
                    if let Err(cleanup) = _engine_keepalive.drain_native_retired() {
                        error = Error::Execution(format!(
                            "{error}; additionally failed to clean retired native snapshots: {cleanup}"
                        ));
                    }
                    if context.native_commit_is_durable() {
                        _engine_keepalive
                            .inner
                            .native_poisoned
                            .store(true, Ordering::Release);
                        error = context.error_after_native_commit(error);
                    }
                    yield Err(error);
                    return;
                }
            }
        }
        context.metrics.finish();
        if let Err(mut error) = context.cleanup_spill_after_tasks().await {
            context.release_catalog_snapshot();
            if let Err(cleanup) = _engine_keepalive.drain_native_retired() {
                error = Error::Execution(format!(
                    "{error}; additionally failed to clean retired native snapshots: {cleanup}"
                ));
            }
            if context.native_commit_is_durable() {
                _engine_keepalive
                    .inner
                    .native_poisoned
                    .store(true, Ordering::Release);
                error = context.error_after_native_commit(error);
            }
            yield Err(error);
            return;
        }
        context.release_catalog_snapshot();
        if let Err(error) = _engine_keepalive.drain_native_retired() {
            if context.native_commit_is_durable() {
                _engine_keepalive
                    .inner
                    .native_poisoned
                    .store(true, Ordering::Release);
                yield Err(context.error_after_native_commit(error));
            } else {
                yield Err(error);
            }
        }
    })
}

fn normalize_locations<I, S>(locations: I) -> Vec<String>
where
    I: IntoIterator<Item = S>,
    S: Into<String>,
{
    locations.into_iter().map(Into::into).collect()
}

fn validate_config(config: &EngineConfig) -> Result<()> {
    if config.memory_limit == 0 {
        return Err(Error::InvalidArgument(
            "memory_limit must be greater than zero".to_owned(),
        ));
    }
    if config.batch_size == 0 {
        return Err(Error::InvalidArgument(
            "batch_size must be greater than zero".to_owned(),
        ));
    }
    if config.compute_threads == 0 || config.io_concurrency == 0 {
        return Err(Error::InvalidArgument(
            "compute_threads and io_concurrency must be greater than zero".to_owned(),
        ));
    }
    if config.max_concurrent_queries == 0 {
        return Err(Error::InvalidArgument(
            "max_concurrent_queries must be greater than zero".to_owned(),
        ));
    }
    if config.s3.anonymous && config.s3.credential_provider.is_some() {
        return Err(Error::InvalidArgument(
            "S3 anonymous access and a credential provider are mutually exclusive".to_owned(),
        ));
    }
    if let Some(endpoint) = &config.s3.endpoint {
        crate::storage::validate_endpoint(endpoint, config.s3.allow_http)?;
    }
    config.csv_scan.validate()?;
    config.execution.validate()?;
    config.spill.validate()?;
    ensure_directory_parent(&config.spill.directory)
}

fn ensure_directory_parent(path: &Path) -> Result<()> {
    match path.parent() {
        Some(parent) if !parent.as_os_str().is_empty() && !parent.exists() => {
            std::fs::create_dir_all(parent)
                .map_err(|error| Error::io(Some(parent.to_path_buf()), error))
        }
        _ => Ok(()),
    }
}

#[cfg(test)]
mod tests;

#[path = "engine/memory_snapshot.rs"]
mod memory_snapshot;

#[path = "engine/native_write.rs"]
mod native_write;
