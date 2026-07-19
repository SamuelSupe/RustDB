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
use tokio::sync::{Mutex as AsyncMutex, OwnedSemaphorePermit, Semaphore};
use uuid::Uuid;

use crate::{
    Catalog, CsvOptions, EngineConfig, Error, ParquetOptions, PreparedStatement, QueryMetrics,
    Result, TableEntry,
    catalog::PersistentCatalog,
    command::{ParsedStatement, SessionCommand, ViewTable},
    datasource::{
        MetadataCache, NativeSystemTable, RegisteredCsvTable, RegisteredParquetTable,
        SystemTableKind,
    },
    runtime::{
        ComputeRuntime, GlobalComputeScheduler, MemoryPool, QueryContext, QueryControl,
        RecordBatchStream, SpillIoPool, SpillManager, SpillQuotaPool, boxed_record_batch_stream,
        scavenge_orphans,
    },
    sql::{LogicalPlan, StatementPlan},
    storage::{
        NativeCheckReport, NativeDatabase, NativeRepairPlan, NativeRepairReport, RemoteTempDir,
        RemoteTempKind, scavenge_remote_temp_orphans,
    },
};

pub use memory_snapshot::EngineMemorySnapshot;
pub use transaction::{
    CommitInfo, Transaction, TransactionAccessMode, TransactionOptions,
    TransactionPreparedStatement,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MigrationInfo {
    from_version: u32,
    to_version: u32,
    backup_path: Option<std::path::PathBuf>,
}

impl MigrationInfo {
    pub fn from_version(&self) -> u32 {
        self.from_version
    }

    pub fn to_version(&self) -> u32 {
        self.to_version
    }

    pub fn backup_path(&self) -> Option<&Path> {
        self.backup_path.as_deref()
    }

    pub fn migrated(&self) -> bool {
        self.from_version != self.to_version
    }
}

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
    transactions: transaction_manager::TransactionManager,
    native_commit: parking_lot::Mutex<()>,
    import_gate: AsyncMutex<()>,
    native_poisoned: AtomicBool,
}

impl Engine {
    /// Performs a strictly read-only integrity check of a Native database.
    ///
    /// Integrity failures are accumulated in the returned report. This path
    /// never opens the database, acquires its lock, replays WAL, or cleans
    /// temporary files.
    pub fn check_native(path: impl AsRef<Path>) -> Result<NativeCheckReport> {
        NativeDatabase::check(path)
    }

    /// Builds a strictly read-only, conservative Native repair plan.
    pub fn plan_native_repair(path: impl AsRef<Path>) -> Result<NativeRepairPlan> {
        NativeDatabase::plan_repair(path)
    }

    /// Revalidates and applies a conservative Native repair plan.
    pub fn apply_native_repair(path: impl AsRef<Path>) -> Result<NativeRepairReport> {
        NativeDatabase::apply_repair(path)
    }

    pub fn migrate(path: impl AsRef<Path>) -> Result<MigrationInfo> {
        let migration = NativeDatabase::migrate(path)?;
        Ok(MigrationInfo {
            from_version: migration.from_version,
            to_version: migration.to_version,
            backup_path: migration.backup_path,
        })
    }

    pub fn new(config: EngineConfig) -> Result<Self> {
        config.validate()?;
        Self::from_validated_config(config, None)
    }

    pub fn open(path: impl AsRef<Path>, config: EngineConfig) -> Result<Self> {
        config.validate()?;
        let database = NativeDatabase::open_with_storage(path, config.native_storage.clone())?;
        Self::from_validated_config(config, Some(database))
    }

    pub fn restore_from(
        backup: impl AsRef<Path>,
        destination: impl AsRef<Path>,
        config: EngineConfig,
    ) -> Result<Self> {
        config.validate()?;
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
        scavenge_remote_temp_orphans(&config.spill.directory, config.spill.orphan_ttl)?;
        let memory = MemoryPool::named_root("engine", config.memory_limit);
        SpillManager::protect_io_headroom(&memory, config.spill.io_threads)?;
        let admission = Arc::new(Semaphore::new(config.max_concurrent_queries));
        let compute = ComputeRuntime::new(config.compute_threads)?;
        let compute_scheduler = GlobalComputeScheduler::new(config.compute_threads)?;
        let metadata_cache = MetadataCache::new(config.metadata_cache_bytes);
        let persistent_catalog = match database.as_ref() {
            Some(database) => {
                let entries = external_source_api::persistent_entries(
                    &config,
                    metadata_cache.clone(),
                    database,
                )?;
                PersistentCatalog::new(database.catalog_generation(), entries)?
            }
            None => PersistentCatalog::default(),
        };
        let spill_quota = SpillQuotaPool::new(config.spill.clone())?;
        let spill_io = SpillIoPool::new(config.spill.io_threads)?;
        let transactions = transaction_manager::TransactionManager::default();
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
                transactions,
                native_commit: parking_lot::Mutex::new(()),
                import_gate: AsyncMutex::new(()),
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

    /// Returns an error when this engine has observed an ambiguous persistent
    /// Native publication and must be reopened before accepting more work.
    pub fn health_check(&self) -> Result<()> {
        self.ensure_native_healthy()
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

    /// Creates a consistent local-directory or S3/MinIO Native backup.
    ///
    /// An S3 location is an object prefix. Its manifest is published only
    /// after every immutable backup object has completed and passed hashing.
    pub async fn backup_to_location(&self, location: &str) -> Result<()> {
        if !location.starts_with("s3://") {
            return self.backup_to(local_location_path(location)?);
        }
        let temporary =
            RemoteTempDir::create(&self.inner.config.spill.directory, RemoteTempKind::Backup)?;
        let snapshot = temporary.path().join("snapshot");
        if let Err(error) = self.backup_to(&snapshot) {
            return temporary.finish(Err(error));
        }
        let location = location.to_owned();
        let s3 = self.inner.config.s3.clone();
        self.run_owned_remote_backup(async move {
            let result = crate::storage::upload_remote_backup(&snapshot, &location, &s3).await;
            temporary.finish(result)
        })
        .await
    }

    /// Runs a remote backup to completion even when its caller abandons the
    /// public async future. The retained Engine keeps the runtime alive; the
    /// storage worker either publishes the manifest or performs its own
    /// multipart/object cleanup before returning.
    async fn run_owned_remote_backup<F>(&self, future: F) -> Result<()>
    where
        F: std::future::Future<Output = Result<()>> + Send + 'static,
    {
        let (sender, receiver) = tokio::sync::oneshot::channel();
        let engine_keepalive = self.clone();
        self.inner.compute.spawn_background(async move {
            let _engine_keepalive = engine_keepalive;
            let result = future.await;
            if let Err(result) = sender.send(result)
                && let Err(error) = result
            {
                tracing::error!(%error, "abandoned remote backup reached a terminal error");
            }
        });
        receiver.await.map_err(|_| {
            Error::Internal("engine-owned remote backup worker stopped unexpectedly".to_owned())
        })?
    }

    /// Restores a local-directory or S3/MinIO backup into a new Native path.
    pub async fn restore_from_location(
        backup: &str,
        destination: impl AsRef<Path>,
        config: EngineConfig,
    ) -> Result<Self> {
        config.validate()?;
        if !backup.starts_with("s3://") {
            return Self::restore_from(local_location_path(backup)?, destination, config);
        }
        std::fs::create_dir_all(&config.spill.directory)
            .map_err(|error| Error::io(Some(config.spill.directory.clone()), error))?;
        scavenge_remote_temp_orphans(&config.spill.directory, config.spill.orphan_ttl)?;
        let backup = backup.to_owned();
        let destination = destination.as_ref().to_owned();
        let (sender, receiver) = tokio::sync::oneshot::channel();
        let _worker = tokio::spawn(async move {
            let result = async {
                let downloaded = crate::storage::download_remote_backup(
                    &backup,
                    &config.spill.directory,
                    &config.s3,
                )
                .await?;
                let restored = Self::restore_from(downloaded.snapshot(), destination, config);
                downloaded.finish(restored)
            }
            .await;
            if let Err(result) = sender.send(result)
                && let Err(error) = result
            {
                tracing::error!(%error, "abandoned remote restore reached a terminal error");
            }
        });
        receiver.await.map_err(|_| {
            Error::Internal("owned remote restore worker stopped unexpectedly".to_owned())
        })?
    }

    pub fn session(&self) -> Session {
        let catalog = Catalog::with_persistent(self.inner.persistent_catalog.clone());
        if let Some(database) = self.inner.database.as_ref() {
            for (name, kind) in [
                (
                    "information_schema.schemata",
                    SystemTableKind::InformationSchemata,
                ),
                (
                    "information_schema.tables",
                    SystemTableKind::InformationTables,
                ),
                (
                    "information_schema.columns",
                    SystemTableKind::InformationColumns,
                ),
                ("rustdb_system.tables", SystemTableKind::NativeTables),
                ("rustdb_system.wal", SystemTableKind::Wal),
            ] {
                catalog
                    .register(TableEntry::new(
                        name,
                        Arc::new(NativeSystemTable::new(Arc::clone(database), kind)),
                    ))
                    .expect("system table registration is infallible");
            }
        }
        Session {
            engine: self.clone(),
            catalog,
            sql_transaction: Arc::new(AsyncMutex::new(None)),
            native_transaction: None,
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

    #[cfg(test)]
    pub(crate) fn transaction_counts_for_test(&self) -> (usize, usize) {
        self.inner.transactions.counts()
    }

    #[allow(dead_code)]
    pub(crate) fn oldest_transaction_snapshot_generation(&self) -> Option<u64> {
        self.inner.transactions.oldest_snapshot_generation()
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
    sql_transaction: Arc<AsyncMutex<Option<Transaction>>>,
    native_transaction: Option<Arc<transaction::TransactionWorkspace>>,
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

    fn schema_names(&self) -> Vec<String> {
        match self.native_transaction.as_ref() {
            Some(transaction) => transaction.schema_names(),
            None => self
                .engine
                .inner
                .database
                .as_ref()
                .map(|database| database.schema_names())
                .unwrap_or_else(|| vec![crate::catalog_name::DEFAULT_SCHEMA.to_owned()]),
        }
    }

    fn ensure_schema_for(&self, name: &str) -> Result<()> {
        let schema = crate::catalog_name::schema_of(name);
        if self.schema_names().iter().any(|name| name == schema) {
            return Ok(());
        }
        Err(Error::Catalog(format!("schema '{schema}' does not exist")))
    }

    pub fn prepare(&self, sql: &str) -> Result<PreparedStatement> {
        PreparedStatement::new(self.clone(), sql)
    }

    pub fn begin_transaction(&self, options: TransactionOptions) -> Result<Transaction> {
        Transaction::begin(self, options)
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
        let name = crate::catalog_name::local(&name.into(), "registered table")?;
        self.ensure_schema_for(&name)?;
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
        let name = crate::catalog_name::local(&name.into(), "registered table")?;
        self.ensure_schema_for(&name)?;
        let locations = normalize_locations(locations);
        let provider =
            RegisteredCsvTable::try_new(locations, options, &self.engine.inner.config).await?;
        self.catalog
            .register(TableEntry::new(name, Arc::new(provider)))
    }

    pub async fn refresh_table(&self, name: &str) -> Result<SchemaRef> {
        let name = crate::catalog_name::local(name, "registered table")?;
        let entry = self
            .catalog
            .table(&name)
            .ok_or_else(|| Error::Catalog(format!("table '{name}' does not exist")))?;
        let replacement = entry.provider().refreshed().await?.ok_or_else(|| {
            Error::Catalog(format!(
                "table '{name}' is not a refreshable external table"
            ))
        })?;
        let schema = replacement.schema();
        self.catalog
            .replace_provider(&name, entry.provider(), replacement)?;
        Ok(schema)
    }

    pub async fn execute(&self, sql: &str) -> Result<QueryResult> {
        let parse_started = Instant::now();
        let parsed = crate::command::parse(sql);
        let parse_time = parse_started.elapsed();
        let parsed = parsed?;

        if is_transaction_control(&parsed) {
            let ParsedStatement::Command(command) = parsed else {
                unreachable!("transaction control is always a command")
            };
            return self.execute_transaction_control(command, parse_time).await;
        }

        let mut transaction = self.sql_transaction.lock().await;
        if transaction
            .as_ref()
            .is_some_and(Transaction::can_release_session)
        {
            transaction.take();
        }
        if let Some(transaction) = transaction.as_ref() {
            return transaction.execute(sql).await;
        }
        drop(transaction);

        self.execute_parsed(parsed, parse_time).await
    }

    pub(super) async fn execute_direct(&self, sql: &str) -> Result<QueryResult> {
        let parse_started = Instant::now();
        let parsed = crate::command::parse(sql)?;
        self.execute_parsed(parsed, parse_started.elapsed()).await
    }

    pub(super) async fn execute_http_read_only_direct(&self, sql: &str) -> Result<QueryResult> {
        self.execute_http_read_only_direct_with_memory_limit(sql, None)
            .await
    }

    pub(crate) async fn execute_http_read_only_direct_with_memory_limit(
        &self,
        sql: &str,
        memory_limit: Option<usize>,
    ) -> Result<QueryResult> {
        let parse_started = Instant::now();
        let parsed = crate::command::parse(sql)?;
        self.execute_parsed_mode_with_memory_limit(
            parsed,
            parse_started.elapsed(),
            true,
            memory_limit,
        )
        .await
    }

    async fn execute_parsed(
        &self,
        parsed: ParsedStatement,
        parse_time: Duration,
    ) -> Result<QueryResult> {
        self.execute_parsed_mode(parsed, parse_time, false).await
    }

    async fn execute_parsed_mode(
        &self,
        parsed: ParsedStatement,
        parse_time: Duration,
        http_read_only: bool,
    ) -> Result<QueryResult> {
        self.execute_parsed_mode_with_memory_limit(parsed, parse_time, http_read_only, None)
            .await
    }

    async fn execute_parsed_mode_with_memory_limit(
        &self,
        parsed: ParsedStatement,
        parse_time: Duration,
        http_read_only: bool,
        memory_limit: Option<usize>,
    ) -> Result<QueryResult> {
        let admission_started = Instant::now();
        let permit = self.acquire_query_permit().await?;
        let admission_wait = admission_started.elapsed();

        match parsed {
            ParsedStatement::Command(command) => {
                self.execute_command(command, permit, admission_wait, parse_time)
                    .await
            }
            ParsedStatement::Query(statement) => {
                self.execute_query(
                    *statement,
                    permit,
                    admission_wait,
                    parse_time,
                    http_read_only,
                    memory_limit,
                )
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
        http_read_only: bool,
        memory_limit: Option<usize>,
    ) -> Result<QueryResult> {
        // Start query accounting before file-function schema discovery so the
        // reported elapsed time includes planning and metadata preparation.
        let context = self.query_context_with_memory_limit(memory_limit)?;
        if http_read_only {
            context.enable_http_read_only();
        }
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
        let mut transaction = self.sql_transaction.lock().await;
        if transaction
            .as_ref()
            .is_some_and(Transaction::can_release_session)
        {
            transaction.take();
        }
        if let Some(transaction) = transaction.as_ref() {
            return transaction.execute_statement(statement).await;
        }
        drop(transaction);
        self.execute_prepared_direct(statement).await
    }

    pub(super) async fn execute_prepared_direct(
        &self,
        statement: sqlparser::ast::Statement,
    ) -> Result<QueryResult> {
        self.execute_prepared_mode(statement, false).await
    }

    pub(crate) async fn execute_prepared_http_read_only_with_memory_limit(
        &self,
        statement: sqlparser::ast::Statement,
        memory_limit: Option<usize>,
    ) -> Result<QueryResult> {
        self.execute_prepared_mode_with_memory_limit(statement, true, memory_limit)
            .await
    }

    async fn execute_prepared_mode(
        &self,
        statement: sqlparser::ast::Statement,
        http_read_only: bool,
    ) -> Result<QueryResult> {
        self.execute_prepared_mode_with_memory_limit(statement, http_read_only, None)
            .await
    }

    async fn execute_prepared_mode_with_memory_limit(
        &self,
        statement: sqlparser::ast::Statement,
        http_read_only: bool,
        memory_limit: Option<usize>,
    ) -> Result<QueryResult> {
        let admission_started = Instant::now();
        let permit = self.acquire_query_permit().await?;
        let admission_wait = admission_started.elapsed();
        let context = self.query_context_with_memory_limit(memory_limit)?;
        if http_read_only {
            context.enable_http_read_only();
        }
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
        if let SessionCommand::CopyTo(command) = command {
            return self
                .execute_copy_to(command, permit, admission_wait, parse_time)
                .await;
        }
        if let SessionCommand::Maintenance(command) = command {
            return self
                .execute_maintenance(command, permit, admission_wait, parse_time)
                .await;
        }
        if let SessionCommand::NativeAlter(command) = command {
            return self
                .execute_native_alter(command, permit, admission_wait, parse_time)
                .await;
        }
        if let SessionCommand::CreatePersistentView {
            name,
            query,
            replace,
        } = command
        {
            return self
                .execute_persistent_view_create(
                    name,
                    query,
                    replace,
                    permit,
                    admission_wait,
                    parse_time,
                )
                .await;
        }
        if let SessionCommand::DropView { name, if_exists } = command {
            return self
                .execute_view_drop(name, if_exists, permit, admission_wait, parse_time)
                .await;
        }
        if let SessionCommand::NativeDelete(command) = command {
            return self
                .execute_native_delete(command, permit, admission_wait, parse_time)
                .await;
        }
        if let SessionCommand::NativeDropTable(command) = command {
            return self
                .execute_native_drop_table(command, permit, admission_wait, parse_time)
                .await;
        }
        if let SessionCommand::NativeUpdate(command) = command {
            return self
                .execute_native_update(command, permit, admission_wait, parse_time)
                .await;
        }
        if let SessionCommand::NativeTruncate(command) = command {
            return self
                .execute_native_truncate(command, permit, admission_wait, parse_time)
                .await;
        }
        if let SessionCommand::NativeSchema(command) = command {
            return self
                .execute_native_schema(command, permit, admission_wait, parse_time)
                .await;
        }
        let context = self.query_context()?;
        context.metrics.record_query_admission_wait(admission_wait);
        context.metrics.record_sql_parse_time(parse_time);
        let batch = async {
            match command {
                SessionCommand::BeginTransaction { .. }
                | SessionCommand::CommitTransaction
                | SessionCommand::RollbackTransaction => {
                    unreachable!("transaction control bypasses query admission")
                }
                SessionCommand::ShowTables => crate::command::show_tables(&self.pin_catalog()?),
                SessionCommand::ShowSchemas => crate::command::show_schemas(self.schema_names()),
                SessionCommand::Describe { name } => {
                    crate::command::describe(&self.pin_catalog()?, &name)
                }
                SessionCommand::RefreshTable { name } => {
                    self.refresh_table(&name).await?;
                    crate::command::status("REFRESH TABLE")
                }
                SessionCommand::CreateTempView {
                    name,
                    query,
                    replace,
                } => {
                    self.ensure_schema_for(&name)?;
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
                SessionCommand::CreatePersistentView { .. } => {
                    unreachable!("handled before command dispatch")
                }
                SessionCommand::DropView { .. } => unreachable!("handled before command dispatch"),
                SessionCommand::CopyTo(_) => unreachable!("handled before command dispatch"),
                SessionCommand::Maintenance(_) => {
                    unreachable!("handled before command dispatch")
                }
                SessionCommand::NativeWrite(_) => unreachable!("handled before command dispatch"),
                SessionCommand::NativeAlter(_) => unreachable!("handled before command dispatch"),
                SessionCommand::NativeDelete(_) => {
                    unreachable!("handled before command dispatch")
                }
                SessionCommand::NativeDropTable(_) => {
                    unreachable!("handled before command dispatch")
                }
                SessionCommand::NativeUpdate(_) => {
                    unreachable!("handled before command dispatch")
                }
                SessionCommand::NativeTruncate(_) => {
                    unreachable!("handled before command dispatch")
                }
                SessionCommand::NativeSchema(_) => {
                    unreachable!("handled before command dispatch")
                }
            }
        }
        .await;
        let batch = match batch {
            Ok(batch) => batch,
            Err(error) => return Err(context.error_with_cleanup(error)),
        };
        self.batch_result(batch, permit, context)
    }

    async fn execute_transaction_control(
        &self,
        command: SessionCommand,
        parse_time: Duration,
    ) -> Result<QueryResult> {
        let context = self.query_context()?;
        context.metrics.record_sql_parse_time(parse_time);
        let batch = match command {
            SessionCommand::BeginTransaction { read_only } => {
                let mut active = self.sql_transaction.lock().await;
                if active
                    .as_ref()
                    .is_some_and(Transaction::can_release_session)
                {
                    active.take();
                }
                if active.is_some() {
                    return Err(context.error_with_cleanup(Error::InvalidArgument(
                        "a transaction is already active for this session".to_owned(),
                    )));
                }
                let options = if read_only {
                    TransactionOptions::read_only()
                } else {
                    TransactionOptions::read_write()
                };
                *active = Some(
                    Transaction::begin(self, options)
                        .map_err(|error| context.error_with_cleanup(error))?,
                );
                crate::command::status("BEGIN")
            }
            SessionCommand::CommitTransaction => {
                let mut active = self.sql_transaction.lock().await;
                let transaction = active.as_mut().ok_or_else(|| {
                    context.error_with_cleanup(Error::InvalidArgument(
                        "no transaction is active".to_owned(),
                    ))
                })?;
                let outcome = transaction.commit();
                if let Err(Error::NativeCommitPostCommitFailure {
                    path,
                    transaction_id,
                    generation,
                    ..
                }) = &outcome
                {
                    context.mark_native_commit(path.clone(), transaction_id.clone(), *generation);
                }
                if outcome.is_err() && transaction.can_release_session() {
                    active.take();
                }
                let commit = outcome.map_err(|error| {
                    let error = context.error_with_cleanup(error);
                    context.error_after_durable_outcome(error)
                })?;
                if let Some(generation) = commit.committed_generation()
                    && let Some(database) = self.engine.inner.database.as_ref()
                {
                    context.mark_native_commit(
                        database.path().to_path_buf(),
                        commit.transaction_id().to_string(),
                        generation,
                    );
                }
                active.take();
                crate::command::status("COMMIT")
            }
            SessionCommand::RollbackTransaction => {
                let mut active = self.sql_transaction.lock().await;
                let transaction = active.as_mut().ok_or_else(|| {
                    context.error_with_cleanup(Error::InvalidArgument(
                        "no transaction is active".to_owned(),
                    ))
                })?;
                let outcome = transaction.rollback();
                if outcome.is_err() && transaction.can_release_session() {
                    active.take();
                }
                outcome.map_err(|error| context.error_with_cleanup(error))?;
                active.take();
                crate::command::status("ROLLBACK")
            }
            _ => unreachable!("non-transaction command entered transaction control"),
        }?;
        self.batch_result_without_admission(batch, context)
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
        let catalog = self.pin_catalog()?;
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
        let catalog = self.pin_catalog()?;
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

    fn batch_result_without_admission(
        &self,
        batch: RecordBatch,
        context: Arc<QueryContext>,
    ) -> Result<QueryResult> {
        let schema = batch.schema();
        let stream = boxed_record_batch_stream(futures::stream::once(async move { Ok(batch) }));
        Ok(query_result_inner(
            schema,
            stream,
            context,
            None,
            self.engine.clone(),
        ))
    }

    fn query_context(&self) -> Result<Arc<QueryContext>> {
        self.query_context_with_memory_limit(None)
    }

    fn query_context_with_memory_limit(
        &self,
        memory_limit: Option<usize>,
    ) -> Result<Arc<QueryContext>> {
        let query_id = Uuid::new_v4();
        let memory_limit = memory_limit
            .unwrap_or(self.engine.inner.config.memory_limit)
            .min(self.engine.inner.config.memory_limit);
        if memory_limit == 0 {
            return Err(Error::InvalidArgument(
                "query memory limit must be greater than zero".into(),
            ));
        }
        let query_memory = self
            .engine
            .inner
            .memory
            .child(format!("query-{query_id}"), memory_limit);
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

    pub(super) fn pin_catalog(&self) -> Result<Catalog> {
        match self.native_transaction.as_ref() {
            Some(transaction) => transaction.pin_catalog(&self.catalog),
            None => Ok(self.catalog.pin()),
        }
    }
}

fn is_transaction_control(parsed: &ParsedStatement) -> bool {
    matches!(
        parsed,
        ParsedStatement::Command(
            SessionCommand::BeginTransaction { .. }
                | SessionCommand::CommitTransaction
                | SessionCommand::RollbackTransaction
        )
    )
}

fn query_result(
    schema: SchemaRef,
    stream: RecordBatchStream,
    context: Arc<QueryContext>,
    permit: OwnedSemaphorePermit,
    engine: Engine,
) -> QueryResult {
    query_result_inner(schema, stream, context, Some(permit), engine)
}

fn query_result_inner(
    schema: SchemaRef,
    stream: RecordBatchStream,
    context: Arc<QueryContext>,
    permit: Option<OwnedSemaphorePermit>,
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

    pub(crate) async fn cancel_and_quiesce(&mut self) -> Result<()> {
        self.context.cancel();
        self.context.cleanup_spill_after_tasks().await
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
    permit: Option<OwnedSemaphorePermit>,
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
                    if !context.durable_outcome_is_committed()
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
                    let mut native_cleanup_failed = false;
                    if let Err(cleanup) = _engine_keepalive.drain_native_retired() {
                        native_cleanup_failed = true;
                        error = Error::Execution(format!(
                            "{error}; additionally failed to clean retired native snapshots: {cleanup}"
                        ));
                    }
                    if context.durable_outcome_is_committed() {
                        // Result delivery can be cancelled after the catalog
                        // commit is already durable. That outcome must be
                        // reported as committed without poisoning an otherwise
                        // healthy engine. Only a native cleanup failure here
                        // requires reopen; commit/install failures poison at
                        // their source before reaching this wrapper.
                        if native_cleanup_failed {
                            _engine_keepalive
                                .inner
                                .native_poisoned
                                .store(true, Ordering::Release);
                        }
                        error = context.error_after_durable_outcome(error);
                    }
                    yield Err(error);
                    return;
                }
            }
        }
        context.metrics.finish();
        if let Err(mut error) = context.cleanup_spill_after_tasks().await {
            context.release_catalog_snapshot();
            let mut native_cleanup_failed = false;
            if let Err(cleanup) = _engine_keepalive.drain_native_retired() {
                native_cleanup_failed = true;
                error = Error::Execution(format!(
                    "{error}; additionally failed to clean retired native snapshots: {cleanup}"
                ));
            }
            if context.durable_outcome_is_committed() {
                if native_cleanup_failed {
                    _engine_keepalive
                        .inner
                        .native_poisoned
                        .store(true, Ordering::Release);
                }
                error = context.error_after_durable_outcome(error);
            }
            yield Err(error);
            return;
        }
        context.release_catalog_snapshot();
        if let Err(error) = _engine_keepalive.drain_native_retired() {
            if context.durable_outcome_is_committed() {
                _engine_keepalive
                    .inner
                    .native_poisoned
                    .store(true, Ordering::Release);
                yield Err(context.error_after_durable_outcome(error));
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

fn local_location_path(location: &str) -> Result<std::path::PathBuf> {
    if !location.starts_with("file://") {
        return Ok(std::path::PathBuf::from(location));
    }
    let url = url::Url::parse(location)
        .map_err(|error| Error::InvalidArgument(format!("invalid file URI: {error}")))?;
    if !url.username().is_empty() || url.password().is_some() {
        return Err(Error::InvalidArgument(
            "file URI must not contain user information".to_owned(),
        ));
    }
    if url.query().is_some() || url.fragment().is_some() {
        return Err(Error::InvalidArgument(
            "file URI must not contain a query or fragment".to_owned(),
        ));
    }
    url.to_file_path()
        .map_err(|()| Error::InvalidArgument("file URI is not a local path".to_owned()))
}

#[cfg(test)]
mod tests;

#[path = "engine/copy.rs"]
mod copy;
#[path = "engine/copy_sink.rs"]
mod copy_sink;
#[path = "engine/external_source.rs"]
mod external_source_api;
#[path = "engine/import.rs"]
mod import;
#[path = "engine/maintenance.rs"]
mod maintenance;
#[path = "engine/memory_snapshot.rs"]
mod memory_snapshot;

#[path = "engine/native_alter.rs"]
mod native_alter;
#[path = "engine/native_delete.rs"]
mod native_delete;
#[path = "engine/native_drop.rs"]
mod native_drop;
#[path = "engine/native_matches.rs"]
mod native_matches;
#[path = "engine/native_schema.rs"]
mod native_schema;
#[path = "engine/native_truncate.rs"]
mod native_truncate;
#[path = "engine/native_update.rs"]
mod native_update;
#[path = "engine/native_view.rs"]
mod native_view;
#[path = "engine/native_write.rs"]
mod native_write;
#[path = "engine/transaction.rs"]
mod transaction;
#[path = "engine/transaction_manager.rs"]
mod transaction_manager;
