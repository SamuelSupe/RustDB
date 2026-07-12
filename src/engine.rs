use std::{path::Path, sync::Arc};

use arrow::{datatypes::SchemaRef, record_batch::RecordBatch};
use async_stream::stream;
use futures::StreamExt;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use uuid::Uuid;

use crate::{
    Catalog, CsvOptions, EngineConfig, Error, ParquetOptions, QueryMetrics, Result, TableEntry,
    command::{SessionCommand, ViewTable},
    datasource::{MetadataCache, RegisteredCsvTable, RegisteredParquetTable},
    runtime::{
        ComputeRuntime, MemoryPool, QueryContext, QueryControl, RecordBatchStream, SpillIoPool,
        SpillManager, SpillQuotaPool, boxed_record_batch_stream, scavenge_orphans,
    },
    sql::{LogicalPlan, StatementPlan},
};

#[derive(Clone)]
pub struct Engine {
    inner: Arc<EngineInner>,
}

struct EngineInner {
    config: EngineConfig,
    memory: MemoryPool,
    admission: Arc<Semaphore>,
    compute: ComputeRuntime,
    metadata_cache: MetadataCache,
    spill_quota: SpillQuotaPool,
    spill_io: SpillIoPool,
}

impl Engine {
    pub fn new(config: EngineConfig) -> Result<Self> {
        validate_config(&config)?;
        std::fs::create_dir_all(&config.spill.directory)
            .map_err(|error| Error::io(Some(config.spill.directory.clone()), error))?;
        scavenge_orphans(&config.spill.directory, config.spill.orphan_ttl)?;
        let memory = MemoryPool::named_root("engine", config.memory_limit);
        SpillManager::protect_io_headroom(&memory, config.spill.io_threads)?;
        let admission = Arc::new(Semaphore::new(config.max_concurrent_queries));
        let compute = ComputeRuntime::new(config.compute_threads)?;
        let metadata_cache = MetadataCache::new(config.metadata_cache_bytes);
        let spill_quota = SpillQuotaPool::new(config.spill.clone())?;
        let spill_io = SpillIoPool::new(config.spill.io_threads)?;
        Ok(Self {
            inner: Arc::new(EngineInner {
                config,
                memory,
                admission,
                compute,
                metadata_cache,
                spill_quota,
                spill_io,
            }),
        })
    }

    pub fn config(&self) -> &EngineConfig {
        &self.inner.config
    }

    pub fn session(&self) -> Session {
        Session {
            engine: self.clone(),
            catalog: Catalog::default(),
        }
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
        let permit = Arc::clone(&self.engine.inner.admission)
            .acquire_owned()
            .await
            .map_err(|_| Error::Internal("query admission controller closed".to_owned()))?;

        if let Some(command) = crate::command::parse(sql)? {
            return self.execute_command(command, permit).await;
        }

        self.execute_query(sql, permit).await
    }

    async fn execute_query(&self, sql: &str, permit: OwnedSemaphorePermit) -> Result<QueryResult> {
        // Start query accounting before file-function schema discovery so the
        // reported elapsed time includes planning and metadata preparation.
        let context = self.query_context()?;
        let plan = match self
            .prepare_statement_for_query(sql, Some(Arc::clone(&context)))
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

    async fn execute_command(
        &self,
        command: SessionCommand,
        permit: OwnedSemaphorePermit,
    ) -> Result<QueryResult> {
        let context = self.query_context()?;
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
        let prepared = crate::table_function::prepare_with_cache_for_query(
            &self.catalog,
            &self.engine.inner.config,
            &self.engine.inner.metadata_cache,
            sql,
            context.clone(),
        )
        .await?;
        let crate::table_function::PreparedSql {
            statement,
            generated_tables,
        } = prepared;
        let planned = async {
            let bound = crate::sql::bind_statement(&self.catalog, statement)?;
            if let Some(context) = context.as_ref() {
                crate::execution::prepare_plan(bound.logical_plan(), Arc::clone(context)).await?;
                context.seal_object_snapshots();
            }
            crate::sql::optimize_statement(bound, context.as_deref())
        }
        .await;
        for name in generated_tables {
            self.catalog.unregister(&name);
        }
        planned
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
        )?);
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
                    if let Err(error) = context.check_cancelled() {
                        context.metrics.finish();
                        let error = context.tasks.first_failure().unwrap_or(error);
                        yield Err(context.error_with_cleanup_after_tasks(error).await);
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
                    yield Err(context.error_with_cleanup_after_tasks(error).await);
                    return;
                }
            }
        }
        context.metrics.finish();
        if let Err(error) = context.cleanup_spill_after_tasks().await {
            yield Err(error);
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
mod tests {
    use std::{
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        time::Duration,
    };

    use arrow::{
        array::{Int64Array, StringArray},
        datatypes::Schema,
        record_batch::RecordBatch,
    };
    use futures::{StreamExt, TryStreamExt, future::try_join_all};

    use crate::{
        CsvHeader, CsvOptions, Engine, EngineConfig, Error, QueryResult, SpillConfig,
        sql::StatementPlan,
    };

    #[test]
    fn rejects_zero_sized_batches() {
        let config = EngineConfig {
            batch_size: 0,
            ..EngineConfig::default()
        };
        assert!(Engine::new(config).is_err());
    }

    #[tokio::test]
    async fn session_stream_preserves_producer_error_after_task_cancellation() {
        let directory = tempfile::tempdir().unwrap();
        let values = directory.path().join("values.csv");
        std::fs::write(&values, "id\n1\n2\n").unwrap();
        let session = Engine::new(
            EngineConfig::builder()
                .spill_directory(directory.path().join("spill"))
                .build(),
        )
        .unwrap()
        .session();

        let mut result = session.execute("SELECT 1 / 0").await.unwrap();
        let error = result.stream().next().await.unwrap().unwrap_err();
        assert!(
            matches!(&error, Error::Execution(message) if message.contains("division by zero")),
            "producer error was replaced by {error}"
        );
        assert!(result.stream().next().await.is_none());

        let sql = format!(
            "SELECT (SELECT id FROM read_csv('{}', header = true))",
            values.display()
        );
        let mut result = session.execute(&sql).await.unwrap();
        let error = result.stream().next().await.unwrap().unwrap_err();
        assert!(
            matches!(&error, Error::Execution(message) if message.contains("scalar subquery returned more than one row")),
            "producer error was replaced by {error}"
        );
        assert!(result.stream().next().await.is_none());
    }

    #[tokio::test]
    async fn query_result_streams_keep_runtime_alive_after_session_drop() {
        let directory = tempfile::tempdir().unwrap();
        let mut result = {
            let engine = Engine::new(
                EngineConfig::builder()
                    .compute_threads(1)
                    .spill_directory(directory.path().join("spill"))
                    .build(),
            )
            .unwrap();
            let session = engine.session();
            let result = session.execute("SELECT 1 AS value").await.unwrap();
            drop(session);
            drop(engine);
            result
        };
        let batches = tokio::time::timeout(
            Duration::from_secs(2),
            result.stream().try_collect::<Vec<_>>(),
        )
        .await
        .expect("QueryResult must outlive its creating Engine and Session")
        .unwrap();
        assert_single_value(&batches);
        drop(result);

        let stream = {
            let engine = Engine::new(
                EngineConfig::builder()
                    .compute_threads(1)
                    .spill_directory(directory.path().join("spill"))
                    .build(),
            )
            .unwrap();
            let session = engine.session();
            let stream = session
                .execute("SELECT 1 AS value")
                .await
                .unwrap()
                .into_stream();
            drop(session);
            drop(engine);
            stream
        };

        let batches = tokio::time::timeout(Duration::from_secs(2), stream.try_collect::<Vec<_>>())
            .await
            .expect("stream must outlive its creating Engine and Session")
            .unwrap();
        assert_single_value(&batches);
    }

    fn assert_single_value(batches: &[RecordBatch]) {
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 1);
        assert_eq!(
            batches[0]
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            1
        );
    }

    #[tokio::test]
    async fn file_table_function_binding_preserves_original_source_position() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("values.csv");
        std::fs::write(&path, "id\n1\n").unwrap();
        let session = Engine::new(
            EngineConfig::builder()
                .spill_directory(directory.path().join("spill"))
                .build(),
        )
        .unwrap()
        .session();
        let sql = format!(
            "SELECT id, count(*)\nFROM read_csv('{}', header = true)\nGROUP BY 3",
            path.display()
        );

        let error = match session.execute(&sql).await {
            Err(error) => error.to_string(),
            Ok(_) => panic!("invalid GROUP BY ordinal unexpectedly succeeded"),
        };
        assert_eq!(
            error,
            "invalid argument: GROUP BY position 3 is out of range (select list has 2 items) at line 3, column 10"
        );
        assert!(session.catalog().table_names().is_empty());

        let view_sql = format!(
            "CREATE TEMP VIEW invalid_view AS\nSELECT id, count(*)\nFROM read_csv('{}', header = true)\nGROUP BY 3",
            path.display()
        );
        let error = match session.execute(&view_sql).await {
            Err(error) => error.to_string(),
            Ok(_) => panic!("invalid CREATE VIEW ordinal unexpectedly succeeded"),
        };
        assert_eq!(
            error,
            "invalid argument: GROUP BY position 3 is out of range (select list has 2 items) at line 4, column 10"
        );
        assert!(session.catalog().table_names().is_empty());
    }

    #[tokio::test]
    async fn creates_describes_queries_and_drops_temp_views() {
        let directory = tempfile::tempdir().unwrap();
        let config = EngineConfig {
            spill: SpillConfig {
                directory: directory.path().join("spill"),
                ..SpillConfig::default()
            },
            ..EngineConfig::default()
        };
        let session = Engine::new(config).unwrap().session();

        collect(
            session
                .execute("CREATE TEMP VIEW answer AS SELECT 42 AS value")
                .await
                .unwrap(),
        )
        .await;
        let show = collect(session.execute("SHOW TABLES").await.unwrap()).await;
        assert_eq!(
            show[0]
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "answer"
        );
        let describe = collect(session.execute("DESCRIBE answer").await.unwrap()).await;
        assert_eq!(
            describe[0]
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "value"
        );
        let rows = collect(session.execute("SELECT value FROM answer").await.unwrap()).await;
        assert_eq!(
            rows[0]
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            42
        );

        collect(
            session
                .execute("CREATE OR REPLACE TEMP VIEW answer AS SELECT 7 AS value")
                .await
                .unwrap(),
        )
        .await;
        let rows = collect(session.execute("SELECT * FROM answer").await.unwrap()).await;
        assert_eq!(
            rows[0]
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            7
        );

        collect(session.execute("DROP VIEW answer").await.unwrap()).await;
        assert!(session.catalog().table("answer").is_none());
        assert!(session.execute("DROP VIEW answer").await.is_err());
        collect(session.execute("DROP VIEW IF EXISTS answer").await.unwrap()).await;
    }

    #[tokio::test]
    async fn explain_analyze_metrics_exclude_the_explanation_row() {
        let directory = tempfile::tempdir().unwrap();
        let session = Engine::new(
            EngineConfig::builder()
                .spill_directory(directory.path().join("spill"))
                .build(),
        )
        .unwrap()
        .session();
        let mut result = session.execute("EXPLAIN ANALYZE SELECT 1").await.unwrap();
        let batches = result.stream().try_collect::<Vec<_>>().await.unwrap();
        assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
        let metrics = result.metrics().snapshot();
        assert_eq!(metrics.rows_returned, 1);
        assert_eq!(metrics.batches_returned, 1);
    }

    #[tokio::test]
    async fn view_keeps_file_provider_and_drop_view_preserves_external_table() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("values.csv");
        std::fs::write(&path, "id,label\n1,one\n2,two\n").unwrap();
        let config = EngineConfig {
            spill: SpillConfig {
                directory: directory.path().join("spill"),
                ..SpillConfig::default()
            },
            ..EngineConfig::default()
        };
        let session = Engine::new(config).unwrap().session();
        session
            .register_csv(
                "external",
                [path.to_string_lossy().into_owned()],
                CsvOptions {
                    header: CsvHeader::Present,
                    ..CsvOptions::default()
                },
            )
            .await
            .unwrap();
        let direct = format!(
            "SELECT id FROM read_csv('{}', header = true) LIMIT 1",
            path.display()
        );
        collect(session.execute(&direct).await.unwrap()).await;
        assert_eq!(session.catalog().table_names(), vec!["external".to_owned()]);

        let create = format!(
            "CREATE TEMP VIEW file_view AS SELECT id, label FROM read_csv('{}', header = true)",
            path.display()
        );
        collect(session.execute(&create).await.unwrap()).await;
        assert_eq!(
            session.catalog().table_names(),
            vec!["external".to_owned(), "file_view".to_owned()]
        );

        let rows = collect(
            session
                .execute("SELECT label FROM file_view LIMIT 1")
                .await
                .unwrap(),
        )
        .await;
        assert_eq!(
            rows[0]
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "one"
        );
        assert!(session.execute("DROP VIEW external").await.is_err());
        assert!(session.catalog().table("external").is_some());
    }

    #[tokio::test]
    async fn view_reuses_the_file_set_fixed_during_query_preparation() {
        let directory = tempfile::tempdir().unwrap();
        let data = directory.path().join("parts");
        std::fs::create_dir(&data).unwrap();
        std::fs::write(data.join("a.csv"), "id\n1\n").unwrap();
        let session = Engine::new(EngineConfig {
            spill: SpillConfig {
                directory: directory.path().join("spill"),
                ..SpillConfig::default()
            },
            ..EngineConfig::default()
        })
        .unwrap()
        .session();
        let create = format!(
            "CREATE TEMP VIEW file_view AS SELECT id FROM read_csv('{}/*.csv', header = true)",
            data.display()
        );
        collect(session.execute(&create).await.unwrap()).await;

        let StatementPlan::Query(plan) = session
            .prepare_statement("SELECT count(*) FROM file_view")
            .await
            .unwrap()
        else {
            panic!("expected query plan");
        };
        let context = session.query_context().unwrap();
        crate::execution::prepare_plan(&plan, Arc::clone(&context))
            .await
            .unwrap();
        context.seal_object_snapshots();

        std::fs::write(data.join("b.csv"), "id\n2\n").unwrap();
        let batches = crate::execution::execute(StatementPlan::Query(plan), context)
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(
            batches[0]
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            1
        );
    }

    #[tokio::test]
    async fn view_rebinds_replaced_dependencies_and_rejects_cycles() {
        let directory = tempfile::tempdir().unwrap();
        let first = directory.path().join("first.csv");
        let second = directory.path().join("second.csv");
        std::fs::write(&first, "id\n1\n").unwrap();
        std::fs::write(&second, "id\n2\n").unwrap();
        let session = Engine::new(EngineConfig {
            compute_threads: 2,
            spill: SpillConfig {
                directory: directory.path().join("spill"),
                ..SpillConfig::default()
            },
            ..EngineConfig::default()
        })
        .unwrap()
        .session();
        let options = CsvOptions {
            header: CsvHeader::Present,
            ..CsvOptions::default()
        };
        session
            .register_csv(
                "base",
                [first.to_string_lossy().into_owned()],
                options.clone(),
            )
            .await
            .unwrap();
        collect(
            session
                .execute("CREATE TEMP VIEW current_base AS SELECT id FROM base")
                .await
                .unwrap(),
        )
        .await;
        session
            .register_csv("base", [second.to_string_lossy().into_owned()], options)
            .await
            .unwrap();
        let rows = collect(
            session
                .execute("SELECT id FROM current_base")
                .await
                .unwrap(),
        )
        .await;
        assert_eq!(
            rows[0]
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            2
        );

        collect(
            session
                .execute("CREATE TEMP VIEW a AS SELECT 1 AS value")
                .await
                .unwrap(),
        )
        .await;
        collect(
            session
                .execute("CREATE TEMP VIEW b AS SELECT value FROM a")
                .await
                .unwrap(),
        )
        .await;
        collect(
            session
                .execute("CREATE OR REPLACE TEMP VIEW a AS SELECT value FROM b")
                .await
                .unwrap(),
        )
        .await;
        let error = tokio::time::timeout(Duration::from_secs(3), async {
            match session.execute("SELECT value FROM a").await {
                Ok(result) => result
                    .into_stream()
                    .try_collect::<Vec<_>>()
                    .await
                    .unwrap_err(),
                Err(error) => error,
            }
        })
        .await
        .expect("cyclic views must fail without hanging");
        assert!(error.to_string().contains("view cycle"));
    }

    #[tokio::test]
    async fn concurrent_file_queries_clean_only_their_generated_tables() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("values.csv");
        std::fs::write(&path, "id\n1\n2\n3\n").unwrap();
        let session = Engine::new(EngineConfig {
            max_concurrent_queries: 8,
            spill: SpillConfig {
                directory: directory.path().join("spill"),
                ..SpillConfig::default()
            },
            ..EngineConfig::default()
        })
        .unwrap()
        .session();
        let sql = format!(
            "SELECT count(*) FROM read_csv('{}', header = true)",
            path.display()
        );

        let queries = (0..32).map(|_| {
            let session = session.clone();
            let sql = sql.clone();
            async move {
                let result = session.execute(&sql).await?;
                let batches = result.into_stream().try_collect::<Vec<_>>().await?;
                let values = batches[0]
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                if values.value(0) != 3 {
                    return Err(Error::Execution("unexpected concurrent count".into()));
                }
                Ok(())
            }
        });
        try_join_all(queries).await.unwrap();

        assert!(
            session
                .catalog()
                .table_names()
                .iter()
                .all(|name| !name.starts_with("__rustdb_file_"))
        );
    }

    #[tokio::test]
    async fn registered_csv_discovers_files_per_query_and_freezes_each_snapshot() {
        let directory = tempfile::tempdir().unwrap();
        let data = directory.path().join("parts");
        std::fs::create_dir(&data).unwrap();
        std::fs::write(data.join("a.csv"), "id\n1\n").unwrap();
        let session = Engine::new(EngineConfig {
            spill: SpillConfig {
                directory: directory.path().join("spill"),
                ..SpillConfig::default()
            },
            ..EngineConfig::default()
        })
        .unwrap()
        .session();
        session
            .register_csv(
                "dynamic_csv",
                [format!("{}/*.csv", data.display())],
                CsvOptions {
                    header: CsvHeader::Present,
                    ..CsvOptions::default()
                },
            )
            .await
            .unwrap();

        assert_eq!(query_count(&session, "dynamic_csv").await, 1);
        std::fs::write(data.join("b.csv"), "id\n2\n").unwrap();
        assert_eq!(query_count(&session, "dynamic_csv").await, 2);

        let StatementPlan::Query(plan) = session
            .prepare_statement("SELECT count(*) FROM dynamic_csv")
            .await
            .unwrap()
        else {
            panic!("expected query plan");
        };
        let context = session.query_context().unwrap();
        crate::execution::prepare_plan(&plan, Arc::clone(&context))
            .await
            .unwrap();
        context.seal_object_snapshots();
        std::fs::write(data.join("c.csv"), "id\n3\n").unwrap();
        let fixed = crate::execution::execute(StatementPlan::Query(plan), context)
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(first_i64(&fixed), 2);

        assert_eq!(query_count(&session, "dynamic_csv").await, 3);
        std::fs::remove_file(data.join("a.csv")).unwrap();
        assert_eq!(query_count(&session, "dynamic_csv").await, 2);
    }

    #[tokio::test]
    async fn dynamic_statistics_are_query_scoped_and_drive_join_planning() {
        let directory = tempfile::tempdir().unwrap();
        let left = directory.path().join("left");
        let right = directory.path().join("right");
        std::fs::create_dir_all(&left).unwrap();
        std::fs::create_dir_all(&right).unwrap();
        let left_rows = (0..100)
            .map(|value| value.to_string())
            .collect::<Vec<_>>()
            .join("\n");
        std::fs::write(left.join("a.csv"), format!("id\n{left_rows}\n")).unwrap();
        std::fs::write(right.join("a.csv"), "id\n1\n").unwrap();

        let session = Engine::new(EngineConfig {
            compute_threads: 3,
            max_concurrent_queries: 4,
            spill: SpillConfig {
                directory: directory.path().join("spill"),
                ..SpillConfig::default()
            },
            ..EngineConfig::default()
        })
        .unwrap()
        .session();
        let options = CsvOptions {
            header: CsvHeader::Present,
            ..CsvOptions::default()
        };
        session
            .register_csv(
                "left_rows",
                [format!("{}/*.csv", left.display())],
                options.clone(),
            )
            .await
            .unwrap();
        session
            .register_csv(
                "right_rows",
                [format!("{}/*.csv", right.display())],
                options,
            )
            .await
            .unwrap();

        let added_rows = (0..2_000)
            .map(|value| value.to_string())
            .collect::<Vec<_>>()
            .join("\n");
        std::fs::write(right.join("b.csv"), format!("id\n{added_rows}\n")).unwrap();
        let sql = "EXPLAIN SELECT l.id FROM left_rows l \
                   JOIN right_rows r ON l.id = r.id";
        let first = session.execute(sql).await.unwrap();

        // Keep the first QueryResult alive while a second query fixes a newer
        // file snapshot. Neither snapshot may be published to the Catalog.
        std::fs::write(right.join("c.csv"), "id\n2001\n").unwrap();
        let second = session.execute(sql).await.unwrap();
        let first = explain_value(collect(first).await);
        let second = explain_value(collect(second).await);

        let first_right = scan_line(&first, "right_rows");
        let second_right = scan_line(&second, "right_rows");
        assert!(first_right.contains("files=2"), "{first_right}");
        assert!(second_right.contains("files=3"), "{second_right}");
        assert!(first.contains("lane_limit=3"), "{first}");
        assert!(first.contains("partitions=64"), "{first}");
        assert!(first.contains("repartition_seeds=2"), "{first}");
        assert!(first.contains("fallback=sort_merge"), "{first}");

        // The newly discovered right side is now larger, so the optimizer
        // swaps the inner join and keeps the smaller left relation as build.
        assert!(
            first.find("Scan table=right_rows").unwrap()
                < first.find("Scan table=left_rows").unwrap(),
            "{first}"
        );
        assert_eq!(
            session
                .catalog()
                .table("right_rows")
                .unwrap()
                .provider()
                .statistics()
                .file_count,
            1
        );
    }

    #[tokio::test]
    async fn refresh_table_atomically_replaces_the_registered_csv_schema() {
        let directory = tempfile::tempdir().unwrap();
        let data = directory.path().join("parts");
        std::fs::create_dir(&data).unwrap();
        let first = data.join("a.csv");
        std::fs::write(&first, "id\n1\n").unwrap();
        let session = Engine::new(EngineConfig {
            spill: SpillConfig {
                directory: directory.path().join("spill"),
                ..SpillConfig::default()
            },
            ..EngineConfig::default()
        })
        .unwrap()
        .session();
        session
            .register_csv(
                "dynamic_csv",
                [format!("{}/*.csv", data.display())],
                CsvOptions {
                    header: CsvHeader::Present,
                    ..CsvOptions::default()
                },
            )
            .await
            .unwrap();

        std::fs::remove_file(first).unwrap();
        std::fs::write(data.join("b.csv"), "id,label\n2,two\n").unwrap();
        assert!(session.execute("SELECT * FROM dynamic_csv").await.is_err());

        let schema = session.refresh_table("dynamic_csv").await.unwrap();
        assert_eq!(schema.fields().len(), 2);
        assert_eq!(schema.field(1).name(), "label");
        let rows = collect(
            session
                .execute("SELECT label FROM dynamic_csv")
                .await
                .unwrap(),
        )
        .await;
        assert_eq!(
            rows[0]
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "two"
        );

        std::fs::write(data.join("b.csv"), "id,label,extra\n2,two,x\n").unwrap();
        collect(session.execute("REFRESH TABLE dynamic_csv").await.unwrap()).await;
        let described = collect(session.execute("DESCRIBE dynamic_csv").await.unwrap()).await;
        assert_eq!(described[0].num_rows(), 3);
    }

    #[tokio::test]
    async fn stream_error_cleans_spill_before_query_result_is_dropped() {
        let directory = tempfile::tempdir().unwrap();
        let engine = Engine::new(EngineConfig {
            spill: SpillConfig {
                directory: directory.path().join("spill"),
                ..SpillConfig::default()
            },
            ..EngineConfig::default()
        })
        .unwrap();
        let session = engine.session();
        let context = session.query_context().unwrap();
        let schema = Arc::new(Schema::empty());
        context
            .spill
            .write_record_batches(
                "before-error",
                Arc::clone(&schema),
                [RecordBatch::new_empty(Arc::clone(&schema))],
            )
            .unwrap();
        let query_directory = context.spill.directory().to_owned();
        assert!(query_directory.is_dir());

        let permit = Arc::clone(&engine.inner.admission)
            .acquire_owned()
            .await
            .unwrap();
        let input = crate::runtime::boxed_record_batch_stream(futures::stream::once(async {
            Err(Error::Execution("injected stream failure".into()))
        }));
        let mut result = super::query_result(schema, input, context, permit, engine.clone());
        assert!(matches!(
            result.stream().next().await,
            Some(Err(Error::Execution(message))) if message == "injected stream failure"
        ));

        // `result` intentionally remains alive for this assertion.
        assert!(!query_directory.exists());
    }

    #[tokio::test]
    async fn stream_prefers_first_task_failure_to_cancelled_sibling() {
        let directory = tempfile::tempdir().unwrap();
        let engine = Engine::new(
            EngineConfig::builder()
                .spill_directory(directory.path().join("spill"))
                .build(),
        )
        .unwrap();
        let context = engine.session().query_context().unwrap();
        context.record_task_failure(&Error::ResourceExhausted(
            "injected operator resource failure".to_owned(),
        ));
        let permit = Arc::clone(&engine.inner.admission)
            .acquire_owned()
            .await
            .unwrap();
        let schema = Arc::new(Schema::empty());
        let input = crate::runtime::boxed_record_batch_stream(futures::stream::once(async {
            Err(Error::Cancelled)
        }));
        let mut result = super::query_result(schema, input, context, permit, engine.clone());

        let error = result.stream().next().await.unwrap().unwrap_err();
        assert!(
            matches!(error, Error::ResourceExhausted(message) if message == "injected operator resource failure")
        );
        assert!(result.stream().next().await.is_none());
    }

    #[tokio::test]
    async fn successful_stream_returns_terminal_cleanup_failure_once() {
        let directory = tempfile::tempdir().unwrap();
        let engine = Engine::new(EngineConfig {
            spill: SpillConfig {
                directory: directory.path().join("spill"),
                ..SpillConfig::default()
            },
            ..EngineConfig::default()
        })
        .unwrap();
        let context = engine.session().query_context().unwrap();
        let cleanup_attempts = Arc::new(AtomicUsize::new(0));
        let observed = Arc::clone(&cleanup_attempts);
        context.set_spill_cleanup_hook(move || {
            observed.fetch_add(1, Ordering::Relaxed);
            Err(Error::ResourceExhausted(
                "injected successful-query cleanup failure".to_owned(),
            ))
        });
        let permit = Arc::clone(&engine.inner.admission)
            .acquire_owned()
            .await
            .unwrap();
        let schema = Arc::new(Schema::empty());
        let input = crate::runtime::boxed_record_batch_stream(futures::stream::empty());
        let mut result = super::query_result(schema, input, context, permit, engine.clone());

        let error = result.stream().next().await.unwrap().unwrap_err();
        assert!(
            matches!(error, Error::ResourceExhausted(message) if message == "injected successful-query cleanup failure")
        );
        assert!(result.stream().next().await.is_none());
        drop(result);
        assert_eq!(cleanup_attempts.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn stream_error_preserves_execution_and_cleanup_failures() {
        let directory = tempfile::tempdir().unwrap();
        let engine = Engine::new(EngineConfig {
            spill: SpillConfig {
                directory: directory.path().join("spill"),
                ..SpillConfig::default()
            },
            ..EngineConfig::default()
        })
        .unwrap();
        let context = engine.session().query_context().unwrap();
        context.set_spill_cleanup_hook(|| {
            Err(Error::ResourceExhausted(
                "injected error-path cleanup failure".to_owned(),
            ))
        });
        let permit = Arc::clone(&engine.inner.admission)
            .acquire_owned()
            .await
            .unwrap();
        let schema = Arc::new(Schema::empty());
        let input = crate::runtime::boxed_record_batch_stream(futures::stream::once(async {
            Err(Error::Execution("injected operator failure".to_owned()))
        }));
        let mut result = super::query_result(schema, input, context, permit, engine.clone());

        let error = result.stream().next().await.unwrap().unwrap_err();
        let message = error.to_string();
        assert!(message.contains("injected operator failure"));
        assert!(message.contains("injected error-path cleanup failure"));
    }

    #[tokio::test]
    async fn cancelled_stream_preserves_cleanup_failure() {
        let directory = tempfile::tempdir().unwrap();
        let engine = Engine::new(EngineConfig {
            spill: SpillConfig {
                directory: directory.path().join("spill"),
                ..SpillConfig::default()
            },
            ..EngineConfig::default()
        })
        .unwrap();
        let context = engine.session().query_context().unwrap();
        context.set_spill_cleanup_hook(|| {
            Err(Error::ResourceExhausted(
                "injected cancellation cleanup failure".to_owned(),
            ))
        });
        let permit = Arc::clone(&engine.inner.admission)
            .acquire_owned()
            .await
            .unwrap();
        let schema = Arc::new(Schema::empty());
        let input_schema = Arc::clone(&schema);
        let input = crate::runtime::boxed_record_batch_stream(futures::stream::once(async move {
            Ok(RecordBatch::new_empty(input_schema))
        }));
        let mut result = super::query_result(schema, input, context, permit, engine.clone());
        result.cancel();

        let error = result.stream().next().await.unwrap().unwrap_err();
        let message = error.to_string();
        assert!(message.contains("query cancelled"));
        assert!(message.contains("injected cancellation cleanup failure"));
    }

    async fn collect(result: QueryResult) -> Vec<arrow::record_batch::RecordBatch> {
        result.into_stream().try_collect::<Vec<_>>().await.unwrap()
    }

    fn explain_value(batches: Vec<RecordBatch>) -> String {
        batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0)
            .to_owned()
    }

    fn scan_line<'a>(explain: &'a str, table: &str) -> &'a str {
        explain
            .lines()
            .find(|line| line.contains(&format!("Scan table={table} ")))
            .unwrap_or_else(|| panic!("missing scan for {table}: {explain}"))
    }

    async fn query_count(session: &crate::Session, table: &str) -> i64 {
        let batches = collect(
            session
                .execute(&format!("SELECT count(*) FROM {table}"))
                .await
                .unwrap(),
        )
        .await;
        first_i64(&batches)
    }

    fn first_i64(batches: &[RecordBatch]) -> i64 {
        batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0)
    }
}
