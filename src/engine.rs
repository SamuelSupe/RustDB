use std::{path::Path, sync::Arc};

use arrow::{datatypes::SchemaRef, record_batch::RecordBatch};
use async_stream::stream;
use futures::StreamExt;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use uuid::Uuid;

use crate::{
    Catalog, CsvOptions, EngineConfig, Error, ParquetOptions, QueryMetrics, Result, TableEntry,
    command::{SessionCommand, ViewTable},
    datasource::{CsvTable, MetadataCache, ParquetTable},
    runtime::{
        ComputeRuntime, MemoryPool, QueryContext, QueryControl, RecordBatchStream,
        boxed_record_batch_stream,
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
}

impl Engine {
    pub fn new(config: EngineConfig) -> Result<Self> {
        validate_config(&config)?;
        std::fs::create_dir_all(&config.temp_dir)
            .map_err(|error| Error::io(Some(config.temp_dir.clone()), error))?;
        let memory = MemoryPool::named_root("engine", config.memory_limit);
        let admission = Arc::new(Semaphore::new(config.max_concurrent_queries));
        let compute = ComputeRuntime::new(config.compute_threads)?;
        let metadata_cache = MetadataCache::new(config.metadata_cache_bytes);
        Ok(Self {
            inner: Arc::new(EngineInner {
                config,
                memory,
                admission,
                compute,
                metadata_cache,
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
        let provider = ParquetTable::try_new_with_cache(
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
        let provider = CsvTable::try_new(locations, options, &self.engine.inner.config).await?;
        self.catalog
            .register(TableEntry::new(name, Arc::new(provider)))
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
        let plan = self
            .prepare_statement_for_query(sql, Some(Arc::clone(&context)))
            .await?;
        let schema = plan.schema();
        let stream = crate::execution::execute(plan, Arc::clone(&context)).await?;
        let stream = self.engine.inner.compute.pipe(stream, Arc::clone(&context));
        Ok(query_result(schema, stream, context, permit))
    }

    async fn execute_command(
        &self,
        command: SessionCommand,
        permit: OwnedSemaphorePermit,
    ) -> Result<QueryResult> {
        let context = self.query_context()?;
        let batch = match command {
            SessionCommand::ShowTables => crate::command::show_tables(&self.catalog)?,
            SessionCommand::Describe { name } => crate::command::describe(&self.catalog, &name)?,
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
                crate::command::status("CREATE VIEW")?
            }
            SessionCommand::DropView { name, if_exists } => {
                if !self.catalog.drop_view(&name) && !if_exists {
                    return Err(Error::Catalog(format!("view '{name}' does not exist")));
                }
                crate::command::status("DROP VIEW")?
            }
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
            context,
        )
        .await?;
        let planned = crate::sql::plan_sql(&self.catalog, &prepared.sql);
        for name in prepared.generated_tables {
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
        Ok(query_result(schema, stream, context, permit))
    }

    fn query_context(&self) -> Result<Arc<QueryContext>> {
        let query_id = Uuid::new_v4();
        let query_memory = self.engine.inner.memory.child(
            format!("query-{query_id}"),
            self.engine.inner.config.memory_limit,
        );
        Ok(Arc::new(QueryContext::with_query_id_and_batch_size(
            query_id,
            query_memory,
            &self.engine.inner.config.temp_dir,
            self.engine.inner.config.batch_size,
        )?))
    }
}

fn query_result(
    schema: SchemaRef,
    stream: RecordBatchStream,
    context: Arc<QueryContext>,
    permit: OwnedSemaphorePermit,
) -> QueryResult {
    let stream = instrument_output(stream, Arc::clone(&context), permit);
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
) -> RecordBatchStream {
    boxed_record_batch_stream(stream! {
        let _permit = permit;
        while let Some(item) = input.next().await {
            if let Err(error) = context.check_cancelled() {
                context.metrics.finish();
                let _ = context.spill.cleanup();
                yield Err(error);
                return;
            }
            match item {
                Ok(batch) => {
                    context.metrics.record_output(
                        u64::try_from(batch.num_rows()).unwrap_or(u64::MAX),
                        1,
                        u64::try_from(batch.get_array_memory_size()).unwrap_or(u64::MAX),
                    );
                    yield Ok(batch);
                }
                Err(error) => {
                    context.metrics.finish();
                    // A QueryResult may remain alive after the consumer sees
                    // an execution error.  Clean this query's files now rather
                    // than waiting for QueryContext::drop().
                    let _ = context.spill.cleanup();
                    yield Err(error);
                    return;
                }
            }
        }
        context.metrics.finish();
        if let Err(error) = context.spill.cleanup() {
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
    ensure_directory_parent(&config.temp_dir)
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
    use std::{sync::Arc, time::Duration};

    use arrow::{
        array::{Int64Array, StringArray},
        datatypes::Schema,
        record_batch::RecordBatch,
    };
    use futures::{StreamExt, TryStreamExt, future::try_join_all};

    use crate::{
        CsvHeader, CsvOptions, Engine, EngineConfig, Error, QueryResult, sql::StatementPlan,
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
    async fn creates_describes_queries_and_drops_temp_views() {
        let directory = tempfile::tempdir().unwrap();
        let config = EngineConfig {
            temp_dir: directory.path().join("spill"),
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
    async fn view_keeps_file_provider_and_drop_view_preserves_external_table() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("values.csv");
        std::fs::write(&path, "id,label\n1,one\n2,two\n").unwrap();
        let config = EngineConfig {
            temp_dir: directory.path().join("spill"),
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
            temp_dir: directory.path().join("spill"),
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
            temp_dir: directory.path().join("spill"),
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
            temp_dir: directory.path().join("spill"),
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
    async fn stream_error_cleans_spill_before_query_result_is_dropped() {
        let directory = tempfile::tempdir().unwrap();
        let engine = Engine::new(EngineConfig {
            temp_dir: directory.path().join("spill"),
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
        let mut result = super::query_result(schema, input, context, permit);
        assert!(matches!(
            result.stream().next().await,
            Some(Err(Error::Execution(message))) if message == "injected stream failure"
        ));

        // `result` intentionally remains alive for this assertion.
        assert!(!query_directory.exists());
    }

    async fn collect(result: QueryResult) -> Vec<arrow::record_batch::RecordBatch> {
        result.into_stream().try_collect::<Vec<_>>().await.unwrap()
    }
}
