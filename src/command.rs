use std::sync::Arc;

use arrow::{
    array::{ArrayRef, StringArray},
    datatypes::{DataType, Field, Schema, SchemaRef},
    record_batch::{RecordBatch, RecordBatchOptions},
};
use async_trait::async_trait;
use futures::StreamExt;
use sqlparser::ast::{
    CreateTableOptions, DescribeAlias, ObjectName, ObjectType, ShowStatementOptions, Spanned,
    Statement, TransactionAccessMode as SqlAccessMode, TransactionIsolationLevel, TransactionMode,
};

use crate::datasource::{MetadataCache, ScanRequest, ScanTask, TableProvider, TableStatistics};
use crate::runtime::{
    MemoryBatchStream, QueryContext, RecordBatchStream, boxed_memory_batch_stream,
    boxed_record_batch_stream,
};
use crate::sql::{LogicalPlan, StatementPlan};
use crate::{Catalog, EngineConfig, Error, Result};

#[path = "command/copy.rs"]
mod copy;
#[path = "command/maintenance.rs"]
mod maintenance;
#[path = "command/native_alter.rs"]
mod native_alter;
#[path = "command/native_delete.rs"]
mod native_delete;
#[path = "command/native_drop.rs"]
mod native_drop;
#[path = "command/native_schema.rs"]
mod native_schema;
#[path = "command/native_truncate.rs"]
mod native_truncate;
#[path = "command/native_update.rs"]
mod native_update;
#[path = "command/native_write.rs"]
mod native_write;
#[path = "command/source.rs"]
mod source;

pub(crate) use copy::{CopyCommand, CopyCsvOptions, CopyFormat, CopyToCommand};
pub(crate) use maintenance::MaintenanceCommand;
pub(crate) use native_alter::{NativeAlterCommand, NativeAlterOperation};
pub(crate) use native_delete::NativeDeleteCommand;
pub(crate) use native_drop::NativeDropTableCommand;
pub(crate) use native_schema::NativeSchemaCommand;
pub(crate) use native_truncate::NativeTruncateCommand;
pub(crate) use native_update::NativeUpdateCommand;
pub(crate) use native_write::{NativeWriteCommand, NativeWriteKind};

pub(crate) enum ParsedStatement {
    Command(SessionCommand),
    Query(Box<Statement>),
}

pub(crate) enum SessionCommand {
    BeginTransaction {
        read_only: bool,
    },
    CommitTransaction,
    RollbackTransaction,
    ShowTables,
    ShowSchemas,
    Describe {
        name: String,
    },
    RefreshTable {
        name: String,
    },
    CreateTempView {
        name: String,
        query: String,
        replace: bool,
    },
    CreatePersistentView {
        name: String,
        query: String,
        replace: bool,
    },
    DropView {
        name: String,
        if_exists: bool,
    },
    CopyTo(CopyToCommand),
    Maintenance(MaintenanceCommand),
    NativeWrite(NativeWriteCommand),
    NativeAlter(NativeAlterCommand),
    NativeDelete(NativeDeleteCommand),
    NativeDropTable(NativeDropTableCommand),
    NativeSchema(NativeSchemaCommand),
    NativeUpdate(NativeUpdateCommand),
    NativeTruncate(NativeTruncateCommand),
}

pub(crate) fn parse(sql: &str) -> Result<ParsedStatement> {
    if let Some(command) = maintenance::parse_custom(sql)? {
        return Ok(ParsedStatement::Command(command));
    }
    if let Some(command) = parse_refresh_table(sql)? {
        return Ok(ParsedStatement::Command(command));
    }
    let mut statements = crate::sql::parse_statements(sql)?;
    if statements.len() != 1 {
        return Err(Error::InvalidArgument(
            "exactly one SQL statement is required".into(),
        ));
    }
    let statement = statements.remove(0);
    if let Some(command) = copy::parse(&statement)? {
        return Ok(ParsedStatement::Command(match command {
            CopyCommand::From(command) => SessionCommand::NativeWrite(command),
            CopyCommand::To(command) => SessionCommand::CopyTo(command),
        }));
    }
    if let Some(command) = native_write::parse(&statement, sql)? {
        return Ok(ParsedStatement::Command(SessionCommand::NativeWrite(
            command,
        )));
    }
    if let Some(command) = native_alter::parse(&statement)? {
        return Ok(ParsedStatement::Command(SessionCommand::NativeAlter(
            command,
        )));
    }
    if let Some(command) = native_delete::parse(&statement)? {
        return Ok(ParsedStatement::Command(SessionCommand::NativeDelete(
            command,
        )));
    }
    if let Some(command) = native_drop::parse(&statement)? {
        return Ok(ParsedStatement::Command(SessionCommand::NativeDropTable(
            command,
        )));
    }
    if let Some(command) = native_schema::parse(&statement)? {
        return Ok(ParsedStatement::Command(SessionCommand::NativeSchema(
            command,
        )));
    }
    if let Some(command) = native_update::parse(&statement)? {
        return Ok(ParsedStatement::Command(SessionCommand::NativeUpdate(
            command,
        )));
    }
    if let Some(command) = native_truncate::parse(&statement)? {
        return Ok(ParsedStatement::Command(SessionCommand::NativeTruncate(
            command,
        )));
    }
    if let Some(command) = maintenance::parse_statement(&statement)? {
        return Ok(ParsedStatement::Command(SessionCommand::Maintenance(
            command,
        )));
    }
    let command = match &statement {
        Statement::StartTransaction {
            modes,
            modifier,
            statements,
            exception,
            has_end_keyword,
            ..
        } => {
            if modifier.is_some()
                || !statements.is_empty()
                || exception.is_some()
                || *has_end_keyword
            {
                return Err(Error::Unsupported(
                    "procedural, chained, and modified BEGIN forms are not supported".into(),
                ));
            }
            let mut access = None;
            for mode in modes {
                match mode {
                    TransactionMode::AccessMode(mode) => {
                        if access.replace(*mode).is_some() {
                            return Err(Error::InvalidArgument(
                                "transaction access mode was specified more than once".into(),
                            ));
                        }
                    }
                    TransactionMode::IsolationLevel(TransactionIsolationLevel::Snapshot) => {}
                    TransactionMode::IsolationLevel(_) => {
                        return Err(Error::Unsupported(
                            "v0.8 transactions support SNAPSHOT isolation only".into(),
                        ));
                    }
                }
            }
            Some(SessionCommand::BeginTransaction {
                read_only: access == Some(SqlAccessMode::ReadOnly),
            })
        }
        Statement::Commit {
            chain,
            end,
            modifier,
        } => {
            if *chain || *end || modifier.is_some() {
                return Err(Error::Unsupported(
                    "chained and procedural COMMIT forms are not supported".into(),
                ));
            }
            Some(SessionCommand::CommitTransaction)
        }
        Statement::Rollback { chain, savepoint } => {
            if *chain || savepoint.is_some() {
                return Err(Error::Unsupported(
                    "ROLLBACK TO SAVEPOINT and chained rollback are not supported".into(),
                ));
            }
            Some(SessionCommand::RollbackTransaction)
        }
        Statement::ShowTables {
            terse,
            history,
            extended,
            full,
            external,
            show_options,
        } => {
            if *terse
                || *history
                || *extended
                || *full
                || *external
                || !plain_show_options(show_options)
            {
                return Err(Error::Unsupported(
                    "SHOW TABLES modifiers are not supported".into(),
                ));
            }
            Some(SessionCommand::ShowTables)
        }
        Statement::ShowSchemas {
            terse,
            history,
            show_options,
        } => {
            if *terse || *history || !plain_show_options(show_options) {
                return Err(Error::Unsupported(
                    "SHOW SCHEMAS modifiers are not supported".to_owned(),
                ));
            }
            Some(SessionCommand::ShowSchemas)
        }
        Statement::ExplainTable {
            describe_alias: DescribeAlias::Describe | DescribeAlias::Desc,
            hive_format,
            table_name,
            ..
        } => {
            if hive_format.is_some() {
                return Err(Error::Unsupported(
                    "DESCRIBE formatting modifiers are not supported".into(),
                ));
            }
            Some(SessionCommand::Describe {
                name: simple_name(table_name, "table")?,
            })
        }
        Statement::CreateView(view) => {
            if view.or_alter
                || view.materialized
                || view.secure
                || view.if_not_exists
                || view.name_before_not_exists
                || !view.columns.is_empty()
                || view.options != CreateTableOptions::None
                || !view.cluster_by.is_empty()
                || view.comment.is_some()
                || view.with_no_schema_binding
                || view.copy_grants
                || view.to.is_some()
                || view.params.is_some()
            {
                return Err(Error::Unsupported(
                    "CREATE TEMP VIEW modifiers other than OR REPLACE are not supported".into(),
                ));
            }
            let command = if view.temporary {
                SessionCommand::CreateTempView {
                    name: simple_name(&view.name, "view")?,
                    query: source::suffix_with_location(sql, view.query.span())
                        .unwrap_or_else(|| view.query.to_string()),
                    replace: view.or_replace,
                }
            } else {
                SessionCommand::CreatePersistentView {
                    name: simple_name(&view.name, "view")?,
                    query: source::suffix_with_location(sql, view.query.span())
                        .unwrap_or_else(|| view.query.to_string()),
                    replace: view.or_replace,
                }
            };
            Some(command)
        }
        Statement::Drop {
            object_type: ObjectType::View,
            if_exists,
            names,
            cascade,
            restrict,
            purge,
            temporary,
            table,
        } => {
            if *cascade || *restrict || *purge || *temporary || table.is_some() || names.len() != 1
            {
                return Err(Error::Unsupported(
                    "DROP VIEW supports one name and optional IF EXISTS only".into(),
                ));
            }
            Some(SessionCommand::DropView {
                name: simple_name(&names[0], "view")?,
                if_exists: *if_exists,
            })
        }
        _ => None,
    };
    Ok(match command {
        Some(command) => ParsedStatement::Command(command),
        None => ParsedStatement::Query(Box::new(statement)),
    })
}

fn parse_refresh_table(sql: &str) -> Result<Option<SessionCommand>> {
    let trimmed = sql.trim();
    let Some((first, rest)) = take_word(trimmed) else {
        return Ok(None);
    };
    if !first.eq_ignore_ascii_case("refresh") {
        return Ok(None);
    }
    let Some((second, table)) = take_word(rest) else {
        return Err(Error::InvalidArgument(
            "REFRESH TABLE requires a table name".to_owned(),
        ));
    };
    if !second.eq_ignore_ascii_case("table") {
        return Err(Error::Unsupported(
            "only REFRESH TABLE is supported".to_owned(),
        ));
    }
    let table = table.trim();
    if table.is_empty() {
        return Err(Error::InvalidArgument(
            "REFRESH TABLE requires a table name".to_owned(),
        ));
    }
    let mut statements = crate::sql::parse_statements(&format!("DESCRIBE {table}"))?;
    if statements.len() != 1 {
        return Err(Error::InvalidArgument(
            "REFRESH TABLE accepts exactly one table name".to_owned(),
        ));
    }
    let Statement::ExplainTable { table_name, .. } = statements.remove(0) else {
        return Err(Error::InvalidArgument(
            "invalid REFRESH TABLE name".to_owned(),
        ));
    };
    Ok(Some(SessionCommand::RefreshTable {
        name: simple_name(&table_name, "table")?,
    }))
}

fn take_word(input: &str) -> Option<(&str, &str)> {
    let input = input.trim_start();
    if input.is_empty() {
        return None;
    }
    let end = input.find(char::is_whitespace).unwrap_or(input.len());
    Some((&input[..end], &input[end..]))
}

fn simple_name(name: &ObjectName, kind: &str) -> Result<String> {
    crate::catalog_name::object(name, kind)
}

fn plain_show_options(options: &ShowStatementOptions) -> bool {
    options.show_in.is_none()
        && options.starts_with.is_none()
        && options.limit.is_none()
        && options.limit_from.is_none()
        && options.filter_position.is_none()
}

pub(crate) fn show_tables(catalog: &Catalog) -> Result<RecordBatch> {
    let names = catalog
        .table_names()
        .into_iter()
        .filter(|name| !name.starts_with("__rustdb_file_"));
    let schema = Arc::new(Schema::new(vec![Field::new("name", DataType::Utf8, false)]));
    Ok(RecordBatch::try_new(
        schema,
        vec![Arc::new(StringArray::from_iter_values(names))],
    )?)
}

pub(crate) fn show_schemas(names: impl IntoIterator<Item = String>) -> Result<RecordBatch> {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "schema_name",
        DataType::Utf8,
        false,
    )]));
    Ok(RecordBatch::try_new(
        schema,
        vec![Arc::new(StringArray::from_iter_values(names))],
    )?)
}

pub(crate) fn describe(catalog: &Catalog, name: &str) -> Result<RecordBatch> {
    let entry = catalog
        .table(name)
        .ok_or_else(|| Error::Catalog(format!("table or view '{name}' does not exist")))?;
    let table_schema = entry.provider().schema();
    let names = table_schema.fields().iter().map(|field| field.name());
    let types = table_schema
        .fields()
        .iter()
        .map(|field| field.data_type().to_string());
    let nullable = table_schema
        .fields()
        .iter()
        .map(|field| if field.is_nullable() { "YES" } else { "NO" });
    let empty = vec![None::<&str>; table_schema.fields().len()];
    let schema = Arc::new(Schema::new(vec![
        Field::new("column_name", DataType::Utf8, false),
        Field::new("column_type", DataType::Utf8, false),
        Field::new("null", DataType::Utf8, false),
        Field::new("key", DataType::Utf8, true),
        Field::new("default", DataType::Utf8, true),
        Field::new("extra", DataType::Utf8, true),
    ]));
    Ok(RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from_iter_values(names)),
            Arc::new(StringArray::from_iter_values(types)),
            Arc::new(StringArray::from_iter_values(nullable)),
            Arc::new(StringArray::from(empty.clone())),
            Arc::new(StringArray::from(empty.clone())),
            Arc::new(StringArray::from(empty)),
        ],
    )?)
}

pub(crate) fn status(message: &str) -> Result<RecordBatch> {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "status",
        DataType::Utf8,
        false,
    )]));
    Ok(RecordBatch::try_new(
        schema,
        vec![Arc::new(StringArray::from(vec![message]))],
    )?)
}

pub(crate) struct ViewTable {
    name: String,
    query: String,
    catalog: Option<Catalog>,
    config: EngineConfig,
    metadata_cache: MetadataCache,
    schema: SchemaRef,
}

impl ViewTable {
    pub(crate) fn new(
        name: String,
        query: String,
        plan: LogicalPlan,
        catalog: Catalog,
        config: EngineConfig,
        metadata_cache: MetadataCache,
    ) -> Self {
        Self {
            schema: Arc::clone(plan.schema().arrow()),
            name,
            query,
            // A view must not share the mutable map that will subsequently
            // contain the view itself; that would create an Arc cycle.
            catalog: Some(catalog.pin()),
            config,
            metadata_cache,
        }
    }

    pub(crate) fn persistent(
        name: String,
        query: String,
        schema: SchemaRef,
        config: EngineConfig,
        metadata_cache: MetadataCache,
    ) -> Self {
        Self {
            name,
            query,
            catalog: None,
            config,
            metadata_cache,
            schema,
        }
    }

    async fn current_plan(&self, context: Option<Arc<QueryContext>>) -> Result<LogicalPlan> {
        let catalog = context
            .as_ref()
            .and_then(|context| context.catalog_snapshot())
            .or_else(|| self.catalog.as_ref().map(Catalog::pin))
            .ok_or_else(|| {
                Error::Internal(format!(
                    "persistent view '{}' is missing its query catalog snapshot",
                    self.name
                ))
            })?;
        let prepared = crate::table_function::prepare_with_cache_for_query(
            &catalog,
            &self.config,
            &self.metadata_cache,
            &self.query,
            context.clone(),
        )
        .await?;
        let crate::table_function::PreparedSql {
            statement,
            generated_tables,
        } = prepared;
        let _generated_tables =
            crate::table_function::GeneratedTablesGuard::new(&catalog, generated_tables);
        let planned = crate::sql::bind_statement(&catalog, statement);
        let StatementPlan::Query(plan) = planned? else {
            return Err(Error::Internal(
                "temporary view query produced an EXPLAIN plan".into(),
            ));
        };
        if plan.schema().arrow().as_ref() != self.schema.as_ref() {
            return Err(Error::Catalog(format!(
                "view '{}' changed schema after a dependency was replaced",
                self.name
            )));
        }
        Ok(plan)
    }

    async fn scan_internal(
        &self,
        request: ScanRequest,
        context: Arc<QueryContext>,
    ) -> Result<MemoryBatchStream> {
        request.reject_unsupported_exact("view provider")?;
        let expansion = context.enter_view(&self.name)?;
        let plan = context.view_plan(&self.name).ok_or_else(|| {
            Error::Internal(format!(
                "temporary view '{}' was not prepared before execution",
                self.name
            ))
        })?;
        let output_schema = request.projected_schema(&self.schema)?;
        let projection = request.projection;
        let mut remaining = request.limit.unwrap_or(usize::MAX);
        let mut input =
            crate::execution::execute_internal(StatementPlan::Query(plan), Arc::clone(&context))
                .await?;
        Ok(boxed_memory_batch_stream(async_stream::try_stream! {
            let _expansion = expansion;
            while remaining != 0 {
                let Some(batch) = input.next().await else { break };
                context.check_cancelled()?;
                let mut batch = batch?;
                if let Some(projection) = projection.as_deref() {
                    let projected = project_batch(
                        batch.batch().clone(),
                        Some(projection),
                        &output_schema,
                    )?;
                    batch = batch.replace(projected, "view projection")?;
                }
                if batch.num_rows() > remaining {
                    let sliced = batch.batch().slice(0, remaining);
                    batch = batch.replace(sliced, "view limit")?;
                }
                remaining = remaining.saturating_sub(batch.num_rows());
                yield batch;
            }
        }))
    }
}

#[async_trait]
impl TableProvider for ViewTable {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn statistics(&self) -> TableStatistics {
        TableStatistics::default()
    }

    async fn prepare(&self, context: Arc<QueryContext>) -> Result<()> {
        let _preparation = context.enter_view_preparation(&self.name)?;
        if let Some(plan) = context.view_plan(&self.name) {
            return crate::execution::prepare_plan(&plan, context).await;
        }

        let plan = self.current_plan(Some(Arc::clone(&context))).await?;
        crate::execution::prepare_plan(&plan, Arc::clone(&context)).await?;
        let StatementPlan::Query(plan) =
            crate::sql::optimize_statement(StatementPlan::Query(plan), Some(context.as_ref()))?
        else {
            unreachable!("view planning preserves statement kind")
        };
        context.cache_view_plan(&self.name, plan)
    }

    async fn scan(
        &self,
        request: ScanRequest,
        context: Arc<QueryContext>,
    ) -> Result<RecordBatchStream> {
        let mut input = self.scan_internal(request, context).await?;
        Ok(boxed_record_batch_stream(async_stream::try_stream! {
            while let Some(batch) = input.next().await {
                yield batch?.into_public();
            }
        }))
    }

    async fn scan_tasks(
        &self,
        request: ScanRequest,
        context: Arc<QueryContext>,
        _target_tasks: usize,
    ) -> Result<Vec<ScanTask>> {
        Ok(vec![ScanTask::new(
            0,
            self.scan_internal(request, context).await?,
        )])
    }
}

fn project_batch(
    batch: RecordBatch,
    projection: Option<&[usize]>,
    schema: &SchemaRef,
) -> Result<RecordBatch> {
    let Some(projection) = projection else {
        return Ok(batch);
    };
    let columns =
        projection
            .iter()
            .map(|index| {
                batch.columns().get(*index).cloned().ok_or_else(|| {
                    Error::Internal(format!("view projection index {index} is invalid"))
                })
            })
            .collect::<Result<Vec<ArrayRef>>>()?;
    let options = RecordBatchOptions::new().with_row_count(Some(batch.num_rows()));
    Ok(RecordBatch::try_new_with_options(
        Arc::clone(schema),
        columns,
        &options,
    )?)
}

#[cfg(test)]
mod tests {
    use sqlparser::ast::Statement;

    use super::{ParsedStatement, SessionCommand, parse};

    #[test]
    fn parses_view_lifecycle_commands() {
        let ParsedStatement::Command(command) =
            parse("CREATE OR REPLACE TEMP VIEW v AS SELECT 1").unwrap()
        else {
            panic!("CREATE TEMP VIEW was not dispatched as a command");
        };
        assert!(matches!(
            command,
            SessionCommand::CreateTempView {
                name,
                replace: true,
                ..
            } if name == "v"
        ));
        assert!(matches!(
            parse("DROP VIEW IF EXISTS v").unwrap(),
            ParsedStatement::Command(SessionCommand::DropView {
                name,
                if_exists: true
            }) if name == "v"
        ));
    }

    #[test]
    fn parses_unqualified_and_schema_qualified_refresh_table() {
        assert!(matches!(
            parse("REFRESH TABLE dynamic_data;").unwrap(),
            ParsedStatement::Command(SessionCommand::RefreshTable { name })
                if name == "dynamic_data"
        ));
        assert!(matches!(
            parse("REFRESH TABLE catalog.dynamic_data").unwrap(),
            ParsedStatement::Command(SessionCommand::RefreshTable { name })
                if name == "catalog.dynamic_data"
        ));
    }

    #[test]
    fn leaves_select_for_the_query_planner() {
        let ParsedStatement::Query(statement) = parse("SELECT 1").unwrap() else {
            panic!("SELECT was not dispatched to the query planner");
        };
        assert!(matches!(statement.as_ref(), Statement::Query(_)));
    }

    #[test]
    fn query_dispatch_still_requires_exactly_one_statement() {
        let error = match parse("SELECT 1; SELECT 2") {
            Ok(_) => panic!("multiple queries were accepted"),
            Err(error) => error,
        };
        assert_eq!(
            error.to_string(),
            "invalid argument: exactly one SQL statement is required"
        );
    }
}
