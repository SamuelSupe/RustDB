use std::sync::Arc;

use arrow::{
    array::{ArrayRef, StringArray},
    datatypes::{DataType, Field, Schema, SchemaRef},
    record_batch::{RecordBatch, RecordBatchOptions},
};
use async_trait::async_trait;
use futures::StreamExt;
use sqlparser::{
    ast::{
        CreateTableOptions, DescribeAlias, ObjectName, ObjectNamePart, ObjectType,
        ShowStatementOptions, Spanned, Statement,
    },
    dialect::DuckDbDialect,
    parser::Parser,
};

use crate::datasource::{MetadataCache, ScanRequest, ScanTask, TableProvider, TableStatistics};
use crate::runtime::{
    MemoryBatchStream, QueryContext, RecordBatchStream, boxed_memory_batch_stream,
    boxed_record_batch_stream,
};
use crate::sql::{LogicalPlan, StatementPlan};
use crate::{Catalog, EngineConfig, Error, Result};

#[path = "command/source.rs"]
mod source;

pub(crate) enum SessionCommand {
    ShowTables,
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
    DropView {
        name: String,
        if_exists: bool,
    },
}

pub(crate) fn parse(sql: &str) -> Result<Option<SessionCommand>> {
    if let Some(command) = parse_refresh_table(sql)? {
        return Ok(Some(command));
    }
    let mut statements = Parser::parse_sql(&DuckDbDialect {}, sql)?;
    if statements.len() != 1 {
        return Err(Error::InvalidArgument(
            "exactly one SQL statement is required".into(),
        ));
    }
    let command = match statements.remove(0) {
        Statement::ShowTables {
            terse,
            history,
            extended,
            full,
            external,
            show_options,
        } => {
            if terse
                || history
                || extended
                || full
                || external
                || !plain_show_options(&show_options)
            {
                return Err(Error::Unsupported(
                    "SHOW TABLES modifiers are not supported".into(),
                ));
            }
            Some(SessionCommand::ShowTables)
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
                name: simple_name(&table_name, "table")?,
            })
        }
        Statement::CreateView(view) => {
            if !view.temporary {
                return Err(Error::Unsupported(
                    "only CREATE TEMP VIEW is supported".into(),
                ));
            }
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
            Some(SessionCommand::CreateTempView {
                name: simple_name(&view.name, "view")?,
                query: source::suffix_with_location(sql, view.query.span())
                    .unwrap_or_else(|| view.query.to_string()),
                replace: view.or_replace,
            })
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
            if cascade || restrict || purge || temporary || table.is_some() || names.len() != 1 {
                return Err(Error::Unsupported(
                    "DROP VIEW supports one name and optional IF EXISTS only".into(),
                ));
            }
            Some(SessionCommand::DropView {
                name: simple_name(&names[0], "view")?,
                if_exists,
            })
        }
        _ => None,
    };
    Ok(command)
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
    let mut statements = Parser::parse_sql(&DuckDbDialect {}, &format!("DESCRIBE {table}"))?;
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
    let [ObjectNamePart::Identifier(identifier)] = name.0.as_slice() else {
        return Err(Error::Unsupported(format!(
            "qualified {kind} names are not supported"
        )));
    };
    Ok(identifier.value.clone())
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
    catalog: Catalog,
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
            catalog,
            config,
            metadata_cache,
        }
    }

    async fn current_plan(&self, context: Option<Arc<QueryContext>>) -> Result<LogicalPlan> {
        let prepared = crate::table_function::prepare_with_cache_for_query(
            &self.catalog,
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
        let planned = crate::sql::bind_statement(&self.catalog, statement);
        for name in generated_tables {
            self.catalog.unregister(&name);
        }
        let StatementPlan::Query(plan) = planned? else {
            return Err(Error::Internal(
                "temporary view query produced an EXPLAIN plan".into(),
            ));
        };
        if plan.schema().arrow().as_ref() != self.schema.as_ref() {
            return Err(Error::Catalog(format!(
                "temporary view '{}' changed schema after a dependency was replaced",
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
        let _expansion = context.enter_view(&self.name)?;
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
    use super::{SessionCommand, parse};

    #[test]
    fn parses_view_lifecycle_commands() {
        let command = parse("CREATE OR REPLACE TEMP VIEW v AS SELECT 1")
            .unwrap()
            .unwrap();
        assert!(matches!(
            command,
            SessionCommand::CreateTempView {
                name,
                replace: true,
                ..
            } if name == "v"
        ));
        assert!(matches!(
            parse("DROP VIEW IF EXISTS v").unwrap().unwrap(),
            SessionCommand::DropView {
                name,
                if_exists: true
            } if name == "v"
        ));
    }

    #[test]
    fn parses_refresh_table_and_rejects_qualified_names() {
        assert!(matches!(
            parse("REFRESH TABLE dynamic_data;").unwrap().unwrap(),
            SessionCommand::RefreshTable { name } if name == "dynamic_data"
        ));
        assert!(parse("REFRESH TABLE catalog.dynamic_data").is_err());
    }

    #[test]
    fn leaves_select_for_the_query_planner() {
        assert!(parse("SELECT 1").unwrap().is_none());
    }
}
