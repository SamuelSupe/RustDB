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
        ShowStatementOptions, Statement,
    },
    dialect::DuckDbDialect,
    parser::Parser,
};

use crate::datasource::{MetadataCache, ScanRequest, TableProvider, TableStatistics};
use crate::runtime::{QueryContext, RecordBatchStream, boxed_record_batch_stream};
use crate::sql::{LogicalPlan, StatementPlan};
use crate::{Catalog, EngineConfig, Error, Result};

pub(crate) enum SessionCommand {
    ShowTables,
    Describe {
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
                query: view.query.to_string(),
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
            context,
        )
        .await?;
        let planned = crate::sql::plan_sql(&self.catalog, &prepared.sql);
        for name in prepared.generated_tables {
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
        let plan = match context.view_plan(&self.name) {
            Some(plan) => plan,
            None => {
                let plan = self.current_plan(Some(Arc::clone(&context))).await?;
                context.cache_view_plan(&self.name, plan.clone())?;
                plan
            }
        };
        crate::execution::prepare_plan(&plan, context).await
    }

    async fn scan(
        &self,
        request: ScanRequest,
        context: Arc<QueryContext>,
    ) -> Result<RecordBatchStream> {
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
            crate::execution::execute(StatementPlan::Query(plan), Arc::clone(&context)).await?;
        let stream = async_stream::try_stream! {
            let _expansion = expansion;
            while remaining != 0 {
                let Some(batch) = input.next().await else {
                    break;
                };
                context.check_cancelled()?;
                let batch = project_batch(batch?, projection.as_deref(), &output_schema)?;
                let batch = if batch.num_rows() > remaining {
                    batch.slice(0, remaining)
                } else {
                    batch
                };
                remaining = remaining.saturating_sub(batch.num_rows());
                yield batch;
            }
        };
        Ok(boxed_record_batch_stream(stream))
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
    fn leaves_select_for_the_query_planner() {
        assert!(parse("SELECT 1").unwrap().is_none());
    }
}
