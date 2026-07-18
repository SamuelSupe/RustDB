use std::{sync::Arc, time::Duration};

use tokio::sync::OwnedSemaphorePermit;

use super::{QueryResult, Session, query_result};
use crate::{
    Error, Result,
    command::NativeUpdateCommand,
    runtime::{
        BatchEnvelope, QueryContext, boxed_memory_batch_stream, estimate_schema_batch_bytes,
    },
    sql::{BoundExpr, ScalarValue},
    storage::NativeWritePlan,
};

#[path = "native_update/scan.rs"]
mod scan;

struct ReturningProjection {
    expressions: Vec<BoundExpr>,
    schema: arrow::datatypes::SchemaRef,
}

impl Session {
    pub(super) async fn execute_native_update(
        &self,
        command: NativeUpdateCommand,
        permit: OwnedSemaphorePermit,
        admission_wait: Duration,
        parse_time: Duration,
    ) -> Result<QueryResult> {
        let NativeUpdateCommand {
            name,
            target_sql,
            qualifier,
            from_sql,
            assignments: assignment_ast,
            selection,
            returning,
        } = command;
        let database = self.engine.inner.database.as_ref().ok_or_else(|| {
            Error::Unsupported("native UPDATE requires Engine::open(path, config)".to_owned())
        })?;
        let transaction = self.native_transaction.clone();
        let mutation = transaction
            .as_ref()
            .map(|transaction| transaction.begin_mutation())
            .transpose()?;
        self.engine.ensure_native_healthy()?;
        let context = self.query_context()?;
        context.metrics.record_query_admission_wait(admission_wait);
        context.metrics.record_sql_parse_time(parse_time);
        let snapshot = match transaction.as_ref() {
            Some(transaction) => transaction.working_snapshot(&name),
            None => database.table_snapshot(&name).ok(),
        }
        .ok_or_else(|| Error::Catalog(format!("native table '{name}' does not exist")))?;
        let schema = snapshot.schema();
        let matches = match from_sql {
            Some(from_sql) => {
                let selection = selection.as_deref().expect("UPDATE FROM requires WHERE");
                let sql = update_match_query(
                    &target_sql,
                    &qualifier,
                    &from_sql,
                    selection,
                    &assignment_ast,
                    &schema,
                )?;
                let (result_schema, stream) = self
                    .native_match_stream(&sql, Arc::clone(&context))
                    .await
                    .map_err(|error| context.error_with_cleanup(error))?;
                Some(
                    super::native_matches::UpdateMatches::collect(
                        stream,
                        result_schema,
                        &schema,
                        &context,
                    )
                    .await
                    .map_err(|error| context.error_with_cleanup(error))?,
                )
            }
            None => {
                context.set_catalog_snapshot(self.pin_catalog()?)?;
                None
            }
        };
        let catalog = context.catalog_snapshot().ok_or_else(|| {
            Error::Internal("native UPDATE has no fixed catalog snapshot".to_owned())
        })?;
        if catalog.local_table(&name).is_some()
            && !transaction
                .as_ref()
                .is_some_and(|transaction| transaction.is_staged(&name))
        {
            return Err(context.error_with_cleanup(Error::Catalog(format!(
                "native UPDATE target '{name}' is shadowed by a session-local table or view"
            ))));
        }
        let generation = catalog.persistent_generation().unwrap_or(0);
        let (plan, snapshot) = match transaction.as_ref() {
            Some(transaction) => {
                let snapshot = transaction.working_snapshot(&name).ok_or_else(|| {
                    Error::Catalog(format!("native table '{name}' does not exist"))
                })?;
                let plan = transaction.plan_write(
                    &self.engine,
                    &name,
                    crate::storage::NativeWriteMode::Update,
                    snapshot.schema(),
                    0,
                )?;
                (plan, snapshot)
            }
            None => database.plan_update(&name, generation)?,
        };
        if snapshot.schema().as_ref() != schema.as_ref() {
            return Err(context.error_with_cleanup(Error::Catalog(format!(
                "native table '{name}' changed schema while UPDATE was being planned"
            ))));
        }
        let predicate = match (selection, matches.as_ref()) {
            (_, Some(_)) => BoundExpr::literal(ScalarValue::Boolean(true)),
            (Some(expression), None) => {
                crate::sql::bind_table_filter(&expression, Arc::clone(&schema), &qualifier)?
            }
            (None, None) => BoundExpr::literal(ScalarValue::Boolean(true)),
        };
        let assignments = if matches.is_some() {
            Vec::new()
        } else {
            bind_assignments(&qualifier, &schema, assignment_ast)?
        };
        let returning = returning
            .map(|items| {
                crate::sql::bind_table_projection(&items, Arc::clone(&schema), &qualifier).map(
                    |(expressions, schema)| ReturningProjection {
                        expressions,
                        schema,
                    },
                )
            })
            .transpose()?;
        let batch_memory =
            estimate_schema_batch_bytes(schema.as_ref(), self.engine.inner.config.batch_size);
        let workspace = snapshot
            .max_delete_vector_memory_size()?
            .saturating_add(batch_memory.saturating_mul(3))
            .max(1);
        let memory = context
            .reserve_memory(workspace, "native UPDATE scan")
            .await
            .map_err(|error| context.error_with_cleanup(error))?;
        let status = if returning.is_none() {
            Some(
                crate::command::status("UPDATE")
                    .and_then(|batch| {
                        BatchEnvelope::try_new(batch, &context.memory, "UPDATE status")
                    })
                    .map_err(|error| context.error_with_cleanup(error))?,
            )
        } else {
            None
        };
        let result_schema = match returning.as_ref() {
            Some(returning) => Arc::clone(&returning.schema),
            None => crate::command::status("UPDATE")?.schema(),
        };
        let engine = self.engine.clone();
        let update_context = Arc::clone(&context);
        let transaction_catalog = self.catalog.clone();
        let sink = boxed_memory_batch_stream(async_stream::try_stream! {
            let output = run_update(
                engine,
                update_context,
                plan,
                snapshot,
                predicate,
                assignments,
                matches,
                returning,
                memory,
                status,
                transaction,
                transaction_catalog,
                mutation,
            )
            .await?;
            for batch in output {
                yield batch;
            }
        });
        let stream = self.engine.inner.compute.pipe(sink, Arc::clone(&context));
        Ok(query_result(
            result_schema,
            stream,
            context,
            permit,
            self.engine.clone(),
        ))
    }
}

fn bind_assignments(
    table: &str,
    schema: &arrow::datatypes::SchemaRef,
    assignments: Vec<(String, sqlparser::ast::Expr)>,
) -> Result<Vec<(usize, BoundExpr)>> {
    assignments
        .into_iter()
        .map(|(name, expression)| {
            let index = schema
                .fields()
                .iter()
                .position(|field| field.name().eq_ignore_ascii_case(&name))
                .ok_or_else(|| Error::Catalog(format!("column '{name}' does not exist")))?;
            let expression = crate::sql::bind_table_value(
                &expression,
                Arc::clone(schema),
                table,
                schema.field(index).data_type(),
            )?;
            Ok((index, expression))
        })
        .collect()
}

#[allow(clippy::too_many_arguments)]
async fn run_update(
    engine: super::Engine,
    context: Arc<QueryContext>,
    plan: NativeWritePlan,
    snapshot: Arc<crate::storage::NativeTableSnapshot>,
    predicate: BoundExpr,
    assignments: Vec<(usize, BoundExpr)>,
    matches: Option<super::native_matches::UpdateMatches>,
    returning: Option<ReturningProjection>,
    memory: crate::runtime::MemoryReservation,
    status: Option<BatchEnvelope>,
    transaction: Option<Arc<super::transaction::TransactionWorkspace>>,
    transaction_catalog: crate::Catalog,
    _mutation: Option<super::transaction::MutationLease>,
) -> Result<Vec<BatchEnvelope>> {
    let database =
        engine.inner.database.as_ref().cloned().ok_or_else(|| {
            Error::Internal("persistent engine lost its native database".to_owned())
        })?;
    let root: Arc<std::path::Path> = Arc::from(database.path());
    let batch_size = engine.inner.config.batch_size;
    let return_memory = context.memory.clone();
    let write = context
        .spill
        .run_query_io(move |control| {
            let writer = database.start_write(plan)?;
            let result = scan::apply(
                &root,
                &snapshot,
                &predicate,
                &assignments,
                matches.as_ref(),
                returning.as_ref(),
                &return_memory,
                batch_size,
                &control,
                writer,
            );
            drop(memory);
            result
        })
        .await;
    super::native_write::poison_on_native_write_error(&engine, write.as_ref().err());
    let (prepared, returned) = write?;
    if let Err(error) = context.check_cancelled() {
        if let Some(prepared) = prepared {
            let path = engine.database_path().map(std::path::Path::to_path_buf);
            return engine.inner.spill_io.run(move || {
                super::native_write::abort_prepared(prepared, path.as_deref(), error)
            });
        }
        return Err(error);
    }
    if let Some(prepared) = prepared {
        match transaction {
            Some(transaction) => {
                let stage_engine = engine.clone();
                engine.inner.spill_io.run(move || {
                    transaction.stage_prepared(&stage_engine, &transaction_catalog, prepared)
                })?;
                context.mark_transaction_mutation_applied();
            }
            None => {
                let commit_engine = engine.clone();
                let commit_context = Arc::clone(&context);
                engine.inner.spill_io.run(move || {
                    super::native_write::commit_prepared(commit_engine, commit_context, prepared)
                })?;
            }
        }
    }
    Ok(match status {
        Some(status) => vec![status],
        None => returned,
    })
}

fn update_match_query(
    target_sql: &str,
    qualifier: &str,
    from_sql: &str,
    selection: &sqlparser::ast::Expr,
    assignments: &[(String, sqlparser::ast::Expr)],
    schema: &arrow::datatypes::SchemaRef,
) -> Result<String> {
    let qualifier = crate::catalog_name::quote(qualifier);
    let keys = schema
        .fields()
        .iter()
        .map(|field| format!("{qualifier}.{}", quote_identifier(field.name())));
    let assignments = assignments
        .iter()
        .map(|(name, expression)| (name.to_ascii_lowercase(), expression))
        .collect::<std::collections::HashMap<_, _>>();
    let values = schema.fields().iter().map(|field| {
        assignments
            .get(&field.name().to_ascii_lowercase())
            .map(|expression| format!("({expression}) AS {}", quote_identifier(field.name())))
            .unwrap_or_else(|| format!("{qualifier}.{}", quote_identifier(field.name())))
    });
    let projections = keys.chain(values).collect::<Vec<_>>().join(", ");
    Ok(format!(
        "SELECT {projections} FROM {target_sql} JOIN {from_sql} ON {selection}"
    ))
}

fn quote_identifier(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}
