use std::{sync::Arc, time::Duration};

use tokio::sync::OwnedSemaphorePermit;

use super::{QueryResult, Session, query_result};
use crate::{
    Error, Result,
    command::NativeDeleteCommand,
    runtime::{
        BatchEnvelope, QueryContext, boxed_memory_batch_stream, estimate_schema_batch_bytes,
    },
    sql::{BoundExpr, ScalarValue},
};

#[path = "native_delete/scan.rs"]
mod scan;

struct ReturningProjection {
    expressions: Vec<BoundExpr>,
    schema: arrow::datatypes::SchemaRef,
}

impl Session {
    pub(super) async fn execute_native_delete(
        &self,
        command: NativeDeleteCommand,
        permit: OwnedSemaphorePermit,
        admission_wait: Duration,
        parse_time: Duration,
    ) -> Result<QueryResult> {
        let NativeDeleteCommand {
            name,
            target_sql,
            qualifier,
            using_sql,
            selection,
            returning,
        } = command;
        let database = self.engine.inner.database.as_ref().ok_or_else(|| {
            Error::Unsupported("native DELETE requires Engine::open(path, config)".to_owned())
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
        let matches = match using_sql {
            Some(using_sql) => {
                let selection = selection.as_deref().expect("DELETE USING requires WHERE");
                let sql =
                    delete_match_query(&target_sql, &qualifier, &using_sql, selection, &schema);
                let (_, stream) = self
                    .native_match_stream(&sql, Arc::clone(&context))
                    .await
                    .map_err(|error| context.error_with_cleanup(error))?;
                Some(
                    super::native_matches::DeleteMatches::collect(stream, &schema, &context)
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
            Error::Internal("native DELETE has no fixed catalog snapshot".to_owned())
        })?;
        if catalog.local_table(&name).is_some()
            && !transaction
                .as_ref()
                .is_some_and(|transaction| transaction.is_staged(&name))
        {
            return Err(context.error_with_cleanup(Error::Catalog(format!(
                "native DELETE target '{name}' is shadowed by a session-local table or view"
            ))));
        }
        let generation = catalog.persistent_generation().unwrap_or(0);
        let (plan, snapshot) = match transaction.as_ref() {
            Some(transaction) => {
                let plan = transaction.plan_write(
                    &self.engine,
                    &name,
                    crate::storage::NativeWriteMode::Delete,
                    snapshot.schema(),
                    0,
                )?;
                (plan, snapshot)
            }
            None => database.plan_delete(&name, generation)?,
        };
        if snapshot.schema().as_ref() != schema.as_ref() {
            return Err(context.error_with_cleanup(Error::Catalog(format!(
                "native table '{name}' changed schema while DELETE was being planned"
            ))));
        }
        let predicate = match (selection, matches.as_ref()) {
            (_, Some(_)) => BoundExpr::literal(ScalarValue::Boolean(true)),
            (Some(expression), None) => {
                crate::sql::bind_table_filter(&expression, Arc::clone(&schema), &qualifier)?
            }
            (None, None) => BoundExpr::literal(ScalarValue::Boolean(true)),
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
        let workspace = snapshot
            .max_delete_vector_memory_size()?
            .saturating_add(estimate_schema_batch_bytes(
                schema.as_ref(),
                self.engine.inner.config.batch_size,
            ))
            .max(1);
        let memory = context
            .reserve_memory(workspace, "native DELETE scan")
            .await
            .map_err(|error| context.error_with_cleanup(error))?;
        let status = if returning.is_none() {
            Some(
                crate::command::status("DELETE")
                    .and_then(|batch| {
                        BatchEnvelope::try_new(batch, &context.memory, "DELETE status")
                    })
                    .map_err(|error| context.error_with_cleanup(error))?,
            )
        } else {
            None
        };
        let result_schema = match returning.as_ref() {
            Some(returning) => Arc::clone(&returning.schema),
            None => crate::command::status("DELETE")?.schema(),
        };
        let engine = self.engine.clone();
        let delete_context = Arc::clone(&context);
        let transaction_catalog = self.catalog.clone();
        let sink = boxed_memory_batch_stream(async_stream::try_stream! {
            let output = run_delete(
                engine,
                delete_context,
                plan,
                snapshot,
                predicate,
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

#[allow(clippy::too_many_arguments)]
async fn run_delete(
    engine: super::Engine,
    context: Arc<QueryContext>,
    plan: crate::storage::NativeWritePlan,
    snapshot: Arc<crate::storage::NativeTableSnapshot>,
    predicate: BoundExpr,
    matches: Option<super::native_matches::DeleteMatches>,
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
            let writer = database.start_delete(plan)?;
            let result = scan::apply(
                &root,
                &snapshot,
                &predicate,
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

fn delete_match_query(
    target_sql: &str,
    qualifier: &str,
    using_sql: &str,
    selection: &sqlparser::ast::Expr,
    schema: &arrow::datatypes::SchemaRef,
) -> String {
    let columns = schema
        .fields()
        .iter()
        .map(|field| {
            format!(
                "{}.{}",
                crate::catalog_name::quote(qualifier),
                quote_identifier(field.name())
            )
        })
        .collect::<Vec<_>>()
        .join(", ");
    format!("SELECT DISTINCT {columns} FROM {target_sql} JOIN {using_sql} ON {selection}")
}

fn quote_identifier(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}
