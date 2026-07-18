use std::{sync::Arc, time::Duration};

use tokio::sync::OwnedSemaphorePermit;

use super::{QueryResult, Session, query_result};
use crate::{
    Error, Result,
    command::NativeTruncateCommand,
    runtime::{BatchEnvelope, QueryContext, boxed_memory_batch_stream},
    storage::NativeWriteMode,
};

impl Session {
    pub(super) async fn execute_native_truncate(
        &self,
        command: NativeTruncateCommand,
        permit: OwnedSemaphorePermit,
        admission_wait: Duration,
        parse_time: Duration,
    ) -> Result<QueryResult> {
        let database = self.engine.inner.database.as_ref().ok_or_else(|| {
            Error::Unsupported("native TRUNCATE requires Engine::open(path, config)".to_owned())
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
        let catalog = self.pin_catalog()?;
        context.set_catalog_snapshot(catalog.clone())?;
        if catalog.local_table(&command.name).is_some()
            && !transaction
                .as_ref()
                .is_some_and(|transaction| transaction.is_staged(&command.name))
        {
            return Err(context.error_with_cleanup(Error::Catalog(format!(
                "native TRUNCATE target '{}' is shadowed by a session-local table or view",
                command.name
            ))));
        }
        let generation = catalog.persistent_generation().unwrap_or(0);
        let snapshot = match transaction.as_ref() {
            Some(transaction) => transaction.working_snapshot(&command.name),
            None => Some(database.table_snapshot(&command.name)?),
        }
        .ok_or_else(|| Error::Catalog(format!("native table '{}' does not exist", command.name)))?;
        let plan = match transaction.as_ref() {
            Some(transaction) => transaction.plan_write(
                &self.engine,
                &command.name,
                NativeWriteMode::Truncate,
                snapshot.schema(),
                0,
            )?,
            None => database.plan_write(
                &command.name,
                NativeWriteMode::Truncate,
                generation,
                snapshot.schema(),
                0,
            )?,
        };
        let write_engine = self.engine.clone();
        let writer = self.engine.inner.spill_io.run(move || {
            write_engine
                .inner
                .database
                .as_ref()
                .ok_or_else(|| {
                    Error::Internal("persistent engine lost its native database".to_owned())
                })?
                .start_write(plan)
        });
        super::native_write::poison_on_native_write_error(&self.engine, writer.as_ref().err());
        let writer = writer?;
        let status = crate::command::status("TRUNCATE")
            .and_then(|batch| BatchEnvelope::try_new(batch, &context.memory, "TRUNCATE status"))
            .map_err(|error| context.error_with_cleanup(error))?;
        let engine = self.engine.clone();
        let truncate_context = Arc::clone(&context);
        let transaction_catalog = self.catalog.clone();
        let sink = boxed_memory_batch_stream(futures::stream::once(async move {
            run_truncate(
                engine,
                truncate_context,
                writer,
                status,
                transaction,
                transaction_catalog,
                mutation,
            )
            .await
        }));
        let schema = crate::command::status("TRUNCATE")?.schema();
        let stream = self.engine.inner.compute.pipe(sink, Arc::clone(&context));
        Ok(query_result(
            schema,
            stream,
            context,
            permit,
            self.engine.clone(),
        ))
    }
}

async fn run_truncate(
    engine: super::Engine,
    context: Arc<QueryContext>,
    writer: crate::storage::NativeTableWriter,
    status: BatchEnvelope,
    transaction: Option<Arc<super::transaction::TransactionWorkspace>>,
    transaction_catalog: crate::Catalog,
    _mutation: Option<super::transaction::MutationLease>,
) -> Result<BatchEnvelope> {
    if let Err(error) = context.check_cancelled() {
        return super::native_write::abort_writer(
            writer,
            &engine.inner.spill_io,
            engine.database_path(),
            error,
        );
    }
    let prepared = engine.inner.spill_io.run(move || writer.finish())?;
    if let Err(error) = context.check_cancelled() {
        let path = engine.database_path().map(std::path::Path::to_path_buf);
        return engine
            .inner
            .spill_io
            .run(move || super::native_write::abort_prepared(prepared, path.as_deref(), error));
    }
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
    Ok(status)
}
