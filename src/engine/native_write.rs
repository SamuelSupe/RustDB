use std::{
    sync::{Arc, atomic::Ordering},
    time::Duration,
};

use futures::StreamExt;
use sqlparser::ast::Statement;
use tokio::sync::OwnedSemaphorePermit;

use super::{QueryResult, Session, query_result};
use crate::{
    Error, Result,
    command::{NativeWriteCommand, NativeWriteKind},
    runtime::{
        BatchEnvelope, MemoryBatchStream, QueryContext, SpillIoPool, boxed_memory_batch_stream,
    },
    sql::{BoundExpr, StatementPlan},
    storage::NativeWriteMode,
};

struct ReturningProjection {
    expressions: Vec<BoundExpr>,
    schema: arrow::datatypes::SchemaRef,
}

struct WriteRun {
    engine: super::Engine,
    writer: crate::storage::NativeTableWriter,
    context: Arc<QueryContext>,
    message: &'static str,
    returning: Option<ReturningProjection>,
    transaction: Option<Arc<super::transaction::TransactionWorkspace>>,
    transaction_catalog: crate::Catalog,
    import: Option<crate::NativeImportIntent>,
    _mutation: Option<super::transaction::MutationLease>,
}

impl Session {
    pub(super) async fn execute_native_write(
        &self,
        command: NativeWriteCommand,
        permit: OwnedSemaphorePermit,
        admission_wait: Duration,
        parse_time: Duration,
    ) -> Result<QueryResult> {
        let NativeWriteCommand {
            name,
            qualifier,
            query,
            kind,
            returning,
            import,
        } = command;
        let database = self.engine.inner.database.as_ref().ok_or_else(|| {
            Error::Unsupported("native writes require Engine::open(path, config)".to_owned())
        })?;
        let transaction = self.native_transaction.clone();
        if import.is_some() && transaction.is_some() {
            return Err(Error::Unsupported(
                "Native import cannot run inside an explicit transaction".to_owned(),
            ));
        }
        let mutation = transaction
            .as_ref()
            .map(|transaction| transaction.begin_mutation())
            .transpose()?;
        self.engine.ensure_native_healthy()?;
        let context = self.query_context()?;
        context.metrics.record_query_admission_wait(admission_wait);
        context.metrics.record_sql_parse_time(parse_time);
        let statement = Statement::Query(query);
        let prepared = self
            .prepare_ast_for_query(statement, Some(Arc::clone(&context)))
            .await
            .map_err(|error| context.error_with_cleanup(error))?;
        let StatementPlan::Query(plan) = prepared else {
            return Err(context.error_with_cleanup(Error::Internal(
                "native write source produced an EXPLAIN plan".to_owned(),
            )));
        };
        let catalog = context.catalog_snapshot().ok_or_else(|| {
            context.error_with_cleanup(Error::Internal(
                "native write has no fixed catalog snapshot".to_owned(),
            ))
        })?;
        if catalog.local_table(&name).is_some()
            && !transaction
                .as_ref()
                .is_some_and(|transaction| transaction.is_staged(&name))
        {
            return Err(context.error_with_cleanup(Error::Catalog(format!(
                "native write target '{}' is shadowed by a session-local table or view",
                name
            ))));
        }
        let generation = catalog.persistent_generation().unwrap_or(0);
        let source_bytes = context.object_snapshot_bytes()?;
        let mode = match kind {
            NativeWriteKind::Create | NativeWriteKind::Import => NativeWriteMode::Create,
            NativeWriteKind::Replace | NativeWriteKind::Compact | NativeWriteKind::Alter => {
                NativeWriteMode::Replace
            }
            NativeWriteKind::Append | NativeWriteKind::CopyFrom => NativeWriteMode::Append,
        };
        let target_exists = transaction
            .as_ref()
            .and_then(|transaction| transaction.working_snapshot(&name))
            .is_some()
            || catalog.persistent_table(&name).is_some();
        if matches!(
            kind,
            NativeWriteKind::Append | NativeWriteKind::CopyFrom | NativeWriteKind::Compact
        ) && !target_exists
        {
            return Err(context.error_with_cleanup(Error::Catalog(format!(
                "native table '{}' does not exist",
                name
            ))));
        }
        let source_schema = Arc::clone(plan.schema().arrow());
        let write_plan = match transaction.as_ref() {
            Some(transaction) => {
                transaction.plan_write(&self.engine, &name, mode, source_schema, source_bytes)?
            }
            None => database.plan_write(&name, mode, generation, source_schema, source_bytes)?,
        };
        let returning = returning
            .map(|items| {
                crate::sql::bind_table_projection(&items, write_plan.schema(), &qualifier).map(
                    |(expressions, schema)| ReturningProjection {
                        expressions,
                        schema,
                    },
                )
            })
            .transpose()?;
        let write_engine = self.engine.clone();
        let write_memory = context.memory.clone();
        let writer = self.engine.inner.spill_io.run(move || {
            write_engine
                .inner
                .database
                .as_ref()
                .ok_or_else(|| {
                    Error::Internal("persistent engine lost its native database".to_owned())
                })?
                .start_write_with_memory(write_plan, write_memory)
        });
        poison_on_native_write_error(&self.engine, writer.as_ref().err());
        let writer = writer?;
        let input =
            crate::execution::execute_internal(StatementPlan::Query(plan), Arc::clone(&context))
                .await
                .map_err(|error| context.error_with_cleanup(error))?;
        let message = match kind {
            NativeWriteKind::Create => "CREATE TABLE",
            NativeWriteKind::Replace => "CREATE OR REPLACE TABLE",
            NativeWriteKind::Append => "INSERT",
            NativeWriteKind::CopyFrom => "COPY FROM",
            NativeWriteKind::Import => "IMPORT",
            NativeWriteKind::Compact => "COMPACT",
            NativeWriteKind::Alter => "ALTER TABLE",
        };
        let result_schema = match returning.as_ref() {
            Some(returning) => Arc::clone(&returning.schema),
            None => crate::command::status(message)?.schema(),
        };
        let engine = self.engine.clone();
        let sink_context = Arc::clone(&context);
        let transaction_catalog = self.catalog.clone();
        let sink = boxed_memory_batch_stream(async_stream::try_stream! {
            let output = run_write(input, WriteRun {
                engine,
                writer,
                context: sink_context,
                message,
                returning,
                transaction,
                transaction_catalog,
                import,
                _mutation: mutation,
            })
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

async fn run_write(input: MemoryBatchStream, run: WriteRun) -> Result<Vec<BatchEnvelope>> {
    let WriteRun {
        engine,
        writer,
        context,
        message,
        returning,
        transaction,
        transaction_catalog,
        import,
        _mutation,
    } = run;
    let (writer, returned) = write_input(
        input,
        writer,
        Arc::clone(&context),
        &engine.inner.spill_io,
        returning.as_ref(),
    )
    .await?;
    if let Err(error) = context.check_cancelled() {
        return abort_writer(
            writer,
            &engine.inner.spill_io,
            engine.database_path(),
            error,
        );
    }
    let status = if returning.is_none() {
        match crate::command::status(message)
            .and_then(|batch| BatchEnvelope::try_new(batch, &context.memory, "native write status"))
        {
            Ok(status) => Some(status),
            Err(error) => {
                return abort_writer(
                    writer,
                    &engine.inner.spill_io,
                    engine.database_path(),
                    error,
                );
            }
        }
    } else {
        None
    };
    let prepared = engine.inner.spill_io.run(move || writer.finish())?;
    if let Err(error) = context.check_cancelled() {
        let database_path = engine.database_path().map(std::path::Path::to_path_buf);
        return engine
            .inner
            .spill_io
            .run(move || abort_prepared(prepared, database_path.as_deref(), error));
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
                commit_prepared_with_import(commit_engine, commit_context, prepared, import)
            })?;
        }
    }
    Ok(match status {
        Some(status) => vec![status],
        None => returned,
    })
}

pub(super) fn commit_prepared(
    engine: super::Engine,
    context: Arc<QueryContext>,
    prepared: crate::storage::PreparedSnapshot,
) -> Result<()> {
    commit_prepared_with_import(engine, context, prepared, None)
}

fn commit_prepared_with_import(
    engine: super::Engine,
    context: Arc<QueryContext>,
    prepared: crate::storage::PreparedSnapshot,
    import: Option<crate::NativeImportIntent>,
) -> Result<()> {
    let _gate = engine.inner.native_commit.lock();
    let database_path = engine.database_path().map(std::path::Path::to_path_buf);
    let precommit_error = if engine.inner.native_poisoned.load(Ordering::Acquire) {
        Some(Error::native_storage(
            database_path
                .as_deref()
                .unwrap_or_else(|| std::path::Path::new("native database")),
            "engine state requires reopen after a native commit failure",
        ))
    } else {
        context.check_cancelled().err()
    };
    if let Some(error) = precommit_error {
        return abort_prepared(prepared, database_path.as_deref(), error);
    }
    let Some(database) = engine.inner.database.as_ref() else {
        return abort_prepared(
            prepared,
            database_path.as_deref(),
            Error::Internal("persistent engine lost its native database".to_owned()),
        );
    };
    let commit_result = match import {
        Some(intent) => database.commit_import(prepared, intent),
        None => database.commit_write(prepared),
    };
    let commit = match commit_result {
        Ok(commit) => commit,
        Err(error) => {
            if let Error::NativeCommitPostCommitFailure {
                path,
                transaction_id,
                generation,
                ..
            } = &error
            {
                context.mark_native_commit(path.clone(), transaction_id.clone(), *generation);
            }
            if native_commit_error_requires_reopen(&error) {
                engine.inner.native_poisoned.store(true, Ordering::Release);
            }
            return Err(error);
        }
    };
    install_native_commit(&engine, Some(&context), commit)
}

pub(super) fn install_native_commit(
    engine: &super::Engine,
    context: Option<&QueryContext>,
    commit: crate::storage::NativeCommit,
) -> Result<()> {
    let database =
        engine.inner.database.as_ref().ok_or_else(|| {
            Error::Internal("persistent engine lost its native database".to_owned())
        })?;
    let generation = commit.generation();
    let previous_generation = commit.previous_generation();
    if let Some(context) = context {
        context.mark_native_commit(
            database.path().to_path_buf(),
            commit.transaction_id().to_owned(),
            generation,
        );
    }
    let installed = super::external_source_api::persistent_entries(
        &engine.inner.config,
        engine.inner.metadata_cache.clone(),
        database,
    )
    .and_then(|entries| {
        engine
            .inner
            .persistent_catalog
            .publish(previous_generation, entries)
            .map(|_| ())
    });
    if let Err(error) = installed {
        engine.inner.native_poisoned.store(true, Ordering::Release);
        return Err(Error::native_commit_post_commit_failure(
            database.path(),
            commit.transaction_id(),
            generation,
            format!(
                "catalog generation {generation} committed but could not be installed in memory; reopen the engine: {error}"
            ),
        ));
    }
    if let Err(error) = database.drain_retired() {
        engine.inner.native_poisoned.store(true, Ordering::Release);
        return Err(Error::native_commit_post_commit_failure(
            database.path(),
            commit.transaction_id(),
            generation,
            format!(
                "catalog generation {generation} committed but retired snapshot cleanup failed; reopen the engine: {error}"
            ),
        ));
    }
    Ok(())
}

pub(super) fn install_native_catalog_commit(
    engine: &super::Engine,
    context: &QueryContext,
    outcome: Result<Option<crate::storage::NativeCommit>>,
) -> Result<()> {
    let commit = match outcome {
        Ok(commit) => commit,
        Err(error) => {
            if native_commit_error_requires_reopen(&error) {
                engine.inner.native_poisoned.store(true, Ordering::Release);
            }
            return Err(error);
        }
    };
    match commit {
        Some(commit) => install_native_commit(engine, Some(context), commit),
        None => Ok(()),
    }
}

pub(super) fn native_commit_error_requires_reopen(error: &Error) -> bool {
    !matches!(
        error,
        Error::Catalog(_)
            | Error::InvalidArgument(_)
            | Error::Unsupported(_)
            | Error::ResourceExhausted(_)
            | Error::NativeDiskQuotaExceeded { .. }
            | Error::Cancelled
            | Error::TransactionConflict { .. }
            | Error::TransactionClosed { .. }
    )
}

pub(super) fn poison_on_native_write_error(engine: &super::Engine, error: Option<&Error>) {
    if error.is_some_and(|error| {
        matches!(
            error,
            Error::NativeStorage { .. }
                | Error::CommitOutcomeUnknown { .. }
                | Error::NativeCommitPostCommitFailure { .. }
        )
    }) {
        engine.inner.native_poisoned.store(true, Ordering::Release);
    }
}

async fn write_input(
    mut input: MemoryBatchStream,
    mut writer: crate::storage::NativeTableWriter,
    context: Arc<QueryContext>,
    io: &SpillIoPool,
    returning: Option<&ReturningProjection>,
) -> Result<(crate::storage::NativeTableWriter, Vec<BatchEnvelope>)> {
    let mut returned = Vec::new();
    loop {
        if let Err(error) = context.check_cancelled() {
            return abort_writer(writer, io, None, error);
        }
        let Some(item) = input.next().await else {
            break;
        };
        let envelope = match item {
            Ok(envelope) => envelope,
            Err(error) => return abort_writer(writer, io, None, error),
        };
        let batch = envelope.batch().clone();
        let returning_batch = match returning {
            Some(returning) => match crate::execution::project_expressions(
                &returning.expressions,
                Arc::clone(&returning.schema),
                &batch,
            )
            .and_then(|batch| BatchEnvelope::try_new(batch, &context.memory, "INSERT RETURNING"))
            {
                Ok(batch) => Some(batch),
                Err(error) => return abort_writer(writer, io, None, error),
            },
            None => None,
        };
        let (returned_writer, result) = io.run(move || {
            let mut writer = writer;
            let result = writer.write_batch(&batch);
            Ok((writer, result))
        })?;
        writer = returned_writer;
        drop(envelope);
        if let Err(error) = result {
            return abort_writer(writer, io, None, error);
        }
        if let Some(batch) = returning_batch {
            returned.push(batch);
        }
    }
    Ok((writer, returned))
}

pub(super) fn abort_writer<T>(
    writer: crate::storage::NativeTableWriter,
    io: &SpillIoPool,
    database_path: Option<&std::path::Path>,
    error: Error,
) -> Result<T> {
    match io.run(move || writer.abort()) {
        Ok(()) => Err(error),
        Err(cleanup) => Err(Error::native_storage(
            database_path.unwrap_or_else(|| std::path::Path::new("native staging")),
            format!("{error}; native staging cleanup failed: {cleanup}"),
        )),
    }
}

pub(super) fn abort_prepared<T>(
    prepared: crate::storage::PreparedSnapshot,
    database_path: Option<&std::path::Path>,
    error: Error,
) -> Result<T> {
    match prepared.abort() {
        Ok(()) => Err(error),
        Err(cleanup) => Err(Error::native_storage(
            database_path.unwrap_or_else(|| std::path::Path::new("native staging")),
            format!("{error}; prepared native snapshot cleanup failed: {cleanup}"),
        )),
    }
}
