use std::{
    sync::{Arc, atomic::Ordering},
    time::Duration,
};

use futures::StreamExt;
use sqlparser::ast::Statement;
use tokio::sync::OwnedSemaphorePermit;

use super::{QueryResult, Session, query_result};
use crate::{
    Error, Result, TableEntry,
    command::{NativeWriteCommand, NativeWriteKind},
    datasource::NativeSegmentTable,
    runtime::{
        BatchEnvelope, MemoryBatchStream, QueryContext, SpillIoPool, boxed_memory_batch_stream,
    },
    sql::StatementPlan,
    storage::NativeWriteMode,
};

impl Session {
    pub(super) async fn execute_native_write(
        &self,
        command: NativeWriteCommand,
        permit: OwnedSemaphorePermit,
        admission_wait: Duration,
        parse_time: Duration,
    ) -> Result<QueryResult> {
        let database = self.engine.inner.database.as_ref().ok_or_else(|| {
            Error::Unsupported("native writes require Engine::open(path, config)".to_owned())
        })?;
        let write_permit = Arc::clone(&self.engine.inner.native_write_admission)
            .acquire_owned()
            .await
            .map_err(|_| Error::Internal("native write admission controller closed".to_owned()))?;
        self.engine.ensure_native_healthy()?;
        let context = self.query_context()?;
        context.metrics.record_query_admission_wait(admission_wait);
        context.metrics.record_sql_parse_time(parse_time);
        let statement = Statement::Query(command.query);
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
        if catalog.local_table(&command.name).is_some() {
            return Err(context.error_with_cleanup(Error::Catalog(format!(
                "native write target '{}' is shadowed by a session-local table or view",
                command.name
            ))));
        }
        let generation = catalog.persistent_generation().unwrap_or(0);
        let source_bytes = context.object_snapshot_bytes()?;
        let mode = match command.kind {
            NativeWriteKind::Create => NativeWriteMode::Create,
            NativeWriteKind::Replace => NativeWriteMode::Replace,
            NativeWriteKind::Append => NativeWriteMode::Append,
        };
        if command.kind == NativeWriteKind::Append
            && catalog.persistent_table(&command.name).is_none()
        {
            return Err(context.error_with_cleanup(Error::Catalog(format!(
                "native table '{}' does not exist",
                command.name
            ))));
        }
        let source_schema = Arc::clone(plan.schema().arrow());
        let write_plan =
            database.plan_write(&command.name, mode, generation, source_schema, source_bytes)?;
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
        })?;
        let input =
            crate::execution::execute_internal(StatementPlan::Query(plan), Arc::clone(&context))
                .await
                .map_err(|error| context.error_with_cleanup(error))?;
        let message = match command.kind {
            NativeWriteKind::Create => "CREATE TABLE",
            NativeWriteKind::Replace => "CREATE OR REPLACE TABLE",
            NativeWriteKind::Append => "INSERT",
        };
        let status_schema = crate::command::status(message)?.schema();
        let engine = self.engine.clone();
        let sink_context = Arc::clone(&context);
        let sink = boxed_memory_batch_stream(futures::stream::once(async move {
            run_write(engine, input, writer, sink_context, message, write_permit).await
        }));
        let stream = self.engine.inner.compute.pipe(sink, Arc::clone(&context));
        Ok(query_result(
            status_schema,
            stream,
            context,
            permit,
            self.engine.clone(),
        ))
    }
}

async fn run_write(
    engine: super::Engine,
    input: MemoryBatchStream,
    writer: crate::storage::NativeTableWriter,
    context: Arc<QueryContext>,
    message: &'static str,
    _write_permit: OwnedSemaphorePermit,
) -> Result<BatchEnvelope> {
    let writer = write_input(input, writer, Arc::clone(&context), &engine.inner.spill_io).await?;
    if let Err(error) = context.check_cancelled() {
        return abort_writer(
            writer,
            &engine.inner.spill_io,
            engine.database_path(),
            error,
        );
    }
    let status = match crate::command::status(message)
        .and_then(|batch| BatchEnvelope::try_new(batch, &context.memory, "native write status"))
    {
        Ok(status) => status,
        Err(error) => {
            return abort_writer(
                writer,
                &engine.inner.spill_io,
                engine.database_path(),
                error,
            );
        }
    };
    let prepared = engine.inner.spill_io.run(move || writer.finish())?;
    if let Err(error) = context.check_cancelled() {
        let database_path = engine.database_path().map(std::path::Path::to_path_buf);
        return engine
            .inner
            .spill_io
            .run(move || abort_prepared(prepared, database_path.as_deref(), error));
    }
    let expected_generation = prepared.expected_generation();
    let commit_engine = engine.clone();
    let commit_context = Arc::clone(&context);
    let commit = engine.inner.spill_io.run(move || {
        let _gate = commit_engine.inner.native_commit.lock();
        let database_path = commit_engine
            .database_path()
            .map(std::path::Path::to_path_buf);
        let precommit_error = if commit_engine
            .inner
            .native_poisoned
            .load(Ordering::Acquire)
        {
            Some(Error::native_storage(
                database_path
                    .as_deref()
                    .unwrap_or_else(|| std::path::Path::new("native database")),
                "engine state requires reopen after a native commit failure",
            ))
        } else if let Err(error) = commit_context.check_cancelled() {
            Some(error)
        } else if commit_engine.inner.persistent_catalog.generation() != expected_generation {
            Some(Error::Catalog(format!(
                "persistent catalog changed: expected generation {expected_generation}, found {}",
                commit_engine.inner.persistent_catalog.generation()
            )))
        } else {
            None
        };
        if let Some(error) = precommit_error {
            return abort_prepared(prepared, database_path.as_deref(), error);
        }
        let Some(database) = commit_engine.inner.database.as_ref() else {
            return abort_prepared(
                prepared,
                database_path.as_deref(),
                Error::Internal("persistent engine lost its native database".to_owned()),
            );
        };
        let commit = match database.commit_write(prepared) {
            Ok(commit) => commit,
            Err(error) => {
                if native_commit_error_requires_reopen(&error) {
                    commit_engine
                        .inner
                        .native_poisoned
                        .store(true, Ordering::Release);
                }
                return Err(error);
            }
        };
        let generation = commit.generation();
        commit_context.mark_native_commit(
            database.path().to_path_buf(),
            commit.transaction_id().to_owned(),
            generation,
        );
        let entries = native_entries(&commit_engine, database);
        if let Err(error) = commit_engine
            .inner
            .persistent_catalog
            .publish(expected_generation, entries)
        {
            commit_engine
                .inner
                .native_poisoned
                .store(true, Ordering::Release);
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
            commit_engine
                .inner
                .native_poisoned
                .store(true, Ordering::Release);
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
    });
    commit?;
    Ok(status)
}

fn native_commit_error_requires_reopen(error: &Error) -> bool {
    !matches!(
        error,
        Error::Catalog(_)
            | Error::InvalidArgument(_)
            | Error::Unsupported(_)
            | Error::ResourceExhausted(_)
            | Error::Cancelled
    )
}

async fn write_input(
    mut input: MemoryBatchStream,
    mut writer: crate::storage::NativeTableWriter,
    context: Arc<QueryContext>,
    io: &SpillIoPool,
) -> Result<crate::storage::NativeTableWriter> {
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
        let (returned, result) = io.run(move || {
            let mut writer = writer;
            let result = writer.write_batch(&batch);
            Ok((writer, result))
        })?;
        writer = returned;
        drop(envelope);
        if let Err(error) = result {
            return abort_writer(writer, io, None, error);
        }
    }
    Ok(writer)
}

fn abort_writer<T>(
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

fn abort_prepared<T>(
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

fn native_entries(
    engine: &super::Engine,
    database: &crate::storage::NativeDatabase,
) -> Vec<TableEntry> {
    database
        .table_snapshots()
        .into_iter()
        .map(|(name, snapshot)| {
            let provider = NativeSegmentTable::new(
                database.path(),
                snapshot,
                &engine.inner.config,
                engine.inner.metadata_cache.clone(),
            );
            TableEntry::new(name, Arc::new(provider))
        })
        .collect()
}
