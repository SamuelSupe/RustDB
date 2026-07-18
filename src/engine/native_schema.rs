use std::time::Duration;

use tokio::sync::OwnedSemaphorePermit;

use super::{QueryResult, Session};
use crate::{Error, Result, command::NativeSchemaCommand};

impl Session {
    pub(super) async fn execute_native_schema(
        &self,
        command: NativeSchemaCommand,
        permit: OwnedSemaphorePermit,
        admission_wait: Duration,
        parse_time: Duration,
    ) -> Result<QueryResult> {
        if self.engine.inner.database.is_none() {
            return Err(Error::Unsupported(
                "schema DDL requires Engine::open(path, config)".to_owned(),
            ));
        }
        let transaction = self.native_transaction.clone();
        let _mutation = transaction
            .as_ref()
            .map(|transaction| transaction.begin_mutation())
            .transpose()?;
        let context = self.query_context()?;
        context.metrics.record_query_admission_wait(admission_wait);
        context.metrics.record_sql_parse_time(parse_time);

        let message = command_name(&command);
        if let Some(transaction) = transaction {
            let changed = match &command {
                NativeSchemaCommand::Create {
                    name,
                    if_not_exists,
                } => transaction.stage_schema_create(name, *if_not_exists)?,
                NativeSchemaCommand::Drop { name, if_exists } => {
                    transaction.stage_schema_drop(name, *if_exists)?
                }
            };
            if changed {
                context.mark_transaction_mutation_applied();
            }
        } else {
            let catalog = self.pin_catalog()?;
            let generation = catalog.persistent_generation().unwrap_or(0);
            let engine = self.engine.clone();
            let commit_context = context.clone();
            self.engine.inner.spill_io.run(move || {
                engine.ensure_native_healthy()?;
                let _gate = engine.inner.native_commit.lock();
                engine.ensure_native_healthy()?;
                let database = engine.inner.database.as_ref().ok_or_else(|| {
                    Error::Internal("persistent engine lost its native database".to_owned())
                })?;
                let commit = match command {
                    NativeSchemaCommand::Create {
                        name,
                        if_not_exists,
                    } => database.commit_schema_create(generation, &name, if_not_exists),
                    NativeSchemaCommand::Drop { name, if_exists } => {
                        database.commit_schema_drop(generation, &name, if_exists)
                    }
                };
                super::native_write::install_native_catalog_commit(&engine, &commit_context, commit)
            })?;
        }
        self.batch_result(crate::command::status(message)?, permit, context)
    }
}

fn command_name(command: &NativeSchemaCommand) -> &'static str {
    match command {
        NativeSchemaCommand::Create { .. } => "CREATE SCHEMA",
        NativeSchemaCommand::Drop { .. } => "DROP SCHEMA",
    }
}
