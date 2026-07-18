use std::time::Duration;

use tokio::sync::OwnedSemaphorePermit;

use super::{QueryResult, Session};
use crate::{Error, Result, command::NativeDropTableCommand};

impl Session {
    pub(super) async fn execute_native_drop_table(
        &self,
        command: NativeDropTableCommand,
        permit: OwnedSemaphorePermit,
        admission_wait: Duration,
        parse_time: Duration,
    ) -> Result<QueryResult> {
        if self.engine.inner.database.is_none() {
            return Err(Error::Unsupported(
                "native DROP TABLE requires Engine::open(path, config)".to_owned(),
            ));
        }
        let transaction = self.native_transaction.clone();
        let _mutation = transaction
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
                "native DROP TABLE target '{}' is shadowed by a session-local table or view",
                command.name
            ))));
        }

        match transaction {
            Some(transaction) => {
                if transaction.stage_drop(&self.catalog, &command.name, command.if_exists)? {
                    context.mark_transaction_mutation_applied();
                }
            }
            None => {
                let generation = catalog.persistent_generation().unwrap_or(0);
                let engine = self.engine.clone();
                let name = command.name.clone();
                let if_exists = command.if_exists;
                let commit_context = context.clone();
                self.engine.inner.spill_io.run(move || {
                    engine.ensure_native_healthy()?;
                    let _gate = engine.inner.native_commit.lock();
                    engine.ensure_native_healthy()?;
                    let database = engine.inner.database.as_ref().ok_or_else(|| {
                        Error::Internal("persistent engine lost its native database".to_owned())
                    })?;
                    let commit = database.commit_table_drop(generation, &name, if_exists);
                    super::native_write::install_native_catalog_commit(
                        &engine,
                        &commit_context,
                        commit,
                    )
                })?;
            }
        }
        let batch = crate::command::status("DROP TABLE")?;
        self.batch_result(batch, permit, context)
    }
}
