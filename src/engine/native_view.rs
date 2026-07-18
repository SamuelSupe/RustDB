use std::{sync::Arc, time::Duration};

use tokio::sync::OwnedSemaphorePermit;

use super::{QueryResult, Session};
use crate::{Error, Result, TableEntry, command::ViewTable};

impl Session {
    pub(super) async fn execute_persistent_view_create(
        &self,
        name: String,
        query: String,
        replace: bool,
        permit: OwnedSemaphorePermit,
        admission_wait: Duration,
        parse_time: Duration,
    ) -> Result<QueryResult> {
        if self.engine.inner.database.is_none() {
            return Err(Error::Unsupported(
                "persistent CREATE VIEW requires Engine::open(path, config)".to_owned(),
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
        if self.catalog.is_local_view(&name)
            && !transaction
                .as_ref()
                .is_some_and(|transaction| transaction.working_view(&name).is_some())
        {
            return Err(context.error_with_cleanup(Error::Catalog(format!(
                "cannot replace temporary view '{name}' with a persistent view"
            ))));
        }
        if self.catalog.local_table(&name).is_some() && !self.catalog.is_local_view(&name) {
            return Err(context.error_with_cleanup(Error::Catalog(format!(
                "cannot replace session-local table '{name}' with a persistent view"
            ))));
        }
        let plan = self
            .prepare_view_plan(&query, Some(Arc::clone(&context)))
            .await
            .map_err(|error| context.error_with_cleanup(error))?;
        let schema = Arc::clone(plan.schema().arrow());
        let provider = Arc::new(ViewTable::new(
            name.clone(),
            query.clone(),
            plan,
            self.catalog.clone(),
            self.engine.inner.config.clone(),
            self.engine.inner.metadata_cache.clone(),
        ));
        match transaction {
            Some(transaction) => {
                transaction.stage_view(
                    &self.catalog,
                    &name,
                    query,
                    schema,
                    TableEntry::new(name.clone(), provider),
                    replace,
                )?;
                context.mark_transaction_mutation_applied();
            }
            None => {
                let catalog = context.catalog_snapshot().ok_or_else(|| {
                    Error::Internal("persistent view has no fixed catalog snapshot".to_owned())
                })?;
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
                    let commit = database
                        .commit_view_create(generation, &name, query, schema, replace)
                        .map(Some);
                    super::native_write::install_native_catalog_commit(
                        &engine,
                        &commit_context,
                        commit,
                    )
                })?;
            }
        }
        self.batch_result(crate::command::status("CREATE VIEW")?, permit, context)
    }

    pub(super) async fn execute_view_drop(
        &self,
        name: String,
        if_exists: bool,
        permit: OwnedSemaphorePermit,
        admission_wait: Duration,
        parse_time: Duration,
    ) -> Result<QueryResult> {
        let transaction = self.native_transaction.clone();
        let staged_persistent = transaction
            .as_ref()
            .is_some_and(|transaction| transaction.working_view(&name).is_some());
        if self.catalog.is_local_view(&name) && !staged_persistent {
            if transaction.is_some() {
                return Err(Error::Unsupported(
                    "temporary views cannot be dropped inside an explicit transaction".to_owned(),
                ));
            }
            self.catalog.drop_view(&name);
            let context = self.query_context()?;
            context.metrics.record_query_admission_wait(admission_wait);
            context.metrics.record_sql_parse_time(parse_time);
            return self.batch_result(crate::command::status("DROP VIEW")?, permit, context);
        }
        if self.engine.inner.database.is_none() {
            return if if_exists {
                let context = self.query_context()?;
                self.batch_result(crate::command::status("DROP VIEW")?, permit, context)
            } else {
                Err(Error::Catalog(format!("view '{name}' does not exist")))
            };
        }
        let _mutation = transaction
            .as_ref()
            .map(|transaction| transaction.begin_mutation())
            .transpose()?;
        let context = self.query_context()?;
        context.metrics.record_query_admission_wait(admission_wait);
        context.metrics.record_sql_parse_time(parse_time);
        let catalog = self.pin_catalog()?;
        context.set_catalog_snapshot(catalog.clone())?;
        match transaction {
            Some(transaction) => {
                if transaction.stage_view_drop(&self.catalog, &name, if_exists)? {
                    context.mark_transaction_mutation_applied();
                }
            }
            None => {
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
                    let commit = database.commit_view_drop(generation, &name, if_exists);
                    super::native_write::install_native_catalog_commit(
                        &engine,
                        &commit_context,
                        commit,
                    )
                })?;
            }
        }
        self.batch_result(crate::command::status("DROP VIEW")?, permit, context)
    }
}
