use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, atomic::Ordering},
};

use super::{State, TransactionWorkspace};
use crate::{Catalog, Error, Result, storage::NativeTableSnapshot};

struct Checkpoint {
    catalog: crate::catalog::TransactionCatalogCheckpoint,
    expected_schemas: BTreeMap<String, bool>,
    working_schemas: BTreeSet<String>,
    schema_updates: BTreeMap<String, bool>,
    expected: BTreeMap<String, Option<Arc<NativeTableSnapshot>>>,
    working: BTreeMap<String, Arc<NativeTableSnapshot>>,
    working_views: BTreeMap<String, Arc<crate::storage::NativeView>>,
    write_names: Vec<String>,
    drops: BTreeSet<String>,
    renames: BTreeMap<String, String>,
    expected_views: BTreeMap<String, Option<Arc<crate::storage::NativeView>>>,
    view_updates: BTreeMap<String, Option<Arc<crate::storage::NativeView>>>,
}

pub(in crate::engine) struct StatementSavepoint {
    workspace: Arc<TransactionWorkspace>,
    engine: crate::Engine,
    catalog: Catalog,
    checkpoint: Option<Checkpoint>,
}

impl TransactionWorkspace {
    pub(in crate::engine) fn begin_statement(
        self: &Arc<Self>,
        engine: crate::Engine,
        catalog: Catalog,
    ) -> Result<StatementSavepoint> {
        if self
            .statement_active
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return Err(Error::InvalidArgument(
                "a transaction may have only one active native mutation result".to_owned(),
            ));
        }
        let checkpoint = {
            let state = self.state.lock();
            Checkpoint::capture(&state, catalog.transaction_checkpoint())
        };
        Ok(StatementSavepoint {
            workspace: Arc::clone(self),
            engine,
            catalog,
            checkpoint: Some(checkpoint),
        })
    }

    fn rollback_statement(
        &self,
        engine: &crate::Engine,
        catalog: &Catalog,
        checkpoint: Checkpoint,
    ) -> Result<()> {
        let new_writes = {
            let mut state = self.state.lock();
            if state.writes.len() < checkpoint.write_names.len() {
                return Err(Error::Internal(
                    "transaction statement checkpoint is newer than workspace state".to_owned(),
                ));
            }
            let new_writes = state.writes.split_off(checkpoint.write_names.len());
            if let Some(database) = engine.inner.database.as_ref() {
                for (write, original_name) in state.writes.iter_mut().zip(&checkpoint.write_names) {
                    if write.name() != original_name {
                        write.rename_to(original_name.clone());
                        database.rename_transaction_write(write.transaction_id(), original_name);
                    }
                }
            }
            checkpoint.restore(&mut state);
            new_writes
        };
        catalog.restore_transaction_checkpoint(checkpoint.catalog);
        if new_writes.is_empty() {
            return Ok(());
        }
        let _gate = engine.inner.native_commit.lock();
        let database = engine.inner.database.as_ref().ok_or_else(|| {
            Error::Internal("persistent engine lost its native database".to_owned())
        })?;
        database
            .abort_transaction_writes(new_writes)
            .inspect_err(|_| {
                engine.inner.native_poisoned.store(true, Ordering::Release);
            })
    }
}

impl Checkpoint {
    fn capture(state: &State, catalog: crate::catalog::TransactionCatalogCheckpoint) -> Self {
        Self {
            catalog,
            expected_schemas: state.expected_schemas.clone(),
            working_schemas: state.working_schemas.clone(),
            schema_updates: state.schema_updates.clone(),
            expected: state.expected.clone(),
            working: state.working.clone(),
            working_views: state.working_views.clone(),
            write_names: state
                .writes
                .iter()
                .map(|write| write.name().to_owned())
                .collect(),
            drops: state.drops.clone(),
            renames: state.renames.clone(),
            expected_views: state.expected_views.clone(),
            view_updates: state.view_updates.clone(),
        }
    }

    fn restore(&self, state: &mut State) {
        state.expected_schemas.clone_from(&self.expected_schemas);
        state.working_schemas.clone_from(&self.working_schemas);
        state.schema_updates.clone_from(&self.schema_updates);
        state.expected.clone_from(&self.expected);
        state.working.clone_from(&self.working);
        state.working_views.clone_from(&self.working_views);
        state.drops.clone_from(&self.drops);
        state.renames.clone_from(&self.renames);
        state.expected_views.clone_from(&self.expected_views);
        state.view_updates.clone_from(&self.view_updates);
    }
}

impl StatementSavepoint {
    pub(in crate::engine) fn commit(mut self) {
        self.checkpoint.take();
    }

    pub(in crate::engine) fn rollback(mut self) -> Result<()> {
        let Some(checkpoint) = self.checkpoint.take() else {
            return Ok(());
        };
        self.workspace
            .rollback_statement(&self.engine, &self.catalog, checkpoint)
    }
}

impl Drop for StatementSavepoint {
    fn drop(&mut self) {
        if let Some(checkpoint) = self.checkpoint.take()
            && let Err(error) =
                self.workspace
                    .rollback_statement(&self.engine, &self.catalog, checkpoint)
        {
            tracing::error!(
                %error,
                transaction_id = %self.workspace.transaction_id,
                "failed to rollback abandoned transaction statement"
            );
        }
        self.workspace
            .statement_active
            .store(false, Ordering::Release);
    }
}
