use std::{
    mem,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use futures::Stream;
use parking_lot::Mutex;
use uuid::Uuid;

use super::{QueryResult, Session};
use crate::{
    Error, ParameterValue, PreparedStatement, Result,
    command::{ParsedStatement, SessionCommand},
    runtime::{RecordBatchStream, TaskGroup, boxed_record_batch_stream},
};

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[non_exhaustive]
pub enum TransactionAccessMode {
    ReadOnly,
    #[default]
    ReadWrite,
}

#[derive(Clone, Copy, Debug, Default)]
#[non_exhaustive]
pub struct TransactionOptions {
    access_mode: TransactionAccessMode,
}

impl TransactionOptions {
    pub fn read_only() -> Self {
        Self {
            access_mode: TransactionAccessMode::ReadOnly,
        }
    }

    pub fn read_write() -> Self {
        Self::default()
    }

    pub fn with_access_mode(mut self, access_mode: TransactionAccessMode) -> Self {
        self.access_mode = access_mode;
        self
    }

    pub fn access_mode(&self) -> TransactionAccessMode {
        self.access_mode
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CommitInfo {
    transaction_id: Uuid,
    snapshot_generation: u64,
    committed_generation: Option<u64>,
}

impl CommitInfo {
    pub fn transaction_id(&self) -> Uuid {
        self.transaction_id
    }

    pub fn snapshot_generation(&self) -> u64 {
        self.snapshot_generation
    }

    pub fn committed_generation(&self) -> Option<u64> {
        self.committed_generation
    }
}

pub struct Transaction {
    session: Session,
    shared: Arc<Shared>,
    finished: bool,
}

pub struct TransactionPreparedStatement {
    session: Session,
    shared: Arc<Shared>,
    prepared: PreparedStatement,
}

struct Shared {
    transaction_id: Uuid,
    snapshot_generation: u64,
    access_mode: TransactionAccessMode,
    _snapshot_lease: super::transaction_manager::TransactionLease,
    workspace: Arc<TransactionWorkspace>,
    state: Mutex<State>,
}

#[path = "transaction/workspace.rs"]
mod workspace;
pub(super) use workspace::{MutationLease, TransactionWorkspace};

struct State {
    lifecycle: Lifecycle,
    active_results: usize,
    rollback_pending: bool,
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum Lifecycle {
    Active,
    Committed { committed_generation: Option<u64> },
    RolledBack,
    Indeterminate,
}

struct ResultGuard {
    shared: Arc<Shared>,
    engine: super::Engine,
    context: Option<Arc<crate::runtime::QueryContext>>,
    completed: bool,
}

struct TransactionResultStream {
    input: RecordBatchStream,
    guard: Option<ResultGuard>,
    tasks: TaskGroup,
    failed: bool,
}

impl Transaction {
    pub(super) fn begin(session: &Session, options: TransactionOptions) -> Result<Self> {
        if options.access_mode == TransactionAccessMode::ReadWrite
            && session.engine.inner.database.is_none()
        {
            return Err(Error::Unsupported(
                "read-write transactions require Engine::open(path, config)".to_owned(),
            ));
        }
        let transaction_id = Uuid::new_v4();
        let _gate = session.engine.inner.native_commit.lock();
        let catalog = session.catalog.pin();
        let (snapshot_generation, schemas, tables, views) =
            match session.engine.inner.database.as_ref() {
                Some(database) => database.transaction_snapshot(),
                None => (
                    catalog.persistent_generation().unwrap_or(0),
                    std::collections::BTreeSet::from([
                        crate::catalog_name::DEFAULT_SCHEMA.to_owned()
                    ]),
                    Default::default(),
                    Default::default(),
                ),
            };
        if catalog.persistent_generation().unwrap_or(0) != snapshot_generation {
            return Err(Error::Internal(
                "transaction catalog snapshot does not match native storage".to_owned(),
            ));
        }
        let workspace =
            TransactionWorkspace::new(transaction_id, snapshot_generation, schemas, tables, views);
        let snapshot_lease = session.engine.inner.transactions.register(
            transaction_id,
            snapshot_generation,
            options.access_mode == TransactionAccessMode::ReadWrite,
        );
        let session = Session {
            engine: session.engine.clone(),
            catalog,
            sql_transaction: Arc::new(tokio::sync::Mutex::new(None)),
            native_transaction: Some(Arc::clone(&workspace)),
        };
        Ok(Self {
            session,
            shared: Arc::new(Shared {
                transaction_id,
                snapshot_generation,
                access_mode: options.access_mode,
                _snapshot_lease: snapshot_lease,
                workspace,
                state: Mutex::new(State {
                    lifecycle: Lifecycle::Active,
                    active_results: 0,
                    rollback_pending: false,
                }),
            }),
            finished: false,
        })
    }

    pub fn id(&self) -> Uuid {
        self.shared.transaction_id
    }

    pub fn snapshot_generation(&self) -> u64 {
        self.shared.snapshot_generation
    }

    pub fn access_mode(&self) -> TransactionAccessMode {
        self.shared.access_mode
    }

    /// Returns the commit identity once the durable outcome is known to be
    /// committed, including when commit returned a post-commit handling error.
    pub fn commit_info(&self) -> Option<CommitInfo> {
        let Lifecycle::Committed {
            committed_generation,
        } = self.shared.state.lock().lifecycle
        else {
            return None;
        };
        Some(CommitInfo {
            transaction_id: self.shared.transaction_id,
            snapshot_generation: self.shared.snapshot_generation,
            committed_generation,
        })
    }

    pub(super) fn can_release_session(&self) -> bool {
        let state = self.shared.state.lock();
        state.lifecycle != Lifecycle::Active && state.active_results == 0 && !state.rollback_pending
    }

    pub async fn execute(&self, sql: &str) -> Result<QueryResult> {
        reject_non_query_command(sql, self.shared.access_mode)?;
        let guard = self.shared.begin_result(&self.session.engine)?;
        match self.session.execute_direct(sql).await {
            Ok(result) => Ok(attach_guard(result, guard)),
            Err(error) => Err(error),
        }
    }

    pub(super) async fn execute_statement(
        &self,
        statement: sqlparser::ast::Statement,
    ) -> Result<QueryResult> {
        let guard = self.shared.begin_result(&self.session.engine)?;
        match self.session.execute_prepared_direct(statement).await {
            Ok(result) => Ok(attach_guard(result, guard)),
            Err(error) => Err(error),
        }
    }

    pub fn prepare(&self, sql: &str) -> Result<TransactionPreparedStatement> {
        self.shared.ensure_active()?;
        Ok(TransactionPreparedStatement {
            session: self.session.clone(),
            shared: Arc::clone(&self.shared),
            prepared: PreparedStatement::new(self.session.clone(), sql)?,
        })
    }

    pub fn commit(&mut self) -> Result<CommitInfo> {
        let mut state = self.shared.state.lock();
        ensure_lifecycle(&self.shared, state.lifecycle)?;
        if state.active_results != 0 {
            return Err(Error::InvalidArgument(format!(
                "transaction {} has {} active result stream(s); consume to end-of-stream, or drop and wait for query cleanup before commit",
                self.shared.transaction_id, state.active_results
            )));
        }
        let committed_generation = match self.shared.workspace.commit(&self.session.engine) {
            Ok(generation) => generation,
            Err(error) => {
                state.lifecycle = match &error {
                    Error::NativeCommitPostCommitFailure { generation, .. } => {
                        Lifecycle::Committed {
                            committed_generation: Some(*generation),
                        }
                    }
                    Error::CommitOutcomeUnknown { .. } => Lifecycle::Indeterminate,
                    _ => Lifecycle::RolledBack,
                };
                self.finished = true;
                return Err(error);
            }
        };
        state.lifecycle = Lifecycle::Committed {
            committed_generation,
        };
        self.finished = true;
        Ok(CommitInfo {
            transaction_id: self.shared.transaction_id,
            snapshot_generation: self.shared.snapshot_generation,
            committed_generation,
        })
    }

    pub fn rollback(&mut self) -> Result<()> {
        self.shared.rollback(&self.session.engine)?;
        self.finished = true;
        Ok(())
    }
}

impl TransactionPreparedStatement {
    pub fn parameter_count(&self) -> usize {
        self.prepared.parameter_count()
    }

    pub async fn execute(&self, parameters: &[ParameterValue]) -> Result<QueryResult> {
        let statement = self.prepared.instantiate(parameters)?;
        let guard = self.shared.begin_result(&self.session.engine)?;
        match self.session.execute_prepared_direct(statement).await {
            Ok(result) => Ok(attach_guard(result, guard)),
            Err(error) => Err(error),
        }
    }
}

impl Shared {
    fn ensure_active(&self) -> Result<()> {
        ensure_lifecycle(self, self.state.lock().lifecycle)
    }

    fn begin_result(self: &Arc<Self>, engine: &super::Engine) -> Result<ResultGuard> {
        let mut state = self.state.lock();
        ensure_lifecycle(self, state.lifecycle)?;
        state.active_results = state.active_results.checked_add(1).ok_or_else(|| {
            Error::ResourceExhausted("transaction active-result counter overflowed".to_owned())
        })?;
        Ok(ResultGuard {
            shared: Arc::clone(self),
            engine: engine.clone(),
            context: None,
            completed: false,
        })
    }

    fn rollback(&self, engine: &super::Engine) -> Result<()> {
        let mut state = self.state.lock();
        ensure_lifecycle(self, state.lifecycle)?;
        if state.active_results != 0 {
            return Err(Error::InvalidArgument(format!(
                "transaction {} has {} active result stream(s); consume to end-of-stream, or drop and wait for query cleanup before rollback",
                self.transaction_id, state.active_results
            )));
        }
        self.workspace.rollback(engine)?;
        state.lifecycle = Lifecycle::RolledBack;
        Ok(())
    }

    fn finish_result(&self, engine: &super::Engine, rollback_mutation: bool) {
        let rollback = {
            let mut state = self.state.lock();
            state.active_results = state.active_results.saturating_sub(1);
            if rollback_mutation && state.lifecycle == Lifecycle::Active {
                state.lifecycle = Lifecycle::RolledBack;
                state.rollback_pending = true;
            }
            state.active_results == 0 && state.rollback_pending
        };
        if rollback {
            let outcome = self.workspace.rollback(engine);
            self.state.lock().rollback_pending = false;
            if let Err(error) = outcome {
                tracing::error!(%error, transaction_id = %self.transaction_id, "failed to finish deferred transaction rollback");
            }
        }
    }
}

impl Drop for Transaction {
    fn drop(&mut self) {
        if self.finished {
            return;
        }
        let rollback = {
            let mut state = self.shared.state.lock();
            if state.lifecycle != Lifecycle::Active {
                false
            } else {
                state.lifecycle = Lifecycle::RolledBack;
                state.rollback_pending = state.active_results != 0;
                !state.rollback_pending
            }
        };
        if rollback && let Err(error) = self.shared.workspace.rollback(&self.session.engine) {
            tracing::error!(%error, transaction_id = %self.shared.transaction_id, "failed to rollback dropped transaction");
        }
    }
}

impl Drop for ResultGuard {
    fn drop(&mut self) {
        let rollback_mutation = !self.completed
            && self
                .context
                .as_ref()
                .is_some_and(|context| context.transaction_mutation_was_applied());
        self.shared.finish_result(&self.engine, rollback_mutation);
    }
}

impl Stream for TransactionResultStream {
    type Item = Result<arrow::record_batch::RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        let result = this.input.as_mut().poll_next(context);
        match &result {
            Poll::Ready(Some(Err(_))) => this.failed = true,
            Poll::Ready(None) => {
                if let Some(guard) = this.guard.as_mut() {
                    guard.completed = !this.failed;
                }
                drop(this.guard.take());
            }
            _ => {}
        }
        result
    }
}

impl Drop for TransactionResultStream {
    fn drop(&mut self) {
        let input = mem::replace(
            &mut self.input,
            boxed_record_batch_stream(futures::stream::empty()),
        );
        drop(input);
        if let Some(guard) = self.guard.take() {
            self.tasks.close();
            if self.tasks.active_tasks() == 0 {
                drop(guard);
            } else {
                self.tasks.reap(move || drop(guard));
            }
        }
    }
}

fn ensure_lifecycle(shared: &Shared, lifecycle: Lifecycle) -> Result<()> {
    if lifecycle == Lifecycle::Active {
        return Ok(());
    }
    Err(Error::TransactionClosed {
        transaction_id: shared.transaction_id.to_string(),
        state: match lifecycle {
            Lifecycle::Active => "active",
            Lifecycle::Committed { .. } => "committed",
            Lifecycle::RolledBack => "rolled back",
            Lifecycle::Indeterminate => "commit outcome unknown",
        },
    })
}

fn reject_non_query_command(sql: &str, mode: TransactionAccessMode) -> Result<()> {
    match crate::command::parse(sql)? {
        ParsedStatement::Query(_)
        | ParsedStatement::Command(
            SessionCommand::ShowTables
            | SessionCommand::ShowSchemas
            | SessionCommand::Describe { .. }
            | SessionCommand::CopyTo(_),
        ) => Ok(()),
        ParsedStatement::Command(
            SessionCommand::NativeWrite(_)
            | SessionCommand::NativeAlter(_)
            | SessionCommand::NativeDelete(_)
            | SessionCommand::NativeDropTable(_)
            | SessionCommand::NativeSchema(_)
            | SessionCommand::NativeUpdate(_)
            | SessionCommand::NativeTruncate(_),
        ) if mode == TransactionAccessMode::ReadOnly => Err(Error::Unsupported(
            "read-only transactions cannot execute native DML".to_owned(),
        )),
        ParsedStatement::Command(
            SessionCommand::NativeWrite(_)
            | SessionCommand::NativeAlter(_)
            | SessionCommand::NativeDelete(_)
            | SessionCommand::NativeDropTable(_)
            | SessionCommand::NativeSchema(_)
            | SessionCommand::NativeUpdate(_)
            | SessionCommand::NativeTruncate(_),
        ) => Ok(()),
        ParsedStatement::Command(SessionCommand::CreatePersistentView { .. })
            if mode == TransactionAccessMode::ReadOnly =>
        {
            Err(Error::Unsupported(
                "read-only transactions cannot execute persistent DDL".to_owned(),
            ))
        }
        ParsedStatement::Command(SessionCommand::CreatePersistentView { .. }) => Ok(()),
        ParsedStatement::Command(SessionCommand::DropView { .. })
            if mode == TransactionAccessMode::ReadOnly =>
        {
            Err(Error::Unsupported(
                "read-only transactions cannot execute persistent DDL".to_owned(),
            ))
        }
        ParsedStatement::Command(SessionCommand::DropView { .. }) => Ok(()),
        ParsedStatement::Command(_) if mode == TransactionAccessMode::ReadOnly => {
            Err(Error::Unsupported(
                "read-only transactions cannot execute session mutations".to_owned(),
            ))
        }
        ParsedStatement::Command(_) => Err(Error::Unsupported(
            "session mutations inside explicit transactions are introduced in v0.8 alpha.2"
                .to_owned(),
        )),
    }
}

fn attach_guard(mut result: QueryResult, mut guard: ResultGuard) -> QueryResult {
    guard.context = Some(Arc::clone(&result.context));
    let input = mem::replace(
        &mut result.stream,
        boxed_record_batch_stream(futures::stream::empty()),
    );
    result.stream = boxed_record_batch_stream(TransactionResultStream {
        input,
        guard: Some(guard),
        tasks: result.context.tasks.clone(),
        failed: false,
    });
    result
}

#[cfg(test)]
#[path = "transaction/tests.rs"]
mod tests;
