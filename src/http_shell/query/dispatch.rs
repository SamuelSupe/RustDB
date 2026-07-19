use std::{sync::Arc, time::Duration};

use futures::FutureExt;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use super::{
    ActiveTask, Job, ManagerInner, QueryJournal, QueryObserver, ResultStore, cancel_and_persist,
    execution, fail_admission, fail_record, log_terminal, persist_or_log, persist_terminal,
};
use crate::{Engine, Error};

pub(super) fn spawn(inner: Arc<ManagerInner>, engine: Engine, mut receiver: mpsc::Receiver<Job>) {
    let active = ActiveTask::start(&inner);
    tokio::spawn(async move {
        let _active = active;
        loop {
            let job = tokio::select! {
                _ = inner.shutdown.cancelled() => break,
                job = receiver.recv() => match job { Some(job) => job, None => break },
            };
            let engine = engine.clone();
            let store = Arc::clone(&inner.store);
            let journal = Arc::clone(&inner.journal);
            let maximum = inner.config.max_query_time;
            let memory_limit = usize::try_from(inner.config.query_memory_limit_bytes)
                .expect("query memory limit was validated");
            let observer = inner.observer.clone();
            let shutdown = inner.shutdown.clone();
            let active = ActiveTask::start(&inner);
            let record = Arc::clone(&job.record);
            tokio::spawn(async move {
                let _active = active;
                let task = run_admitted(
                    engine,
                    store,
                    journal.clone(),
                    job,
                    maximum,
                    memory_limit,
                    shutdown,
                    observer.clone(),
                );
                if std::panic::AssertUnwindSafe(task)
                    .catch_unwind()
                    .await
                    .is_err()
                {
                    fail_record(
                        &record,
                        Error::Internal("HTTP query task panicked".to_owned()),
                    );
                    persist_or_log(&journal, &record, "panic terminal state");
                    log_terminal(&record);
                    if let Some(observer) = &observer {
                        observer.terminal(&record, record.state.read().started_at_ms.is_some());
                    }
                }
            });
        }
    });
}

#[allow(clippy::too_many_arguments)]
async fn run_admitted(
    engine: Engine,
    store: Arc<ResultStore>,
    journal: Arc<QueryJournal>,
    job: Job,
    maximum: Duration,
    memory_limit: usize,
    shutdown: CancellationToken,
    observer: Option<QueryObserver>,
) {
    let Job {
        record,
        request,
        admission,
        queued_at,
    } = job;
    let ticket = tokio::select! {
        _ = shutdown.cancelled() => {
            if let Err(error) = cancel_and_persist(&record, &journal) {
                tracing::error!(%error, query_id = %record.id, "failed to persist queued query cancellation during shutdown");
            }
            log_terminal(&record);
            None
        }
        _ = record.cancel.cancelled() => {
            if let Err(error) = cancel_and_persist(&record, &journal) {
                tracing::error!(%error, query_id = %record.id, "failed to persist queued query cancellation");
            }
            log_terminal(&record);
            None
        },
        decision = admission.wait() => match decision {
            Ok(ticket) => Some(ticket),
            Err(error) => {
                fail_admission(&record, error);
                persist_terminal(&journal, &record);
                log_terminal(&record);
                if let Some(observer) = &observer {
                    observer.terminal(&record, false);
                }
                return;
            }
        },
    };
    let Some(_ticket) = ticket else {
        if let Some(observer) = &observer {
            observer.terminal(&record, false);
        }
        return;
    };
    if record.terminal() {
        if let Some(observer) = &observer {
            observer.terminal(&record, false);
        }
        return;
    }
    let was_running = execution::run(
        engine,
        store,
        journal,
        Arc::clone(&record),
        request,
        execution::RunOptions {
            maximum,
            memory_limit,
            observer: observer.as_ref(),
            scheduler_wait: queued_at.elapsed(),
        },
    )
    .await;
    if let Some(observer) = &observer {
        observer.terminal(&record, was_running);
    }
}
