use std::{sync::Arc, time::Duration};

use futures::StreamExt;

use super::{
    HttpQueryMetrics, QueryJournal, QueryRecord, QueryRequest, QueryState, ResultStore,
    StoredResult, fail_record, log_terminal, metrics, now_ms, persist_or_log_async,
    persist_terminal_async, terminal_error,
};
use crate::{Engine, Error, HttpReadOnlyPolicy};

pub(super) struct RunOptions<'a> {
    pub(super) maximum: Duration,
    pub(super) memory_limit: usize,
    pub(super) observer: Option<&'a super::QueryObserver>,
    pub(super) scheduler_wait: Duration,
}

pub(super) async fn run(
    engine: Engine,
    store: Arc<ResultStore>,
    journal: Arc<QueryJournal>,
    record: Arc<QueryRecord>,
    request: QueryRequest,
    options: RunOptions<'_>,
) -> bool {
    {
        let mut state = record.state.write();
        if state.phase != QueryState::Queued {
            return false;
        }
        state.phase = QueryState::Running;
        state.started_at_ms = Some(now_ms());
    }
    if let Some(observer) = options.observer {
        observer.started(options.scheduler_wait);
    }
    if let Err(error) = journal.upsert_async(record.persisted()).await {
        fail_record(
            &record,
            Error::Execution(format!("failed to persist running query state: {error}")),
        );
        persist_or_log_async(&journal, &record, "running-state failure").await;
        log_terminal(&record);
        return true;
    }
    let timeout = match request.timeout(options.maximum) {
        Ok(timeout) => timeout,
        Err(error) => {
            fail_record(&record, error);
            persist_terminal_async(&journal, &record).await;
            log_terminal(&record);
            return true;
        }
    };
    match execute_and_store(
        &engine,
        &store,
        &record,
        &request,
        timeout,
        options.memory_limit,
    )
    .await
    {
        Ok((result, metrics)) => {
            let _transition = record.lock_transition().await;
            if record.terminal() {
                let terminal_phase = record.state.read().phase;
                match terminal_phase {
                    QueryState::Interrupted => {
                        if let Err(error) = result
                            .mark_interrupted_async(
                                "query interrupted during bounded server shutdown",
                            )
                            .await
                        {
                            tracing::error!(
                                %error,
                                query_id = %record.id,
                                "failed to seal completed batches as interrupted"
                            );
                        } else {
                            record.publish_interrupted_result(result);
                        }
                    }
                    _ => {
                        if let Err(error) = result.delete_async().await {
                            tracing::error!(%error, query_id = %record.id, "failed to remove a terminal Query result");
                        }
                    }
                }
                log_terminal(&record);
                return true;
            }
            let finished_at_ms = now_ms();
            let durable = record.pending_success(finished_at_ms, metrics.clone());
            if let Err(error) = journal.upsert_async(durable).await {
                if let Err(cleanup) = result.delete_async().await {
                    tracing::error!(%cleanup, query_id = %record.id, "failed to remove an unpublished HTTP result");
                }
                record.fail_stably(
                    "query.journal_failed",
                    "query terminal state could not be persisted",
                    crate::RetryClass::Unknown,
                );
                tracing::error!(%error, query_id = %record.id, "failed to persist successful HTTP query before publication");
                persist_or_log_async(&journal, &record, "success-publication failure").await;
                log_terminal(&record);
                return true;
            }
            record.publish_success(finished_at_ms, metrics, result);
            log_terminal(&record);
            true
        }
        Err(failure) => {
            let _transition = record.lock_transition().await;
            if record.terminal() {
                log_terminal(&record);
                return true;
            }
            if record.interruption_requested() {
                record.interrupt();
                log_terminal(&record);
                return true;
            }
            match failure {
                JobFailure::Cancelled => {
                    let mut state = record.state.write();
                    state.phase = QueryState::Cancelled;
                    state.finished_at_ms = Some(now_ms());
                    state.error = Some(terminal_error(
                        "query.cancelled",
                        "query was cancelled",
                        &record.id,
                    ));
                    state.result_available = false;
                    state.result_summary = None;
                }
                JobFailure::Timeout => {
                    let mut state = record.state.write();
                    state.phase = QueryState::Failed;
                    state.finished_at_ms = Some(now_ms());
                    state.error = Some(terminal_error(
                        "query.timeout",
                        "query exceeded its time limit",
                        &record.id,
                    ));
                    state.result_available = false;
                    state.result_summary = None;
                }
                JobFailure::Engine(error) => fail_record(&record, error),
            }
            persist_terminal_async(&journal, &record).await;
            log_terminal(&record);
            true
        }
    }
}

async fn execute_and_store(
    engine: &Engine,
    store: &ResultStore,
    record: &QueryRecord,
    request: &QueryRequest,
    timeout: Duration,
    memory_limit: usize,
) -> std::result::Result<(StoredResult, HttpQueryMetrics), JobFailure> {
    let deadline = tokio::time::Instant::now() + timeout;
    let session = engine.session();
    let parameters = request.parameter_values().map_err(JobFailure::Engine)?;
    let execute = async {
        if parameters.is_empty() {
            session
                .execute_http_read_only_with_memory_limit(&request.sql, memory_limit)
                .await
        } else {
            HttpReadOnlyPolicy::validate(&request.sql)?;
            session
                .prepare(&request.sql)?
                .execute_http_read_only_with_memory_limit(&parameters, memory_limit)
                .await
        }
    };
    tokio::pin!(execute);
    let mut result = tokio::select! {
        _ = record.cancel.cancelled() => return Err(JobFailure::Cancelled),
        _ = tokio::time::sleep_until(deadline) => return Err(JobFailure::Timeout),
        result = &mut execute => result.map_err(JobFailure::Engine)?,
    };
    let _live_metrics = record.register_live_metrics(result.metrics());
    let cancellation = result.cancellation_handle();
    let writer = store
        .writer_async(&record.id, result.schema())
        .await
        .map_err(JobFailure::Engine)?
        .preserve_on_drop_when(record.interrupted_flag());
    let outcome = loop {
        let next = tokio::select! {
            _ = record.cancel.cancelled() => {
                cancellation.cancel();
                break Err(JobFailure::Cancelled);
            }
            _ = tokio::time::sleep_until(deadline) => {
                cancellation.cancel();
                break Err(JobFailure::Timeout);
            }
            next = result.stream().next() => next,
        };
        let Some(batch) = next else {
            break Ok(());
        };
        let batch = match batch {
            Ok(batch) => batch,
            Err(error) => break Err(JobFailure::Engine(error)),
        };
        let write = writer.write(batch);
        tokio::pin!(write);
        let write_result = tokio::select! {
            _ = record.cancel.cancelled() => {
                cancellation.cancel();
                break Err(JobFailure::Cancelled);
            }
            _ = tokio::time::sleep_until(deadline) => {
                cancellation.cancel();
                break Err(JobFailure::Timeout);
            }
            result = &mut write => result,
        };
        if let Err(error) = write_result {
            break Err(JobFailure::Engine(error));
        }
    };
    let cleanup_error = if outcome.is_err() {
        result.cancel_and_quiesce().await.err()
    } else {
        None
    };
    let metrics = metrics(result.metrics().snapshot());
    drop(result);
    match outcome {
        Ok(()) => {
            // All engine output has been consumed. Finalizing the tiny durable
            // result manifest is a commit boundary and must not be abandoned
            // by a cancellation race, which would otherwise delete batches
            // that were already acknowledged as committed.
            writer
                .finish()
                .await
                .map(|result| (result, metrics))
                .map_err(JobFailure::Engine)
        }
        Err(error) => {
            if record.interruption_requested() {
                record.interrupt();
                match writer
                    .interrupt("query interrupted during bounded server shutdown")
                    .await
                {
                    Ok(result) => {
                        record.publish_interrupted_result(result);
                    }
                    Err(cleanup) => tracing::error!(
                        %cleanup,
                        query_id = %record.id,
                        "failed to preserve interrupted HTTP result"
                    ),
                }
            } else if let Err(cleanup) = writer.abort().await {
                tracing::error!(%cleanup, query_id = %record.id, "failed to clean partial HTTP result");
            }
            match cleanup_error {
                Some(cleanup) => Err(JobFailure::Engine(Error::Execution(format!(
                    "{}; additionally failed to quiesce query resources: {cleanup}",
                    failure_message(&error)
                )))),
                None => Err(error),
            }
        }
    }
}

enum JobFailure {
    Cancelled,
    Timeout,
    Engine(Error),
}

fn failure_message(failure: &JobFailure) -> String {
    match failure {
        JobFailure::Cancelled => "query was cancelled".to_owned(),
        JobFailure::Timeout => "query exceeded its time limit".to_owned(),
        JobFailure::Engine(error) => error.to_string(),
    }
}
