use std::sync::Arc;

use crate::{Error, Result};

use super::{
    ManagerInner, QueryRecord,
    journal::{DeleteReason, PersistedQuery, QueryJournal},
    request::now_ms,
};
use crate::http_shell::{QueryState, error::HttpError};

pub(super) fn delete_terminal_record(
    inner: &ManagerInner,
    query_id: &str,
    record: &Arc<QueryRecord>,
    reason: DeleteReason,
) -> Result<()> {
    if !record.begin_delete() {
        return Err(Error::Execution(
            "HTTP query deletion is already active".into(),
        ));
    }
    let removed = {
        let mut records = inner.records.write();
        if records
            .get(query_id)
            .is_some_and(|current| Arc::ptr_eq(current, record))
        {
            records.remove(query_id);
            true
        } else {
            false
        }
    };
    if !removed {
        record.restore_access();
        return Ok(());
    }

    if let Err(error) = inner.store.delete_query_artifacts(query_id) {
        restore_record(inner, record);
        return Err(error);
    }
    {
        let mut state = record.state.write();
        state.result.take();
        state.result_available = false;
        state.result_summary = None;
    }

    match inner.journal.delete(query_id, reason) {
        Ok(true) => {
            inner
                .idempotency
                .lock()
                .retain(|_, value| value.query_id != query_id);
            Ok(())
        }
        Ok(false) => {
            record.fail_stably(
                "query.state_conflict",
                "query state was inconsistent during deletion",
                crate::RetryClass::Never,
            );
            restore_record(inner, record);
            persist_or_log(&inner.journal, record, "delete-state conflict");
            Err(Error::Execution(
                "query journal did not contain the deleted query".into(),
            ))
        }
        Err(error) => {
            record.fail_stably(
                "query.delete_failed",
                "query deletion could not be persisted",
                crate::RetryClass::Safe,
            );
            restore_record(inner, record);
            persist_or_log(&inner.journal, record, "delete failure");
            Err(error)
        }
    }
}

fn restore_record(inner: &ManagerInner, record: &Arc<QueryRecord>) {
    record.restore_access();
    inner
        .records
        .write()
        .insert(record.id.clone(), Arc::clone(record));
}

pub(super) fn fail_record(record: &QueryRecord, error: Error) {
    let mapped = HttpError::from_engine(&error, String::new());
    let mut body = *mapped.body;
    body.request_id = None;
    body.query_id = Some(record.id.clone());
    let mut state = record.state.write();
    state.phase = if matches!(error, Error::Cancelled) {
        QueryState::Cancelled
    } else {
        QueryState::Failed
    };
    state.finished_at_ms = Some(now_ms());
    state.error = Some(body);
    state.result_available = false;
    state.result_summary = None;
    tracing::warn!(
        query_id = %record.id,
        principal_id = record.owner.audit_id(),
        error_code = %state.error.as_ref().map_or("server.internal", |error| error.error.as_str()),
        "HTTP query failed"
    );
}

#[cfg(test)]
pub(super) fn cancel_and_persist(record: &QueryRecord, journal: &QueryJournal) -> Result<()> {
    let _transition = record.blocking_lock_transition();
    if record.terminal() {
        return Ok(());
    }
    record.cancel();
    journal.upsert(record.persisted()).map(|_| ())
}

pub(super) async fn cancel_and_persist_async(
    record: &QueryRecord,
    journal: &QueryJournal,
) -> Result<()> {
    let persisted = {
        let _transition = record.lock_transition().await;
        if record.terminal() {
            return Ok(());
        }
        record.cancel();
        record.persisted()
    };
    journal.upsert_async(persisted).await.map(|_| ())
}

pub(super) async fn cancel_for_shutdown(record: &QueryRecord) -> Option<PersistedQuery> {
    let _transition = record.lock_transition().await;
    if record.state.read().phase != QueryState::Queued {
        return None;
    }
    record.cancel();
    Some(record.persisted())
}

pub(super) async fn interrupt_for_shutdown(record: &QueryRecord) -> Option<PersistedQuery> {
    let _transition = record.lock_transition().await;
    record.interrupt();
    let interrupted = record.state.read().phase == QueryState::Interrupted;
    interrupted.then(|| record.persisted())
}

pub(super) fn persist_or_log(journal: &QueryJournal, record: &QueryRecord, context: &str) {
    if let Err(error) = journal.upsert(record.persisted()) {
        tracing::error!(%error, query_id = %record.id, context, "failed to persist HTTP query state");
    }
}

pub(super) async fn persist_or_log_async(
    journal: &QueryJournal,
    record: &QueryRecord,
    context: &str,
) {
    if let Err(error) = journal.upsert_async(record.persisted()).await {
        tracing::error!(%error, query_id = %record.id, context, "failed to persist HTTP query state");
    }
}

pub(super) async fn persist_terminal_async(journal: &QueryJournal, record: &QueryRecord) {
    if let Err(error) = journal.upsert_async(record.persisted()).await {
        tracing::error!(%error, query_id = %record.id, "failed to persist terminal HTTP query state");
        discard_record_result_async(record).await;
        record.fail_stably(
            "query.journal_failed",
            "query terminal state could not be persisted",
            crate::RetryClass::Unknown,
        );
        persist_or_log_async(journal, record, "journal-failure terminal state").await;
    }
}

async fn discard_record_result_async(record: &QueryRecord) {
    let result = {
        let mut state = record.state.write();
        state.result_available = false;
        state.result_summary = None;
        state.result.take()
    };
    if let Some(result) = result
        && let Err(error) = result.delete_async().await
    {
        tracing::error!(%error, query_id = %record.id, "failed to discard inaccessible HTTP result");
    }
}

pub(super) fn log_terminal(record: &QueryRecord) {
    let status = record.status();
    let metrics = status.metrics.unwrap_or_default();
    let duration_ms = status
        .started_at_ms
        .zip(status.finished_at_ms)
        .map(|(started, finished)| finished.saturating_sub(started))
        .unwrap_or(0);
    tracing::info!(
        query_id = %status.query_id,
        principal_id = record.owner.audit_id(),
        state = ?status.state,
        duration_ms,
        rows_returned = metrics.rows_returned,
        rows_scanned = metrics.rows_scanned,
        bytes_scanned = metrics.bytes_scanned,
        peak_memory_bytes = metrics.peak_memory_bytes,
        spill_read_bytes = metrics.spill_read_bytes,
        spill_write_bytes = metrics.spill_write_bytes,
        "HTTP query finished"
    );
}
