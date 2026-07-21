use std::{collections::HashSet, sync::Arc};

use tokio::time::Instant;

use super::{
    ManagerInner, QueryRecord,
    journal::{PersistedQuery, terminal_state},
    lifecycle::{cancel_for_shutdown, interrupt_for_shutdown},
};
use crate::{Error, Result, http_shell::QueryState};

const INTERRUPT_FRACTION: f64 = 0.75;
const ABORT_FRACTION: f64 = 0.825;
const IO_BARRIER_FRACTION: f64 = 0.925;

pub(super) async fn execute(inner: &Arc<ManagerInner>, deadline: Instant) -> Result<()> {
    let started = Instant::now();
    let grace = deadline.saturating_duration_since(started);
    let interrupt_at = started + grace.mul_f64(INTERRUPT_FRACTION);
    let abort_at = started + grace.mul_f64(ABORT_FRACTION);
    let io_barrier_at = started + grace.mul_f64(IO_BARRIER_FRACTION);
    let records = inner.records.read().values().cloned().collect::<Vec<_>>();
    let mut failures = Vec::new();

    let queued = collect_queued(&records, interrupt_at, &mut failures).await;
    let (idle, mut queued_failures) = tokio::join!(
        inner.tasks.wait_idle_until(interrupt_at),
        persist_all(inner, queued, interrupt_at),
    );
    failures.append(&mut queued_failures);
    if idle {
        return shutdown_result(failures, None);
    }

    for record in &records {
        if matches!(
            record.state.read().phase,
            QueryState::Queued | QueryState::Running
        ) {
            record.signal_interrupt();
        }
    }
    if !inner.tasks.wait_idle_until(abort_at).await {
        inner.tasks.abort_all();
    }
    if !inner.tasks.wait_idle_until(io_barrier_at).await {
        return shutdown_result(failures, Some(inner.tasks.active()));
    }
    if !inner.service_io.wait_idle_until(io_barrier_at).await {
        failures
            .push("service I/O did not become idle before interrupted result sealing".to_owned());
        return shutdown_result(failures, None);
    }

    let durable_terminal = inner
        .journal
        .load()
        .into_iter()
        .filter(|query| terminal_state(query.state))
        .map(|query| query.query_id)
        .collect::<HashSet<_>>();
    let interrupted =
        collect_interrupted(&records, &durable_terminal, deadline, &mut failures).await;
    failures.append(&mut seal_and_persist(inner, interrupted, deadline).await);
    shutdown_result(failures, None)
}

async fn collect_queued(
    records: &[Arc<QueryRecord>],
    deadline: Instant,
    failures: &mut Vec<String>,
) -> Vec<PersistedQuery> {
    let mut persisted = Vec::new();
    for record in records {
        if record.state.read().phase != QueryState::Queued {
            continue;
        }
        match tokio::time::timeout_at(deadline, cancel_for_shutdown(record)).await {
            Ok(Some(query)) => persisted.push(query),
            Ok(None) => {}
            Err(_) => failures.push(format!("{}: queued cancellation timed out", record.id)),
        }
    }
    persisted
}

async fn collect_interrupted(
    records: &[Arc<QueryRecord>],
    durable_terminal: &HashSet<String>,
    deadline: Instant,
    failures: &mut Vec<String>,
) -> Vec<PersistedQuery> {
    let mut persisted = Vec::new();
    for record in records {
        if durable_terminal.contains(&record.id) {
            continue;
        }
        match tokio::time::timeout_at(deadline, interrupt_for_shutdown(record)).await {
            Ok(Some(query)) => persisted.push(query),
            Ok(None) => {}
            Err(_) => failures.push(format!("{}: interruption timed out", record.id)),
        }
    }
    persisted
}

async fn persist_all(
    inner: &ManagerInner,
    queries: Vec<PersistedQuery>,
    deadline: Instant,
) -> Vec<String> {
    let mut failures = Vec::new();
    for query in queries {
        let query_id = query.query_id.clone();
        match tokio::time::timeout_at(deadline, inner.journal.upsert_async(query)).await {
            Ok(Ok(_)) => {}
            Ok(Err(error)) => failures.push(format!("{query_id}: {error}")),
            Err(_) => failures.push(format!("{query_id}: persistence timed out")),
        }
    }
    failures
}

async fn seal_and_persist(
    inner: &ManagerInner,
    queries: Vec<PersistedQuery>,
    deadline: Instant,
) -> Vec<String> {
    let mut failures = Vec::new();
    for query in queries {
        let query_id = query.query_id.clone();
        let seal = inner.store.seal_interrupted_prefix(
            &query_id,
            "query interrupted during bounded server shutdown",
        );
        match tokio::time::timeout_at(deadline, seal).await {
            Ok(Ok(_)) => {}
            Ok(Err(error)) => failures.push(format!("{query_id}: result seal failed: {error}")),
            Err(_) => failures.push(format!("{query_id}: result seal timed out")),
        }
        match tokio::time::timeout_at(deadline, inner.journal.upsert_async(query)).await {
            Ok(Ok(_)) => {}
            Ok(Err(error)) => failures.push(format!("{query_id}: {error}")),
            Err(_) => failures.push(format!("{query_id}: persistence timed out")),
        }
    }
    failures
}

fn shutdown_result(failures: Vec<String>, active: Option<usize>) -> Result<()> {
    if active.is_none() && failures.is_empty() {
        return Ok(());
    }
    let active = active
        .map(|count| format!("shutdown deadline elapsed with {count} active tasks"))
        .unwrap_or_default();
    let persistence = if failures.is_empty() {
        String::new()
    } else {
        format!("shutdown persistence failures: {}", failures.join("; "))
    };
    let separator = if !active.is_empty() && !persistence.is_empty() {
        "; "
    } else {
        ""
    };
    Err(Error::Execution(format!(
        "{active}{separator}{persistence}"
    )))
}
