use std::{
    sync::Arc,
    time::{Duration, SystemTime},
};

use super::{
    ManagerInner, journal::DeleteReason, lifecycle::delete_terminal_record, request::now_ms,
};
use crate::http_shell::QueryState;

pub(super) fn spawn(inner: Arc<ManagerInner>) {
    let tasks = Arc::clone(&inner.tasks);
    tasks.spawn(async move {
        let interval = inner
            .store
            .ttl()
            .min(Duration::from_secs(60))
            .max(Duration::from_secs(1));
        loop {
            tokio::select! {
                _ = inner.shutdown.cancelled() => break,
                _ = tokio::time::sleep(interval) => expire_records(&inner),
            }
        }
    });
}

fn expire_records(inner: &ManagerInner) {
    let now = SystemTime::now();
    let ttl = inner.store.ttl();
    let expired = inner
        .records
        .read()
        .iter()
        .filter_map(|(id, record)| {
            let state = record.state.read();
            if !matches!(
                state.phase,
                QueryState::Succeeded
                    | QueryState::Failed
                    | QueryState::Cancelled
                    | QueryState::Interrupted
            ) {
                return None;
            }
            let result_expired = state
                .result
                .as_ref()
                .is_some_and(|result| result.expired(ttl, now));
            let terminal_expired = state.finished_at_ms.is_some_and(|finished| {
                now_ms().saturating_sub(finished)
                    >= u64::try_from(ttl.as_millis()).unwrap_or(u64::MAX)
            });
            (result_expired || terminal_expired).then(|| (id.clone(), Arc::clone(record)))
        })
        .collect::<Vec<_>>();
    for (id, record) in expired {
        if let Err(error) = delete_terminal_record(inner, &id, &record, DeleteReason::Ttl) {
            tracing::error!(%error, query_id = %id, "failed to expire HTTP query result");
        }
    }
}
