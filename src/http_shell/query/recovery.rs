use std::{collections::HashMap, sync::Arc};

use crate::RetryClass;

use super::{
    IdempotencyRecord, QueryRecord,
    journal::{PersistedQuery, QueryJournal},
    request::{now_ms, terminal_error},
};
use crate::http_shell::{
    QueryState,
    result_store::{RecoveredResult, ResultStore, StoredResultState},
};

pub(super) fn recover_records(
    store: &ResultStore,
    journal: &QueryJournal,
) -> (
    HashMap<String, Arc<QueryRecord>>,
    HashMap<String, IdempotencyRecord>,
) {
    let persisted = journal.load();
    let mut digest_counts = HashMap::new();
    for query in &persisted {
        *digest_counts
            .entry(query.scoped_idempotency_digest.clone())
            .or_insert(0_usize) += 1;
    }
    let mut recovered = HashMap::new();
    for result in store.take_recovered() {
        let id = result.query_id().to_owned();
        if recovered.insert(id.clone(), result).is_some() {
            tracing::error!(query_id = %id, "duplicate recovered HTTP result was quarantined");
        }
    }

    let mut records = HashMap::new();
    let mut idempotency = HashMap::new();
    for mut query in persisted {
        let stored = recovered.remove(&query.query_id);
        let duplicate_digest = digest_counts
            .get(&query.scoped_idempotency_digest)
            .copied()
            .unwrap_or(0)
            > 1;
        let mut changed = false;
        let result = if duplicate_digest {
            delete_recovered_result(store, stored.as_ref());
            fail_persisted(
                &mut query,
                "query.state_conflict",
                "query idempotency state was inconsistent",
                RetryClass::Never,
            );
            changed = true;
            None
        } else if query.state == QueryState::Succeeded && query.result_available {
            match stored.as_ref().map(RecoveredResult::state) {
                Some(StoredResultState::Completed) => {
                    stored.as_ref().and_then(RecoveredResult::result)
                }
                Some(StoredResultState::Invalidated) => {
                    fail_persisted(
                        &mut query,
                        "query.result_invalidated",
                        "query result was invalidated by a server upgrade",
                        RetryClass::Never,
                    );
                    changed = true;
                    None
                }
                _ => {
                    fail_persisted(
                        &mut query,
                        "query.result_unavailable",
                        "completed query result is unavailable",
                        RetryClass::Never,
                    );
                    changed = true;
                    None
                }
            }
        } else {
            if stored
                .as_ref()
                .is_some_and(|value| value.state() == StoredResultState::Completed)
            {
                delete_recovered_result(store, stored.as_ref());
            }
            if query.state == QueryState::Succeeded {
                fail_persisted(
                    &mut query,
                    "query.result_unavailable",
                    "completed query result is unavailable",
                    RetryClass::Never,
                );
                changed = true;
            }
            None
        };
        if changed && let Err(error) = journal.upsert(query.clone()) {
            tracing::error!(%error, query_id = %query.query_id, "failed to persist reconciled HTTP query state");
        }
        if !duplicate_digest {
            idempotency.insert(
                query.scoped_idempotency_digest.clone(),
                IdempotencyRecord {
                    request_hash: query.request_hash.clone(),
                    query_id: query.query_id.clone(),
                },
            );
        }
        let record = Arc::new(QueryRecord::from_persisted(query, result));
        records.insert(record.id.clone(), record);
    }
    for (_, result) in recovered {
        delete_recovered_result(store, Some(&result));
        tracing::warn!(
            query_id = result.query_id(),
            state = ?result.state(),
            error = result.error().unwrap_or("orphaned result"),
            "orphaned HTTP result was quarantined during recovery"
        );
    }
    (records, idempotency)
}

fn delete_recovered_result(store: &ResultStore, result: Option<&RecoveredResult>) {
    if let Some(result) = result
        && let Err(error) = store.delete_query_artifacts(result.query_id())
    {
        tracing::error!(%error, query_id = result.query_id(), "failed to delete quarantined HTTP result");
    }
}

fn fail_persisted(query: &mut PersistedQuery, code: &str, message: &str, retry: RetryClass) {
    let mut error = terminal_error(code, message, &query.query_id);
    error.retry = retry;
    query.state = QueryState::Failed;
    query.finished_at_ms = Some(now_ms());
    query.error = Some(error);
    query.result_available = false;
}
