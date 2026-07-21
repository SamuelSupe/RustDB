use std::{cmp::Ordering, collections::HashMap, sync::Arc, time::Duration};

use axum::http::StatusCode;
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use serde::{Deserialize, Serialize};

use super::QueryRecord;
use crate::http_shell::{
    error::HttpError,
    security::AuthenticatedActor,
    types::{QueryListRequest, QueryListResponse},
};

const DEFAULT_LIMIT: usize = 50;
const MAX_LIMIT: usize = 200;
const MAX_CURSOR_BYTES: usize = 256;

#[derive(Deserialize, Serialize)]
struct Cursor {
    created_at_ms: u64,
    query_id: String,
}

pub(super) fn execute(
    records: &HashMap<String, Arc<QueryRecord>>,
    actor: &AuthenticatedActor,
    request: QueryListRequest,
    ttl: Duration,
    request_id: &str,
) -> Result<QueryListResponse, HttpError> {
    let limit = request.limit.unwrap_or(DEFAULT_LIMIT);
    if limit == 0 || limit > MAX_LIMIT {
        return Err(invalid_request(
            request_id,
            format!("query list limit must be between 1 and {MAX_LIMIT}"),
        ));
    }
    let cursor = request
        .cursor
        .as_deref()
        .map(|value| decode_cursor(value, request_id))
        .transpose()?;
    let mut visible = records
        .values()
        .filter(|record| actor.can_access_query(record.owner()) && !record.deleting())
        .filter(|record| {
            request
                .state
                .is_none_or(|state| record.state.read().phase == state)
        })
        .filter(|record| {
            request
                .created_after_ms
                .is_none_or(|created| record.created_at_ms > created)
        })
        .filter(|record| {
            cursor
                .as_ref()
                .is_none_or(|cursor| after_cursor(record, cursor))
        })
        .cloned()
        .collect::<Vec<_>>();
    visible.sort_by(|left, right| compare_records(left, right));
    let has_more = visible.len() > limit;
    visible.truncate(limit);
    let next_cursor = has_more
        .then(|| visible.last())
        .flatten()
        .map(|record| encode_cursor(record, request_id))
        .transpose()?;
    Ok(QueryListResponse {
        queries: visible
            .into_iter()
            .map(|record| record.status_with_ttl(ttl))
            .collect(),
        next_cursor,
    })
}

fn compare_records(left: &QueryRecord, right: &QueryRecord) -> Ordering {
    right
        .created_at_ms
        .cmp(&left.created_at_ms)
        .then_with(|| right.id.cmp(&left.id))
}

fn after_cursor(record: &QueryRecord, cursor: &Cursor) -> bool {
    record.created_at_ms < cursor.created_at_ms
        || (record.created_at_ms == cursor.created_at_ms && record.id < cursor.query_id)
}

fn encode_cursor(record: &QueryRecord, request_id: &str) -> Result<String, HttpError> {
    let bytes = serde_json::to_vec(&Cursor {
        created_at_ms: record.created_at_ms,
        query_id: record.id.clone(),
    })
    .map_err(|error| {
        HttpError::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            "server.internal",
            format!("failed to encode query cursor: {error}"),
            request_id.to_owned(),
        )
    })?;
    Ok(URL_SAFE_NO_PAD.encode(bytes))
}

fn decode_cursor(value: &str, request_id: &str) -> Result<Cursor, HttpError> {
    if value.is_empty() || value.len() > MAX_CURSOR_BYTES {
        return Err(invalid_request(request_id, "query cursor is invalid"));
    }
    let bytes = URL_SAFE_NO_PAD
        .decode(value)
        .map_err(|_| invalid_request(request_id, "query cursor is invalid"))?;
    let cursor: Cursor = serde_json::from_slice(&bytes)
        .map_err(|_| invalid_request(request_id, "query cursor is invalid"))?;
    if cursor.query_id.is_empty() || cursor.query_id.len() > 128 {
        return Err(invalid_request(request_id, "query cursor is invalid"));
    }
    Ok(cursor)
}

fn invalid_request(request_id: &str, message: impl Into<String>) -> HttpError {
    HttpError::new(
        StatusCode::BAD_REQUEST,
        "request.invalid",
        message,
        request_id.to_owned(),
    )
}
