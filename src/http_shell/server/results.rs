use std::sync::Arc;

use arrow::ipc::writer::FileWriter;
use axum::{
    Json,
    body::Body,
    extract::{Extension, Path, Query, State, rejection::QueryRejection},
    http::{HeaderMap, HeaderValue, StatusCode, header},
    response::{IntoResponse, Response},
};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use serde::Deserialize;
use serde_json::{Value, json};

use crate::{Error, Result};

use super::{ServerState, with_request_id};
use crate::http_shell::{
    arrow_transport::{
        ARROW_RESULT_MEDIA_TYPE, BATCH_SEQ_HEADER, NEXT_BATCH_SEQ_HEADER, RESULT_COMPLETE_HEADER,
        RESULT_STATE_HEADER,
    },
    error::HttpError,
    json::{batch_rows, schema_columns},
    rejection,
    request_context::RequestContext,
    result_store::StoredResultState,
    types::{JsonResultPage, PageMetadata},
};

const DEFAULT_PAGE_ROWS: usize = 1_000;
const MAX_PAGE_ROWS: usize = 10_000;
const MAX_PAGE_BYTES: usize = 16 * 1024 * 1024;

#[derive(Deserialize)]
pub(super) struct ResultQuery {
    cursor: Option<String>,
    offset: Option<u64>,
    limit: Option<usize>,
    batch_seq: Option<u64>,
}

pub(super) async fn query_results(
    State(state): State<ServerState>,
    Extension(context): Extension<RequestContext>,
    Path(query_id): Path<String>,
    query: std::result::Result<Query<ResultQuery>, QueryRejection>,
    headers: HeaderMap,
) -> std::result::Result<Response, HttpError> {
    let request_id = context.request_id().to_owned();
    let Query(query) = query.map_err(|error| rejection::query(error, &request_id))?;
    if accepts_arrow(&headers) {
        if query.cursor.is_some()
            || query.offset.is_some()
            || query.limit.is_some()
            || query.batch_seq.is_none()
        {
            return Err(invalid_request(
                "Arrow results require only the batch_seq query parameter",
                &request_id,
                &query_id,
            ));
        }
        return arrow_result(
            &state,
            context.actor(),
            &query_id,
            query.batch_seq.unwrap_or(0),
            &request_id,
        )
        .await;
    }
    if query.batch_seq.is_some() {
        return Err(invalid_request(
            "batch_seq requires Accept: application/vnd.apache.arrow.file",
            &request_id,
            &query_id,
        ));
    }
    if query.cursor.is_some() && query.offset.is_some() {
        return Err(HttpError::new(
            StatusCode::BAD_REQUEST,
            "request.invalid",
            "cursor and offset are mutually exclusive",
            request_id,
        )
        .query(query_id));
    }
    let offset = match query.cursor.as_deref() {
        Some(cursor) => decode_cursor(cursor).map_err(|error| {
            HttpError::from_engine(&error, request_id.clone()).query(query_id.clone())
        })?,
        None => query.offset.unwrap_or(0),
    };
    let limit = query.limit.unwrap_or(DEFAULT_PAGE_ROWS);
    if !(1..=MAX_PAGE_ROWS).contains(&limit) {
        return Err(HttpError::new(
            StatusCode::BAD_REQUEST,
            "request.invalid",
            "limit must be between 1 and 10000",
            request_id,
        )
        .query(query_id));
    }
    let _result_read = Arc::clone(&state.result_read_slots)
        .acquire_owned()
        .await
        .map_err(|_| {
            HttpError::new(
                StatusCode::SERVICE_UNAVAILABLE,
                "server.shutting_down",
                "the result reader is shutting down",
                request_id.clone(),
            )
            .query(query_id.clone())
        })?;
    let read_guard = state.result_reads.start().ok_or_else(|| {
        HttpError::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "server.shutting_down",
            "the result reader is shutting down",
            request_id.clone(),
        )
        .query(query_id.clone())
    })?;
    let result = state
        .queries
        .completed_result(context.actor(), &query_id, &request_id)?;
    let batches = result
        .read(offset, limit, read_guard)
        .await
        .map_err(|error| {
            HttpError::from_engine(&error, request_id.clone()).query(query_id.clone())
        })?;
    let page = build_page(result.schema(), result.rows(), offset, batches).map_err(|error| {
        HttpError::from_engine(&error, request_id.clone()).query(query_id.clone())
    })?;
    let ndjson = accepts_ndjson(&headers);
    let response = if ndjson {
        ndjson_response(&page).map_err(|error| {
            HttpError::from_engine(&error, request_id.clone()).query(query_id.clone())
        })?
    } else {
        Json(page).into_response()
    };
    Ok(with_request_id(response, &request_id))
}

async fn arrow_result(
    state: &ServerState,
    actor: &crate::http_shell::security::AuthenticatedActor,
    query_id: &str,
    batch_seq: u64,
    request_id: &str,
) -> std::result::Result<Response, HttpError> {
    let _result_read = Arc::clone(&state.result_read_slots)
        .acquire_owned()
        .await
        .map_err(|_| shutting_down(query_id, request_id))?;
    let read_guard = state
        .result_reads
        .start()
        .ok_or_else(|| shutting_down(query_id, request_id))?;
    let status = state.queries.status(actor, query_id, request_id)?;
    let Some(snapshot) = state.queries.result_snapshot(actor, query_id, request_id)? else {
        if status.state == crate::http_shell::QueryState::Succeeded {
            return Err(HttpError::new(
                StatusCode::INTERNAL_SERVER_ERROR,
                "query.result_unavailable",
                "completed query result is unavailable",
                request_id.to_owned(),
            )
            .query(query_id));
        }
        if batch_seq != 0 {
            return Err(invalid_request(
                "result batch sequence is beyond the committed result",
                request_id,
                query_id,
            ));
        }
        drop(read_guard);
        return Ok(arrow_pending(batch_seq, 0, status.state, request_id));
    };
    let chunks = snapshot
        .chunks_from(batch_seq)
        .map_err(|error| HttpError::from_engine(&error, request_id.to_owned()).query(query_id))?;
    if let Some(chunk) = chunks.first() {
        let bytes = snapshot
            .read_chunk_bytes(chunk.seq, read_guard)
            .await
            .map_err(|error| {
                HttpError::from_engine(&error, request_id.to_owned()).query(query_id)
            })?;
        let next = chunk.seq.saturating_add(1);
        let complete =
            snapshot.state() == StoredResultState::Completed && next == snapshot.next_batch_seq();
        state.metrics.result_bytes(bytes.len() as u64);
        return Ok(arrow_bytes(
            bytes,
            chunk.seq,
            next,
            chunk.rows,
            snapshot.state(),
            complete,
            request_id,
        ));
    }
    match snapshot.state() {
        StoredResultState::Running => {
            drop(read_guard);
            Ok(arrow_pending(
                batch_seq,
                snapshot.next_batch_seq(),
                status.state,
                request_id,
            ))
        }
        StoredResultState::Completed => {
            let bytes = empty_arrow(snapshot.schema()).map_err(|error| {
                HttpError::from_engine(&error, request_id.to_owned()).query(query_id)
            })?;
            drop(read_guard);
            state.metrics.result_bytes(bytes.len() as u64);
            Ok(arrow_bytes(
                bytes,
                batch_seq,
                batch_seq,
                0,
                StoredResultState::Completed,
                true,
                request_id,
            ))
        }
        StoredResultState::Failed | StoredResultState::Invalidated => Err(HttpError::new(
            StatusCode::CONFLICT,
            "query.no_result",
            snapshot
                .error()
                .unwrap_or("the query did not produce a result"),
            request_id.to_owned(),
        )
        .query(query_id)),
    }
}

fn arrow_pending(
    requested: u64,
    next: u64,
    state: crate::http_shell::QueryState,
    request_id: &str,
) -> Response {
    let mut response = StatusCode::NO_CONTENT.into_response();
    insert_u64(response.headers_mut(), BATCH_SEQ_HEADER, requested);
    insert_u64(response.headers_mut(), NEXT_BATCH_SEQ_HEADER, next);
    insert_static(
        response.headers_mut(),
        RESULT_STATE_HEADER,
        query_state_name(state),
    );
    insert_static(response.headers_mut(), RESULT_COMPLETE_HEADER, "false");
    response
        .headers_mut()
        .insert(header::RETRY_AFTER, HeaderValue::from_static("1"));
    with_request_id(response, request_id)
}

fn arrow_bytes(
    bytes: Vec<u8>,
    seq: u64,
    next: u64,
    rows: u64,
    state: StoredResultState,
    complete: bool,
    request_id: &str,
) -> Response {
    let mut response = Response::new(Body::from(bytes));
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static(ARROW_RESULT_MEDIA_TYPE),
    );
    insert_u64(response.headers_mut(), BATCH_SEQ_HEADER, seq);
    insert_u64(response.headers_mut(), NEXT_BATCH_SEQ_HEADER, next);
    insert_u64(response.headers_mut(), "x-rustdb-batch-rows", rows);
    insert_static(
        response.headers_mut(),
        RESULT_STATE_HEADER,
        stored_state_name(state),
    );
    insert_static(
        response.headers_mut(),
        RESULT_COMPLETE_HEADER,
        if complete { "true" } else { "false" },
    );
    with_request_id(response, request_id)
}

fn empty_arrow(schema: arrow::datatypes::SchemaRef) -> Result<Vec<u8>> {
    let mut output = Vec::new();
    FileWriter::try_new(&mut output, &schema)?.finish()?;
    Ok(output)
}

fn accepts_arrow(headers: &HeaderMap) -> bool {
    accepts_media(headers, ARROW_RESULT_MEDIA_TYPE)
}

fn accepts_media(headers: &HeaderMap, expected: &str) -> bool {
    headers
        .get(header::ACCEPT)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| {
            value.split(',').any(|range| {
                let mut parts = range.split(';');
                parts
                    .next()
                    .is_some_and(|media| media.trim().eq_ignore_ascii_case(expected))
                    && !parts.any(|parameter| {
                        parameter
                            .trim()
                            .strip_prefix("q=")
                            .and_then(|value| value.parse::<f32>().ok())
                            .is_some_and(|quality| quality == 0.0)
                    })
            })
        })
}

fn insert_u64(headers: &mut HeaderMap, name: &'static str, value: u64) {
    if let Ok(value) = HeaderValue::from_str(&value.to_string()) {
        headers.insert(name, value);
    }
}

fn insert_static(headers: &mut HeaderMap, name: &'static str, value: &'static str) {
    headers.insert(name, HeaderValue::from_static(value));
}

fn query_state_name(state: crate::http_shell::QueryState) -> &'static str {
    match state {
        crate::http_shell::QueryState::Queued => "queued",
        crate::http_shell::QueryState::Running => "running",
        crate::http_shell::QueryState::Succeeded => "succeeded",
        crate::http_shell::QueryState::Failed => "failed",
        crate::http_shell::QueryState::Cancelled => "cancelled",
    }
}

fn stored_state_name(state: StoredResultState) -> &'static str {
    match state {
        StoredResultState::Running => "running",
        StoredResultState::Completed => "succeeded",
        StoredResultState::Failed => "failed",
        StoredResultState::Invalidated => "invalidated",
    }
}

fn invalid_request(message: &str, request_id: &str, query_id: &str) -> HttpError {
    HttpError::new(
        StatusCode::BAD_REQUEST,
        "request.invalid",
        message,
        request_id.to_owned(),
    )
    .query(query_id)
}

fn shutting_down(query_id: &str, request_id: &str) -> HttpError {
    HttpError::new(
        StatusCode::SERVICE_UNAVAILABLE,
        "server.shutting_down",
        "the result reader is shutting down",
        request_id.to_owned(),
    )
    .query(query_id)
}

fn accepts_ndjson(headers: &HeaderMap) -> bool {
    accepts_media(headers, "application/x-ndjson")
}

fn build_page(
    schema: arrow::datatypes::SchemaRef,
    total_rows: u64,
    offset: u64,
    batches: Vec<arrow::record_batch::RecordBatch>,
) -> Result<JsonResultPage> {
    let mut rows = Vec::new();
    let mut encoded_bytes = 0usize;
    'batches: for batch in batches {
        for row_index in 0..batch.num_rows() {
            let row = batch_rows(&batch, row_index, 1)?
                .into_iter()
                .next()
                .ok_or_else(|| Error::Internal("result row conversion returned no row".into()))?;
            let row_bytes = serde_json::to_vec(&row)
                .map_err(|error| Error::Internal(format!("failed to encode result row: {error}")))?
                .len();
            if encoded_bytes.saturating_add(row_bytes) > MAX_PAGE_BYTES {
                if rows.is_empty() {
                    return Err(Error::ResourceExhausted(
                        "one result row exceeds the 16 MiB HTTP page limit".into(),
                    ));
                }
                break 'batches;
            }
            encoded_bytes = encoded_bytes.saturating_add(row_bytes);
            rows.push(row);
        }
    }
    let next_offset = offset.saturating_add(u64::try_from(rows.len()).unwrap_or(u64::MAX));
    let complete = next_offset >= total_rows;
    Ok(JsonResultPage {
        schema: schema_columns(&schema),
        rows,
        page: PageMetadata {
            offset,
            row_count: usize::try_from(next_offset.saturating_sub(offset)).unwrap_or(usize::MAX),
            complete,
            next_cursor: (!complete).then(|| encode_cursor(next_offset)),
        },
    })
}

fn ndjson_response(page: &JsonResultPage) -> Result<Response> {
    let mut output = String::new();
    append_json_line(
        &mut output,
        &json!({ "type": "schema", "columns": page.schema }),
    )?;
    for row in &page.rows {
        append_json_line(&mut output, &json!({ "type": "row", "values": row }))?;
    }
    append_json_line(&mut output, &json!({ "type": "page", "page": page.page }))?;
    let mut response = Response::new(Body::from(output));
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/x-ndjson"),
    );
    Ok(response)
}

fn append_json_line(output: &mut String, value: &Value) -> Result<()> {
    output
        .push_str(&serde_json::to_string(value).map_err(|error| {
            Error::Internal(format!("failed to encode NDJSON result: {error}"))
        })?);
    output.push('\n');
    Ok(())
}

fn encode_cursor(offset: u64) -> String {
    URL_SAFE_NO_PAD.encode(offset.to_be_bytes())
}

fn decode_cursor(cursor: &str) -> Result<u64> {
    if cursor.len() > 2_048 {
        return Err(Error::InvalidArgument("result cursor is too long".into()));
    }
    let bytes = URL_SAFE_NO_PAD
        .decode(cursor)
        .map_err(|_| Error::InvalidArgument("result cursor is invalid".into()))?;
    let bytes: [u8; 8] = bytes
        .try_into()
        .map_err(|_| Error::InvalidArgument("result cursor is invalid".into()))?;
    Ok(u64::from_be_bytes(bytes))
}

#[cfg(test)]
mod tests {
    use std::{io::Cursor, sync::Arc};

    use arrow::{
        datatypes::{DataType, Field, Schema},
        ipc::reader::FileReader,
    };

    use super::{decode_cursor, empty_arrow, encode_cursor};

    #[test]
    fn cursor_roundtrips_without_exposing_a_decimal_offset() {
        let cursor = encode_cursor(42);
        assert_eq!(decode_cursor(&cursor).unwrap(), 42);
        assert_ne!(cursor, "42");
    }

    #[test]
    fn empty_arrow_completion_preserves_schema_without_a_fake_batch() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            true,
        )]));
        let bytes = empty_arrow(Arc::clone(&schema)).unwrap();
        let mut reader = FileReader::try_new(Cursor::new(bytes), None).unwrap();
        assert_eq!(reader.schema(), schema);
        assert!(reader.next().is_none());
    }
}
