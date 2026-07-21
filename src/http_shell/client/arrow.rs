use std::io::Cursor;

use arrow::ipc::reader::FileReader;
use reqwest::{StatusCode, header};

use super::{RemoteClient, RemoteError, RemoteResult, success};
use crate::http_shell::arrow_transport::{
    ARROW_RESULT_MEDIA_TYPE, ArrowResultBatch, ArrowResultPoll, ArrowResultState, BATCH_SEQ_HEADER,
    NEXT_BATCH_SEQ_HEADER, RESULT_COMPLETE_HEADER, RESULT_STATE_HEADER,
};

impl RemoteClient {
    /// Pulls one committed Arrow IPC result batch by sequence number.
    pub async fn arrow_batch(
        &self,
        query_id: &str,
        batch_seq: u64,
    ) -> RemoteResult<ArrowResultPoll> {
        let mut url = self.url(&format!("queries/{query_id}/results"))?;
        url.query_pairs_mut()
            .append_pair("batch_seq", &batch_seq.to_string());
        let response = self
            .authenticated(self.client.get(url))
            .header(header::ACCEPT, ARROW_RESULT_MEDIA_TYPE)
            .send()
            .await
            .map_err(RemoteError::transport)?;
        let response = success(response).await?;
        let next_batch_seq = header_u64(response.headers(), NEXT_BATCH_SEQ_HEADER, query_id)?;
        let state = result_state(response.headers(), query_id)?;
        let complete = response
            .headers()
            .get(RESULT_COMPLETE_HEADER)
            .and_then(|value| value.to_str().ok())
            .is_some_and(|value| value.eq_ignore_ascii_case("true"));
        if response.status() == StatusCode::NO_CONTENT {
            if complete {
                return Err(RemoteError::protocol_for_query(
                    query_id,
                    "completed Arrow result omitted its schema",
                ));
            }
            return Ok(ArrowResultPoll::Pending {
                next_batch_seq,
                state,
            });
        }
        let returned_seq = header_u64(response.headers(), BATCH_SEQ_HEADER, query_id)?;
        if returned_seq != batch_seq
            || !matches!(
                next_batch_seq,
                value if value == batch_seq || value == batch_seq.saturating_add(1)
            )
        {
            return Err(RemoteError::protocol_for_query(
                query_id,
                "Arrow result returned a discontinuous batch sequence",
            ));
        }
        let bytes = response.bytes().await.map_err(RemoteError::transport)?;
        let mut reader = FileReader::try_new(Cursor::new(bytes), None)
            .map_err(|error| RemoteError::protocol_for_query(query_id, error.to_string()))?;
        let schema = reader.schema();
        let Some(batch) = reader
            .next()
            .transpose()
            .map_err(|error| RemoteError::protocol_for_query(query_id, error.to_string()))?
        else {
            if complete && next_batch_seq == batch_seq {
                return Ok(ArrowResultPoll::Complete {
                    next_batch_seq,
                    schema,
                    state,
                });
            }
            return Err(RemoteError::protocol_for_query(
                query_id,
                "pending Arrow result contained no batch",
            ));
        };
        if next_batch_seq != batch_seq.saturating_add(1) {
            return Err(RemoteError::protocol_for_query(
                query_id,
                "Arrow batch did not advance its sequence",
            ));
        }
        if reader.next().is_some() {
            return Err(RemoteError::protocol_for_query(
                query_id,
                "Arrow result contained more than one batch",
            ));
        }
        Ok(ArrowResultPoll::Batch(ArrowResultBatch {
            batch_seq: returned_seq,
            next_batch_seq,
            result_complete: complete,
            state,
            batch,
        }))
    }
}

fn result_state(headers: &header::HeaderMap, query_id: &str) -> RemoteResult<ArrowResultState> {
    let value = headers
        .get(RESULT_STATE_HEADER)
        .and_then(|value| value.to_str().ok())
        .ok_or_else(|| {
            RemoteError::protocol_for_query(
                query_id,
                format!("response omitted valid {RESULT_STATE_HEADER}"),
            )
        })?;
    match value {
        "queued" => Ok(ArrowResultState::Queued),
        "running" => Ok(ArrowResultState::Running),
        "completed" | "succeeded" => Ok(ArrowResultState::Completed),
        "interrupted" => Ok(ArrowResultState::Interrupted),
        "failed" => Ok(ArrowResultState::Failed),
        "cancelled" => Ok(ArrowResultState::Cancelled),
        "invalidated" => Ok(ArrowResultState::Invalidated),
        other => Err(RemoteError::protocol_for_query(
            query_id,
            format!("response contained unknown {RESULT_STATE_HEADER} value '{other}'"),
        )),
    }
}

fn header_u64(headers: &header::HeaderMap, name: &str, query_id: &str) -> RemoteResult<u64> {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse().ok())
        .ok_or_else(|| {
            RemoteError::protocol_for_query(query_id, format!("response omitted valid {name}"))
        })
}

#[cfg(test)]
mod tests {
    use reqwest::header::{HeaderMap, HeaderValue};

    use super::*;

    #[test]
    fn interrupted_result_state_is_preserved() {
        let mut headers = HeaderMap::new();
        headers.insert(RESULT_STATE_HEADER, HeaderValue::from_static("interrupted"));
        assert_eq!(
            result_state(&headers, "query-1").unwrap(),
            ArrowResultState::Interrupted
        );
    }
}
