use std::time::{SystemTime, UNIX_EPOCH};

use axum::http::StatusCode;
use sha2::{Digest, Sha256};

use super::record::QueryRecord;
use crate::http_shell::{
    error::{ErrorBody, HttpError},
    types::{QueryRequest, SubmitResponse},
};
use crate::{Error, Result, RetryClass};

pub(super) fn validate_idempotency_key(
    key: &str,
    request_id: &str,
) -> std::result::Result<(), HttpError> {
    if !(16..=128).contains(&key.len()) || !key.bytes().all(|byte| byte.is_ascii_graphic()) {
        return Err(HttpError::new(
            StatusCode::BAD_REQUEST,
            "idempotency.invalid_key",
            "Idempotency-Key must contain 16-128 printable ASCII characters",
            request_id.to_owned(),
        ));
    }
    Ok(())
}

pub(super) fn request_hash(request: &QueryRequest) -> Result<String> {
    let bytes = serde_json::to_vec(request)
        .map_err(|error| Error::Internal(format!("failed to hash query request: {error}")))?;
    let digest = Sha256::digest(bytes);
    Ok(digest.iter().map(|byte| format!("{byte:02x}")).collect())
}

pub(super) fn submit_response(record: &QueryRecord, replayed: bool) -> SubmitResponse {
    SubmitResponse {
        query_id: record.id.clone(),
        state: record.state.read().phase,
        replayed,
        status_url: format!("/v2/queries/{}", record.id),
        results_url: format!("/v2/queries/{}/results", record.id),
    }
}

pub(super) fn terminal_error(code: &str, message: &str, query_id: &str) -> ErrorBody {
    ErrorBody {
        query_id: Some(query_id.into()),
        ..ErrorBody::new(code, message, RetryClass::Unknown)
    }
}

pub(super) fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| u64::try_from(duration.as_millis()).unwrap_or(u64::MAX))
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::validate_idempotency_key;

    #[test]
    fn rejects_short_and_non_ascii_idempotency_keys() {
        assert!(validate_idempotency_key("short", "request").is_err());
        assert!(validate_idempotency_key("这是一个不安全的key", "request").is_err());
        assert!(validate_idempotency_key("0123456789abcdef", "request").is_ok());
        let _ = Duration::ZERO;
    }
}
