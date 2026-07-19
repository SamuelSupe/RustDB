use axum::{
    Json,
    http::{HeaderValue, StatusCode, header},
    response::{IntoResponse, Response},
};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{Error, RetryClass};

#[derive(Clone, Debug, Deserialize, Serialize)]
#[non_exhaustive]
pub struct ErrorBody {
    pub error: String,
    pub message: String,
    #[serde(default)]
    pub retry: RetryClass,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub request_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub query_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub details: Option<Value>,
}

impl ErrorBody {
    pub fn new(error: impl Into<String>, message: impl Into<String>, retry: RetryClass) -> Self {
        Self {
            error: error.into(),
            message: message.into(),
            retry,
            request_id: None,
            query_id: None,
            details: None,
        }
    }
}

pub(crate) struct HttpError {
    pub status: StatusCode,
    pub body: Box<ErrorBody>,
    pub retry_after: Option<u64>,
}

impl HttpError {
    pub(crate) fn new(
        status: StatusCode,
        code: impl Into<String>,
        message: impl Into<String>,
        request_id: impl Into<Option<String>>,
    ) -> Self {
        Self {
            status,
            body: Box::new(ErrorBody {
                request_id: request_id.into(),
                ..ErrorBody::new(code, message, RetryClass::Never)
            }),
            retry_after: None,
        }
    }

    pub(crate) fn query(mut self, query_id: impl Into<String>) -> Self {
        self.body.query_id = Some(query_id.into());
        self
    }

    pub(crate) fn retry_after(mut self, seconds: u64) -> Self {
        self.retry_after = Some(seconds);
        self.body.retry = RetryClass::Safe;
        self
    }

    pub(crate) fn retry_class(mut self, retry: RetryClass) -> Self {
        self.body.retry = retry;
        self
    }

    pub(crate) fn from_engine(error: &Error, request_id: String) -> Self {
        let response = match error {
            Error::SqlParse(_) => Self::new(
                StatusCode::BAD_REQUEST,
                error.code().as_str(),
                error.to_string(),
                request_id,
            ),
            Error::InvalidArgument(message) => Self::new(
                StatusCode::BAD_REQUEST,
                error.code().as_str(),
                message.clone(),
                request_id,
            ),
            Error::Unsupported(message) => Self::new(
                StatusCode::UNPROCESSABLE_ENTITY,
                error.code().as_str(),
                message.clone(),
                request_id,
            ),
            Error::Catalog(message) => Self::new(
                StatusCode::UNPROCESSABLE_ENTITY,
                error.code().as_str(),
                message.clone(),
                request_id,
            ),
            Error::ResourceExhausted(_) | Error::NativeDiskQuotaExceeded { .. } => Self::new(
                StatusCode::INSUFFICIENT_STORAGE,
                error.code().as_str(),
                "query resources were exhausted",
                request_id,
            ),
            Error::Cancelled => Self::new(
                StatusCode::CONFLICT,
                error.code().as_str(),
                "query was cancelled",
                request_id,
            ),
            _ => Self::new(
                StatusCode::INTERNAL_SERVER_ERROR,
                error.code().as_str(),
                "the server could not complete the request",
                request_id,
            ),
        };
        response.retry_class(error.retry_class())
    }
}

impl IntoResponse for HttpError {
    fn into_response(self) -> Response {
        let request_id = self.body.request_id.clone();
        let mut response = (self.status, Json(self.body)).into_response();
        response
            .headers_mut()
            .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
        if self.status == StatusCode::UNAUTHORIZED {
            response.headers_mut().insert(
                header::WWW_AUTHENTICATE,
                HeaderValue::from_static("Bearer realm=\"rustdb\""),
            );
        }
        if let Some(seconds) = self.retry_after
            && let Ok(value) = HeaderValue::from_str(&seconds.to_string())
        {
            response.headers_mut().insert(header::RETRY_AFTER, value);
        }
        if let Some(request_id) = request_id
            && let Ok(value) = HeaderValue::from_str(&request_id)
        {
            response.headers_mut().insert("x-request-id", value);
        }
        response
    }
}

#[cfg(test)]
mod tests {
    use crate::{Error, RetryClass};

    use super::HttpError;

    #[test]
    fn engine_errors_keep_the_stable_code_and_retry_contract() {
        let exhausted = HttpError::from_engine(
            &Error::ResourceExhausted("memory budget".into()),
            "request-1".into(),
        );
        assert_eq!(exhausted.body.error, "query.resource_exhausted");
        assert_eq!(exhausted.body.retry, RetryClass::Safe);

        let invalid = HttpError::from_engine(
            &Error::InvalidArgument("bad value".into()),
            "request-2".into(),
        );
        assert_eq!(invalid.body.error, "request.invalid");
        assert_eq!(invalid.body.retry, RetryClass::Never);
    }
}
