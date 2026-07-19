use axum::{
    Json,
    http::{HeaderValue, StatusCode, header},
    response::{IntoResponse, Response},
};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::Error;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ErrorBody {
    pub error: String,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub request_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub query_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub details: Option<Value>,
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
                error: code.into(),
                message: message.into(),
                request_id: request_id.into(),
                query_id: None,
                details: None,
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
        self
    }

    pub(crate) fn from_engine(error: &Error, request_id: String) -> Self {
        match error {
            Error::SqlParse(_) => Self::new(
                StatusCode::BAD_REQUEST,
                "sql.parse",
                error.to_string(),
                request_id,
            ),
            Error::InvalidArgument(message) => Self::new(
                StatusCode::BAD_REQUEST,
                "request.invalid",
                message.clone(),
                request_id,
            ),
            Error::Unsupported(message) => Self::new(
                StatusCode::UNPROCESSABLE_ENTITY,
                "sql.unsupported",
                message.clone(),
                request_id,
            ),
            Error::Catalog(message) => Self::new(
                StatusCode::UNPROCESSABLE_ENTITY,
                "sql.catalog",
                message.clone(),
                request_id,
            ),
            Error::ResourceExhausted(_) | Error::NativeDiskQuotaExceeded { .. } => Self::new(
                StatusCode::INSUFFICIENT_STORAGE,
                "query.resource_exhausted",
                "query resources were exhausted",
                request_id,
            ),
            Error::Cancelled => Self::new(
                StatusCode::CONFLICT,
                "query.cancelled",
                "query was cancelled",
                request_id,
            ),
            _ => Self::new(
                StatusCode::INTERNAL_SERVER_ERROR,
                "server.internal",
                "the server could not complete the request",
                request_id,
            ),
        }
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
