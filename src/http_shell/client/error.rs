use std::{fmt, time::Duration};

use reqwest::{Response, StatusCode, header};
use serde_json::Value;

use crate::{Error, RetryClass};

use super::super::error::ErrorBody;

pub type RemoteResult<T> = std::result::Result<T, RemoteError>;

/// A structured failure returned by the remote query client.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct RemoteError {
    pub http_status: Option<u16>,
    pub code: Box<str>,
    pub retry_class: RetryClass,
    pub message: Box<str>,
    pub request_id: Option<Box<str>>,
    pub query_id: Option<Box<str>>,
    pub retry_after: Option<Duration>,
    pub details: Option<Box<Value>>,
}

impl RemoteError {
    pub(crate) async fn from_response(response: Response) -> Self {
        let status = response.status();
        let retry_after = retry_after(response.headers());
        let header_request_id = response
            .headers()
            .get("x-request-id")
            .and_then(|value| value.to_str().ok())
            .map(Box::<str>::from);
        let body = response.json::<ErrorBody>().await.ok();
        Self::from_response_parts(status, retry_after, header_request_id, body)
    }

    pub(crate) fn transport(error: reqwest::Error) -> Self {
        Self::client("client.transport", error.to_string(), RetryClass::Unknown)
    }

    pub(crate) fn protocol(message: impl Into<Box<str>>) -> Self {
        Self::client("client.protocol", message, RetryClass::Unknown)
    }

    pub(crate) fn protocol_for_query(
        query_id: impl Into<String>,
        message: impl Into<Box<str>>,
    ) -> Self {
        Self {
            query_id: Some(query_id.into().into_boxed_str()),
            ..Self::protocol(message)
        }
    }

    pub(crate) fn configuration(message: impl Into<Box<str>>) -> Self {
        Self::client("client.configuration", message, RetryClass::Never)
    }

    pub(crate) fn with_query_id(mut self, query_id: &str) -> Self {
        if self.query_id.is_none() {
            self.query_id = Some(query_id.into());
        }
        self
    }

    pub(crate) fn submit_retryable(&self) -> bool {
        self.http_status.is_none() || self.retry_class == RetryClass::Safe
    }

    fn from_response_parts(
        status: StatusCode,
        retry_after: Option<Duration>,
        header_request_id: Option<Box<str>>,
        body: Option<ErrorBody>,
    ) -> Self {
        match body {
            Some(body) => Self {
                http_status: Some(status.as_u16()),
                code: body.error.into_boxed_str(),
                retry_class: body.retry,
                message: body.message.into_boxed_str(),
                request_id: body
                    .request_id
                    .map(String::into_boxed_str)
                    .or(header_request_id),
                query_id: body.query_id.map(String::into_boxed_str),
                retry_after,
                details: body.details.map(Box::new),
            },
            None => Self {
                http_status: Some(status.as_u16()),
                code: if status == StatusCode::UNAUTHORIZED {
                    "auth.unauthorized".into()
                } else {
                    "remote.http".into()
                },
                retry_class: RetryClass::Never,
                message: if status == StatusCode::UNAUTHORIZED {
                    "remote authentication failed".into()
                } else {
                    format!("remote HTTP request failed with status {status}").into_boxed_str()
                },
                request_id: header_request_id,
                query_id: None,
                retry_after,
                details: None,
            },
        }
    }

    fn client(code: &str, message: impl Into<Box<str>>, retry_class: RetryClass) -> Self {
        Self {
            http_status: None,
            code: code.into(),
            retry_class,
            message: message.into(),
            request_id: None,
            query_id: None,
            retry_after: None,
            details: None,
        }
    }
}

impl fmt::Display for RemoteError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}: {}", self.code, self.message)?;
        if let Some(status) = self.http_status {
            write!(formatter, " (HTTP {status})")?;
        }
        if let Some(query_id) = &self.query_id {
            write!(formatter, " [query {query_id}]")?;
        }
        if let Some(request_id) = &self.request_id {
            write!(formatter, " [request {request_id}]")?;
        }
        Ok(())
    }
}

impl std::error::Error for RemoteError {}

impl From<RemoteError> for Error {
    fn from(error: RemoteError) -> Self {
        Self::Execution(error.to_string())
    }
}

fn retry_after(headers: &header::HeaderMap) -> Option<Duration> {
    headers
        .get(header::RETRY_AFTER)?
        .to_str()
        .ok()?
        .parse::<u64>()
        .ok()
        .map(Duration::from_secs)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn server_error_keeps_the_machine_readable_contract() {
        let body = ErrorBody {
            error: "query.busy".into(),
            message: "try later".into(),
            retry: RetryClass::Safe,
            request_id: Some("request-7".into()),
            query_id: Some("query-9".into()),
            details: Some(serde_json::json!({"queue_depth": 4})),
        };
        let error = RemoteError::from_response_parts(
            StatusCode::TOO_MANY_REQUESTS,
            Some(Duration::from_secs(3)),
            None,
            Some(body),
        );
        assert_eq!(error.http_status, Some(429));
        assert_eq!(error.code.as_ref(), "query.busy");
        assert_eq!(error.retry_class, RetryClass::Safe);
        assert_eq!(error.request_id.as_deref(), Some("request-7"));
        assert_eq!(error.query_id.as_deref(), Some("query-9"));
        assert_eq!(error.retry_after, Some(Duration::from_secs(3)));
        assert_eq!(error.details.unwrap()["queue_depth"], 4);
    }

    #[test]
    fn response_header_supplies_a_missing_request_id() {
        let error = RemoteError::from_response_parts(
            StatusCode::BAD_REQUEST,
            None,
            Some("header-request".into()),
            Some(ErrorBody::new(
                "request.invalid",
                "bad input",
                RetryClass::Never,
            )),
        );
        assert_eq!(error.request_id.as_deref(), Some("header-request"));
    }
}
