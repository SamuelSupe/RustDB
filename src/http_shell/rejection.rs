use axum::{
    extract::rejection::{JsonRejection, QueryRejection},
    http::StatusCode,
};

use super::error::HttpError;

pub(crate) fn json(error: JsonRejection, request_id: &str) -> HttpError {
    let status = error.status();
    let (code, message) = match status {
        StatusCode::UNSUPPORTED_MEDIA_TYPE => (
            "request.content_type",
            "request body must use application/json",
        ),
        StatusCode::PAYLOAD_TOO_LARGE => (
            "request.body_too_large",
            "request body exceeds the configured limit",
        ),
        _ => ("request.invalid_json", "request body is not valid JSON"),
    };
    HttpError::new(status, code, message, request_id.to_owned())
}

pub(crate) fn query(error: QueryRejection, request_id: &str) -> HttpError {
    HttpError::new(
        error.status(),
        "request.invalid_query",
        "query parameters are invalid",
        request_id.to_owned(),
    )
}

#[cfg(test)]
mod tests {
    use axum::{
        Json,
        body::Body,
        extract::FromRequest,
        http::{Request, StatusCode},
    };

    use super::json;

    #[tokio::test]
    async fn malformed_json_has_a_stable_problem_code() {
        let request = Request::builder()
            .header("content-type", "application/json")
            .body(Body::from("{"))
            .unwrap();
        let error = Json::<serde_json::Value>::from_request(request, &())
            .await
            .expect_err("malformed JSON must fail");
        let problem = json(error, "request-1");
        assert_eq!(problem.status, StatusCode::BAD_REQUEST);
        assert_eq!(problem.body.error, "request.invalid_json");
    }
}
