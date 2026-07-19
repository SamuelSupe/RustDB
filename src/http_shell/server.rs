use std::{
    future::Future,
    net::SocketAddr,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use axum::{
    Json, Router,
    body::Body,
    extract::{DefaultBodyLimit, Path, Query, Request, State},
    http::{HeaderMap, HeaderValue, StatusCode, header},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use axum_server::{Handle, tls_rustls::RustlsConfig};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use hyper_util::rt::TokioTimer;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;
use tower::limit::ConcurrencyLimitLayer;
use tower_http::timeout::RequestBodyTimeoutLayer;
use tower_http::{compression::CompressionLayer, trace::TraceLayer};
use uuid::Uuid;

use crate::{Engine, Error, Result};

use super::{
    error::HttpError,
    json::{batch_rows, schema_columns},
    query::{QueryManager, QueryManagerConfig},
    result_read::ResultReadTracker,
    result_store::ResultStoreConfig,
    security::{
        BearerToken, SecurityState, ServerEndpoint, TlsMaterial, default_state_root,
        write_managed_profile_bundle,
    },
    types::{InfoResponse, JsonResultPage, PageMetadata, QueryRequest},
};

const DEFAULT_LISTEN: &str = "127.0.0.1:7400";
const DEFAULT_PAGE_ROWS: usize = 1_000;
const MAX_PAGE_ROWS: usize = 10_000;
const MAX_PAGE_BYTES: usize = 16 * 1024 * 1024;
const MAX_HTTP_REQUESTS: usize = 128;
const REQUEST_BODY_TIMEOUT: Duration = Duration::from_secs(15);
const HEADER_READ_TIMEOUT: Duration = Duration::from_secs(15);

#[derive(Clone, Debug)]
pub struct HttpServerConfig {
    pub listen: SocketAddr,
    pub advertise_url: Option<String>,
    pub state_root: PathBuf,
    pub result_directory: Option<PathBuf>,
    pub result_ttl: Duration,
    pub result_global_limit_bytes: Option<u64>,
    pub result_query_limit_bytes: Option<u64>,
    pub query: QueryManagerConfig,
    pub shutdown_grace: Duration,
}

impl Default for HttpServerConfig {
    fn default() -> Self {
        Self {
            listen: DEFAULT_LISTEN
                .parse()
                .expect("valid default listen address"),
            advertise_url: None,
            state_root: default_state_root().unwrap_or_default(),
            result_directory: None,
            result_ttl: Duration::from_secs(60 * 60),
            result_global_limit_bytes: None,
            result_query_limit_bytes: None,
            query: QueryManagerConfig::default(),
            shutdown_grace: Duration::from_secs(30),
        }
    }
}

#[derive(Clone)]
struct ServerState {
    queries: QueryManager,
    token: BearerToken,
    ready: Arc<AtomicBool>,
    result_read_slots: Arc<Semaphore>,
    result_reads: Arc<ResultReadTracker>,
}

#[derive(Deserialize)]
struct ResultQuery {
    cursor: Option<String>,
    offset: Option<u64>,
    limit: Option<usize>,
}

#[derive(Serialize)]
struct HealthResponse {
    status: &'static str,
}

pub async fn serve(engine: Engine, config: HttpServerConfig) -> Result<()> {
    serve_with_shutdown(engine, config, async {
        if let Err(error) = tokio::signal::ctrl_c().await {
            tracing::error!(%error, "failed to receive shutdown signal");
        }
    })
    .await
}

#[doc(hidden)]
pub async fn serve_with_shutdown<F>(
    engine: Engine,
    config: HttpServerConfig,
    shutdown: F,
) -> Result<()>
where
    F: Future<Output = ()> + Send + 'static,
{
    install_crypto_provider();
    if config.state_root.as_os_str().is_empty() {
        return Err(Error::InvalidArgument(
            "HOME is not set; configure an explicit HTTP state root".into(),
        ));
    }
    let database = engine.database_path().ok_or_else(|| {
        Error::InvalidArgument("rustdb serve requires a persistent Native database".into())
    })?;
    let endpoint = ServerEndpoint::resolve(config.listen, config.advertise_url.as_deref())?;
    let security = SecurityState::for_native_database(&config.state_root, database)?;
    let _state_lock = security.acquire_server_lock()?;
    let tls = TlsMaterial::load_or_create(&security, &endpoint)?;
    let token = BearerToken::load_or_create(&security)?;
    let bundle = security.directory().join("connection.rustdb-profile");
    write_managed_profile_bundle(
        &bundle,
        tls.public_url(),
        tls.ca_certificate_path(),
        &security.token_path(),
    )?;
    let result_directory = config
        .result_directory
        .clone()
        .unwrap_or_else(|| security.directory().join("results"));
    let mut result_config = ResultStoreConfig::new(result_directory);
    result_config.ttl = config.result_ttl;
    result_config.global_limit_bytes = config.result_global_limit_bytes;
    result_config.query_limit_bytes = config.result_query_limit_bytes;
    let queries = QueryManager::new(engine, config.query.clone(), result_config)?;
    let ready = Arc::new(AtomicBool::new(true));
    let result_read_slots = Arc::new(Semaphore::new(1));
    let result_reads = ResultReadTracker::new();
    let state = ServerState {
        queries: queries.clone(),
        token,
        ready: Arc::clone(&ready),
        result_read_slots: Arc::clone(&result_read_slots),
        result_reads: Arc::clone(&result_reads),
    };
    let app = router(state);
    let tls_config =
        RustlsConfig::from_pem_file(tls.server_certificate_path(), tls.server_private_key_path())
            .await
            .map_err(|error| Error::io(tls.server_identity_path().to_owned(), error))?;
    let handle = Handle::new();
    let shutdown_handle = handle.clone();
    let grace = config.shutdown_grace;
    let server_stopped = CancellationToken::new();
    let _stop_on_drop = server_stopped.clone().drop_guard();
    let stop_cleanup = server_stopped.clone();
    let shutdown_task = tokio::spawn(async move {
        tokio::select! {
            _ = shutdown => {}
            _ = stop_cleanup.cancelled() => {}
        }
        ready.store(false, Ordering::Release);
        result_read_slots.close();
        result_reads.close();
        queries.begin_shutdown();
        shutdown_handle.graceful_shutdown(Some(grace));
        result_reads.wait_idle().await;
        queries.shutdown().await
    });
    eprintln!("RustDB HTTP Shell listening at {}", tls.public_url());
    eprintln!("connection profile bundle: {}", bundle.display());
    let mut server = axum_server::bind_rustls(config.listen, tls_config);
    server
        .http_builder()
        .http1()
        .timer(TokioTimer::new())
        .header_read_timeout(Some(HEADER_READ_TIMEOUT));
    let serve_result = server.handle(handle).serve(app.into_make_service()).await;
    server_stopped.cancel();
    let shutdown_result = shutdown_task
        .await
        .map_err(|error| Error::Internal(format!("HTTP shutdown task panicked: {error}")))?;
    match (serve_result, shutdown_result) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(error), Ok(())) => Err(Error::io(None, error)),
        (Ok(()), Err(error)) => Err(error),
        (Err(serve), Err(shutdown)) => Err(Error::Execution(format!(
            "HTTP server failed: {serve}; shutdown also failed: {shutdown}"
        ))),
    }
}

fn install_crypto_provider() {
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
}

fn router(state: ServerState) -> Router {
    let protected = Router::new()
        .route("/v1/info", get(info))
        .route("/v1/queries", post(submit_query))
        .route(
            "/v1/queries/{query_id}",
            get(query_status).delete(delete_query),
        )
        .route("/v1/queries/{query_id}/cancel", post(cancel_query))
        .route("/v1/queries/{query_id}/results", get(query_results))
        .route_layer(middleware::from_fn_with_state(state.clone(), require_auth));
    Router::new()
        .route("/healthz", get(health))
        .route("/readyz", get(ready))
        .merge(protected)
        .with_state(state)
        .layer(DefaultBodyLimit::max(1024 * 1024))
        .layer(RequestBodyTimeoutLayer::new(REQUEST_BODY_TIMEOUT))
        .layer(ConcurrencyLimitLayer::new(MAX_HTTP_REQUESTS))
        .layer(CompressionLayer::new().gzip(true))
        .layer(TraceLayer::new_for_http())
}

async fn require_auth(
    State(state): State<ServerState>,
    request: Request,
    next: Next,
) -> std::result::Result<Response, HttpError> {
    let request_id = request_id();
    authorize(&state, request.headers(), &request_id)?;
    Ok(next.run(request).await)
}

async fn health() -> Json<HealthResponse> {
    Json(HealthResponse { status: "ok" })
}

async fn ready(State(state): State<ServerState>) -> Response {
    let ready = state.ready.load(Ordering::Acquire);
    (
        if ready {
            StatusCode::OK
        } else {
            StatusCode::SERVICE_UNAVAILABLE
        },
        Json(HealthResponse {
            status: if ready { "ready" } else { "not_ready" },
        }),
    )
        .into_response()
}

async fn info(
    State(state): State<ServerState>,
    headers: HeaderMap,
) -> std::result::Result<Response, HttpError> {
    let request_id = request_id();
    authorize(&state, &headers, &request_id)?;
    let response = Json(InfoResponse {
        protocol_version: "v1",
        server_version: env!("CARGO_PKG_VERSION"),
        read_only: true,
        capabilities: &[
            "background_queries",
            "cursor_pagination",
            "offset_pagination",
            "json_results",
            "ndjson_results",
            "typed_parameters",
        ],
    })
    .into_response();
    Ok(with_request_id(response, &request_id))
}

async fn submit_query(
    State(state): State<ServerState>,
    headers: HeaderMap,
    Json(request): Json<QueryRequest>,
) -> std::result::Result<Response, HttpError> {
    let request_id = request_id();
    authorize(&state, &headers, &request_id)?;
    let fingerprint = sql_fingerprint(&request.sql);
    let key = headers
        .get("idempotency-key")
        .and_then(|value| value.to_str().ok())
        .ok_or_else(|| {
            HttpError::new(
                StatusCode::BAD_REQUEST,
                "idempotency.required",
                "Idempotency-Key header is required",
                request_id.clone(),
            )
        })?;
    let accepted = state.queries.submit(key, request, &request_id)?;
    tracing::info!(
        request_id = %request_id,
        query_id = %accepted.query_id,
        sql_fingerprint = %fingerprint,
        replayed = accepted.replayed,
        state = ?accepted.state,
        "HTTP query accepted"
    );
    let location = accepted.status_url.clone();
    let mut response = (StatusCode::ACCEPTED, Json(accepted)).into_response();
    if let Ok(value) = HeaderValue::from_str(&location) {
        response.headers_mut().insert(header::LOCATION, value);
    }
    Ok(with_request_id(response, &request_id))
}

async fn query_status(
    State(state): State<ServerState>,
    Path(query_id): Path<String>,
    headers: HeaderMap,
) -> std::result::Result<Response, HttpError> {
    let request_id = request_id();
    authorize(&state, &headers, &request_id)?;
    let status = state.queries.status(&query_id, &request_id)?;
    Ok(with_request_id(Json(status).into_response(), &request_id))
}

async fn cancel_query(
    State(state): State<ServerState>,
    Path(query_id): Path<String>,
    headers: HeaderMap,
) -> std::result::Result<Response, HttpError> {
    let request_id = request_id();
    authorize(&state, &headers, &request_id)?;
    let status = state.queries.cancel(&query_id, &request_id)?;
    Ok(with_request_id(
        (StatusCode::ACCEPTED, Json(status)).into_response(),
        &request_id,
    ))
}

async fn delete_query(
    State(state): State<ServerState>,
    Path(query_id): Path<String>,
    headers: HeaderMap,
) -> std::result::Result<Response, HttpError> {
    let request_id = request_id();
    authorize(&state, &headers, &request_id)?;
    let _result_operation = Arc::clone(&state.result_read_slots)
        .acquire_owned()
        .await
        .map_err(|_| {
            HttpError::new(
                StatusCode::SERVICE_UNAVAILABLE,
                "server.shutting_down",
                "the result store is shutting down",
                request_id.clone(),
            )
            .query(query_id.clone())
        })?;
    let _operation_guard = state.result_reads.start().ok_or_else(|| {
        HttpError::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "server.shutting_down",
            "the result store is shutting down",
            request_id.clone(),
        )
        .query(query_id.clone())
    })?;
    state.queries.delete(&query_id, &request_id)?;
    Ok(with_request_id(
        StatusCode::NO_CONTENT.into_response(),
        &request_id,
    ))
}

async fn query_results(
    State(state): State<ServerState>,
    Path(query_id): Path<String>,
    Query(query): Query<ResultQuery>,
    headers: HeaderMap,
) -> std::result::Result<Response, HttpError> {
    let request_id = request_id();
    authorize(&state, &headers, &request_id)?;
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
    let result = state.queries.completed_result(&query_id, &request_id)?;
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

fn accepts_ndjson(headers: &HeaderMap) -> bool {
    headers
        .get(header::ACCEPT)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| {
            value.split(',').any(|range| {
                let mut parts = range.split(';');
                parts
                    .next()
                    .is_some_and(|media| media.trim().eq_ignore_ascii_case("application/x-ndjson"))
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

fn authorize(
    state: &ServerState,
    headers: &HeaderMap,
    request_id: &str,
) -> std::result::Result<(), HttpError> {
    let Some(value) = headers.get(header::AUTHORIZATION) else {
        return Err(HttpError::new(
            StatusCode::UNAUTHORIZED,
            "auth.required",
            "Bearer authentication is required",
            request_id.to_owned(),
        ));
    };
    let valid = value
        .to_str()
        .ok()
        .and_then(|value| value.strip_prefix("Bearer "))
        .is_some_and(|token| state.token.verify(token));
    if !valid {
        return Err(HttpError::new(
            StatusCode::UNAUTHORIZED,
            "auth.invalid",
            "Bearer token is invalid",
            request_id.to_owned(),
        ));
    }
    Ok(())
}

fn with_request_id(mut response: Response, request_id: &str) -> Response {
    if let Ok(value) = HeaderValue::from_str(request_id) {
        response.headers_mut().insert("x-request-id", value);
    }
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response
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

fn request_id() -> String {
    Uuid::new_v4().simple().to_string()
}

fn sql_fingerprint(sql: &str) -> String {
    let digest = Sha256::digest(sql.as_bytes());
    digest[..16]
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::{decode_cursor, encode_cursor};

    #[test]
    fn cursor_roundtrips_without_exposing_a_decimal_offset() {
        let cursor = encode_cursor(42);
        assert_eq!(decode_cursor(&cursor).unwrap(), 42);
        assert_ne!(cursor, "42");
    }
}
