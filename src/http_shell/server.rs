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

use crate::{Engine, Error, Result};
use axum::{
    Json, Router,
    body::Body,
    extract::{DefaultBodyLimit, Extension, Path, Request, State, rejection::JsonRejection},
    http::{HeaderMap, HeaderValue, StatusCode, header},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use axum_server::{Handle, tls_rustls::RustlsConfig};
use chrono::Utc;
use hyper_util::rt::TokioTimer;
use serde::Serialize;
use sha2::{Digest, Sha256};
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;
use tower::limit::ConcurrencyLimitLayer;
use tower_http::timeout::RequestBodyTimeoutLayer;
use tower_http::{compression::CompressionLayer, trace::TraceLayer};

use super::{
    error::HttpError,
    metrics::HttpMetrics,
    query::{QueryManager, QueryManagerConfig},
    rejection,
    request_context::RequestContext,
    result_read::ResultReadTracker,
    result_store::ResultStoreConfig,
    security::{
        AuditEvent, AuditKind, AuditLog, AuthenticatedActor, AuthenticationError, Authenticator,
        Permission, PrincipalStore, SecurityState, ServerEndpoint, TlsMaterial, default_state_root,
        write_managed_profile_bundle,
    },
    types::{InfoResponse, QueryRequest},
};

mod results;

const DEFAULT_LISTEN: &str = "127.0.0.1:7400";
const MAX_HTTP_REQUESTS: usize = 128;
const MAX_RESULT_READS: usize = 16;
const REQUEST_BODY_TIMEOUT: Duration = Duration::from_secs(15);
const HEADER_READ_TIMEOUT: Duration = Duration::from_secs(15);

#[derive(Clone, Debug)]
#[non_exhaustive]
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
    /// Explicit development escape hatch. Authentication is enabled by default.
    pub no_auth: bool,
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
            result_global_limit_bytes: Some(10 * 1024 * 1024 * 1024),
            result_query_limit_bytes: Some(2 * 1024 * 1024 * 1024),
            query: QueryManagerConfig::default(),
            shutdown_grace: Duration::from_secs(30),
            no_auth: false,
        }
    }
}

#[derive(Clone)]
struct ServerState {
    engine: Engine,
    queries: QueryManager,
    authenticator: Authenticator,
    ready: Arc<AtomicBool>,
    result_read_slots: Arc<Semaphore>,
    result_reads: Arc<ResultReadTracker>,
    metrics: Arc<HttpMetrics>,
    audit: AuditLog,
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
    let (authenticator, bundle) = if config.no_auth {
        (Authenticator::explicitly_disabled(), None)
    } else {
        let principal_store = PrincipalStore::new(security.clone());
        let authenticator = principal_store.load_or_bootstrap()?;
        let bundle = security.directory().join("connection.rustdb-profile");
        write_managed_profile_bundle(
            &bundle,
            tls.public_url(),
            tls.ca_certificate_path(),
            &principal_store.connection_token_path()?,
        )?;
        (authenticator, Some(bundle))
    };
    let result_directory = config
        .result_directory
        .clone()
        .unwrap_or_else(|| security.directory().join("results"));
    let mut result_config = ResultStoreConfig::new(result_directory);
    result_config.ttl = config.result_ttl;
    result_config.global_limit_bytes = config.result_global_limit_bytes;
    result_config.query_limit_bytes = config.result_query_limit_bytes;
    let mut query_config = config.query.clone();
    if let Some(limit) = result_config.query_limit_bytes {
        query_config.query_result_limit_bytes = query_config.query_result_limit_bytes.min(limit);
    }
    if let Some(limit) = result_config.global_limit_bytes {
        query_config.query_result_limit_bytes = query_config.query_result_limit_bytes.min(limit);
    }
    let metrics = Arc::new(HttpMetrics::default());
    let audit = AuditLog::open(&security)?;
    let queries = QueryManager::new_observed(
        engine.clone(),
        query_config,
        result_config,
        Arc::clone(&metrics),
        audit.clone(),
    )?;
    let ready = Arc::new(AtomicBool::new(true));
    let result_read_slots = Arc::new(Semaphore::new(MAX_RESULT_READS));
    let result_reads = ResultReadTracker::new();
    let state = ServerState {
        engine,
        queries: queries.clone(),
        authenticator,
        ready: Arc::clone(&ready),
        result_read_slots: Arc::clone(&result_read_slots),
        result_reads: Arc::clone(&result_reads),
        metrics: Arc::clone(&metrics),
        audit: audit.clone(),
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
    let shutdown_audit = audit.clone();
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
        let outcome = queries.shutdown().await;
        record_audit(
            &shutdown_audit,
            AuditEvent {
                kind: AuditKind::ServerStopped,
                principal_id: None,
                query_id: None,
                request_id: None,
                sql_fingerprint: None,
                outcome: if outcome.is_ok() { "clean" } else { "failed" },
            },
        );
        outcome
    });
    audit.record(AuditEvent {
        kind: AuditKind::ServerStarted,
        principal_id: None,
        query_id: None,
        request_id: None,
        sql_fingerprint: None,
        outcome: "ready",
    })?;
    eprintln!("RustDB HTTP Shell listening at {}", tls.public_url());
    if let Some(bundle) = &bundle {
        eprintln!("connection profile bundle: {}", bundle.display());
    } else {
        eprintln!("authentication explicitly disabled; no connection profile was generated");
    }
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
        .route("/metrics", get(prometheus_metrics))
        .route("/v1/queries", post(submit_query))
        .route(
            "/v1/queries/{query_id}",
            get(query_status).delete(delete_query),
        )
        .route("/v1/queries/{query_id}/cancel", post(cancel_query))
        .route(
            "/v1/queries/{query_id}/results",
            get(results::query_results),
        )
        .route_layer(middleware::from_fn_with_state(state.clone(), require_auth));
    Router::new()
        .route("/healthz", get(health))
        .route("/readyz", get(ready))
        .merge(protected)
        .fallback(not_found)
        .method_not_allowed_fallback(method_not_allowed)
        .layer(middleware::from_fn_with_state(
            state.clone(),
            observe_request,
        ))
        .with_state(state)
        .layer(DefaultBodyLimit::max(1024 * 1024))
        .layer(RequestBodyTimeoutLayer::new(REQUEST_BODY_TIMEOUT))
        .layer(ConcurrencyLimitLayer::new(MAX_HTTP_REQUESTS))
        .layer(CompressionLayer::new().gzip(true))
        .layer(TraceLayer::new_for_http())
}

async fn not_found() -> Response {
    let request_id = RequestContext::new_request_id();
    HttpError::new(
        StatusCode::NOT_FOUND,
        "request.not_found",
        "the requested endpoint does not exist",
        request_id,
    )
    .into_response()
}

async fn method_not_allowed() -> Response {
    let request_id = RequestContext::new_request_id();
    HttpError::new(
        StatusCode::METHOD_NOT_ALLOWED,
        "request.method_not_allowed",
        "the HTTP method is not supported for this endpoint",
        request_id,
    )
    .into_response()
}

async fn observe_request(
    State(state): State<ServerState>,
    request: Request,
    next: Next,
) -> Response {
    state.metrics.request();
    let mut response = next.run(request).await;
    response.headers_mut().insert(
        header::STRICT_TRANSPORT_SECURITY,
        HeaderValue::from_static("max-age=31536000"),
    );
    response.headers_mut().insert(
        "x-content-type-options",
        HeaderValue::from_static("nosniff"),
    );
    response
}

async fn require_auth(
    State(state): State<ServerState>,
    mut request: Request,
    next: Next,
) -> std::result::Result<Response, HttpError> {
    let request_id = RequestContext::new_request_id();
    let actor = match authorize(&state, request.headers(), &request_id) {
        Ok(actor) => actor,
        Err(error) => {
            state.metrics.auth_failure();
            record_audit(
                &state.audit,
                AuditEvent {
                    kind: AuditKind::AuthenticationFailed,
                    principal_id: None,
                    query_id: None,
                    request_id: Some(&request_id),
                    sql_fingerprint: None,
                    outcome: &error.body.error,
                },
            );
            return Err(error);
        }
    };
    let context = RequestContext::with_request_id(request_id, actor);
    let request_id = context.request_id().to_owned();
    request.extensions_mut().insert(context);
    Ok(with_request_id(next.run(request).await, &request_id))
}

async fn prometheus_metrics(
    State(state): State<ServerState>,
    Extension(context): Extension<RequestContext>,
) -> std::result::Result<Response, HttpError> {
    if !context.actor().is_allowed(Permission::Admin) {
        state.metrics.auth_failure();
        record_audit(
            &state.audit,
            AuditEvent {
                kind: AuditKind::AuthorizationFailed,
                principal_id: Some(actor_audit_id(context.actor())),
                query_id: None,
                request_id: Some(context.request_id()),
                sql_fingerprint: None,
                outcome: "auth.forbidden",
            },
        );
        return Err(HttpError::new(
            StatusCode::FORBIDDEN,
            "auth.forbidden",
            "admin permission is required to read metrics",
            context.request_id().to_owned(),
        ));
    }
    let admission = state.queries.admission_snapshot();
    let mut response = Response::new(Body::from(state.metrics.render_prometheus(&admission)));
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("text/plain; version=0.0.4; charset=utf-8"),
    );
    Ok(with_request_id(response, context.request_id()))
}

async fn health() -> Json<HealthResponse> {
    Json(HealthResponse { status: "ok" })
}

async fn ready(State(state): State<ServerState>) -> Response {
    let ready = state.ready.load(Ordering::Acquire) && state.engine.health_check().is_ok();
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
    Extension(context): Extension<RequestContext>,
) -> std::result::Result<Response, HttpError> {
    let request_id = context.request_id();
    let response = Json(InfoResponse {
        protocol_version: "v1",
        server_version: env!("CARGO_PKG_VERSION"),
        read_only: true,
        capabilities: &[
            "background_queries",
            "arrow_ipc_results",
            "batch_sequence_resume",
            "cursor_pagination",
            "offset_pagination",
            "json_results",
            "ndjson_results",
            "typed_parameters",
        ],
    })
    .into_response();
    Ok(with_request_id(response, request_id))
}

async fn submit_query(
    State(state): State<ServerState>,
    Extension(context): Extension<RequestContext>,
    headers: HeaderMap,
    payload: std::result::Result<Json<QueryRequest>, JsonRejection>,
) -> std::result::Result<Response, HttpError> {
    let request_id = context.request_id().to_owned();
    let Json(request) = payload.map_err(|error| rejection::json(error, &request_id))?;
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
    let accepted = match state
        .queries
        .submit(context.actor(), key, request, &request_id)
    {
        Ok(accepted) => accepted,
        Err(error) => {
            state.metrics.query_rejected();
            return Err(error);
        }
    };
    state.metrics.query_submitted();
    record_audit(
        &state.audit,
        AuditEvent {
            kind: AuditKind::QuerySubmitted,
            principal_id: Some(actor_audit_id(context.actor())),
            query_id: Some(&accepted.query_id),
            request_id: Some(&request_id),
            sql_fingerprint: Some(&fingerprint),
            outcome: if accepted.replayed {
                "replayed"
            } else {
                "accepted"
            },
        },
    );
    tracing::info!(
        request_id = %request_id,
        query_id = %accepted.query_id,
        principal_id = context.actor().query_owner().audit_id(),
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
    Extension(context): Extension<RequestContext>,
    Path(query_id): Path<String>,
) -> std::result::Result<Response, HttpError> {
    let request_id = context.request_id().to_owned();
    let status = state
        .queries
        .status(context.actor(), &query_id, &request_id)?;
    Ok(with_request_id(Json(status).into_response(), &request_id))
}

async fn cancel_query(
    State(state): State<ServerState>,
    Extension(context): Extension<RequestContext>,
    Path(query_id): Path<String>,
) -> std::result::Result<Response, HttpError> {
    let request_id = context.request_id().to_owned();
    let status = state
        .queries
        .cancel(context.actor(), &query_id, &request_id)?;
    record_audit(
        &state.audit,
        AuditEvent {
            kind: AuditKind::QueryCancelled,
            principal_id: Some(actor_audit_id(context.actor())),
            query_id: Some(&query_id),
            request_id: Some(&request_id),
            sql_fingerprint: None,
            outcome: "requested",
        },
    );
    Ok(with_request_id(
        (StatusCode::ACCEPTED, Json(status)).into_response(),
        &request_id,
    ))
}

async fn delete_query(
    State(state): State<ServerState>,
    Extension(context): Extension<RequestContext>,
    Path(query_id): Path<String>,
) -> std::result::Result<Response, HttpError> {
    let request_id = context.request_id().to_owned();
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
    state
        .queries
        .delete(context.actor(), &query_id, &request_id)?;
    record_audit(
        &state.audit,
        AuditEvent {
            kind: AuditKind::QueryDeleted,
            principal_id: Some(actor_audit_id(context.actor())),
            query_id: Some(&query_id),
            request_id: Some(&request_id),
            sql_fingerprint: None,
            outcome: "deleted",
        },
    );
    Ok(with_request_id(
        StatusCode::NO_CONTENT.into_response(),
        &request_id,
    ))
}

fn authorize(
    state: &ServerState,
    headers: &HeaderMap,
    request_id: &str,
) -> std::result::Result<super::security::AuthenticatedActor, HttpError> {
    let bearer_token = headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "));
    let actor = state
        .authenticator
        .authenticate(bearer_token, Utc::now())
        .map_err(|error| match error {
            AuthenticationError::Required => HttpError::new(
                StatusCode::UNAUTHORIZED,
                "auth.required",
                "Bearer authentication is required",
                request_id.to_owned(),
            ),
            AuthenticationError::Invalid => HttpError::new(
                StatusCode::UNAUTHORIZED,
                "auth.invalid",
                "Bearer token is invalid",
                request_id.to_owned(),
            ),
        })?;
    if !actor.is_allowed(Permission::Query) {
        return Err(HttpError::new(
            StatusCode::FORBIDDEN,
            "auth.forbidden",
            "the authenticated principal cannot run queries",
            request_id.to_owned(),
        ));
    }
    Ok(actor)
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

fn sql_fingerprint(sql: &str) -> String {
    let digest = Sha256::digest(sql.as_bytes());
    digest[..16]
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn actor_audit_id(actor: &AuthenticatedActor) -> &str {
    actor
        .principal_id()
        .map_or("anonymous", |principal| principal.as_str())
}

fn record_audit(log: &AuditLog, event: AuditEvent<'_>) {
    if let Err(error) = log.record(event) {
        tracing::error!(%error, "failed to persist HTTP audit event");
    }
}
