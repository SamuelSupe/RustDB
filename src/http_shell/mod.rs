//! Read-only, TLS-protected HTTP query shell.

mod admin_socket;
mod arrow_transport;
mod client;
mod error;
mod json;
mod metrics;
mod query;
mod rejection;
mod request_context;
mod result_read;
mod result_store;
mod rss_guard;
pub mod security;
mod server;
mod service_io;
mod service_state;
mod tls_reload;
mod types;

pub use admin_socket::{AdminCommand, AdminResponse, send_admin_command};
pub use arrow_transport::{
    ARROW_RESULT_MEDIA_TYPE, ArrowResultBatch, ArrowResultPoll, ArrowResultState,
};
pub use client::{
    RemoteClient, RemoteClientBuilder, RemoteError, RemoteProfile, RemoteQueryHandle, RemoteResult,
};
pub use error::ErrorBody;
pub use query::QueryManagerConfig;
pub use result_store::ResultStoreConfig;
pub use server::{HttpServerConfig, serve, serve_with_shutdown};
pub use service_state::{
    ServiceStateIssue, ServiceStateReport, check_service_state, repair_service_state,
};
pub use types::{
    HttpQueryMetrics, InfoResponse, JsonResultPage, PageMetadata, QueryListRequest,
    QueryListResponse, QueryRequest, QueryState, QueryStatusResponse, SchemaColumn, SubmitResponse,
    TypedParameter,
};
