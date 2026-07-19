//! Read-only, TLS-protected HTTP query shell.

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
pub mod security;
mod server;
mod types;

pub use arrow_transport::{ARROW_RESULT_MEDIA_TYPE, ArrowResultBatch, ArrowResultPoll};
pub use client::{RemoteClient, RemoteProfile};
pub use error::ErrorBody;
pub use query::QueryManagerConfig;
pub use result_store::ResultStoreConfig;
pub use server::{HttpServerConfig, serve, serve_with_shutdown};
pub use types::{
    HttpQueryMetrics, InfoResponse, JsonResultPage, PageMetadata, QueryRequest, QueryState,
    QueryStatusResponse, SchemaColumn, SubmitResponse, TypedParameter,
};
