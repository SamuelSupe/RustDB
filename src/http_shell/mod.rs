//! Read-only, TLS-protected HTTP query shell.

mod client;
mod error;
mod json;
mod query;
mod result_read;
mod result_store;
pub mod security;
mod server;
mod types;

pub use client::{RemoteClient, RemoteProfile};
pub use error::ErrorBody;
pub use query::QueryManagerConfig;
pub use result_store::ResultStoreConfig;
pub use server::{HttpServerConfig, serve, serve_with_shutdown};
pub use types::{
    HttpQueryMetrics, InfoResponse, JsonResultPage, PageMetadata, QueryRequest, QueryState,
    QueryStatusResponse, SchemaColumn, SubmitResponse, TypedParameter,
};
