use std::fmt;

use reqwest::{Client, Response};
use serde::Deserialize;
use url::Url;
use uuid::Uuid;

use super::{
    security::ClientProfile,
    types::{QueryRequest, SubmitResponse},
};

mod arrow;
mod builder;
mod error;
mod handle;
mod query;

pub use builder::RemoteClientBuilder;
pub use error::{RemoteError, RemoteResult};
pub use handle::RemoteQueryHandle;

const API_ROOT: &str = "v2";

pub type RemoteProfile = ClientProfile;

#[derive(Clone)]
pub struct RemoteClient {
    client: Client,
    base: Url,
    token: String,
    submit_attempts: usize,
}

impl fmt::Debug for RemoteClient {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RemoteClient")
            .field("base", &self.base)
            .field("token", &"[REDACTED]")
            .field("submit_attempts", &self.submit_attempts)
            .finish()
    }
}

#[derive(Deserialize)]
struct ServerInfo {
    protocol_version: String,
    read_only: bool,
}

impl RemoteClient {
    pub fn builder(base: Url, token: impl Into<String>) -> RemoteClientBuilder {
        RemoteClientBuilder::new(base, token)
    }

    pub fn from_profile(profile: &ClientProfile) -> RemoteResult<Self> {
        RemoteClientBuilder::from_profile(profile)?.build()
    }

    pub async fn check_compatibility(&self) -> RemoteResult<()> {
        let response = self
            .authenticated(self.client.get(self.url("info")?))
            .send()
            .await
            .map_err(RemoteError::transport)?;
        let response = success(response).await?;
        let info: ServerInfo = response.json().await.map_err(RemoteError::transport)?;
        if info.protocol_version != API_ROOT || !info.read_only {
            return Err(RemoteError::protocol(format!(
                "remote server protocol '{}' is not a compatible read-only {API_ROOT} endpoint",
                info.protocol_version
            )));
        }
        Ok(())
    }

    /// Submits a query with a fresh idempotency key.
    ///
    /// Call [`Self::submit_with_key`] when the operation must be recoverable
    /// across caller restarts or transport failures.
    pub async fn submit(&self, request: &QueryRequest) -> RemoteResult<SubmitResponse> {
        let key = format!("rustdb-client-{}", Uuid::new_v4().simple());
        self.submit_with_key(request, &key).await
    }

    /// Submits a query with a caller-owned durable idempotency key.
    pub async fn submit_with_key(
        &self,
        request: &QueryRequest,
        idempotency_key: &str,
    ) -> RemoteResult<SubmitResponse> {
        let url = self.url("queries")?;
        let mut last_error = None;
        for attempt in 0..self.submit_attempts {
            let response = self
                .authenticated(self.client.post(url.clone()))
                .header("idempotency-key", idempotency_key)
                .json(request)
                .send()
                .await;
            let decoded = match response {
                Ok(response) => match success(response).await {
                    Ok(response) => response
                        .json::<SubmitResponse>()
                        .await
                        .map_err(RemoteError::transport),
                    Err(error) => Err(error),
                },
                Err(error) => Err(RemoteError::transport(error)),
            };
            match decoded {
                Ok(response) => return Ok(response),
                Err(error) if attempt + 1 < self.submit_attempts && error.submit_retryable() => {
                    if let Some(delay) = error.retry_after {
                        tokio::time::sleep(delay).await;
                    }
                    last_error = Some(error);
                }
                Err(error) => return Err(error),
            }
        }
        Err(last_error
            .unwrap_or_else(|| RemoteError::protocol("remote submission ended without a response")))
    }

    pub async fn submit_handle(&self, request: &QueryRequest) -> RemoteResult<RemoteQueryHandle> {
        let response = self.submit(request).await?;
        Ok(self.query(response.query_id))
    }

    pub async fn submit_handle_with_key(
        &self,
        request: &QueryRequest,
        idempotency_key: &str,
    ) -> RemoteResult<RemoteQueryHandle> {
        let response = self.submit_with_key(request, idempotency_key).await?;
        Ok(self.query(response.query_id))
    }

    pub fn query(&self, query_id: impl Into<String>) -> RemoteQueryHandle {
        RemoteQueryHandle::new(self.clone(), query_id.into())
    }

    fn authenticated(&self, request: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        request.bearer_auth(&self.token)
    }

    fn url(&self, path: &str) -> RemoteResult<Url> {
        self.base
            .join(&format!("{API_ROOT}/{path}"))
            .map_err(|error| {
                RemoteError::configuration(format!(
                    "server URL cannot resolve '{API_ROOT}/{path}': {error}"
                ))
            })
    }
}

async fn success(response: Response) -> RemoteResult<Response> {
    if response.status().is_success() {
        return Ok(response);
    }
    Err(RemoteError::from_response(response).await)
}

#[cfg(test)]
mod tests;
