use std::{fmt, time::Duration};

use reqwest::{Client, Response, StatusCode, header};
use serde::Deserialize;
use url::Url;
use uuid::Uuid;

use crate::{Error, Result};

use super::{
    error::ErrorBody,
    security::ClientProfile,
    types::{JsonResultPage, QueryRequest, QueryState, QueryStatusResponse, SubmitResponse},
};

const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

pub type RemoteProfile = ClientProfile;

#[derive(Clone)]
pub struct RemoteClient {
    client: Client,
    base: Url,
    token: String,
}

impl fmt::Debug for RemoteClient {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RemoteClient")
            .field("base", &self.base)
            .field("token", &"[REDACTED]")
            .finish()
    }
}

#[derive(Deserialize)]
struct ServerInfo {
    protocol_version: String,
    read_only: bool,
}

impl RemoteClient {
    pub fn from_profile(profile: &ClientProfile) -> Result<Self> {
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
        let ca = profile.read_ca()?;
        let ca = reqwest::Certificate::from_pem(&ca)
            .map_err(|error| Error::InvalidArgument(format!("invalid profile CA: {error}")))?;
        let token = profile.read_token()?;
        let client = Client::builder()
            .https_only(true)
            .tls_certs_only([ca])
            .connect_timeout(Duration::from_secs(10))
            .timeout(REQUEST_TIMEOUT)
            .build()
            .map_err(remote_error)?;
        Ok(Self {
            client,
            base: profile.server_url().clone(),
            token,
        })
    }

    pub async fn check_compatibility(&self) -> Result<()> {
        let response = self
            .authenticated(self.client.get(self.url("v1/info")?))
            .send()
            .await
            .map_err(remote_error)?;
        let response = success(response).await?;
        let info: ServerInfo = response.json().await.map_err(remote_error)?;
        if info.protocol_version != "v1" || !info.read_only {
            return Err(Error::Unsupported(format!(
                "remote server protocol '{}' is not a compatible read-only v1 endpoint",
                info.protocol_version
            )));
        }
        Ok(())
    }

    pub async fn submit(&self, request: &QueryRequest) -> Result<SubmitResponse> {
        let key = format!("rustdb-cli-{}", Uuid::new_v4().simple());
        let url = self.url("v1/queries")?;
        let mut last_error = None;
        for attempt in 0..2 {
            let response = self
                .authenticated(self.client.post(url.clone()))
                .header("idempotency-key", &key)
                .json(request)
                .send()
                .await;
            let response = match response {
                Ok(response) => success(response).await?,
                Err(error) if attempt == 0 => {
                    last_error = Some(error);
                    continue;
                }
                Err(error) => return Err(remote_error(error)),
            };
            match response.json().await {
                Ok(response) => return Ok(response),
                Err(error) if attempt == 0 => last_error = Some(error),
                Err(error) => return Err(remote_error(error)),
            }
        }
        match last_error {
            Some(error) => Err(remote_error(error)),
            None => Err(Error::Internal(
                "remote submission ended without a response".to_owned(),
            )),
        }
    }

    pub async fn status(&self, query_id: &str) -> Result<QueryStatusResponse> {
        let response = self
            .authenticated(
                self.client
                    .get(self.url(&format!("v1/queries/{query_id}"))?),
            )
            .send()
            .await
            .map_err(remote_error)?;
        success(response).await?.json().await.map_err(remote_error)
    }

    pub async fn wait(
        &self,
        query_id: &str,
        poll_interval: Duration,
    ) -> Result<QueryStatusResponse> {
        loop {
            let status = self.status(query_id).await?;
            if matches!(
                status.state,
                QueryState::Succeeded | QueryState::Failed | QueryState::Cancelled
            ) {
                return Ok(status);
            }
            tokio::time::sleep(poll_interval).await;
        }
    }

    pub async fn page(
        &self,
        query_id: &str,
        cursor: Option<&str>,
        limit: usize,
    ) -> Result<JsonResultPage> {
        let mut url = self.url(&format!("v1/queries/{query_id}/results"))?;
        {
            let mut query = url.query_pairs_mut();
            query.append_pair("limit", &limit.to_string());
            if let Some(cursor) = cursor {
                query.append_pair("cursor", cursor);
            }
        }
        let response = self
            .authenticated(self.client.get(url))
            .header(header::ACCEPT, "application/json")
            .send()
            .await
            .map_err(remote_error)?;
        success(response).await?.json().await.map_err(remote_error)
    }

    pub async fn cancel(&self, query_id: &str) -> Result<QueryStatusResponse> {
        let response = self
            .authenticated(
                self.client
                    .post(self.url(&format!("v1/queries/{query_id}/cancel"))?),
            )
            .send()
            .await
            .map_err(remote_error)?;
        success(response).await?.json().await.map_err(remote_error)
    }

    pub async fn delete(&self, query_id: &str) -> Result<()> {
        let response = self
            .authenticated(
                self.client
                    .delete(self.url(&format!("v1/queries/{query_id}"))?),
            )
            .send()
            .await
            .map_err(remote_error)?;
        success(response).await.map(|_| ())
    }

    fn authenticated(&self, request: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        request.bearer_auth(&self.token)
    }

    fn url(&self, path: &str) -> Result<Url> {
        self.base.join(path).map_err(|error| {
            Error::InvalidArgument(format!(
                "profile server URL cannot resolve '{path}': {error}"
            ))
        })
    }
}

async fn success(response: Response) -> Result<Response> {
    if response.status().is_success() {
        return Ok(response);
    }
    let status = response.status();
    let body = response.json::<ErrorBody>().await.ok();
    Err(match body {
        Some(body) => Error::Execution(format!(
            "remote {} (HTTP {}): {}{}",
            body.error,
            status.as_u16(),
            body.message,
            body.query_id
                .map(|query_id| format!(" [query {query_id}]"))
                .unwrap_or_default()
        )),
        None if status == StatusCode::UNAUTHORIZED => {
            Error::Execution("remote authentication failed".into())
        }
        None => Error::Execution(format!("remote HTTP request failed with status {status}")),
    })
}

fn remote_error(error: reqwest::Error) -> Error {
    Error::Execution(format!("remote HTTP error: {error}"))
}
