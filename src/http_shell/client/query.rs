use std::time::Duration;

use reqwest::header;

use super::{RemoteClient, RemoteError, RemoteResult, success};
use crate::http_shell::types::{
    JsonResultPage, QueryListRequest, QueryListResponse, QueryState, QueryStatusResponse,
};

impl RemoteClient {
    pub async fn status(&self, query_id: &str) -> RemoteResult<QueryStatusResponse> {
        self.get_json(&format!("queries/{query_id}"), query_id)
            .await
    }

    pub async fn list_queries(
        &self,
        request: &QueryListRequest,
    ) -> RemoteResult<QueryListResponse> {
        let mut url = self.url("queries")?;
        {
            let mut query = url.query_pairs_mut();
            if let Some(state) = request.state {
                query.append_pair("state", query_state_name(state));
            }
            if let Some(created_after_ms) = request.created_after_ms {
                query.append_pair("created_after_ms", &created_after_ms.to_string());
            }
            if let Some(cursor) = &request.cursor {
                query.append_pair("cursor", cursor);
            }
            if let Some(limit) = request.limit {
                query.append_pair("limit", &limit.to_string());
            }
        }
        let response = self
            .authenticated(self.client.get(url))
            .send()
            .await
            .map_err(RemoteError::transport)?;
        success(response)
            .await?
            .json()
            .await
            .map_err(RemoteError::transport)
    }

    pub async fn wait(
        &self,
        query_id: &str,
        poll_interval: Duration,
    ) -> RemoteResult<QueryStatusResponse> {
        loop {
            let status = self.status(query_id).await?;
            if terminal_state(status.state) {
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
    ) -> RemoteResult<JsonResultPage> {
        let mut url = self.url(&format!("queries/{query_id}/results"))?;
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
            .map_err(RemoteError::transport)?;
        success(response)
            .await?
            .json()
            .await
            .map_err(RemoteError::transport)
    }

    pub async fn cancel(&self, query_id: &str) -> RemoteResult<QueryStatusResponse> {
        let response = self
            .authenticated(
                self.client
                    .post(self.url(&format!("queries/{query_id}/cancel"))?),
            )
            .send()
            .await
            .map_err(RemoteError::transport)?;
        success(response)
            .await?
            .json()
            .await
            .map_err(RemoteError::transport)
    }

    pub async fn delete(&self, query_id: &str) -> RemoteResult<()> {
        let response = self
            .authenticated(
                self.client
                    .delete(self.url(&format!("queries/{query_id}"))?),
            )
            .send()
            .await
            .map_err(RemoteError::transport)?;
        success(response).await.map(|_| ())
    }

    async fn get_json<T>(&self, path: &str, query_id: &str) -> RemoteResult<T>
    where
        T: serde::de::DeserializeOwned,
    {
        let response = self
            .authenticated(self.client.get(self.url(path)?))
            .send()
            .await
            .map_err(RemoteError::transport)?;
        success(response)
            .await
            .map_err(|error| error.with_query_id(query_id))?
            .json()
            .await
            .map_err(RemoteError::transport)
    }
}

fn terminal_state(state: QueryState) -> bool {
    !matches!(state, QueryState::Queued | QueryState::Running)
}

fn query_state_name(state: QueryState) -> &'static str {
    match state {
        QueryState::Queued => "queued",
        QueryState::Running => "running",
        QueryState::Succeeded => "succeeded",
        QueryState::Failed => "failed",
        QueryState::Cancelled => "cancelled",
        QueryState::Interrupted => "interrupted",
    }
}
