use std::{fmt, time::Duration};

use super::{RemoteClient, RemoteResult};
use crate::http_shell::{ArrowResultPoll, JsonResultPage, QueryStatusResponse};

/// A lightweight owner for one durable remote query identifier.
#[derive(Clone)]
pub struct RemoteQueryHandle {
    client: RemoteClient,
    query_id: String,
}

impl RemoteQueryHandle {
    pub(crate) fn new(client: RemoteClient, query_id: String) -> Self {
        Self { client, query_id }
    }

    pub fn query_id(&self) -> &str {
        &self.query_id
    }

    pub async fn status(&self) -> RemoteResult<QueryStatusResponse> {
        self.client.status(&self.query_id).await
    }

    pub async fn wait(&self, poll_interval: Duration) -> RemoteResult<QueryStatusResponse> {
        self.client.wait(&self.query_id, poll_interval).await
    }

    pub async fn results(
        &self,
        cursor: Option<&str>,
        limit: usize,
    ) -> RemoteResult<JsonResultPage> {
        self.client.page(&self.query_id, cursor, limit).await
    }

    pub async fn arrow_batch(&self, batch_seq: u64) -> RemoteResult<ArrowResultPoll> {
        self.client.arrow_batch(&self.query_id, batch_seq).await
    }

    pub async fn cancel(&self) -> RemoteResult<QueryStatusResponse> {
        self.client.cancel(&self.query_id).await
    }

    pub async fn delete(&self) -> RemoteResult<()> {
        self.client.delete(&self.query_id).await
    }
}

impl fmt::Debug for RemoteQueryHandle {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RemoteQueryHandle")
            .field("query_id", &self.query_id)
            .finish_non_exhaustive()
    }
}
