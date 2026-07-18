use std::sync::Arc;

use object_store::{ObjectStore, ObjectStoreExt, path::Path as ObjectPath};
use parking_lot::Mutex;
use tokio::runtime::Handle;

use crate::{
    Error, Result,
    runtime::{AsyncCleanupGuard, TaskGroup},
};

use super::multipart::SharedMultipart;

pub(super) struct PendingRemoteCleanup {
    uri: String,
    store: Arc<dyn ObjectStore>,
    data: ObjectPath,
    upload: SharedMultipart,
    guard: AsyncCleanupGuard,
}

impl PendingRemoteCleanup {
    pub(super) fn new(
        uri: String,
        store: Arc<dyn ObjectStore>,
        data: ObjectPath,
        upload: SharedMultipart,
        guard: AsyncCleanupGuard,
    ) -> Self {
        Self {
            uri,
            store,
            data,
            upload,
            guard,
        }
    }

    async fn run(self) -> Result<()> {
        let Self {
            uri,
            store,
            data,
            upload,
            guard,
        } = self;
        let mut failures = Vec::new();
        if let Err(error) = upload.abort().await {
            failures.push(format!("multipart abort failed: {error}"));
        }
        if let Err(error) = store.delete(&data).await
            && !matches!(error, object_store::Error::NotFound { .. })
        {
            failures.push(format!("object deletion failed: {error}"));
        }
        let result = if failures.is_empty() {
            Ok(())
        } else {
            Err(Error::Execution(format!(
                "remote COPY fallback cleanup failed for '{uri}': {}",
                failures.join("; ")
            )))
        };
        // Keep cancellation protection armed until every asynchronous cleanup
        // operation above has reached a terminal result.
        drop(guard);
        result
    }
}

pub(super) fn schedule(tasks: &TaskGroup, cleanup: PendingRemoteCleanup) {
    let uri = cleanup.uri.clone();
    let state = Arc::new(Mutex::new(Some(cleanup)));
    let handle = match Handle::try_current() {
        Ok(handle) => handle,
        Err(error) => {
            tracing::error!(%error, %uri, "remote COPY cleanup has no active Tokio runtime");
            spawn_cleanup_thread(state, uri);
            return;
        }
    };
    let task_state = Arc::clone(&state);
    let spawn = tasks.spawn_cleanup_on(&handle, "remote-copy-drop-cleanup", async move {
        run_once(task_state).await
    });
    if let Err(error) = spawn {
        // A fully quiescent group cannot be reopened. Retain the guard on an
        // independent cleanup thread instead of detaching work from the query
        // runtime and reporting false quiescence.
        tracing::error!(%error, %uri, "query task group rejected remote COPY cleanup");
        spawn_cleanup_thread(state, uri);
    }
}

async fn run_once(state: Arc<Mutex<Option<PendingRemoteCleanup>>>) -> Result<()> {
    let cleanup = state.lock().take();
    match cleanup {
        Some(cleanup) => cleanup.run().await,
        None => Ok(()),
    }
}

fn spawn_cleanup_thread(state: Arc<Mutex<Option<PendingRemoteCleanup>>>, uri: String) {
    let spawn = std::thread::Builder::new()
        .name("rustdb-copy-cleanup".to_owned())
        .spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build();
            match runtime {
                Ok(runtime) => {
                    if let Err(error) = runtime.block_on(run_once(state)) {
                        tracing::error!(%error, "remote COPY cleanup thread failed");
                    }
                }
                Err(error) => {
                    tracing::error!(%error, %uri, "cannot create remote COPY cleanup runtime")
                }
            }
        });
    if let Err(error) = spawn {
        tracing::error!(%error, "cannot start remote COPY cleanup thread");
    }
}
