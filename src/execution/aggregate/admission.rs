use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Instant,
};

use tokio::sync::{Barrier, OwnedSemaphorePermit, Semaphore};

use crate::{Error, Result, runtime::QueryContext};

#[derive(Clone)]
pub(super) struct PartialMergeAdmission {
    ready: Arc<Barrier>,
    merge: Arc<Semaphore>,
    any_spilled: Arc<AtomicBool>,
}

impl PartialMergeAdmission {
    pub(super) fn new(lanes: usize) -> Self {
        Self {
            ready: Arc::new(Barrier::new(lanes.max(1))),
            merge: Arc::new(Semaphore::new(1)),
            any_spilled: Arc::new(AtomicBool::new(false)),
        }
    }

    pub(super) fn mark_spilled(&self) {
        self.any_spilled.store(true, Ordering::SeqCst);
    }

    pub(super) fn any_spilled(&self) -> bool {
        self.any_spilled.load(Ordering::SeqCst)
    }

    pub(super) async fn wait_ready(&self, context: &QueryContext) -> Result<()> {
        let started = Instant::now();
        let result = tokio::select! {
            biased;
            _ = context.control.cancelled() => Err(cancelled(context)),
            _ = self.ready.wait() => Ok(()),
        };
        context.scheduler.record_wait(started.elapsed());
        result
    }

    pub(super) async fn acquire_merge(
        &self,
        context: &QueryContext,
    ) -> Result<OwnedSemaphorePermit> {
        let started = Instant::now();
        let result = tokio::select! {
            biased;
            _ = context.control.cancelled() => Err(cancelled(context)),
            permit = Arc::clone(&self.merge).acquire_owned() => permit.map_err(|_| {
                Error::Internal("parallel aggregate spill merge gate closed unexpectedly".into())
            }),
        };
        context.scheduler.record_wait(started.elapsed());
        result
    }
}

fn cancelled(context: &QueryContext) -> Error {
    context
        .check_cancelled()
        .expect_err("cancelled query has a terminal error")
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use super::PartialMergeAdmission;
    use crate::{
        Error,
        runtime::{MemoryPool, QueryContext},
    };

    #[tokio::test]
    async fn barrier_wait_is_cancelled_when_a_lane_never_arrives() {
        let root = tempfile::tempdir().unwrap();
        let context = Arc::new(QueryContext::new(MemoryPool::new(64 << 20), root.path()).unwrap());
        let admission = PartialMergeAdmission::new(2);
        let waiting = tokio::spawn({
            let context = Arc::clone(&context);
            async move { admission.wait_ready(&context).await }
        });
        tokio::task::yield_now().await;
        context.cancel();

        let error = tokio::time::timeout(Duration::from_secs(1), waiting)
            .await
            .expect("barrier waiter must observe cancellation")
            .unwrap()
            .unwrap_err();
        assert!(matches!(error, Error::Cancelled));
    }

    #[tokio::test]
    async fn merge_wait_is_cancelled_while_the_permit_is_held() {
        let root = tempfile::tempdir().unwrap();
        let context = Arc::new(QueryContext::new(MemoryPool::new(64 << 20), root.path()).unwrap());
        let admission = PartialMergeAdmission::new(1);
        let _held = admission.acquire_merge(&context).await.unwrap();
        let waiting = tokio::spawn({
            let admission = admission.clone();
            let context = Arc::clone(&context);
            async move { admission.acquire_merge(&context).await }
        });
        tokio::task::yield_now().await;
        context.cancel();

        let error = tokio::time::timeout(Duration::from_secs(1), waiting)
            .await
            .expect("merge waiter must observe cancellation")
            .unwrap()
            .unwrap_err();
        assert!(matches!(error, Error::Cancelled));
    }
}
