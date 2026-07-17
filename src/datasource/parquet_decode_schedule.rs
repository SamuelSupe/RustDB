use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::{Duration, Instant},
};

use futures::{FutureExt, Stream, future::BoxFuture};

use crate::{
    Result,
    runtime::{GlobalComputePermit, QueryContext},
};

type AcquireFuture = BoxFuture<'static, Result<GlobalComputePermit>>;

/// Polls an async decoder under one engine-wide compute slot.
///
/// The slot is released whenever the decoder reports `Pending`, so object I/O
/// never occupies compute capacity. A ready item returns the slot to the
/// caller so immediate post-decode alignment can share the same admission.
pub(super) fn next_with_compute<'a, S>(
    stream: &'a mut S,
    context: Arc<QueryContext>,
) -> ScheduledNext<'a, S>
where
    S: Stream + Unpin,
{
    ScheduledNext {
        stream,
        context,
        acquire: None,
        decode_active: Duration::ZERO,
        compute_permit_wait: Duration::ZERO,
        decode_polls: 0,
        pending_polls: 0,
    }
}

pub(super) struct ScheduledNext<'a, S> {
    stream: &'a mut S,
    context: Arc<QueryContext>,
    acquire: Option<AcquireFuture>,
    decode_active: Duration,
    compute_permit_wait: Duration,
    decode_polls: u64,
    pending_polls: u64,
}

impl<S> ScheduledNext<'_, S> {
    fn flush_metrics(&mut self) {
        self.context.metrics.record_parquet_decode_activity(
            self.decode_active,
            self.decode_polls,
            self.pending_polls,
        );
        self.context
            .metrics
            .record_parquet_decode_compute_permit_wait(self.compute_permit_wait);
        self.decode_active = Duration::ZERO;
        self.compute_permit_wait = Duration::ZERO;
        self.decode_polls = 0;
        self.pending_polls = 0;
    }
}

impl<S> Drop for ScheduledNext<'_, S> {
    fn drop(&mut self) {
        self.flush_metrics();
    }
}

impl<S> Future for ScheduledNext<'_, S>
where
    S: Stream + Unpin,
{
    type Output = Result<Option<(S::Item, GlobalComputePermit)>>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        if let Err(error) = this.context.check_cancelled() {
            return Poll::Ready(Err(error));
        }

        if this.acquire.is_none() {
            let context = Arc::clone(&this.context);
            this.acquire = Some(async move { context.acquire_compute().await }.boxed());
        }

        let acquired = match this
            .acquire
            .as_mut()
            .expect("acquire future was initialized")
            .as_mut()
            .poll(cx)
        {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(acquired) => acquired,
        };
        this.acquire = None;
        let permit = match acquired {
            Ok(permit) => permit,
            Err(error) => return Poll::Ready(Err(error)),
        };
        this.compute_permit_wait = this.compute_permit_wait.saturating_add(permit.wait_time());
        if let Err(error) = this.context.check_cancelled() {
            drop(permit);
            return Poll::Ready(Err(error));
        }

        let started = Instant::now();
        let polled = {
            let _active = this.context.scheduler.enter_lane();
            Pin::new(&mut *this.stream).poll_next(cx)
        };
        this.decode_active = this.decode_active.saturating_add(started.elapsed());
        this.decode_polls = this.decode_polls.saturating_add(1);
        if polled.is_pending() {
            this.pending_polls = this.pending_polls.saturating_add(1);
        } else {
            this.flush_metrics();
        }
        match polled {
            Poll::Ready(Some(item)) => Poll::Ready(Ok(Some((item, permit)))),
            Poll::Ready(None) => {
                drop(permit);
                Poll::Ready(Ok(None))
            }
            Poll::Pending => {
                drop(permit);
                Poll::Pending
            }
        }
    }
}

#[cfg(test)]
#[path = "parquet_decode_schedule_tests.rs"]
mod tests;
