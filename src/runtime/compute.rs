use std::{
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Poll},
    time::Instant,
};

use arrow::record_batch::RecordBatch;
use futures::{Stream, StreamExt};
use tokio::{runtime::Runtime, sync::mpsc};

use crate::{Error, Result};

use super::{
    BatchEnvelope, MemoryBatchStream, QueryContext, RecordBatchStream, boxed_record_batch_stream,
};

pub(crate) struct ComputeRuntime {
    runtime: Option<Runtime>,
    threads: usize,
}

impl ComputeRuntime {
    pub(crate) fn new(threads: usize) -> Result<Self> {
        let thread_id = Arc::new(AtomicUsize::new(0));
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(threads)
            .thread_name_fn(move || {
                let id = thread_id.fetch_add(1, Ordering::Relaxed);
                format!("rustdb-compute-{id}")
            })
            .enable_all()
            .build()
            .map_err(|error| Error::Internal(format!("cannot create compute runtime: {error}")))?;
        Ok(Self {
            runtime: Some(runtime),
            threads,
        })
    }

    /// Drives a lazy physical stream on the fixed-size engine runtime. The
    /// bounded channel applies backpressure when an embedded caller is slow.
    pub(crate) fn pipe(
        &self,
        mut input: MemoryBatchStream,
        context: Arc<QueryContext>,
    ) -> RecordBatchStream {
        const QUEUE_BATCHES: usize = 1;
        context.configure_compute_lanes(self.threads);
        let (sender, receiver) = mpsc::channel(QUEUE_BATCHES);
        let producer_context = Arc::clone(&context);
        self.runtime
            .as_ref()
            .expect("compute runtime is available until Engine drop")
            .handle()
            .spawn(async move {
                while let Some(item) = input.next().await {
                    let terminal = item.is_err();
                    if terminal {
                        producer_context.metrics.finish();
                    }
                    let wait_started = Instant::now();
                    let sent = sender.send(item).await;
                    producer_context
                        .scheduler
                        .record_wait(wait_started.elapsed());
                    if sent.is_err() {
                        producer_context.cancel();
                        return;
                    }
                    if terminal {
                        return;
                    }
                }
            });

        boxed_record_batch_stream(PipeReceiver {
            receiver,
            context,
            completed: false,
        })
    }
}

/// Cancels a producer as soon as its consumer is abandoned. Without this
/// guard, a blocking operator could keep scanning and spilling until its next
/// channel send observes that the receiver has gone away.
struct PipeReceiver {
    receiver: mpsc::Receiver<Result<BatchEnvelope>>,
    context: Arc<QueryContext>,
    completed: bool,
}

impl Stream for PipeReceiver {
    type Item = Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.receiver.poll_recv(context) {
            Poll::Ready(Some(Ok(batch))) => Poll::Ready(Some(Ok(batch.into_public()))),
            Poll::Ready(Some(Err(error))) => {
                // The producer treats an error as terminal and drops the
                // sender immediately, so this is completion rather than
                // consumer abandonment.
                self.completed = true;
                Poll::Ready(Some(Err(error)))
            }
            Poll::Ready(None) => {
                self.completed = true;
                Poll::Ready(None)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl Drop for PipeReceiver {
    fn drop(&mut self) {
        if !self.completed {
            self.context.cancel();
            if let Err(error) = self.context.cleanup_spill() {
                tracing::error!(
                    %error,
                    query_id = %self.context.query_id,
                    directory = %self.context.spill.directory().display(),
                    "failed to clean spill resources after query consumer abandonment"
                );
            }
        }
    }
}

impl Drop for ComputeRuntime {
    fn drop(&mut self) {
        if let Some(runtime) = self.runtime.take() {
            // Engine is commonly dropped from an async application. Tokio's
            // background shutdown is non-blocking and valid in that context.
            runtime.shutdown_background();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    };
    use std::time::Duration;

    use arrow::{
        array::Int64Array,
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    };
    use futures::TryStreamExt;

    use super::ComputeRuntime;
    use crate::{
        Error,
        runtime::{BatchEnvelope, MemoryPool, QueryContext, boxed_memory_batch_stream},
    };

    #[tokio::test]
    async fn pipes_batches_from_the_engine_runtime() {
        let temp = tempfile::tempdir().unwrap();
        let context = Arc::new(QueryContext::new(MemoryPool::new(1 << 20), temp.path()).unwrap());
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1_i64, 2]))]).unwrap();
        let ran_on_compute = Arc::new(AtomicBool::new(false));
        let observed = Arc::clone(&ran_on_compute);
        let batch_context = Arc::clone(&context);
        let input = boxed_memory_batch_stream(async_stream::stream! {
            observed.store(
                std::thread::current()
                    .name()
                    .is_some_and(|name| name.starts_with("rustdb-compute")),
                Ordering::Relaxed,
            );
            yield BatchEnvelope::try_new(batch, &batch_context.memory, "test");
        });
        let runtime = ComputeRuntime::new(1).unwrap();
        let batches = runtime
            .pipe(input, context)
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(batches[0].num_rows(), 2);
        assert!(ran_on_compute.load(Ordering::Relaxed));
    }

    #[tokio::test]
    async fn dropping_a_pipe_cancels_the_query_and_cleans_its_directory() {
        let temp = tempfile::tempdir().unwrap();
        let context = Arc::new(QueryContext::new(MemoryPool::new(1 << 20), temp.path()).unwrap());
        let directory = context.spill.directory().to_owned();
        let cleanup_attempts = Arc::new(AtomicUsize::new(0));
        let observed = Arc::clone(&cleanup_attempts);
        context.set_spill_cleanup_hook(move || {
            observed.fetch_add(1, Ordering::Relaxed);
            Err(Error::ResourceExhausted(
                "injected abandoned-consumer cleanup failure".to_owned(),
            ))
        });
        let runtime = ComputeRuntime::new(1).unwrap();
        let input = boxed_memory_batch_stream(futures::stream::pending());

        let output = runtime.pipe(input, Arc::clone(&context));
        drop(output);

        assert!(context.control.is_cancelled());
        assert!(!directory.exists());
        assert_eq!(cleanup_attempts.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn slow_consumer_keeps_every_queued_batch_leased() {
        let temp = tempfile::tempdir().unwrap();
        let pool = MemoryPool::new(1 << 20);
        let context = Arc::new(QueryContext::new(pool.clone(), temp.path()).unwrap());
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]));
        let sample = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(vec![1_i64; 256]))],
        )
        .unwrap();
        let batch_bytes = sample.get_array_memory_size();
        let produced = Arc::new(AtomicUsize::new(0));
        let observed = Arc::clone(&produced);
        let input_context = Arc::clone(&context);
        let input = boxed_memory_batch_stream(async_stream::stream! {
            for value in 0..3_i64 {
                let batch = RecordBatch::try_new(
                    Arc::clone(&schema),
                    vec![Arc::new(Int64Array::from(vec![value; 256]))],
                )
                .unwrap();
                observed.fetch_add(1, Ordering::Release);
                yield BatchEnvelope::try_new(batch, &input_context.memory, "slow consumer test");
            }
        });
        let runtime = ComputeRuntime::new(1).unwrap();
        let output = runtime.pipe(input, Arc::clone(&context));

        tokio::time::timeout(Duration::from_secs(2), async {
            while produced.load(Ordering::Acquire) != 2 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("producer must fill the one-batch queue and retain one pending batch");
        assert!(pool.used() >= batch_bytes.saturating_mul(2));
        assert!(context.metrics.snapshot().peak_memory_bytes >= pool.used() as u64);

        let batches = output.try_collect::<Vec<_>>().await.unwrap();
        assert_eq!(batches.len(), 3);
        assert_eq!(pool.used(), 0);
    }
}
