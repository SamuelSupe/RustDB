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

enum PipeMessage {
    Item(Result<BatchEnvelope>),
    Done,
}

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
        let handle = self
            .runtime
            .as_ref()
            .expect("compute runtime is available until Engine drop")
            .handle();
        let spawn = context
            .tasks
            .spawn_on(handle, "query-producer", async move {
                loop {
                    let item = tokio::select! {
                        biased;
                        item = input.next() => item,
                        _ = producer_context.control.cancelled() => return Ok(()),
                    };
                    let Some(item) = item else { break };
                    let terminal = item.is_err();
                    if terminal {
                        producer_context.metrics.finish();
                        if let Err(error) = &item {
                            producer_context.record_task_failure(error);
                        }
                    }
                    if producer_context.control.is_cancelled() {
                        return Ok(());
                    }
                    let wait_started = Instant::now();
                    let sent = match sender.try_send(PipeMessage::Item(item)) {
                        Ok(()) => true,
                        Err(mpsc::error::TrySendError::Full(message)) => {
                            let backpressure_started = Instant::now();
                            let result = tokio::select! {
                                biased;
                                sent = sender.send(message) => Some(sent),
                                _ = producer_context.control.cancelled() => None,
                            };
                            producer_context
                                .metrics
                                .record_queue_backpressure_wait(backpressure_started.elapsed());
                            let Some(result) = result else { return Ok(()) };
                            result.is_ok()
                        }
                        Err(mpsc::error::TrySendError::Closed(_)) => false,
                    };
                    producer_context
                        .scheduler
                        .record_wait(wait_started.elapsed());
                    if !sent {
                        producer_context.cancel();
                        return Ok(());
                    }
                    if terminal {
                        return Ok(());
                    }
                }
                let sent = sender.send(PipeMessage::Done).await;
                if sent.is_err() {
                    producer_context.cancel();
                }
                Ok(())
            });
        if let Err(error) = spawn {
            context.record_task_failure(&error);
        }

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
    receiver: mpsc::Receiver<PipeMessage>,
    context: Arc<QueryContext>,
    completed: bool,
}

impl Stream for PipeReceiver {
    type Item = Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.receiver.poll_recv(context) {
            Poll::Ready(Some(PipeMessage::Item(Ok(batch)))) => {
                Poll::Ready(Some(Ok(batch.into_public())))
            }
            Poll::Ready(Some(PipeMessage::Item(Err(error)))) => {
                // Keep the cleanup guard armed until the stream wrapper has
                // awaited TaskGroup quiescence for this terminal error.
                Poll::Ready(Some(Err(error)))
            }
            Poll::Ready(Some(PipeMessage::Done)) => {
                self.completed = true;
                Poll::Ready(None)
            }
            Poll::Ready(None) => {
                if let Some(error) = self.context.tasks.first_failure() {
                    Poll::Ready(Some(Err(error)))
                } else if self.context.control.is_cancelled() {
                    Poll::Ready(Some(Err(Error::Cancelled)))
                } else {
                    let error = Error::Internal(
                        "query producer stopped without a terminal stream message".to_owned(),
                    );
                    self.context.record_task_failure(&error);
                    Poll::Ready(Some(Err(error)))
                }
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl Drop for PipeReceiver {
    fn drop(&mut self) {
        if !self.completed {
            self.context.cancel();
            self.context.schedule_cleanup();
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
    use futures::{StreamExt, TryStreamExt};

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
        let cleanup_spill = context.spill.clone();
        context.set_spill_cleanup_hook(move || {
            observed.fetch_add(1, Ordering::Relaxed);
            cleanup_spill.cleanup()
        });
        let runtime = ComputeRuntime::new(1).unwrap();
        let input = boxed_memory_batch_stream(futures::stream::pending());

        let output = runtime.pipe(input, Arc::clone(&context));
        drop(output);

        assert!(context.control.is_cancelled());
        tokio::time::timeout(Duration::from_secs(2), async {
            while cleanup_attempts.load(Ordering::Acquire) == 0 || directory.exists() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("query reaper must clean after the producer stops");
        assert!(!directory.exists());
        assert_eq!(cleanup_attempts.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn an_abandoned_consumer_releases_tasks_memory_and_spill() {
        abandoned_consumers_release_tasks_memory_and_spill(1).await;
    }

    #[tokio::test]
    #[ignore = "release soak; run explicitly before tagging"]
    async fn one_thousand_abandoned_consumers_release_soak() {
        abandoned_consumers_release_tasks_memory_and_spill(1_000).await;
    }

    async fn abandoned_consumers_release_tasks_memory_and_spill(iterations: usize) {
        let root = tempfile::tempdir().unwrap();
        let runtime = ComputeRuntime::new(2).unwrap();
        for iteration in 0..iterations {
            let memory = MemoryPool::new(1 << 20);
            let context = Arc::new(QueryContext::new(memory.clone(), root.path()).unwrap());
            let directory = context.spill.directory().to_owned();
            let output = runtime.pipe(
                boxed_memory_batch_stream(futures::stream::pending()),
                Arc::clone(&context),
            );
            drop(output);

            tokio::time::timeout(Duration::from_secs(2), async {
                while context.tasks.active_tasks() != 0 || directory.exists() {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap_or_else(|_| panic!("abandoned lifecycle {iteration} did not converge"));
            assert_eq!(context.tasks.active_tasks(), 0);
            assert_eq!(memory.used(), 0);
            assert!(!directory.exists());
        }
    }

    #[tokio::test]
    async fn producer_error_is_preserved_and_all_tasks_quiesce() {
        let temp = tempfile::tempdir().unwrap();
        let context = Arc::new(QueryContext::new(MemoryPool::new(1 << 20), temp.path()).unwrap());
        let directory = context.spill.directory().to_owned();
        let input = boxed_memory_batch_stream(futures::stream::once(async {
            Err(Error::Execution("injected producer error".to_owned()))
        }));
        let runtime = ComputeRuntime::new(1).unwrap();

        let error = runtime
            .pipe(input, Arc::clone(&context))
            .try_collect::<Vec<_>>()
            .await
            .unwrap_err();
        assert!(error.to_string().contains("injected producer error"));
        assert!(context.control.is_cancelled());
        tokio::time::timeout(Duration::from_secs(2), async {
            while context.tasks.active_tasks() != 0 || directory.exists() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("producer error must quiesce tasks and clean spill");
        assert_eq!(context.tasks.active_tasks(), 0);
        assert!(!directory.exists());
    }

    #[tokio::test]
    async fn runtime_shutdown_is_not_reported_as_successful_eof() {
        let temp = tempfile::tempdir().unwrap();
        let context = Arc::new(QueryContext::new(MemoryPool::new(1 << 20), temp.path()).unwrap());
        let runtime = ComputeRuntime::new(1).unwrap();
        let mut output = runtime.pipe(
            boxed_memory_batch_stream(futures::stream::pending()),
            Arc::clone(&context),
        );

        drop(runtime);

        let error = tokio::time::timeout(Duration::from_secs(2), output.next())
            .await
            .expect("runtime shutdown must close the public stream")
            .expect("runtime shutdown must emit one terminal error")
            .unwrap_err();
        assert!(
            error.to_string().contains("aborted before completion")
                || error
                    .to_string()
                    .contains("without a terminal stream message"),
            "unexpected error: {error}"
        );
        assert!(context.control.is_cancelled());
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
        assert!(context.metrics.snapshot().queue_backpressure_wait > Duration::ZERO);
        assert_eq!(pool.used(), 0);
    }
}
