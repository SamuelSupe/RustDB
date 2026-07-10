use std::{
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use arrow::record_batch::RecordBatch;
use futures::{Stream, StreamExt};
use tokio::{runtime::Runtime, sync::mpsc};

use crate::{Error, Result};

use super::{QueryContext, RecordBatchStream, boxed_record_batch_stream};

pub(crate) struct ComputeRuntime {
    runtime: Option<Runtime>,
}

impl ComputeRuntime {
    pub(crate) fn new(threads: usize) -> Result<Self> {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(threads)
            .thread_name("rustdb-compute")
            .enable_all()
            .build()
            .map_err(|error| Error::Internal(format!("cannot create compute runtime: {error}")))?;
        Ok(Self {
            runtime: Some(runtime),
        })
    }

    /// Drives a lazy physical stream on the fixed-size engine runtime. The
    /// bounded channel applies backpressure when an embedded caller is slow.
    pub(crate) fn pipe(
        &self,
        mut input: RecordBatchStream,
        context: Arc<QueryContext>,
    ) -> RecordBatchStream {
        const QUEUE_BATCHES: usize = 2;
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
                        let _ = producer_context.spill.cleanup();
                    }
                    if sender.send(item).await.is_err() {
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
    receiver: mpsc::Receiver<Result<RecordBatch>>,
    context: Arc<QueryContext>,
    completed: bool,
}

impl Stream for PipeReceiver {
    type Item = Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let item = self.receiver.poll_recv(context);
        if matches!(&item, Poll::Ready(None)) {
            self.completed = true;
        }
        item
    }
}

impl Drop for PipeReceiver {
    fn drop(&mut self) {
        if !self.completed {
            self.context.cancel();
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
        atomic::{AtomicBool, Ordering},
    };

    use arrow::{
        array::Int64Array,
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    };
    use futures::TryStreamExt;

    use super::ComputeRuntime;
    use crate::runtime::{MemoryPool, QueryContext, boxed_record_batch_stream};

    #[tokio::test]
    async fn pipes_batches_from_the_engine_runtime() {
        let temp = tempfile::tempdir().unwrap();
        let context = Arc::new(QueryContext::new(MemoryPool::new(1 << 20), temp.path()).unwrap());
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1_i64, 2]))]).unwrap();
        let ran_on_compute = Arc::new(AtomicBool::new(false));
        let observed = Arc::clone(&ran_on_compute);
        let input = boxed_record_batch_stream(async_stream::stream! {
            observed.store(
                std::thread::current()
                    .name()
                    .is_some_and(|name| name.starts_with("rustdb-compute")),
                Ordering::Relaxed,
            );
            yield Ok(batch);
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
        let runtime = ComputeRuntime::new(1).unwrap();
        let input = boxed_record_batch_stream(futures::stream::pending());

        let output = runtime.pipe(input, Arc::clone(&context));
        drop(output);

        assert!(context.control.is_cancelled());
        assert!(!directory.exists());
    }
}
