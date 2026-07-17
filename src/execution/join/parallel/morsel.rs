use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use futures::StreamExt;
use tokio_util::sync::CancellationToken;

use crate::{
    Error, Result,
    runtime::{BatchEnvelope, MemoryBatchStream, QueryContext},
};

/// Serializes source polling while allowing one retained Arrow batch to feed
/// several probe lanes through zero-copy slices.
pub(super) struct MorselInput {
    input: MemoryBatchStream,
    current: Option<Arc<BatchEnvelope>>,
    offset: usize,
}

pub(super) struct ProbeMorsel {
    batch: RecordBatch,
    source: Arc<BatchEnvelope>,
}

impl MorselInput {
    pub(super) fn new(input: MemoryBatchStream) -> Self {
        Self {
            input,
            current: None,
            offset: 0,
        }
    }

    pub(super) async fn next(
        &mut self,
        target_rows: usize,
        cancellation: &CancellationToken,
        context: &QueryContext,
    ) -> Result<Option<ProbeMorsel>> {
        loop {
            if cancellation.is_cancelled() {
                return Err(Error::Cancelled);
            }
            context.check_cancelled()?;

            if let Some(source) = &self.current {
                let rows = source.batch().num_rows();
                if self.offset < rows {
                    let start = self.offset;
                    let end = start.saturating_add(target_rows.max(1)).min(rows);
                    let source = Arc::clone(source);
                    let batch = source.batch().slice(start, end - start);
                    self.offset = end;
                    if end == rows {
                        self.current = None;
                        self.offset = 0;
                    }
                    return Ok(Some(ProbeMorsel { batch, source }));
                }
                self.current = None;
                self.offset = 0;
            }

            let next = tokio::select! {
                _ = cancellation.cancelled() => return Err(Error::Cancelled),
                _ = context.control.cancelled() => return Err(Error::Cancelled),
                next = self.input.next() => next,
            };
            let Some(batch) = next else {
                return Ok(None);
            };
            self.current = Some(Arc::new(batch?));
        }
    }
}

impl ProbeMorsel {
    pub(super) fn batch(&self) -> &RecordBatch {
        &self.batch
    }

    /// The source reservation is shared across all slices. Reporting the full
    /// lease is conservative for one-operation workspace admission and does
    /// not acquire the bytes again.
    pub(super) fn retained_bytes(&self) -> usize {
        self.source.memory_size()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::{
        array::{Array, Int64Array},
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    };
    use futures::stream;

    use super::*;
    use crate::runtime::{MemoryPool, QueryContext, boxed_memory_batch_stream};

    #[tokio::test]
    async fn slices_one_accounted_batch_without_duplicate_reservations() {
        let root = tempfile::tempdir().unwrap();
        let context = QueryContext::shared(MemoryPool::new(1 << 20), root.path()).unwrap();
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            false,
        )]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from_iter_values(0..10))])
                .unwrap();
        let envelope = BatchEnvelope::try_new(batch, &context.memory, "morsel input test").unwrap();
        let retained = envelope.memory_size();
        let stream = boxed_memory_batch_stream(stream::iter(vec![Ok(envelope)]));
        let mut input = MorselInput::new(stream);
        let cancellation = CancellationToken::new();

        let mut morsels = Vec::new();
        while let Some(morsel) = input.next(3, &cancellation, &context).await.unwrap() {
            morsels.push(morsel);
        }
        assert_eq!(
            morsels
                .iter()
                .map(|morsel| morsel.batch().num_rows())
                .collect::<Vec<_>>(),
            vec![3, 3, 3, 1]
        );
        assert!(
            morsels
                .iter()
                .all(|morsel| morsel.retained_bytes() == retained)
        );
        let values = morsels
            .iter()
            .flat_map(|morsel| {
                let values = morsel
                    .batch()
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                (0..values.len())
                    .map(|row| values.value(row))
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        assert_eq!(values, (0..10).collect::<Vec<_>>());
        assert_eq!(context.memory.used(), retained);

        let last = morsels.pop().unwrap();
        drop(morsels);
        assert_eq!(context.memory.used(), retained);
        drop(last);
        assert_eq!(context.memory.used(), 0);
    }
}
