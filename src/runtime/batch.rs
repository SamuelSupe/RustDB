use std::{fmt, ops::Deref, pin::Pin, sync::Arc};

use arrow::{
    datatypes::{DataType, IntervalUnit, Schema},
    record_batch::RecordBatch,
};
use futures::{Stream, StreamExt};

use crate::{Error, Result};

use super::{MemoryPool, MemoryReservation, QueryContext, RecordBatchStream};

/// A RecordBatch retained inside the engine together with its query-memory
/// lease. The lease is released only when ownership crosses the public result
/// boundary or the batch is dropped.
pub(crate) struct BatchEnvelope {
    batch: RecordBatch,
    memory: MemoryReservation,
}

impl fmt::Debug for BatchEnvelope {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("BatchEnvelope")
            .field("batch", &self.batch)
            .field("memory_size", &self.memory.size())
            .finish()
    }
}

impl Deref for BatchEnvelope {
    type Target = RecordBatch;

    fn deref(&self) -> &Self::Target {
        &self.batch
    }
}

impl BatchEnvelope {
    pub(crate) fn try_new(
        batch: RecordBatch,
        pool: &MemoryPool,
        owner: &'static str,
    ) -> Result<Self> {
        let bytes = batch.get_array_memory_size();
        let memory = pool.try_reserve(bytes).map_err(|_| {
            Error::ResourceExhausted(format!(
                "{owner} batch requires {bytes} bytes (query limit {}, available {})",
                pool.limit(),
                pool.available(),
            ))
        })?;
        Ok(Self { batch, memory })
    }

    /// Installs a batch into a reservation acquired before decoding or running
    /// a kernel. If the conservative credit was too small, the missing bytes
    /// are acquired immediately; the batch is dropped instead of waiting in an
    /// unleased state when that growth cannot fit.
    pub(crate) fn from_reservation(
        batch: RecordBatch,
        mut memory: MemoryReservation,
        owner: &'static str,
    ) -> Result<Self> {
        let bytes = batch.get_array_memory_size();
        memory.try_resize(bytes).map_err(|_| {
            Error::ResourceExhausted(format!(
                "{owner} batch requires {bytes} bytes (query limit {}, available {}); one batch exceeded its pre-reserved credit",
                memory.pool().limit(),
                memory.pool().available(),
            ))
        })?;
        Ok(Self { batch, memory })
    }

    pub(crate) fn batch(&self) -> &RecordBatch {
        &self.batch
    }

    pub(crate) fn memory_size(&self) -> usize {
        self.memory.size()
    }

    /// Replaces a batch while retaining the same logical lease. Arrow kernels
    /// may allocate before their exact output size is known; if the resized
    /// lease cannot fit, the new batch is dropped with the returned error.
    pub(crate) fn replace(mut self, batch: RecordBatch, owner: &'static str) -> Result<Self> {
        let bytes = batch.get_array_memory_size();
        let current = self.memory.size();
        if bytes > current {
            self.memory.try_grow(bytes - current).map_err(|_| {
                Error::ResourceExhausted(format!(
                    "{owner} batch requires {bytes} bytes (query limit {}, available {})",
                    self.memory.pool().limit(),
                    self.memory.pool().available(),
                ))
            })?;
        }
        let previous = std::mem::replace(&mut self.batch, batch);
        drop(previous);
        self.memory.shrink(self.memory.size().saturating_sub(bytes));
        Ok(self)
    }

    /// Replaces a batch while transferring a workspace reservation acquired
    /// before the kernel ran. The input and workspace leases jointly cover the
    /// old and new buffers until the old batch is dropped, then collapse to the
    /// exact output lease.
    pub(crate) fn replace_with_reservation(
        self,
        batch: RecordBatch,
        workspace: MemoryReservation,
        owner: &'static str,
    ) -> Result<Self> {
        let mut this = self;
        this.memory.absorb(workspace)?;
        this.replace(batch, owner)
    }

    pub(crate) fn into_parts(self) -> (RecordBatch, MemoryReservation) {
        (self.batch, self.memory)
    }

    /// Releases engine accounting immediately before yielding the batch to an
    /// embedding caller, whose retained result memory is outside the budget.
    pub(crate) fn into_public(self) -> RecordBatch {
        let (batch, memory) = self.into_parts();
        drop(memory);
        batch
    }
}

pub(crate) type MemoryBatchStream =
    Pin<Box<dyn Stream<Item = Result<BatchEnvelope>> + Send + 'static>>;

pub(crate) fn boxed_memory_batch_stream<S>(stream: S) -> MemoryBatchStream
where
    S: Stream<Item = Result<BatchEnvelope>> + Send + 'static,
{
    Box::pin(stream)
}

/// Conservative credit for one decoded or kernel-produced array. Exact Arrow
/// buffers are reconciled before the batch becomes visible to another task.
pub(crate) fn estimate_array_bytes(data_type: &DataType, rows: usize) -> usize {
    let validity = rows.div_ceil(8);
    let fixed = |width: usize| {
        rows.saturating_mul(width)
            .saturating_add(validity)
            .saturating_add(256)
    };
    match data_type {
        DataType::Null => 256,
        DataType::Boolean => validity.saturating_mul(2).saturating_add(256),
        DataType::Int8 | DataType::UInt8 => fixed(1),
        DataType::Int16 | DataType::UInt16 | DataType::Float16 => fixed(2),
        DataType::Int32
        | DataType::UInt32
        | DataType::Float32
        | DataType::Date32
        | DataType::Time32(_)
        | DataType::Interval(IntervalUnit::YearMonth) => fixed(4),
        DataType::Int64
        | DataType::UInt64
        | DataType::Float64
        | DataType::Date64
        | DataType::Time64(_)
        | DataType::Timestamp(_, _)
        | DataType::Duration(_)
        | DataType::Interval(IntervalUnit::DayTime) => fixed(8),
        DataType::Decimal128(_, _) | DataType::Interval(IntervalUnit::MonthDayNano) => fixed(16),
        DataType::Decimal256(_, _) => fixed(32),
        DataType::Utf8 | DataType::Binary => rows
            .saturating_mul(36)
            .saturating_add(validity)
            .saturating_add(512),
        DataType::LargeUtf8 | DataType::LargeBinary | DataType::Utf8View | DataType::BinaryView => {
            rows.saturating_mul(40)
                .saturating_add(validity)
                .saturating_add(512)
        }
        DataType::FixedSizeBinary(width) => fixed(usize::try_from(*width).unwrap_or(usize::MAX)),
        DataType::Dictionary(_, value) => estimate_array_bytes(value, rows),
        DataType::Struct(fields) => fields.iter().fold(256usize, |bytes, field| {
            bytes.saturating_add(estimate_array_bytes(field.data_type(), rows))
        }),
        DataType::FixedSizeList(field, width) => estimate_array_bytes(
            field.data_type(),
            rows.saturating_mul(usize::try_from(*width).unwrap_or(usize::MAX)),
        )
        .saturating_add(validity)
        .saturating_add(256),
        DataType::List(field)
        | DataType::ListView(field)
        | DataType::LargeList(field)
        | DataType::LargeListView(field)
        | DataType::Map(field, _) => estimate_array_bytes(field.data_type(), rows)
            .saturating_add(rows.saturating_mul(16))
            .saturating_add(validity)
            .saturating_add(256),
        _ => rows
            .saturating_mul(64)
            .saturating_add(validity)
            .saturating_add(512),
    }
    .max(1)
}

pub(crate) fn estimate_schema_batch_bytes(schema: &Schema, rows: usize) -> usize {
    schema.fields().iter().fold(512usize, |bytes, field| {
        bytes.saturating_add(estimate_array_bytes(field.data_type(), rows))
    })
}

/// Converts a public batch stream at an execution boundary while leaving an
/// already-accounted internal stream untouched. This is primarily useful for
/// small embedded providers and focused operator tests.
pub(crate) trait IntoMemoryBatchStream {
    fn into_memory_batch_stream(
        self,
        context: Arc<QueryContext>,
        owner: &'static str,
    ) -> MemoryBatchStream;
}

impl IntoMemoryBatchStream for MemoryBatchStream {
    fn into_memory_batch_stream(
        self,
        _context: Arc<QueryContext>,
        _owner: &'static str,
    ) -> MemoryBatchStream {
        self
    }
}

impl IntoMemoryBatchStream for RecordBatchStream {
    fn into_memory_batch_stream(
        mut self,
        context: Arc<QueryContext>,
        owner: &'static str,
    ) -> MemoryBatchStream {
        // This compatibility path accepts batches already allocated by an
        // embedding provider or operator test, so no schema contract is
        // available before the first poll. Take an admission token before
        // polling and reconcile exact buffers immediately. File decoders never
        // use this compatibility path: ScanTask/scan.rs reserve a conservative
        // credit from their projected schema and configured batch size.
        let preclaim = 1;
        boxed_memory_batch_stream(async_stream::try_stream! {
            loop {
                let reservation = context.reserve_memory(preclaim, owner).await?;
                let next = tokio::select! {
                    _ = context.control.cancelled() => Err(Error::Cancelled),
                    next = self.next() => Ok(next),
                }?;
                let Some(batch) = next else {
                    break;
                };
                yield BatchEnvelope::from_reservation(batch?, reservation, owner)?;
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::{
        array::Int64Array,
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    };

    use super::BatchEnvelope;
    use crate::runtime::MemoryPool;

    fn batch(values: Vec<i64>) -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)])),
            vec![Arc::new(Int64Array::from(values))],
        )
        .unwrap()
    }

    #[test]
    fn lease_tracks_replacement_and_public_handoff() {
        let pool = MemoryPool::new(1 << 20);
        let envelope = BatchEnvelope::try_new(batch(vec![1, 2, 3]), &pool, "scan").unwrap();
        assert_eq!(pool.used(), envelope.memory_size());
        let envelope = envelope.replace(batch(vec![1]), "filter").unwrap();
        assert_eq!(pool.used(), envelope.memory_size());
        let public = envelope.into_public();
        assert_eq!(public.num_rows(), 1);
        assert_eq!(pool.used(), 0);
    }
}
