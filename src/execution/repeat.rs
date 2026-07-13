use std::{mem::size_of, sync::Arc};

use arrow::{
    array::{Array, Int64Array, UInt32Array, UInt64Array},
    compute::take_record_batch,
    datatypes::SchemaRef,
    record_batch::RecordBatch,
};
use futures::StreamExt;

use crate::{
    Error, Result,
    runtime::{
        BatchEnvelope, MemoryBatchStream, QueryContext, boxed_memory_batch_stream,
        estimate_schema_batch_bytes,
    },
    sql::BoundExpr,
};

use super::expr;

pub(super) fn repeat(
    mut input: MemoryBatchStream,
    count: BoundExpr,
    schema: SchemaRef,
    context: Arc<QueryContext>,
    batch_size: usize,
) -> MemoryBatchStream {
    boxed_memory_batch_stream(async_stream::try_stream! {
        let batch_size = batch_size.max(1);
        while let Some(batch) = input.next().await {
            context.check_cancelled()?;
            let batch = batch?;
            let count_workspace = context
                .reserve_memory_while_holding(
                    expr::projection_workspace_bytes(std::slice::from_ref(&count), batch.batch()),
                    batch.memory_size(),
                    "repeat count workspace",
                )
                .await?;
            let counts = expr::evaluate(&count, batch.batch())?;
            let source = output_source(batch.batch(), Arc::clone(&schema))?;
            let mut row = 0usize;
            let mut remaining = 0u64;

            loop {
                while remaining == 0 && row < batch.num_rows() {
                    remaining = repeat_count(counts.as_ref(), row)?;
                    if remaining == 0 {
                        row += 1;
                    }
                }
                if remaining == 0 {
                    break;
                }
                let estimate = estimate_schema_batch_bytes(schema.as_ref(), batch_size)
                    .saturating_add(batch_size.saturating_mul(size_of::<u32>()))
                    .max(1);
                let workspace = context
                    .reserve_memory_while_holding(
                        estimate,
                        batch.memory_size().saturating_add(count_workspace.size()),
                        "repeat output workspace",
                    )
                    .await?;
                let mut indices = Vec::with_capacity(batch_size);
                while indices.len() < batch_size {
                    if remaining == 0 {
                        if row >= batch.num_rows() {
                            break;
                        }
                        remaining = repeat_count(counts.as_ref(), row)?;
                        if remaining == 0 {
                            row += 1;
                            continue;
                        }
                    }
                    let available = batch_size - indices.len();
                    let take = remaining.min(available as u64) as usize;
                    let index = u32::try_from(row).map_err(|_| {
                        Error::ResourceExhausted(
                            "repeat input batch exceeds UINT32_MAX rows".into(),
                        )
                    })?;
                    indices.extend(std::iter::repeat_n(index, take));
                    remaining -= take as u64;
                    if remaining == 0 {
                        row += 1;
                    }
                }
                if indices.is_empty() {
                    break;
                }
                let output = take_record_batch(&source, &UInt32Array::from(indices))?;
                yield BatchEnvelope::from_reservation(output, workspace, "repeat output")?;
            }
        }
    })
}

fn output_source(batch: &RecordBatch, schema: SchemaRef) -> Result<RecordBatch> {
    let width = schema.fields().len();
    if batch.num_columns() < width {
        return Err(Error::Internal(
            "repeat input is narrower than its output schema".into(),
        ));
    }
    Ok(RecordBatch::try_new(
        schema,
        batch.columns()[..width].to_vec(),
    )?)
}

fn repeat_count(array: &dyn Array, row: usize) -> Result<u64> {
    if array.is_null(row) {
        return Err(Error::Internal("repeat count is NULL".into()));
    }
    if let Some(values) = array.as_any().downcast_ref::<Int64Array>() {
        return u64::try_from(values.value(row))
            .map_err(|_| Error::Internal("repeat count must not be negative".into()));
    }
    if let Some(values) = array.as_any().downcast_ref::<UInt64Array>() {
        return Ok(values.value(row));
    }
    Err(Error::Internal(format!(
        "repeat count must be INT64 or UINT64, got {}",
        array.data_type()
    )))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::{
        array::Int64Array,
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    };
    use futures::{TryStreamExt, stream};

    use super::repeat;
    use crate::{
        Error,
        runtime::{BatchEnvelope, MemoryPool, QueryContext, boxed_memory_batch_stream},
        sql::BoundExpr,
    };

    #[tokio::test]
    async fn reserves_index_capacity_before_allocating_a_large_output_batch() {
        let directory = tempfile::tempdir().unwrap();
        let context = QueryContext::shared(MemoryPool::new(1 << 20), directory.path()).unwrap();
        let input_schema = Arc::new(Schema::new(vec![
            Field::new("value", DataType::Int64, false),
            Field::new("count", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            input_schema,
            vec![
                Arc::new(Int64Array::from(vec![7])),
                Arc::new(Int64Array::from(vec![1])),
            ],
        )
        .unwrap();
        let envelope = BatchEnvelope::try_new(batch, &context.memory, "repeat test").unwrap();
        let input = boxed_memory_batch_stream(stream::once(async move { Ok(envelope) }));
        let output_schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            false,
        )]));
        let error = repeat(
            input,
            BoundExpr::column(1, DataType::Int64, "count"),
            output_schema,
            Arc::clone(&context),
            1_000_000,
        )
        .try_collect::<Vec<_>>()
        .await
        .unwrap_err();
        assert!(matches!(error, Error::ResourceExhausted(_)), "{error}");
        assert_eq!(context.memory.used(), 0);
    }
}
