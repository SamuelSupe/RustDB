use std::sync::Arc;

use arrow::{array::new_null_array, datatypes::SchemaRef, record_batch::RecordBatch};
use futures::StreamExt;

use crate::{
    Error,
    runtime::{BatchEnvelope, MemoryBatchStream, QueryContext, boxed_memory_batch_stream},
};

pub(super) fn scalarize(
    mut input: MemoryBatchStream,
    schema: SchemaRef,
    context: Arc<QueryContext>,
) -> MemoryBatchStream {
    boxed_memory_batch_stream(async_stream::try_stream! {
        let mut value: Option<BatchEnvelope> = None;
        while let Some(batch) = input.next().await {
            context.check_cancelled()?;
            let batch = batch?;
            if batch.batch().num_columns() != 1 {
                Err(Error::Internal("scalar subquery input must contain one column".into()))?;
            }
            if batch.batch().num_rows() == 0 {
                continue;
            }
            if value.is_some() || batch.batch().num_rows() > 1 {
                Err(Error::Execution(
                    "scalar subquery returned more than one row".into(),
                ))?;
            }
            let scalar = RecordBatch::try_new(
                Arc::clone(&schema),
                vec![batch.batch().column(0).slice(0, 1)],
            )?;
            value = Some(batch.replace(scalar, "scalar subquery")?);
        }
        match value {
            Some(value) => yield value,
            None => {
                let value = new_null_array(schema.field(0).data_type(), 1);
                let batch = RecordBatch::try_new(schema, vec![value])?;
                yield BatchEnvelope::try_new(batch, &context.memory, "scalar subquery")?;
            }
        }
    })
}
