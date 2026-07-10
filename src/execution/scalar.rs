use std::sync::Arc;

use arrow::{
    array::{ArrayRef, new_null_array},
    datatypes::SchemaRef,
    record_batch::RecordBatch,
};
use futures::StreamExt;

use crate::{
    Error,
    runtime::{QueryContext, RecordBatchStream, boxed_record_batch_stream},
};

pub(super) fn scalarize(
    mut input: RecordBatchStream,
    schema: SchemaRef,
    context: Arc<QueryContext>,
) -> RecordBatchStream {
    boxed_record_batch_stream(async_stream::try_stream! {
        let mut value: Option<ArrayRef> = None;
        while let Some(batch) = input.next().await {
            context.check_cancelled()?;
            let batch = batch?;
            if batch.num_columns() != 1 {
                Err(Error::Internal("scalar subquery input must contain one column".into()))?;
            }
            if batch.num_rows() == 0 {
                continue;
            }
            if value.is_some() || batch.num_rows() > 1 {
                Err(Error::Execution(
                    "scalar subquery returned more than one row".into(),
                ))?;
            }
            value = Some(batch.column(0).slice(0, 1));
        }
        let value = value.unwrap_or_else(|| new_null_array(schema.field(0).data_type(), 1));
        yield RecordBatch::try_new(schema, vec![value])?;
    })
}
