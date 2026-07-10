use std::pin::Pin;

use arrow::record_batch::RecordBatch;
use futures::Stream;

use crate::Result;

/// The streaming result exchanged between physical operators and exposed by a query.
pub type RecordBatchStream = Pin<Box<dyn Stream<Item = Result<RecordBatch>> + Send + 'static>>;

pub fn boxed_record_batch_stream<S>(stream: S) -> RecordBatchStream
where
    S: Stream<Item = Result<RecordBatch>> + Send + 'static,
{
    Box::pin(stream)
}
