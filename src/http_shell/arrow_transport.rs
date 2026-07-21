use arrow::{datatypes::SchemaRef, record_batch::RecordBatch};

pub const ARROW_RESULT_MEDIA_TYPE: &str = "application/vnd.apache.arrow.file";
pub(crate) const BATCH_SEQ_HEADER: &str = "x-rustdb-batch-seq";
pub(crate) const NEXT_BATCH_SEQ_HEADER: &str = "x-rustdb-next-batch-seq";
pub(crate) const RESULT_COMPLETE_HEADER: &str = "x-rustdb-result-complete";
pub(crate) const RESULT_STATE_HEADER: &str = "x-rustdb-result-state";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ArrowResultState {
    Queued,
    Running,
    Completed,
    Interrupted,
    Failed,
    Cancelled,
    Invalidated,
}

#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct ArrowResultBatch {
    pub batch_seq: u64,
    pub next_batch_seq: u64,
    pub result_complete: bool,
    pub state: ArrowResultState,
    pub batch: RecordBatch,
}

#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum ArrowResultPoll {
    Batch(ArrowResultBatch),
    Pending {
        next_batch_seq: u64,
        state: ArrowResultState,
    },
    Complete {
        next_batch_seq: u64,
        schema: SchemaRef,
        state: ArrowResultState,
    },
}

impl ArrowResultPoll {
    pub fn next_batch_seq(&self) -> u64 {
        match self {
            Self::Batch(value) => value.next_batch_seq,
            Self::Pending { next_batch_seq, .. } | Self::Complete { next_batch_seq, .. } => {
                *next_batch_seq
            }
        }
    }
}
