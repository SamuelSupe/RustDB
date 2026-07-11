use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use futures::StreamExt;

use crate::{
    Result,
    runtime::{BatchEnvelope, MemoryBatchStream, QueryContext},
    sql::BoundExpr,
};

use super::super::{EvaluatedKeys, evaluate_keys_accounted, row_key};
use crate::execution::value::CellValue;

pub(super) struct SortedCursor {
    stream: MemoryBatchStream,
    expressions: Vec<BoundExpr>,
    context: Arc<QueryContext>,
    batch: Option<BatchEnvelope>,
    keys: Option<EvaluatedKeys>,
    row: usize,
}

impl SortedCursor {
    pub(super) fn new(
        stream: MemoryBatchStream,
        expressions: Vec<BoundExpr>,
        context: Arc<QueryContext>,
    ) -> Self {
        Self {
            stream,
            expressions,
            context,
            batch: None,
            keys: None,
            row: 0,
        }
    }

    pub(super) async fn ensure_row(&mut self) -> Result<bool> {
        loop {
            if self
                .batch
                .as_ref()
                .is_some_and(|batch| self.row < batch.num_rows())
            {
                return Ok(true);
            }
            let Some(batch) = self.stream.next().await else {
                self.batch = None;
                self.keys = None;
                return Ok(false);
            };
            let batch = batch?;
            if batch.num_rows() == 0 {
                continue;
            }
            self.keys = Some(evaluate_keys_accounted(
                &self.expressions,
                batch.batch(),
                &self.context,
                "sort-merge join keys",
            )?);
            self.batch = Some(batch);
            self.row = 0;
        }
    }

    pub(super) fn key(&self) -> Result<Vec<CellValue>> {
        row_key(
            self.keys.as_deref().expect("cursor row has evaluated keys"),
            self.row,
        )
    }

    pub(super) fn held_bytes(&self) -> usize {
        self.batch
            .as_ref()
            .map(BatchEnvelope::memory_size)
            .unwrap_or(0)
            .saturating_add(
                self.keys
                    .as_ref()
                    .map(EvaluatedKeys::memory_size)
                    .unwrap_or(0),
            )
    }

    pub(super) fn take_row(&mut self) -> RecordBatch {
        let batch = self.batch.as_ref().expect("cursor row was ensured");
        let row = batch.batch().slice(self.row, 1);
        self.row += 1;
        row
    }

    pub(super) fn take_equal_run(&mut self, key: &[CellValue]) -> Result<RecordBatch> {
        let batch = self.batch.as_ref().expect("cursor row was ensured");
        let start = self.row;
        let keys = self.keys.as_deref().expect("cursor row has evaluated keys");
        while self.row < batch.num_rows() && row_key(keys, self.row)? == key {
            self.row += 1;
        }
        Ok(batch.batch().slice(start, self.row - start))
    }
}
