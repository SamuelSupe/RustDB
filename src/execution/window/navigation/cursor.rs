use std::sync::Arc;

use arrow::{array::ArrayRef, record_batch::RecordBatch};

use crate::runtime::{BatchEnvelope, MemoryReservation, QueryContext, SpillFile};
use crate::sql::BoundExpr;
use crate::{Error, Result};

use super::super::super::{
    expr,
    value::{CellValue, cell},
};
use super::frame::PeerRange;

type BatchIterator = Box<dyn Iterator<Item = Result<RecordBatch>>>;

pub(in crate::execution::window) struct ValueCursor {
    context: Arc<QueryContext>,
    expression: BoundExpr,
    reader: BatchIterator,
    batch: Option<BatchEnvelope>,
    values: Option<ArrayRef>,
    workspace: Option<MemoryReservation>,
    start: u64,
    end: u64,
}

impl ValueCursor {
    pub(in crate::execution::window) fn new(
        file: &SpillFile,
        expression: BoundExpr,
        context: Arc<QueryContext>,
    ) -> Result<Self> {
        Ok(Self {
            reader: Box::new(context.spill.read_file(file)?),
            context,
            expression,
            batch: None,
            values: None,
            workspace: None,
            start: 0,
            end: 0,
        })
    }

    pub(in crate::execution::window) fn new_at(
        file: &SpillFile,
        expression: BoundExpr,
        context: Arc<QueryContext>,
        row: u64,
        offset: u64,
    ) -> Result<Self> {
        let mut reader = context.spill.read_file(file)?;
        reader.seek_batch(offset)?;
        Ok(Self {
            reader: Box::new(reader),
            context,
            expression,
            batch: None,
            values: None,
            workspace: None,
            start: row,
            end: row,
        })
    }

    pub(in crate::execution::window) fn value_at(&mut self, target: u64) -> Result<CellValue> {
        while self.values.is_none() || target >= self.end {
            self.load_next()?.ok_or_else(|| {
                Error::Internal(format!(
                    "window navigation target row {target} is outside its partition"
                ))
            })?;
        }
        if target < self.start {
            return Err(Error::Internal(
                "window navigation cursor moved backwards".into(),
            ));
        }
        let row = usize::try_from(target - self.start)
            .map_err(|_| Error::Execution("window row offset exceeds usize".into()))?;
        cell(self.values.as_ref().expect("cursor loaded"), row)
    }

    fn load_next(&mut self) -> Result<Option<()>> {
        self.values = None;
        self.workspace = None;
        self.batch = None;
        loop {
            self.context.check_cancelled()?;
            let Some(batch) = self.reader.next() else {
                return Ok(None);
            };
            let batch =
                BatchEnvelope::try_new(batch?, &self.context.memory, "window navigation input")?;
            if batch.num_rows() == 0 {
                continue;
            }
            let bytes = expr::projection_workspace_bytes(
                std::slice::from_ref(&self.expression),
                batch.batch(),
            );
            let workspace = self.context.memory.try_reserve(bytes).map_err(|_| {
                Error::ResourceExhausted(format!(
                    "window navigation expression requires {bytes} bytes (query limit {}, available {})",
                    self.context.memory.limit(),
                    self.context.memory.available(),
                ))
            })?;
            let values = expr::evaluate(&self.expression, batch.batch())?;
            self.start = self.end;
            self.end = self
                .end
                .checked_add(u64::try_from(batch.num_rows()).map_err(|_| {
                    Error::Execution("window partition row count exceeds UINT64".into())
                })?)
                .ok_or_else(|| Error::Execution("window row index overflowed UINT64".into()))?;
            self.batch = Some(batch);
            self.workspace = Some(workspace);
            self.values = Some(values);
            return Ok(Some(()));
        }
    }
}

pub(in crate::execution::window) struct PeerCursor {
    context: Arc<QueryContext>,
    reader: BatchIterator,
    batch: Option<BatchEnvelope>,
    row: usize,
    current: PeerRange,
    current_group: Option<u64>,
}

impl PeerCursor {
    pub(in crate::execution::window) fn new(
        file: &SpillFile,
        context: Arc<QueryContext>,
    ) -> Result<Self> {
        Ok(Self {
            reader: Box::new(context.spill.read_file(file)?),
            context,
            batch: None,
            row: 0,
            current: PeerRange {
                group: 0,
                start: 0,
                end: 0,
            },
            current_group: None,
        })
    }

    pub(in crate::execution::window) fn range_at(&mut self, target: u64) -> Result<PeerRange> {
        while target >= self.current.end {
            let length = self.next_length()?.ok_or_else(|| {
                Error::Internal("window peer sidecar ended before navigation input".into())
            })?;
            if length == 0 {
                return Err(Error::Internal(
                    "window peer sidecar contains an empty peer".into(),
                ));
            }
            self.current = PeerRange {
                group: self.next_group()?,
                start: self.current.end,
                end: self.current.end.checked_add(length).ok_or_else(|| {
                    Error::Execution("window peer position overflowed UINT64".into())
                })?,
            };
            self.current_group = Some(self.current.group);
        }
        Ok(self.current)
    }

    pub(in crate::execution::window) fn range_for_group(
        &mut self,
        target: u64,
    ) -> Result<PeerRange> {
        if self.current_group.is_some_and(|current| target < current) {
            return Err(Error::Internal("window peer cursor moved backwards".into()));
        }
        while self.current_group.is_none_or(|current| current < target) {
            let length = self.next_length()?.ok_or_else(|| {
                Error::Internal(format!(
                    "window peer group {target} is outside its partition"
                ))
            })?;
            if length == 0 {
                return Err(Error::Internal(
                    "window peer sidecar contains an empty peer".into(),
                ));
            }
            self.current = PeerRange {
                group: self.next_group()?,
                start: self.current.end,
                end: self.current.end.checked_add(length).ok_or_else(|| {
                    Error::Execution("window peer position overflowed UINT64".into())
                })?,
            };
            self.current_group = Some(self.current.group);
        }
        Ok(self.current)
    }

    fn next_group(&self) -> Result<u64> {
        self.current_group.map_or(Ok(0), |group| {
            group
                .checked_add(1)
                .ok_or_else(|| Error::Execution("window peer group overflowed UINT64".into()))
        })
    }

    fn next_length(&mut self) -> Result<Option<u64>> {
        loop {
            if let Some(batch) = &self.batch
                && self.row < batch.num_rows()
            {
                let lengths = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<arrow::array::UInt64Array>()
                    .ok_or_else(|| {
                        Error::Internal("window peer sidecar length is not UINT64".into())
                    })?;
                let value = lengths.value(self.row);
                self.row += 1;
                return Ok(Some(value));
            }
            self.batch = None;
            self.context.check_cancelled()?;
            let Some(batch) = self.reader.next() else {
                return Ok(None);
            };
            self.batch = Some(BatchEnvelope::try_new(
                batch?,
                &self.context.memory,
                "window navigation peer input",
            )?);
            self.row = 0;
        }
    }
}
