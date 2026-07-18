mod range;

use std::sync::Arc;

use arrow::{
    array::UInt64Array,
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};

use crate::runtime::{BatchEnvelope, QueryContext, SpillFile};
use crate::sql::{WindowExpr, WindowFrame, WindowFrameBound, WindowFrameUnits, WindowFunction};
use crate::{Error, Result};

use super::navigation::cursor::PeerCursor;
use super::sidecar::RangeSidecar;

pub(super) struct FrameSidecar {
    pub(super) file: SpillFile,
}

pub(super) fn build(
    partition: &SpillFile,
    expressions: &[WindowExpr],
    peers: Option<&RangeSidecar>,
    context: &Arc<QueryContext>,
    batch_size: usize,
    rows: u64,
) -> Result<Option<FrameSidecar>> {
    let Some(expression) = expressions.first() else {
        return Ok(None);
    };
    if !needs_index(expressions) {
        return Ok(None);
    }
    let schema = Arc::new(Schema::new(vec![
        Field::new("frame_start", DataType::UInt64, false),
        Field::new("frame_end", DataType::UInt64, false),
    ]));
    let capacity = batch_size.max(1);
    let bytes = capacity.saturating_mul(32).saturating_add(2048);
    let _memory = context.memory.try_reserve(bytes).map_err(|_| {
        Error::ResourceExhausted(format!(
            "window frame index requires {bytes} bytes (query limit {}, available {})",
            context.memory.limit(),
            context.memory.available(),
        ))
    })?;
    let mut writer = context.spill.writer("window-frame-index", schema.clone())?;
    let mut buffer = FrameBuffer::new(capacity);
    match expression.frame.units {
        WindowFrameUnits::Rows => {
            for row in 0..rows {
                context.check_cancelled()?;
                buffer.push(rows_frame(expression.frame, row, rows));
                buffer.flush_if_full(&mut writer, &schema)?;
            }
        }
        WindowFrameUnits::Groups => build_groups(
            expression.frame,
            peers.ok_or_else(|| Error::Internal("GROUPS frame has no peer sidecar".into()))?,
            context,
            rows,
            &mut buffer,
            &mut writer,
            &schema,
        )?,
        WindowFrameUnits::Range => {
            let peers =
                peers.ok_or_else(|| Error::Internal("RANGE frame has no peer sidecar".into()))?;
            if bounded(expression.frame) {
                let mut frames =
                    range::RangeFrames::new(partition, expression, peers, context, rows)?;
                for row in 0..rows {
                    context.check_cancelled()?;
                    buffer.push(frames.frame_at(row)?);
                    buffer.flush_if_full(&mut writer, &schema)?;
                }
            } else {
                build_peer_range(
                    expression.frame,
                    peers,
                    context,
                    rows,
                    &mut buffer,
                    &mut writer,
                    &schema,
                )?;
            }
        }
    }
    buffer.flush(&mut writer, &schema)?;
    Ok(Some(FrameSidecar {
        file: writer.finish(1)?,
    }))
}

fn needs_index(expressions: &[WindowExpr]) -> bool {
    expressions
        .iter()
        .any(|expression| match expression.function {
            WindowFunction::Lead { .. } | WindowFunction::Lag { .. } => false,
            WindowFunction::FirstValue(_) | WindowFunction::LastValue(_) => {
                expression.frame.units == WindowFrameUnits::Groups
                    || expression.frame.units == WindowFrameUnits::Range
                        && bounded(expression.frame)
            }
            WindowFunction::Aggregate(_) => {
                !is_whole(expression)
                    && !is_rows_prefix(expression)
                    && !(expression.frame.units == WindowFrameUnits::Range
                        && expression.frame.start == WindowFrameBound::UnboundedPreceding
                        && expression.frame.end == WindowFrameBound::CurrentRow)
            }
            _ => false,
        })
}

fn is_whole(expression: &WindowExpr) -> bool {
    expression.frame.start == WindowFrameBound::UnboundedPreceding
        && expression.frame.end == WindowFrameBound::UnboundedFollowing
}

fn is_rows_prefix(expression: &WindowExpr) -> bool {
    expression.frame.units == WindowFrameUnits::Rows
        && expression.frame.start == WindowFrameBound::UnboundedPreceding
        && expression.frame.end == WindowFrameBound::CurrentRow
}

fn bounded(frame: WindowFrame) -> bool {
    matches!(
        frame.start,
        WindowFrameBound::Preceding(_) | WindowFrameBound::Following(_)
    ) || matches!(
        frame.end,
        WindowFrameBound::Preceding(_) | WindowFrameBound::Following(_)
    )
}

fn build_peer_range(
    frame: WindowFrame,
    sidecar: &RangeSidecar,
    context: &Arc<QueryContext>,
    rows: u64,
    buffer: &mut FrameBuffer,
    writer: &mut crate::runtime::SpillWriter,
    schema: &Arc<Schema>,
) -> Result<()> {
    let mut peers = PeerCursor::new(&sidecar.file, Arc::clone(context))?;
    for row in 0..rows {
        context.check_cancelled()?;
        let peer = peers.range_at(row)?;
        let start = match frame.start {
            WindowFrameBound::UnboundedPreceding => 0,
            WindowFrameBound::CurrentRow => peer.start,
            WindowFrameBound::UnboundedFollowing => rows,
            WindowFrameBound::Preceding(_) | WindowFrameBound::Following(_) => {
                return Err(Error::Internal(
                    "bounded RANGE was routed through peer-only framing".into(),
                ));
            }
        };
        let end = match frame.end {
            WindowFrameBound::UnboundedPreceding => 0,
            WindowFrameBound::CurrentRow => peer.end,
            WindowFrameBound::UnboundedFollowing => rows,
            WindowFrameBound::Preceding(_) | WindowFrameBound::Following(_) => {
                return Err(Error::Internal(
                    "bounded RANGE was routed through peer-only framing".into(),
                ));
            }
        };
        buffer.push(normalize(start, end));
        buffer.flush_if_full(writer, schema)?;
    }
    Ok(())
}

fn build_groups(
    frame: WindowFrame,
    sidecar: &RangeSidecar,
    context: &Arc<QueryContext>,
    rows: u64,
    buffer: &mut FrameBuffer,
    writer: &mut crate::runtime::SpillWriter,
    schema: &Arc<Schema>,
) -> Result<()> {
    let mut current = PeerCursor::new(&sidecar.file, Arc::clone(context))?;
    let mut starts = PeerCursor::new(&sidecar.file, Arc::clone(context))?;
    let mut ends = PeerCursor::new(&sidecar.file, Arc::clone(context))?;
    for row in 0..rows {
        context.check_cancelled()?;
        let peer = current.range_at(row)?;
        let start = group_start(frame.start, peer.group, sidecar.groups, rows, &mut starts)?;
        let end = group_end(frame.end, peer.group, sidecar.groups, rows, &mut ends)?;
        buffer.push(normalize(start, end));
        buffer.flush_if_full(writer, schema)?;
    }
    Ok(())
}

fn group_start(
    bound: WindowFrameBound,
    group: u64,
    groups: u64,
    rows: u64,
    cursor: &mut PeerCursor,
) -> Result<u64> {
    let target = match bound {
        WindowFrameBound::UnboundedPreceding => return Ok(0),
        WindowFrameBound::Preceding(offset) => group.saturating_sub(offset),
        WindowFrameBound::CurrentRow => group,
        WindowFrameBound::Following(offset) => match group.checked_add(offset) {
            Some(target) if target < groups => target,
            _ => return Ok(rows),
        },
        WindowFrameBound::UnboundedFollowing => return Ok(rows),
    };
    Ok(cursor.range_for_group(target)?.start)
}

fn group_end(
    bound: WindowFrameBound,
    group: u64,
    groups: u64,
    rows: u64,
    cursor: &mut PeerCursor,
) -> Result<u64> {
    let target = match bound {
        WindowFrameBound::UnboundedPreceding => return Ok(0),
        WindowFrameBound::Preceding(offset) if offset > group => return Ok(0),
        WindowFrameBound::Preceding(offset) => group - offset,
        WindowFrameBound::CurrentRow => group,
        WindowFrameBound::Following(offset) => group
            .checked_add(offset)
            .map(|target| target.min(groups.saturating_sub(1)))
            .unwrap_or_else(|| groups.saturating_sub(1)),
        WindowFrameBound::UnboundedFollowing => return Ok(rows),
    };
    Ok(cursor.range_for_group(target)?.end)
}

fn rows_frame(frame: WindowFrame, row: u64, rows: u64) -> (u64, u64) {
    let start = match frame.start {
        WindowFrameBound::UnboundedPreceding => 0,
        WindowFrameBound::Preceding(offset) => row.saturating_sub(offset),
        WindowFrameBound::CurrentRow => row,
        WindowFrameBound::Following(offset) => row
            .checked_add(offset)
            .filter(|target| *target < rows)
            .unwrap_or(rows),
        WindowFrameBound::UnboundedFollowing => rows,
    };
    let end = match frame.end {
        WindowFrameBound::UnboundedPreceding => 0,
        WindowFrameBound::Preceding(offset) if offset > row => 0,
        WindowFrameBound::Preceding(offset) => row - offset + 1,
        WindowFrameBound::CurrentRow => row + 1,
        WindowFrameBound::Following(offset) => row
            .checked_add(offset)
            .and_then(|target| target.checked_add(1))
            .unwrap_or(rows)
            .min(rows),
        WindowFrameBound::UnboundedFollowing => rows,
    };
    normalize(start, end)
}

fn normalize(start: u64, end: u64) -> (u64, u64) {
    if start <= end {
        (start, end)
    } else {
        (start, start)
    }
}

struct FrameBuffer {
    starts: Vec<u64>,
    ends: Vec<u64>,
    capacity: usize,
}

impl FrameBuffer {
    fn new(capacity: usize) -> Self {
        Self {
            starts: Vec::with_capacity(capacity),
            ends: Vec::with_capacity(capacity),
            capacity,
        }
    }

    fn push(&mut self, (start, end): (u64, u64)) {
        self.starts.push(start);
        self.ends.push(end);
    }

    fn flush_if_full(
        &mut self,
        writer: &mut crate::runtime::SpillWriter,
        schema: &Arc<Schema>,
    ) -> Result<()> {
        if self.starts.len() >= self.capacity {
            self.flush(writer, schema)?;
        }
        Ok(())
    }

    fn flush(
        &mut self,
        writer: &mut crate::runtime::SpillWriter,
        schema: &Arc<Schema>,
    ) -> Result<()> {
        if self.starts.is_empty() {
            return Ok(());
        }
        writer.write_batch(&RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(UInt64Array::from(std::mem::take(&mut self.starts))),
                Arc::new(UInt64Array::from(std::mem::take(&mut self.ends))),
            ],
        )?)
    }
}

pub(super) struct FrameCursor {
    context: Arc<QueryContext>,
    reader: Box<dyn Iterator<Item = Result<RecordBatch>>>,
    batch: Option<BatchEnvelope>,
    row: usize,
}

impl FrameCursor {
    pub(super) fn new(sidecar: &FrameSidecar, context: Arc<QueryContext>) -> Result<Self> {
        Ok(Self {
            reader: Box::new(context.spill.read_file(&sidecar.file)?),
            context,
            batch: None,
            row: 0,
        })
    }

    pub(super) fn next_range(&mut self) -> Result<(u64, u64)> {
        loop {
            if let Some(batch) = &self.batch
                && self.row < batch.num_rows()
            {
                let starts = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .ok_or_else(|| Error::Internal("frame start column is not UINT64".into()))?;
                let ends = batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .ok_or_else(|| Error::Internal("frame end column is not UINT64".into()))?;
                let range = (starts.value(self.row), ends.value(self.row));
                self.row += 1;
                return Ok(range);
            }
            self.batch = None;
            self.context.check_cancelled()?;
            let batch = self.reader.next().ok_or_else(|| {
                Error::Internal("window frame index ended before partition input".into())
            })??;
            self.batch = Some(BatchEnvelope::try_new(
                batch,
                &self.context.memory,
                "window frame index input",
            )?);
            self.row = 0;
        }
    }
}
