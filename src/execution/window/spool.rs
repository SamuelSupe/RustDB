use std::mem::size_of;

use arrow::{array::ArrayRef, datatypes::SchemaRef, record_batch::RecordBatch};

use crate::runtime::{MemoryReservation, QueryContext, SpillFile, SpillWriter};
use crate::sql::{AggregateFunction, WindowExpr, WindowFrameBound, WindowFunction};
use crate::{Error, Result};

use super::super::{
    aggregate::state::AggregateState,
    value::{CellValue, cell},
};
use super::keys::payload_bytes;
use super::memory::array_value_payload_bytes;

pub(super) struct PendingPartition {
    key: Vec<CellValue>,
    writer: SpillWriter,
    whole_states: Vec<Option<AggregateState>>,
    retained_payload: Vec<usize>,
    memory: MemoryReservation,
}

pub(super) struct CompletedPartition {
    pub(super) file: SpillFile,
    pub(super) whole_values: Vec<Option<CellValue>>,
    memory: MemoryReservation,
}

impl PendingPartition {
    pub(super) fn retained_bytes(&self) -> usize {
        self.memory.size()
    }

    pub(super) fn new(
        context: &QueryContext,
        schema: SchemaRef,
        key: Vec<CellValue>,
        expressions: &[WindowExpr],
    ) -> Result<Self> {
        let bytes = 1024usize
            .saturating_add(payload_bytes(&key))
            .saturating_add(key.len().saturating_mul(size_of::<CellValue>()))
            .saturating_add(expressions.len().saturating_mul(256));
        let memory = context.memory.try_reserve(bytes).map_err(|_| {
            Error::ResourceExhausted(format!(
                "window partition state requires {bytes} bytes (query limit {}, available {})",
                context.memory.limit(),
                context.memory.available()
            ))
        })?;
        let whole_states = expressions
            .iter()
            .map(|expression| match &expression.function {
                WindowFunction::Aggregate(aggregate) if is_whole(expression) => {
                    Some(AggregateState::new(aggregate))
                }
                _ => None,
            })
            .collect();
        Ok(Self {
            key,
            writer: context.spill.writer("window-partition", schema)?,
            whole_states,
            retained_payload: vec![0; expressions.len()],
            memory,
        })
    }

    pub(super) fn matches(&self, key: &[CellValue]) -> bool {
        self.key == key
    }

    pub(super) fn write(
        &mut self,
        batch: &RecordBatch,
        start: usize,
        len: usize,
        expressions: &[WindowExpr],
        aggregate_inputs: &[Option<ArrayRef>],
    ) -> Result<()> {
        for row in start..start + len {
            for (index, state) in self.whole_states.iter_mut().enumerate() {
                let Some(state) = state else { continue };
                let WindowFunction::Aggregate(aggregate) = &expressions[index].function else {
                    unreachable!("whole window state belongs to an aggregate")
                };
                let payload = if matches!(
                    aggregate.function,
                    AggregateFunction::Min | AggregateFunction::Max
                ) && let Some(array) = &aggregate_inputs[index]
                {
                    array_value_payload_bytes(array, row)?
                } else {
                    0
                };
                // The cloned input and previous state coexist during update.
                self.memory.try_grow(payload)?;
                let value = aggregate_inputs[index]
                    .as_ref()
                    .map(|array| cell(array, row))
                    .transpose()?;
                state.update(aggregate, value)?;
                let stable = self.retained_payload[index].max(payload);
                self.memory.shrink(
                    self.retained_payload[index]
                        .saturating_add(payload)
                        .saturating_sub(stable),
                );
                self.retained_payload[index] = stable;
            }
        }
        self.writer.write_batch(&batch.slice(start, len))
    }

    pub(super) fn finish(mut self) -> Result<CompletedPartition> {
        // MIN/MAX finish clones retained variable payloads while the state is
        // still alive. Credit that second copy before AggregateState::finish.
        let transition_payload = self
            .retained_payload
            .iter()
            .fold(0usize, |bytes, payload| bytes.saturating_add(*payload));
        self.memory.try_grow(transition_payload)?;
        let whole_values = self
            .whole_states
            .iter()
            .map(|state| state.as_ref().map(AggregateState::finish).transpose())
            .collect::<Result<Vec<_>>>()?;
        // The finalized values now own the payload; release the credit for the
        // previous aggregate state after dropping it.
        drop(self.whole_states);
        self.memory.shrink(transition_payload);
        Ok(CompletedPartition {
            file: self.writer.finish(1)?,
            whole_values,
            memory: self.memory,
        })
    }
}

impl CompletedPartition {
    pub(super) fn retained_bytes(&self) -> usize {
        self.memory.size()
    }
}

pub(super) fn aggregate_inputs(
    expressions: &[WindowExpr],
    batch: &RecordBatch,
) -> Result<Vec<Option<ArrayRef>>> {
    expressions
        .iter()
        .map(|expression| match &expression.function {
            WindowFunction::Aggregate(aggregate) => aggregate
                .expr
                .as_ref()
                .map(|expression| super::super::expr::evaluate(expression, batch))
                .transpose(),
            _ => Ok(None),
        })
        .collect()
}

pub(super) fn is_whole(expression: &WindowExpr) -> bool {
    expression.frame.end == WindowFrameBound::UnboundedFollowing
}
