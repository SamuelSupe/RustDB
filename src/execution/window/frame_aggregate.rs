mod index;

use std::{mem::size_of, sync::Arc};

use arrow::{
    array::ArrayRef,
    datatypes::{DataType, Field, Schema, SchemaRef},
    record_batch::RecordBatch,
};

use crate::runtime::{QueryContext, SpillFile, estimate_schema_batch_bytes};
use crate::sql::{
    AggregateFunction, WindowExpr, WindowFrameBound, WindowFrameUnits, WindowFunction,
};
use crate::{Error, Result};

use super::super::{
    aggregate::state::AggregateState,
    value::{CellValue, values_to_array},
};
use super::frame_index::{FrameCursor, FrameSidecar};
use super::memory::{cell_payload_bytes, is_variable};
use super::navigation::cursor::ValueCursor;

pub(super) struct AggregateSidecar {
    pub(super) file: SpillFile,
    pub(super) expression_columns: Vec<Option<usize>>,
}

pub(super) fn build(
    partition: &SpillFile,
    frames: Option<&FrameSidecar>,
    expressions: &[WindowExpr],
    context: &Arc<QueryContext>,
    batch_size: usize,
    rows: u64,
) -> Result<Option<AggregateSidecar>> {
    let indices = expressions
        .iter()
        .enumerate()
        .filter_map(|(index, expression)| generic_aggregate(expression).then_some(index))
        .collect::<Vec<_>>();
    if indices.is_empty() {
        return Ok(None);
    }
    let frames = frames
        .ok_or_else(|| Error::Internal("bounded window aggregate has no frame index".into()))?;
    let schema = schema(expressions, &indices);
    let capacity = batch_size.max(1);
    let base_bytes = estimate_schema_batch_bytes(schema.as_ref(), capacity)
        .saturating_mul(2)
        .saturating_add(
            indices
                .len()
                .saturating_mul(capacity)
                .saturating_mul(size_of::<CellValue>()),
        )
        .saturating_add(2048);
    let mut memory = context.memory.try_reserve(base_bytes).map_err(|_| {
        Error::ResourceExhausted(format!(
            "bounded window aggregate buffer requires {base_bytes} bytes (query limit {}, available {})",
            context.memory.limit(),
            context.memory.available(),
        ))
    })?;
    let mut writer = context
        .spill
        .writer("window-frame-aggregate", Arc::clone(&schema))?;
    let mut frame_cursor = FrameCursor::new(frames, Arc::clone(context))?;
    let mut buffer = ResultBuffer::new(indices.len(), capacity);
    let partition_index = if rows > capacity as u64 && indices.iter().any(|index| {
        matches!(&expressions[*index].function, WindowFunction::Aggregate(aggregate) if aggregate.expr.is_some())
    }) {
        index::PartitionIndex::build(partition, context, rows)?
    } else {
        None
    };

    for _ in 0..rows {
        context.check_cancelled()?;
        let (start, end) = frame_cursor.next_range()?;
        for (column, index) in indices.iter().enumerate() {
            let WindowFunction::Aggregate(aggregate) = &expressions[*index].function else {
                unreachable!()
            };
            if aggregate.function == AggregateFunction::Count && aggregate.expr.is_none() {
                let count = i64::try_from(end - start)
                    .map_err(|_| Error::Execution("count overflowed INT64".into()))?;
                buffer.push(column, CellValue::Int64(count), 0);
                continue;
            }
            let mut state = AggregateState::new(aggregate);
            let mut retained = 0usize;
            if let Some(expression) = &aggregate.expr {
                let mut cursor = match partition_index
                    .as_ref()
                    .and_then(|index| index.locate(start))
                {
                    Some((row, offset)) => ValueCursor::new_at(
                        partition,
                        expression.clone(),
                        Arc::clone(context),
                        row,
                        offset,
                    )?,
                    None => ValueCursor::new(partition, expression.clone(), Arc::clone(context))?,
                };
                for row in start..end {
                    let value = cursor.value_at(row)?;
                    let payload =
                        retained_payload(aggregate.function, &expression.data_type, &value);
                    memory.try_grow(payload)?;
                    state.update(aggregate, Some(value))?;
                    let stable = retained.max(payload);
                    memory.shrink(retained.saturating_add(payload).saturating_sub(stable));
                    retained = stable;
                }
            } else {
                for _ in start..end {
                    state.update(aggregate, None)?;
                }
            }
            memory.try_grow(retained)?;
            let value = state.finish()?;
            drop(state);
            memory.shrink(retained);
            let credit = output_credit(&expressions[*index].data_type, &value);
            memory.try_grow(credit.saturating_sub(retained))?;
            buffer.push(column, value, credit);
        }
        if buffer.len() >= capacity {
            memory.shrink(buffer.flush(&mut writer, &schema, expressions, &indices)?);
        }
    }
    memory.shrink(buffer.flush(&mut writer, &schema, expressions, &indices)?);
    drop(frame_cursor);
    drop(buffer);
    drop(memory);
    let file = writer.finish(1)?;
    let mut expression_columns = vec![None; expressions.len()];
    for (column, index) in indices.iter().enumerate() {
        expression_columns[*index] = Some(column);
    }
    Ok(Some(AggregateSidecar {
        file,
        expression_columns,
    }))
}

fn generic_aggregate(expression: &WindowExpr) -> bool {
    if !matches!(expression.function, WindowFunction::Aggregate(_)) {
        return false;
    }
    let whole = expression.frame.start == WindowFrameBound::UnboundedPreceding
        && expression.frame.end == WindowFrameBound::UnboundedFollowing;
    let rows_prefix = expression.frame.units == WindowFrameUnits::Rows
        && expression.frame.start == WindowFrameBound::UnboundedPreceding
        && expression.frame.end == WindowFrameBound::CurrentRow;
    let range_prefix = expression.frame.units == WindowFrameUnits::Range
        && expression.frame.start == WindowFrameBound::UnboundedPreceding
        && expression.frame.end == WindowFrameBound::CurrentRow;
    !whole && !rows_prefix && !range_prefix
}

fn retained_payload(function: AggregateFunction, data_type: &DataType, value: &CellValue) -> usize {
    if matches!(function, AggregateFunction::Min | AggregateFunction::Max) && is_variable(data_type)
    {
        cell_payload_bytes(value)
    } else {
        0
    }
}

fn output_credit(data_type: &DataType, value: &CellValue) -> usize {
    if !is_variable(data_type) {
        return 0;
    }
    let copies = if matches!(data_type, DataType::LargeUtf8 | DataType::LargeBinary) {
        3
    } else {
        2
    };
    cell_payload_bytes(value).saturating_mul(copies)
}

fn schema(expressions: &[WindowExpr], indices: &[usize]) -> SchemaRef {
    Arc::new(Schema::new(
        indices
            .iter()
            .map(|index| {
                Field::new(
                    format!("frame_aggregate_{index}"),
                    expressions[*index].data_type.clone(),
                    true,
                )
            })
            .collect::<Vec<_>>(),
    ))
}

struct ResultBuffer {
    values: Vec<Vec<CellValue>>,
    credits: Vec<Vec<usize>>,
}

impl ResultBuffer {
    fn new(columns: usize, capacity: usize) -> Self {
        Self {
            values: (0..columns).map(|_| Vec::with_capacity(capacity)).collect(),
            credits: (0..columns).map(|_| Vec::with_capacity(capacity)).collect(),
        }
    }

    fn len(&self) -> usize {
        self.values.first().map(Vec::len).unwrap_or(0)
    }

    fn push(&mut self, column: usize, value: CellValue, credit: usize) {
        self.values[column].push(value);
        self.credits[column].push(credit);
    }

    fn flush(
        &mut self,
        writer: &mut crate::runtime::SpillWriter,
        schema: &SchemaRef,
        expressions: &[WindowExpr],
        indices: &[usize],
    ) -> Result<usize> {
        if self.len() == 0 {
            return Ok(0);
        }
        let released = self.credits.iter_mut().flat_map(std::mem::take).sum();
        let columns = self
            .values
            .iter_mut()
            .zip(indices)
            .map(|(values, index)| {
                let array = values_to_array(values, &expressions[*index].data_type)?;
                values.clear();
                Ok(array)
            })
            .collect::<Result<Vec<ArrayRef>>>()?;
        writer.write_batch(&RecordBatch::try_new(Arc::clone(schema), columns)?)?;
        Ok(released)
    }
}
