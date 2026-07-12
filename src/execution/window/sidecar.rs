use std::{mem::size_of, sync::Arc};

use arrow::{
    array::{ArrayRef, UInt64Array},
    datatypes::{DataType, Field, Schema, SchemaRef},
    record_batch::RecordBatch,
};

use crate::runtime::{BatchEnvelope, QueryContext, SpillFile, estimate_schema_batch_bytes};
use crate::sql::{
    AggregateFunction, WindowExpr, WindowFrameBound, WindowFrameUnits, WindowFunction,
};
use crate::{Error, Result};

use super::super::{
    aggregate::state::AggregateState,
    expr,
    value::{CellValue, cell, values_to_array},
};
use super::{
    keys::{evaluate_keys, row_key},
    memory::{array_value_payload_bytes, cell_payload_bytes, is_variable, row_payload_bytes},
    spool::aggregate_inputs,
};

pub(super) struct RangeSidecar {
    pub(super) file: SpillFile,
    pub(super) expression_columns: Vec<Option<usize>>,
}

pub(super) fn build(
    partition: &SpillFile,
    expressions: &[WindowExpr],
    context: &QueryContext,
    batch_size: usize,
) -> Result<Option<RangeSidecar>> {
    let range_indices = expressions
        .iter()
        .enumerate()
        .filter_map(|(index, expression)| is_range_aggregate(expression).then_some(index))
        .collect::<Vec<_>>();
    if range_indices.is_empty() {
        return Ok(None);
    }
    let schema = sidecar_schema(expressions, &range_indices);
    let capacity = batch_size.max(1);
    // One credit covers the buffered CellValue rows and one covers the Arrow
    // arrays materialized while those rows are still alive.
    let bytes = estimate_schema_batch_bytes(schema.as_ref(), capacity)
        .saturating_mul(2)
        .saturating_add(
            range_indices
                .len()
                .saturating_mul(capacity)
                .saturating_mul(size_of::<CellValue>()),
        )
        .saturating_add(
            expressions[0]
                .order_by
                .len()
                .saturating_mul(size_of::<CellValue>() * 2),
        )
        .saturating_add(
            range_indices
                .len()
                .saturating_mul(size_of::<Vec<CellValue>>()),
        )
        .saturating_add(expressions.len().saturating_mul(256));
    let mut memory = context.memory.try_reserve(bytes).map_err(|_| {
        Error::ResourceExhausted(format!(
            "window RANGE peer buffer requires {bytes} bytes (query limit {}, available {})",
            context.memory.limit(),
            context.memory.available()
        ))
    })?;
    let mut writer = context
        .spill
        .writer("window-range-peers", Arc::clone(&schema))?;
    let mut reader = context.spill.read_file(partition)?;
    let mut states = expressions
        .iter()
        .map(|expression| {
            if is_range_aggregate(expression) {
                let WindowFunction::Aggregate(aggregate) = &expression.function else {
                    unreachable!()
                };
                Some(AggregateState::new(aggregate))
            } else {
                None
            }
        })
        .collect::<Vec<_>>();
    let order = &expressions[0].order_by;
    let order_exprs = order
        .iter()
        .map(|sort| sort.expr.clone())
        .collect::<Vec<_>>();
    let evaluation_exprs = order_exprs
        .iter()
        .cloned()
        .chain(expressions.iter().filter_map(|expression| {
            let WindowFunction::Aggregate(aggregate) = &expression.function else {
                return None;
            };
            aggregate.expr.clone()
        }))
        .collect::<Vec<_>>();
    let mut current_key: Option<Vec<CellValue>> = None;
    let mut current_key_payload = 0usize;
    let mut peer_len = 0u64;
    let mut summaries = SummaryBuffer::new(range_indices.len(), capacity);
    let mut retained_payload = vec![0usize; expressions.len()];

    for batch in &mut reader {
        context.check_cancelled()?;
        let batch = BatchEnvelope::try_new(batch?, &context.memory, "window sidecar input")?;
        let evaluation_bytes = expr::projection_workspace_bytes(&evaluation_exprs, batch.batch());
        let evaluation = context.memory.try_reserve(evaluation_bytes).map_err(|_| {
            Error::ResourceExhausted(format!(
                "window sidecar expression workspace requires {evaluation_bytes} bytes (query limit {}, available {})",
                context.memory.limit(),
                context.memory.available(),
            ))
        })?;
        let keys = evaluate_keys(&order_exprs, batch.batch())?;
        let inputs = aggregate_inputs(expressions, batch.batch())?;
        for row in 0..batch.num_rows() {
            let key_payload = row_payload_bytes(&keys, row)?;
            // The old key and the newly cloned key coexist until replacement.
            memory.try_grow(key_payload)?;
            let key = row_key(&keys, row)?;
            if current_key.as_ref().is_some_and(|current| current != &key) {
                push_summary(
                    &mut summaries,
                    peer_len,
                    &states,
                    &range_indices,
                    expressions,
                    &retained_payload,
                    &mut memory,
                    &mut writer,
                    &schema,
                )?;
                peer_len = 0;
            }
            let previous_key = current_key.replace(key);
            drop(previous_key);
            memory.shrink(current_key_payload);
            current_key_payload = key_payload;
            peer_len = peer_len.checked_add(1).ok_or_else(|| {
                Error::Execution("window peer row count overflowed UINT64".into())
            })?;
            for index in &range_indices {
                let WindowFunction::Aggregate(aggregate) = &expressions[*index].function else {
                    unreachable!()
                };
                let payload = if matches!(
                    aggregate.function,
                    AggregateFunction::Min | AggregateFunction::Max
                ) {
                    inputs[*index]
                        .as_ref()
                        .map(|array| array_value_payload_bytes(array, row))
                        .transpose()?
                        .unwrap_or(0)
                } else {
                    0
                };
                // AggregateState::update consumes the new value, so its clone
                // temporarily coexists with the prior retained MIN/MAX value.
                memory.try_grow(payload)?;
                let value = inputs[*index]
                    .as_ref()
                    .map(|array| cell(array, row))
                    .transpose()?;
                states[*index]
                    .as_mut()
                    .expect("range state initialized")
                    .update(aggregate, value)?;
                let stable = retained_payload[*index].max(payload);
                memory.shrink(
                    retained_payload[*index]
                        .saturating_add(payload)
                        .saturating_sub(stable),
                );
                retained_payload[*index] = stable;
            }
        }
        drop(inputs);
        drop(keys);
        drop(evaluation);
        drop(batch);
    }
    if current_key.is_some() {
        push_summary(
            &mut summaries,
            peer_len,
            &states,
            &range_indices,
            expressions,
            &retained_payload,
            &mut memory,
            &mut writer,
            &schema,
        )?;
    }
    let released = summaries.flush(&mut writer, &schema, expressions, &range_indices)?;
    memory.shrink(released);
    drop(current_key);
    drop(states);
    drop(summaries);
    drop(retained_payload);
    drop(memory);
    let file = writer.finish(1)?;
    let mut expression_columns = vec![None; expressions.len()];
    for (column, index) in range_indices.iter().enumerate() {
        expression_columns[*index] = Some(column + 1);
    }
    Ok(Some(RangeSidecar {
        file,
        expression_columns,
    }))
}

#[allow(clippy::too_many_arguments)]
fn push_summary(
    summaries: &mut SummaryBuffer,
    peer_len: u64,
    states: &[Option<AggregateState>],
    indices: &[usize],
    expressions: &[WindowExpr],
    retained_payload: &[usize],
    memory: &mut crate::runtime::MemoryReservation,
    writer: &mut crate::runtime::SpillWriter,
    schema: &SchemaRef,
) -> Result<()> {
    let bound = indices.iter().fold(0usize, |bytes, index| {
        bytes.saturating_add(summary_credit(
            &expressions[*index].data_type,
            retained_payload[*index],
        ))
    });
    if bound > memory.pool().available() && !summaries.is_empty() {
        let released = summaries.flush(writer, schema, expressions, indices)?;
        memory.shrink(released);
    }
    // Reserve before finish() clones state and before flush copies the buffered
    // values into Arrow arrays.
    memory.try_grow(bound)?;
    let actual = summaries.push(peer_len, states, indices, expressions)?;
    memory.shrink(bound.saturating_sub(actual));
    let released = summaries.flush_if_full(writer, schema, expressions, indices)?;
    memory.shrink(released);
    Ok(())
}

fn summary_credit(data_type: &DataType, payload: usize) -> usize {
    if !is_variable(data_type) {
        return 0;
    }
    // CellValue plus the canonical Arrow array coexist. Large values require
    // one additional cast buffer in values_to_array.
    let copies = if matches!(data_type, DataType::LargeUtf8 | DataType::LargeBinary) {
        3
    } else {
        2
    };
    payload.saturating_mul(copies)
}

fn is_range_aggregate(expression: &WindowExpr) -> bool {
    expression.frame.units == WindowFrameUnits::Range
        && expression.frame.end == WindowFrameBound::CurrentRow
        && matches!(expression.function, WindowFunction::Aggregate(_))
}

fn sidecar_schema(expressions: &[WindowExpr], indices: &[usize]) -> SchemaRef {
    Arc::new(Schema::new(
        std::iter::once(Field::new("peer_rows", DataType::UInt64, false))
            .chain(indices.iter().map(|index| {
                Field::new(
                    format!("window_{index}"),
                    expressions[*index].data_type.clone(),
                    true,
                )
            }))
            .collect::<Vec<_>>(),
    ))
}

struct SummaryBuffer {
    lengths: Vec<u64>,
    values: Vec<Vec<CellValue>>,
    capacity: usize,
}

impl SummaryBuffer {
    fn new(columns: usize, capacity: usize) -> Self {
        Self {
            lengths: Vec::with_capacity(capacity),
            values: (0..columns).map(|_| Vec::with_capacity(capacity)).collect(),
            capacity,
        }
    }

    fn is_empty(&self) -> bool {
        self.lengths.is_empty()
    }

    fn push(
        &mut self,
        length: u64,
        states: &[Option<AggregateState>],
        indices: &[usize],
        expressions: &[WindowExpr],
    ) -> Result<usize> {
        self.lengths.push(length);
        let mut payload = 0usize;
        for (column, index) in indices.iter().enumerate() {
            let value = states[*index]
                .as_ref()
                .expect("range state initialized")
                .finish()?;
            payload = payload.saturating_add(summary_credit(
                &expressions[*index].data_type,
                cell_payload_bytes(&value),
            ));
            self.values[column].push(value);
        }
        Ok(payload)
    }

    fn flush_if_full(
        &mut self,
        writer: &mut crate::runtime::SpillWriter,
        schema: &SchemaRef,
        expressions: &[WindowExpr],
        indices: &[usize],
    ) -> Result<usize> {
        if self.lengths.len() >= self.capacity {
            return self.flush(writer, schema, expressions, indices);
        }
        Ok(0)
    }

    fn flush(
        &mut self,
        writer: &mut crate::runtime::SpillWriter,
        schema: &SchemaRef,
        expressions: &[WindowExpr],
        indices: &[usize],
    ) -> Result<usize> {
        if self.lengths.is_empty() {
            return Ok(0);
        }
        let released = self
            .values
            .iter()
            .zip(indices)
            .fold(0usize, |bytes, (values, index)| {
                values.iter().fold(bytes, |bytes, value| {
                    bytes.saturating_add(summary_credit(
                        &expressions[*index].data_type,
                        cell_payload_bytes(value),
                    ))
                })
            });
        let mut columns: Vec<ArrayRef> = vec![Arc::new(UInt64Array::from(std::mem::take(
            &mut self.lengths,
        )))];
        for (column, index) in indices.iter().enumerate() {
            columns.push(values_to_array(
                &std::mem::take(&mut self.values[column]),
                &expressions[*index].data_type,
            )?);
        }
        writer.write_batch(&RecordBatch::try_new(Arc::clone(schema), columns)?)?;
        Ok(released)
    }
}
