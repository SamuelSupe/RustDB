use std::mem::size_of;

use arrow::{array::ArrayRef, record_batch::RecordBatch};

use crate::Result;
use crate::runtime::{BatchEnvelope, MemoryReservation};
use crate::sql::{
    AggregateFunction, WindowExpr, WindowFrameBound, WindowFrameUnits, WindowFunction,
};

use super::super::{
    frame_aggregate,
    memory::{
        array_value_payload_bytes, is_variable, original_slice_bytes, row_payload_bytes,
        variable_output_bound,
    },
    navigation,
    spool::{CompletedPartition, is_whole},
};
use crate::execution::value::CellValue;

pub(super) struct ChunkPlan {
    pub(super) workspace: usize,
    pub(super) state_growth: usize,
    pub(super) order_credit: usize,
    pub(super) retained_payload: Vec<usize>,
}

#[allow(clippy::too_many_arguments)]
pub(super) fn plan(
    expressions: &[WindowExpr],
    partition: &CompletedPartition,
    inputs: &[Option<ArrayRef>],
    order_keys: &[ArrayRef],
    peer_values: &[Option<CellValue>],
    retained_payload: &[usize],
    previous_order_credit: usize,
    batch: &RecordBatch,
    offset: usize,
    rows: usize,
    navigation: Option<&navigation::NavigationSidecar>,
    navigation_batch: Option<&BatchEnvelope>,
    navigation_row: usize,
    frame_aggregates: Option<&frame_aggregate::AggregateSidecar>,
    frame_aggregate_batch: Option<&BatchEnvelope>,
    frame_aggregate_row: usize,
) -> Result<ChunkPlan> {
    let order_credit = (offset..offset + rows)
        .try_fold(previous_order_credit, |bytes, row| -> Result<usize> {
            Ok(bytes.max(row_payload_bytes(order_keys, row)?))
        })?;
    let order_transient = (offset..offset + rows)
        .try_fold(0usize, |bytes, row| -> Result<usize> {
            Ok(bytes.max(row_payload_bytes(order_keys, row)?))
        })?;
    let mut next_retained = retained_payload.to_vec();
    let mut update_transient = 0usize;
    for (index, expression) in expressions.iter().enumerate() {
        let WindowFunction::Aggregate(aggregate) = &expression.function else {
            continue;
        };
        if !is_rows_prefix(expression)
            || !matches!(
                aggregate.function,
                AggregateFunction::Min | AggregateFunction::Max
            )
            || !is_variable(&expression.data_type)
        {
            continue;
        }
        let Some(input) = inputs[index].as_ref() else {
            continue;
        };
        let maximum = (offset..offset + rows).try_fold(0usize, |bytes, row| -> Result<usize> {
            Ok(bytes.max(array_value_payload_bytes(input, row)?))
        })?;
        update_transient = update_transient.saturating_add(maximum);
        next_retained[index] = next_retained[index].max(maximum);
    }
    let state_growth = order_credit
        .saturating_sub(previous_order_credit)
        .saturating_add(
            next_retained
                .iter()
                .zip(retained_payload)
                .fold(0usize, |bytes, (next, current)| {
                    bytes.saturating_add(next.saturating_sub(*current))
                }),
        );
    let mut workspace = original_slice_bytes(batch, offset, rows)
        .saturating_add(order_transient)
        .saturating_add(update_transient)
        .saturating_add(expressions.len().saturating_mul(size_of::<ArrayRef>()))
        .saturating_add(2048);
    for (index, expression) in expressions.iter().enumerate() {
        let whole_value = matches!(expression.function, WindowFunction::Aggregate(_))
            .then(|| is_whole(expression))
            .unwrap_or(false)
            .then(|| partition.whole_values[index].as_ref())
            .flatten();
        let frame_column = frame_aggregates.and_then(|sidecar| sidecar.expression_columns[index]);
        let range_value = if frame_column.is_none()
            && matches!(expression.function, WindowFunction::Aggregate(_))
            && !is_whole(expression)
            && !is_rows_prefix(expression)
        {
            peer_values[index].as_ref()
        } else {
            None
        };
        let navigation_column = navigation.and_then(|sidecar| sidecar.expression_columns[index]);
        let input = frame_column
            .and_then(|column| frame_aggregate_batch.map(|batch| batch.column(column)))
            .or_else(|| {
                navigation_column
                    .and_then(|column| navigation_batch.map(|batch| batch.column(column)))
            })
            .or_else(|| {
                is_rows_prefix(expression)
                    .then(|| inputs[index].as_ref())
                    .flatten()
            });
        let value_offset = if frame_column.is_some() {
            frame_aggregate_row
        } else if navigation_column.is_some() {
            navigation_row
        } else {
            offset
        };
        workspace = workspace.saturating_add(variable_output_bound(
            expression,
            whole_value,
            range_value,
            input,
            next_retained[index],
            value_offset,
            rows,
        )?);
    }
    Ok(ChunkPlan {
        workspace,
        state_growth,
        order_credit,
        retained_payload: next_retained,
    })
}

pub(super) fn held_bytes(
    replay: &BatchEnvelope,
    partition: &CompletedPartition,
    state: &MemoryReservation,
    peer_batch: Option<&BatchEnvelope>,
    frame_aggregate_batch: Option<&BatchEnvelope>,
    navigation_batch: Option<&BatchEnvelope>,
    evaluation: Option<&MemoryReservation>,
) -> usize {
    replay
        .memory_size()
        .saturating_add(partition.retained_bytes())
        .saturating_add(state.size())
        .saturating_add(peer_batch.map(BatchEnvelope::memory_size).unwrap_or(0))
        .saturating_add(
            frame_aggregate_batch
                .map(BatchEnvelope::memory_size)
                .unwrap_or(0),
        )
        .saturating_add(
            navigation_batch
                .map(BatchEnvelope::memory_size)
                .unwrap_or(0),
        )
        .saturating_add(evaluation.map_or(0, MemoryReservation::size))
}

pub(super) fn is_rows_prefix(expression: &WindowExpr) -> bool {
    expression.frame.units == WindowFrameUnits::Rows
        && expression.frame.start == WindowFrameBound::UnboundedPreceding
        && expression.frame.end == WindowFrameBound::CurrentRow
        && matches!(expression.function, WindowFunction::Aggregate(_))
}
