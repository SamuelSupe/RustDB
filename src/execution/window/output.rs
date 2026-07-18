mod chunk;

use std::sync::Arc;

use arrow::{array::UInt64Array, datatypes::SchemaRef, record_batch::RecordBatch};

use crate::runtime::{BatchEnvelope, MemoryBatchStream, QueryContext, boxed_memory_batch_stream};
use crate::sql::{WindowExpr, WindowFunction};
use crate::{Error, Result};

use super::super::{
    aggregate::state::AggregateState,
    expr,
    value::{CellValue, cell, values_to_array},
};
use super::{
    frame_aggregate, frame_index,
    keys::{evaluate_keys, row_key},
    memory::{array_value_payload_bytes, is_variable},
    navigation, sidecar,
    spool::{CompletedPartition, aggregate_inputs, is_whole},
};
use chunk::{held_bytes, is_rows_prefix, plan as plan_chunk};

pub(super) fn process(
    partition: CompletedPartition,
    expressions: Vec<WindowExpr>,
    schema: SchemaRef,
    context: Arc<QueryContext>,
    batch_size: usize,
) -> MemoryBatchStream {
    boxed_memory_batch_stream(async_stream::try_stream! {
        let sidecar = sidecar::build(
            &partition.file,
            &expressions,
            &context,
            batch_size,
        )?;
        let frame_index = frame_index::build(
            &partition.file,
            &expressions,
            sidecar.as_ref(),
            &context,
            batch_size,
            partition.rows,
        )?;
        let frame_aggregates = frame_aggregate::build(
            &partition.file,
            frame_index.as_ref(),
            &expressions,
            &context,
            batch_size,
            partition.rows,
        )?;
        let mut frame_aggregate_reader = frame_aggregates
            .as_ref()
            .map(|sidecar| context.spill.read_file(&sidecar.file))
            .transpose()?;
        let mut frame_aggregate_batch: Option<BatchEnvelope> = None;
        let mut frame_aggregate_row = 0usize;
        let mut peer_reader = sidecar
            .as_ref()
            .map(|sidecar| context.spill.read_file(&sidecar.file))
            .transpose()?;
        let mut peer_batch: Option<BatchEnvelope> = None;
        let mut peer_row = 0usize;
        let mut peer_remaining = 0u64;
        let navigation = navigation::build(
            &partition.file,
            &expressions,
            sidecar.as_ref(),
            frame_index.as_ref(),
            &context,
            batch_size,
            partition.rows,
        )?;
        let mut navigation_reader = navigation
            .as_ref()
            .map(|sidecar| context.spill.read_file(&sidecar.file))
            .transpose()?;
        let mut navigation_batch: Option<BatchEnvelope> = None;
        let mut navigation_row = 0usize;
        let order_exprs = expressions[0]
            .order_by
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
        let state_bytes = expressions
            .len()
            .saturating_mul(256)
            .saturating_add(order_exprs.len().saturating_mul(size_of::<CellValue>() * 2))
            .saturating_add(512);
        let mut state_memory = context.memory.try_reserve(state_bytes).map_err(|_| {
            Error::ResourceExhausted(format!(
                "window running state requires {state_bytes} bytes (query limit {}, available {})",
                context.memory.limit(),
                context.memory.available()
            ))
        })?;
        // Owned state is declared after its reservation so unwinding drops the
        // values before releasing their lease.
        let mut peer_values = vec![None; expressions.len()];
        let mut peer_payload = 0usize;
        let mut retained_payload = vec![0usize; expressions.len()];

        let mut rows_states = expressions
            .iter()
            .map(|expression| {
                if is_rows_prefix(expression) {
                    let WindowFunction::Aggregate(aggregate) = &expression.function else {
                        unreachable!()
                    };
                    Some(AggregateState::new(aggregate))
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();
        let mut previous_order: Option<Vec<CellValue>> = None;
        let mut previous_order_credit = 0usize;
        let mut row_number = 0i64;
        let mut rank = 1i64;
        let mut dense_rank = 1i64;
        let mut reader = context.spill.read_file(&partition.file)?;

        for replay in &mut reader {
            context.check_cancelled()?;
            let replay = BatchEnvelope::try_new(
                replay?,
                &context.memory,
                "window replay input",
            )?;
            let evaluation = context
                .reserve_memory_while_holding(
                    expr::projection_workspace_bytes(&evaluation_exprs, replay.batch()),
                    held_bytes(
                        &replay,
                        &partition,
                        &state_memory,
                        peer_batch.as_ref(),
                        frame_aggregate_batch.as_ref(),
                        None,
                        None,
                    ),
                    "window replay expression workspace",
                )
                .await?;
            let (order_keys, inputs) = {
                let _compute = context.acquire_compute().await?;
                let _active = context.scheduler.enter_lane();
                #[cfg(test)]
                super::fault_injection::panic_if_armed(context.query_id);
                (
                    evaluate_keys(&order_exprs, replay.batch())?,
                    aggregate_inputs(&expressions, replay.batch())?,
                )
            };
            let mut offset = 0usize;

            while offset < replay.num_rows() {
                context.check_cancelled()?;
                if navigation.is_some()
                    && navigation_batch
                        .as_ref()
                        .is_none_or(|batch| navigation_row >= batch.num_rows())
                {
                    drop(navigation_batch.take());
                    navigation_batch = match navigation_reader.as_mut().and_then(Iterator::next) {
                        Some(batch) => Some(BatchEnvelope::try_new(
                            batch?,
                            &context.memory,
                            "window navigation sidecar input",
                        )?),
                        None => None,
                    };
                    navigation_row = 0;
                    if navigation_batch.is_none() {
                        Err(Error::Internal(
                            "window navigation sidecar ended before partition input".into(),
                        ))?;
                    }
                }
                if frame_aggregates.is_some()
                    && frame_aggregate_batch
                        .as_ref()
                        .is_none_or(|batch| frame_aggregate_row >= batch.num_rows())
                {
                    drop(frame_aggregate_batch.take());
                    frame_aggregate_batch = match frame_aggregate_reader
                        .as_mut()
                        .and_then(Iterator::next)
                    {
                        Some(batch) => Some(BatchEnvelope::try_new(
                            batch?,
                            &context.memory,
                            "window frame aggregate sidecar input",
                        )?),
                        None => None,
                    };
                    frame_aggregate_row = 0;
                    if frame_aggregate_batch.is_none() {
                        Err(Error::Internal(
                            "window frame aggregate sidecar ended before partition input".into(),
                        ))?;
                    }
                }
                if let Some(sidecar_metadata) = sidecar.as_ref()
                    && peer_remaining == 0
                {
                    loop {
                        if peer_batch
                            .as_ref()
                            .is_some_and(|batch| peer_row < batch.num_rows())
                        {
                            break;
                        }
                        // Drop the exhausted batch before allocating/accounting
                        // its successor.
                        drop(peer_batch.take());
                        peer_batch = match peer_reader.as_mut().and_then(Iterator::next) {
                            Some(batch) => Some(BatchEnvelope::try_new(
                                batch?,
                                &context.memory,
                                "window peer sidecar input",
                            )?),
                            None => None,
                        };
                        peer_row = 0;
                        if peer_batch.is_none() {
                            Err(Error::Internal(
                                "window peer sidecar ended before partition input".into(),
                            ))?;
                        }
                    }
                    {
                        let _compute = context.acquire_compute().await?;
                        let _active = context.scheduler.enter_lane();
                        let summary = peer_batch.as_ref().expect("peer row established");
                        let lengths = summary
                            .column(0)
                            .as_any()
                            .downcast_ref::<UInt64Array>()
                            .ok_or_else(|| Error::Internal(
                                "window peer sidecar length column is not UINT64".into(),
                            ))?;
                        peer_remaining = lengths.value(peer_row);
                        if peer_remaining == 0 {
                            Err(Error::Internal(
                                "window peer sidecar contains an empty peer".into(),
                            ))?;
                        }
                        let mapping = &sidecar_metadata.expression_columns;
                        let next_payload = mapping
                            .iter()
                            .enumerate()
                            .try_fold(0usize, |bytes, (index, column)| -> Result<usize> {
                                let Some(column) = column else { return Ok(bytes) };
                                if !is_variable(&expressions[index].data_type) {
                                    return Ok(bytes);
                                }
                                Ok(bytes.saturating_add(array_value_payload_bytes(
                                    summary.column(*column),
                                    peer_row,
                                )?))
                            })?;
                        // New peer values and the prior peer values coexist during
                        // cloning/replacement.
                        state_memory.try_grow(next_payload)?;
                        let mut next_values = vec![None; expressions.len()];
                        for (index, column) in mapping.iter().enumerate() {
                            if let Some(column) = column {
                                next_values[index] = Some(cell(summary.column(*column), peer_row)?);
                            }
                        }
                        let previous_values = std::mem::replace(&mut peer_values, next_values);
                        drop(previous_values);
                        state_memory.shrink(peer_payload);
                        peer_payload = next_payload;
                        peer_row += 1;
                    }
                }

                let peer_rows = sidecar
                    .as_ref()
                    .map(|_| usize::try_from(peer_remaining).unwrap_or(usize::MAX));
                let held = held_bytes(
                    &replay,
                    &partition,
                    &state_memory,
                    peer_batch.as_ref(),
                    frame_aggregate_batch.as_ref(),
                    navigation_batch.as_ref(),
                    Some(&evaluation),
                );
                let mut rows = (replay.num_rows() - offset).min(batch_size.max(1));
                if let Some(peer_rows) = peer_rows {
                    rows = rows.min(peer_rows);
                }
                if let Some(batch) = &navigation_batch {
                    rows = rows.min(batch.num_rows() - navigation_row);
                }
                if let Some(batch) = &frame_aggregate_batch {
                    rows = rows.min(batch.num_rows() - frame_aggregate_row);
                }
                let operation_limit = context.memory.operation_limit();
                let plan = {
                    let _compute = context.acquire_compute().await?;
                    let _active = context.scheduler.enter_lane();
                    loop {
                        let plan = plan_chunk(
                            &expressions,
                            &partition,
                            &inputs,
                            &order_keys,
                            &peer_values,
                            &retained_payload,
                            previous_order_credit,
                            replay.batch(),
                            offset,
                            rows,
                            navigation.as_ref(),
                            navigation_batch.as_ref(),
                            navigation_row,
                            frame_aggregates.as_ref(),
                            frame_aggregate_batch.as_ref(),
                            frame_aggregate_row,
                        )?;
                        let required = held
                            .saturating_add(plan.state_growth)
                            .saturating_add(plan.workspace);
                        if required <= operation_limit {
                            break plan;
                        }
                        if rows == 1 {
                            Err(Error::ResourceExhausted(format!(
                                "window cannot emit one row requiring {} workspace and {} retained bytes (query limit {})",
                                plan.workspace,
                                held.saturating_add(plan.state_growth),
                                operation_limit,
                            )))?;
                        }
                        rows = (rows / 2).max(1);
                    }
                };

                if plan.state_growth != 0 {
                    let growth = context
                        .reserve_memory_while_holding(
                            plan.state_growth,
                            held,
                            "window retained state growth",
                        )
                        .await?;
                    state_memory.absorb(growth)?;
                }
                retained_payload = plan.retained_payload;
                previous_order_credit = plan.order_credit;
                let workspace = context
                    .reserve_memory_while_holding(
                        plan.workspace,
                        held.saturating_add(plan.state_growth),
                        "window output workspace",
                    )
                    .await?;
                let output = {
                    let _compute = context.acquire_compute().await?;
                    let _active = context.scheduler.enter_lane();
                    let mut values = expressions
                        .iter()
                        .map(|_| Vec::with_capacity(rows))
                        .collect::<Vec<Vec<CellValue>>>();

                    for row in offset..offset + rows {
                        row_number = row_number.checked_add(1).ok_or_else(|| {
                            Error::Execution("ROW_NUMBER overflowed INT64".into())
                        })?;
                        let order = row_key(&order_keys, row)?;
                        if previous_order
                            .as_ref()
                            .is_some_and(|previous| previous != &order)
                        {
                            rank = row_number;
                            dense_rank = dense_rank.checked_add(1).ok_or_else(|| {
                                Error::Execution("DENSE_RANK overflowed INT64".into())
                            })?;
                        }
                        previous_order = Some(order);

                        for (index, expression) in expressions.iter().enumerate() {
                            let value = match &expression.function {
                            WindowFunction::RowNumber => CellValue::Int64(row_number),
                            WindowFunction::Rank => CellValue::Int64(rank),
                            WindowFunction::DenseRank => CellValue::Int64(dense_rank),
                            WindowFunction::Ntile(buckets) => CellValue::Int64(ntile(
                                u64::try_from(row_number).map_err(|_| {
                                    Error::Execution("NTILE row number is out of range".into())
                                })?,
                                partition.rows,
                                *buckets,
                            )?),
                            WindowFunction::PercentRank => {
                                let denominator = partition.rows.saturating_sub(1);
                                CellValue::Float64(if denominator == 0 {
                                    0.0
                                } else {
                                    (rank - 1) as f64 / denominator as f64
                                })
                            }
                            WindowFunction::CumeDist => {
                                let peer_end = u64::try_from(row_number)
                                    .map_err(|_| Error::Execution(
                                        "CUME_DIST row number is out of range".into(),
                                    ))?
                                    .checked_add(peer_remaining.saturating_sub(1))
                                    .ok_or_else(|| Error::Execution(
                                        "CUME_DIST peer position overflowed UINT64".into(),
                                    ))?;
                                CellValue::Float64(peer_end as f64 / partition.rows as f64)
                            }
                            WindowFunction::Lead { .. }
                            | WindowFunction::Lag { .. }
                            | WindowFunction::FirstValue(_)
                            | WindowFunction::LastValue(_) => {
                                let column = navigation
                                    .as_ref()
                                    .and_then(|sidecar| sidecar.expression_columns[index])
                                    .ok_or_else(|| Error::Internal(
                                        "window navigation result column is missing".into(),
                                    ))?;
                                cell(
                                    navigation_batch
                                        .as_ref()
                                        .ok_or_else(|| Error::Internal(
                                            "window navigation result batch is missing".into(),
                                        ))?
                                        .column(column),
                                    navigation_row + row - offset,
                                )?
                            }
                            WindowFunction::Aggregate(_) if is_whole(expression) => partition
                                .whole_values[index]
                                .clone()
                                .ok_or_else(|| Error::Internal(
                                    "whole-partition window aggregate has no finalized state".into(),
                                ))?,
                            WindowFunction::Aggregate(_)
                                if frame_aggregates
                                    .as_ref()
                                    .and_then(|sidecar| sidecar.expression_columns[index])
                                    .is_some() =>
                            {
                                let column = frame_aggregates
                                    .as_ref()
                                    .and_then(|sidecar| sidecar.expression_columns[index])
                                    .expect("frame aggregate mapping checked");
                                cell(
                                    frame_aggregate_batch
                                        .as_ref()
                                        .ok_or_else(|| Error::Internal(
                                            "window frame aggregate result batch is missing".into(),
                                        ))?
                                        .column(column),
                                    frame_aggregate_row + row - offset,
                                )?
                            }
                            WindowFunction::Aggregate(aggregate)
                                if is_rows_prefix(expression) =>
                            {
                                let input = inputs[index]
                                    .as_ref()
                                    .map(|array| cell(array, row))
                                    .transpose()?;
                                let state = rows_states[index]
                                    .as_mut()
                                    .expect("ROWS state initialized");
                                state.update(aggregate, input)?;
                                state.finish()?
                            }
                            WindowFunction::Aggregate(_) => peer_values[index]
                                .clone()
                                .ok_or_else(|| Error::Internal(
                                    "RANGE window aggregate has no peer result".into(),
                                ))?,
                            };
                            values[index].push(value);
                        }
                        if sidecar.is_some() {
                            peer_remaining -= 1;
                        }
                    }

                    let mut columns = replay.batch().slice(offset, rows).columns().to_vec();
                    for (index, output_values) in values.iter().enumerate() {
                        columns.push(values_to_array(
                            output_values,
                            &expressions[index].data_type,
                        )?);
                    }
                    // values own the transient variable-width copies credited in
                    // workspace; release them before workspace is reconciled to
                    // the final Arrow batch and before yielding.
                    drop(values);
                    let output = RecordBatch::try_new(Arc::clone(&schema), columns)?;
                    offset += rows;
                    if navigation_batch.is_some() {
                        navigation_row += rows;
                    }
                    if frame_aggregate_batch.is_some() {
                        frame_aggregate_row += rows;
                    }
                    BatchEnvelope::from_reservation(output, workspace, "window output")?
                };
                // Yielding returns to the partition worker, which may block on
                // its output channel. Both compute guards are gone first.
                yield output;
            }
            drop(inputs);
            drop(order_keys);
            drop(evaluation);
        }
        drop(reader);
        drop(peer_reader);
        drop(peer_batch);
        drop(navigation_reader);
        drop(navigation_batch);
        drop(frame_aggregate_reader);
        drop(frame_aggregate_batch);
        if peer_remaining != 0 {
            Err(Error::Internal(
                "window peer sidecar contains more rows than partition input".into(),
            ))?;
        }
        drop(previous_order);
        drop(peer_values);
        drop(rows_states);
        drop(retained_payload);
        drop(state_memory);
        if let Some(sidecar) = sidecar {
            context.spill.remove_file(&sidecar.file)?;
        }
        if let Some(sidecar) = navigation {
            context.spill.remove_file(&sidecar.file)?;
        }
        if let Some(sidecar) = frame_aggregates {
            context.spill.remove_file(&sidecar.file)?;
        }
        if let Some(sidecar) = frame_index {
            context.spill.remove_file(&sidecar.file)?;
        }
        context.spill.remove_file(&partition.file)?;
        drop(partition);
    })
}

fn ntile(row: u64, rows: u64, buckets: u64) -> Result<i64> {
    let quotient = rows / buckets;
    let remainder = rows % buckets;
    let position = row - 1;
    let large_rows = quotient
        .checked_add(1)
        .and_then(|size| size.checked_mul(remainder))
        .ok_or_else(|| Error::Execution("NTILE bucket calculation overflowed UINT64".into()))?;
    let tile = if position < large_rows {
        position / (quotient + 1) + 1
    } else {
        remainder + (position - large_rows) / quotient + 1
    };
    i64::try_from(tile).map_err(|_| Error::Execution("NTILE result overflowed INT64".into()))
}
