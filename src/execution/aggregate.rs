use std::{collections::HashMap, sync::Arc};

use arrow::{
    datatypes::{DataType, Field, Schema, SchemaRef},
    record_batch::RecordBatch,
};
use futures::StreamExt;

use crate::Result;
use crate::runtime::{
    BatchEnvelope, IntoMemoryBatchStream, MemoryBatchStream, QueryContext,
    boxed_memory_batch_stream,
};
use crate::sql::{AggregateExpr, AggregateFunction, BoundExpr};

use super::{
    expr::evaluate,
    value::{CellValue, cell, values_to_array},
};

mod distinct;
mod key;
mod parallel;
mod spill;
pub(super) mod state;

#[cfg(test)]
mod tests;

use key::{GroupKey, GroupKeyEncoder};
use spill::{
    MergeOutcome, PartitionTask, SPILL_PARTITIONS, StateSpiller, merge_partition,
    repartition_partition, spill_states,
};
use state::{GroupState, estimate_group_bytes};

pub(crate) fn aggregate<I>(
    input: I,
    groups: Vec<BoundExpr>,
    aggregates: Vec<AggregateExpr>,
    schema: SchemaRef,
    context: Arc<QueryContext>,
    batch_size: usize,
) -> MemoryBatchStream
where
    I: IntoMemoryBatchStream,
{
    let input = input.into_memory_batch_stream(Arc::clone(&context), "aggregate input");
    boxed_memory_batch_stream(async_stream::try_stream! {
        let mut output = if aggregates.iter().any(|aggregate| aggregate.distinct) {
            distinct::aggregate(
                input,
                groups,
                aggregates,
                schema,
                Arc::clone(&context),
                batch_size,
            )
        } else if parallel::is_supported(&aggregates, &context) {
            parallel::aggregate(
                input,
                groups,
                aggregates,
                schema,
                Arc::clone(&context),
                batch_size,
            )
        } else {
            serial_aggregate(input, groups, aggregates, schema, context, batch_size)
        };
        while let Some(batch) = output.next().await {
            yield batch?;
        }
    })
}

fn serial_aggregate(
    input: MemoryBatchStream,
    groups: Vec<BoundExpr>,
    aggregates: Vec<AggregateExpr>,
    schema: SchemaRef,
    context: Arc<QueryContext>,
    batch_size: usize,
) -> MemoryBatchStream {
    aggregate_with_modes(
        input,
        groups,
        aggregates,
        schema,
        context,
        batch_size,
        InputMode::Raw,
        OutputMode::Final,
    )
}

fn serial_partial_aggregate(
    input: MemoryBatchStream,
    groups: Vec<BoundExpr>,
    aggregates: Vec<AggregateExpr>,
    context: Arc<QueryContext>,
    batch_size: usize,
) -> MemoryBatchStream {
    let schema = partial_schema(&groups, &aggregates);
    aggregate_with_modes(
        input,
        groups,
        aggregates,
        schema,
        context,
        batch_size,
        InputMode::Raw,
        OutputMode::Partial,
    )
}

fn merge_partial_aggregate(
    input: MemoryBatchStream,
    groups: Vec<BoundExpr>,
    aggregates: Vec<AggregateExpr>,
    schema: SchemaRef,
    context: Arc<QueryContext>,
    batch_size: usize,
) -> MemoryBatchStream {
    aggregate_with_modes(
        input,
        groups,
        aggregates,
        schema,
        context,
        batch_size,
        InputMode::Partial,
        OutputMode::Final,
    )
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum InputMode {
    Raw,
    Partial,
}

#[derive(Clone, Copy)]
pub(super) enum OutputMode {
    Final,
    Partial,
}

#[allow(clippy::too_many_arguments)]
fn aggregate_with_modes(
    mut input: MemoryBatchStream,
    groups: Vec<BoundExpr>,
    aggregates: Vec<AggregateExpr>,
    schema: SchemaRef,
    context: Arc<QueryContext>,
    batch_size: usize,
    input_mode: InputMode,
    output_mode: OutputMode,
) -> MemoryBatchStream {
    boxed_memory_batch_stream(async_stream::try_stream! {
        let mut group_index: HashMap<GroupKey, usize> = HashMap::new();
        let mut states = Vec::<GroupState>::new();
        let key_encoder = GroupKeyEncoder::new(&groups);
        // Keep a bounded part of the query budget available for decoding and
        // repartitioning spill batches while aggregate states are resident.
        let state_pool = context.memory.child(
            format!("aggregate-{}", context.query_id),
            aggregate_state_limit(
                context.memory.limit(),
                output_mode,
                context.scheduler.configured_lanes(),
            ),
        );
        let mut reservation = state_pool.reservation();
        let partial_schema = partial_schema(&groups, &aggregates);
        let mut spilled: Option<StateSpiller> = None;

        if groups.is_empty() {
            group_index.insert(GroupKey::Encoded(Vec::new()), 0);
            states.push(GroupState::new(Vec::new(), &aggregates));
            reservation.try_grow(estimate_group_bytes(&states[0]))?;
        }

        while let Some(batch) = input.next().await {
            context.check_cancelled()?;
            let batch = batch?;
            if input_mode == InputMode::Raw
                && groups.is_empty()
                && count_star_only(&aggregates)
            {
                let state = states
                    .first_mut()
                    .ok_or_else(|| crate::Error::Internal("global aggregate state is missing".into()))?;
                for aggregate in &mut state.aggregates {
                    aggregate.add_count_star_batch(batch.batch().num_rows())?;
                }
                continue;
            }
            // Expression kernels and Arrow row encoding retain derived arrays
            // for the complete batch loop. Reserve their workspace before
            // allocating, then resize to the buffers actually retained.
            let workspace_estimate = aggregate_workspace_estimate(batch.batch(), &groups);
            let mut workspace = context.memory.try_reserve(workspace_estimate)?;
            let group_arrays = if input_mode == InputMode::Raw {
                groups
                    .iter()
                    .map(|expr| evaluate(expr, batch.batch()))
                    .collect::<Result<Vec<_>>>()?
            } else {
                (0..groups.len())
                    .map(|index| Arc::clone(batch.batch().column(index)))
                    .collect()
            };
            let encoded_groups = key_encoder.encode(&group_arrays)?;
            let aggregate_arrays = if input_mode == InputMode::Raw {
                Some(
                    aggregates
                        .iter()
                        .map(|aggregate| {
                            aggregate
                                .expr
                                .as_ref()
                                .map(|expr| evaluate(expr, batch.batch()))
                                .transpose()
                        })
                        .collect::<Result<Vec<_>>>()?,
                )
            } else {
                None
            };
            let workspace_bytes = retained_workspace_bytes(
                batch.batch(),
                &group_arrays,
                aggregate_arrays.as_deref(),
                &encoded_groups,
            );
            workspace.try_resize(workspace_bytes)?;
            context.metrics.observe_memory(context.memory.used());

            for row in 0..batch.batch().num_rows() {
                let index_key = key_encoder.key(&encoded_groups, &group_arrays, row)?;
                let index = if let Some(index) = group_index.get(&index_key) {
                    *index
                } else {
                    let state_key = group_arrays
                        .iter()
                        .map(|array| cell(array, row))
                        .collect::<Result<Vec<_>>>()?;
                    let state = GroupState::new(state_key, &aggregates);
                    let bytes = estimate_group_bytes(&state)
                        .saturating_add(index_key.memory_size());
                    if reservation.try_grow(bytes).is_err() {
                        let spiller = spilled.get_or_insert_with(|| {
                            StateSpiller::new(&context, SPILL_PARTITIONS)
                        });
                        spill_states(
                            &mut states,
                            &mut group_index,
                            &groups,
                            &aggregates,
                            Arc::clone(&partial_schema),
                            spiller,
                            &context,
                        )?;
                        reservation.try_resize(0)?;
                        reservation
                            .try_grow(bytes)
                            .map_err(|_| {
                                spill::single_group_error(bytes, &context, reservation.pool().limit())
                            })?;
                    }
                    let index = states.len();
                    states.push(state);
                    group_index.insert(index_key, index);
                    index
                };
                let state = &mut states[index];
                if let Some(aggregate_arrays) = &aggregate_arrays {
                    for (aggregate_index, aggregate) in aggregates.iter().enumerate() {
                        let value = aggregate_arrays[aggregate_index]
                            .as_ref()
                            .map(|array| cell(array, row))
                            .transpose()?;
                        state.aggregates[aggregate_index].update(aggregate, value)?;
                    }
                } else {
                    let mut column = groups.len();
                    for (aggregate_index, aggregate) in aggregates.iter().enumerate() {
                        state.aggregates[aggregate_index].merge_partial(
                            aggregate,
                            batch.batch(),
                            row,
                            &mut column,
                        )?;
                    }
                }
            }
        }

        if let Some(mut spiller) = spilled {
            spill_states(
                &mut states,
                &mut group_index,
                &groups,
                &aggregates,
                Arc::clone(&partial_schema),
                &mut spiller,
                &context,
            )?;
            reservation.try_resize(0)?;
            let mut pending = spiller
                .finish()?
                .into_iter()
                .rev()
                .filter(|files| !files.is_empty())
                .map(PartitionTask::initial)
                .collect::<Vec<_>>();
            while let Some(task) = pending.pop() {
                context.check_cancelled()?;
                match merge_partition(
                    &task.files,
                    &groups,
                    &aggregates,
                    &context,
                    &mut reservation,
                )? {
                    MergeOutcome::Merged(partition_states) => {
                        spill::remove_files(&context, &task.files)?;
                        for chunk in partition_states.chunks(batch_size.max(1)) {
                            context.check_cancelled()?;
                            yield build_output_envelope(
                                chunk,
                                &groups,
                                &aggregates,
                                Arc::clone(&schema),
                                output_mode,
                                &context,
                                reservation.size(),
                            ).await?;
                        }
                        reservation.try_resize(0)?;
                    }
                    MergeOutcome::Repartition => {
                        reservation.try_resize(0)?;
                        let next_depth = task.next_depth()?;
                        let child_partitions = repartition_partition(
                            &task.files,
                            &groups,
                            next_depth,
                            &context,
                        )?;
                        spill::remove_files(&context, &task.files)?;
                        for files in child_partitions.into_iter().rev() {
                            if !files.is_empty() {
                                pending.push(PartitionTask::child(files, next_depth));
                            }
                        }
                    }
                }
            }
        } else {
            for chunk in states.chunks(batch_size.max(1)) {
                context.check_cancelled()?;
                yield build_output_envelope(
                    chunk,
                    &groups,
                    &aggregates,
                    Arc::clone(&schema),
                    output_mode,
                    &context,
                    reservation.size(),
                ).await?;
            }
        }
    })
}

fn aggregate_workspace_estimate(batch: &RecordBatch, groups: &[BoundExpr]) -> usize {
    batch
        .get_array_memory_size()
        .saturating_add(
            batch
                .num_rows()
                .saturating_mul(groups.len().saturating_mul(16).saturating_add(8)),
        )
        .max(1)
}

fn retained_workspace_bytes(
    input: &RecordBatch,
    groups: &[arrow::array::ArrayRef],
    aggregates: Option<&[Option<arrow::array::ArrayRef>]>,
    encoded: &key::EncodedGroupRows,
) -> usize {
    groups
        .iter()
        .map(|array| derived_array_bytes(input, array))
        .chain(
            aggregates
                .into_iter()
                .flatten()
                .filter_map(Option::as_ref)
                .map(|array| derived_array_bytes(input, array)),
        )
        .fold(encoded.memory_size(), usize::saturating_add)
        .max(1)
}

fn derived_array_bytes(input: &RecordBatch, array: &arrow::array::ArrayRef) -> usize {
    if input
        .columns()
        .iter()
        .any(|column| Arc::ptr_eq(column, array))
    {
        0
    } else {
        array.get_array_memory_size()
    }
}

fn build_output_batch(
    states: &[GroupState],
    groups: &[BoundExpr],
    aggregates: &[AggregateExpr],
    schema: SchemaRef,
    mode: OutputMode,
) -> Result<RecordBatch> {
    match mode {
        OutputMode::Final => build_batch(states, groups, aggregates, schema),
        OutputMode::Partial => build_partial_batch(states, groups, aggregates, schema),
    }
}

async fn build_output_envelope(
    states: &[GroupState],
    groups: &[BoundExpr],
    aggregates: &[AggregateExpr],
    schema: SchemaRef,
    mode: OutputMode,
    context: &QueryContext,
    held_bytes: usize,
) -> Result<BatchEnvelope> {
    let estimate = states
        .iter()
        .map(|state| state.output_workspace_bytes(schema.fields().len()))
        .fold(
            schema.fields().len().saturating_mul(512).saturating_add(1),
            usize::saturating_add,
        );
    let workspace = context
        .reserve_memory_while_holding(estimate, held_bytes, "aggregate output workspace")
        .await?;
    let batch = build_output_batch(states, groups, aggregates, schema, mode)?;
    BatchEnvelope::from_reservation(batch, workspace, "aggregate output")
}

fn count_star_only(aggregates: &[AggregateExpr]) -> bool {
    !aggregates.is_empty()
        && aggregates.iter().all(|aggregate| {
            aggregate.function == AggregateFunction::Count && aggregate.expr.is_none()
        })
}

fn aggregate_state_limit(query_limit: usize, output_mode: OutputMode, lanes: usize) -> usize {
    // Keep enough room for one leased input/workspace batch and the active
    // partition writers while resident states are serialized.
    let divisor = if matches!(output_mode, OutputMode::Partial) {
        lanes.max(1).saturating_mul(2)
    } else {
        3
    };
    query_limit.checked_div(divisor).unwrap_or(0).max(1)
}

fn build_batch(
    states: &[GroupState],
    groups: &[BoundExpr],
    aggregates: &[AggregateExpr],
    schema: SchemaRef,
) -> Result<RecordBatch> {
    let mut columns = Vec::with_capacity(groups.len() + aggregates.len());
    for (index, expression) in groups.iter().enumerate() {
        let values = states
            .iter()
            .map(|state| state.key[index].clone())
            .collect::<Vec<_>>();
        columns.push(values_to_array(&values, &expression.data_type)?);
    }
    for (index, expression) in aggregates.iter().enumerate() {
        let values = states
            .iter()
            .map(|state| state.aggregates[index].finish())
            .collect::<Result<Vec<_>>>()?;
        columns.push(values_to_array(&values, &expression.data_type)?);
    }
    Ok(RecordBatch::try_new(schema, columns)?)
}

pub(super) fn partial_schema(groups: &[BoundExpr], aggregates: &[AggregateExpr]) -> SchemaRef {
    let mut fields = groups
        .iter()
        .enumerate()
        .map(|(index, expression)| {
            Field::new(
                format!("__group_{index}"),
                expression.data_type.clone(),
                true,
            )
        })
        .collect::<Vec<_>>();
    for (index, expression) in aggregates.iter().enumerate() {
        if expression.function == AggregateFunction::Avg {
            fields.push(Field::new(
                format!("__agg_{index}_sum"),
                match expression.expr.as_ref().map(|input| &input.data_type) {
                    Some(DataType::Decimal128(_, _)) => DataType::Binary,
                    _ => DataType::Float64,
                },
                false,
            ));
            fields.push(Field::new(
                format!("__agg_{index}_count"),
                DataType::UInt64,
                false,
            ));
        } else if expression.function == AggregateFunction::Sum {
            fields.push(Field::new(
                format!("__agg_{index}_sum"),
                if expression.data_type == DataType::Float64 {
                    DataType::Float64
                } else {
                    DataType::Binary
                },
                false,
            ));
            fields.push(Field::new(
                format!("__agg_{index}_seen"),
                DataType::Boolean,
                false,
            ));
        } else {
            fields.push(Field::new(
                format!("__agg_{index}"),
                expression.data_type.clone(),
                true,
            ));
        }
    }
    Arc::new(Schema::new(fields))
}

fn build_partial_batch(
    states: &[GroupState],
    groups: &[BoundExpr],
    aggregates: &[AggregateExpr],
    schema: SchemaRef,
) -> Result<RecordBatch> {
    let mut columns = Vec::with_capacity(schema.fields().len());
    for (index, expression) in groups.iter().enumerate() {
        let values = states
            .iter()
            .map(|state| state.key[index].clone())
            .collect::<Vec<_>>();
        columns.push(values_to_array(&values, &expression.data_type)?);
    }
    for (index, expression) in aggregates.iter().enumerate() {
        let partials = states
            .iter()
            .map(|state| state.aggregates[index].partial_values())
            .collect::<Result<Vec<_>>>()?;
        let width = if matches!(
            expression.function,
            AggregateFunction::Avg | AggregateFunction::Sum
        ) {
            2
        } else {
            1
        };
        for partial_index in 0..width {
            let values = partials
                .iter()
                .map(|values| values[partial_index].clone())
                .collect::<Vec<_>>();
            let data_type = schema.field(columns.len()).data_type();
            columns.push(values_to_array(&values, data_type)?);
        }
    }
    Ok(RecordBatch::try_new(schema, columns)?)
}
