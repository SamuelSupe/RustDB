use std::sync::Arc;

use arrow::{
    datatypes::{DataType, Field, Schema, SchemaRef},
    record_batch::RecordBatch,
};
use futures::StreamExt;

use crate::Result;
use crate::runtime::{
    BatchEnvelope, IntoMemoryBatchStream, MemoryBatchStream, MemoryReservation, QueryContext,
    boxed_memory_batch_stream,
};
use crate::sql::{AggregateExpr, AggregateFunction, BoundExpr};

use super::{
    expr::evaluate,
    value::{CellValue, cell, values_to_array},
};

mod admission;
mod average;
mod batch;
mod dense_dictionary;
mod distinct;
pub(in crate::execution) mod join_match;
pub(in crate::execution) mod join_sink;
mod key;
mod parallel;
mod spill;
pub(super) mod state;

#[cfg(test)]
mod tests;

use admission::PartialMergeAdmission;
use dense_dictionary::DenseDictionaryBatch;
use key::{EncodedGroupIdCache, GroupIndex, GroupKey, GroupKeyEncoder};
use spill::{
    MergeOutcome, PartitionTask, StateSpiller, adaptive_spill_partitions, merge_partition,
    pop_largest_partition, repartition_partition, spill_largest_partition, spill_states,
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
        None,
        false,
    )
}

fn serial_partial_aggregate(
    input: MemoryBatchStream,
    groups: Vec<BoundExpr>,
    aggregates: Vec<AggregateExpr>,
    context: Arc<QueryContext>,
    batch_size: usize,
    partial_merge_admission: PartialMergeAdmission,
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
        Some(partial_merge_admission),
        true,
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
        None,
        false,
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
    partial_merge_admission: Option<PartialMergeAdmission>,
    lane_already_active: bool,
) -> MemoryBatchStream {
    boxed_memory_batch_stream(async_stream::try_stream! {
        let mut group_index = GroupIndex::new();
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
            let _ = group_index.insert(GroupKey::Encoded(Vec::new()), 0)?;
            states.push(GroupState::new(Vec::new(), &aggregates));
            reservation.try_grow(estimate_group_bytes(&states[0]))?;
        }

        while let Some(batch) = input.next().await {
            context.check_cancelled()?;
            let batch = batch?;
            // Parallel partial lanes already hold this permit across their
            // yielded batch. Serial and final aggregation acquire one here so
            // every batch's CPU work participates in engine-wide fairness.
            let _compute = if lane_already_active {
                None
            } else {
                Some(context.acquire_compute().await?)
            };
            let _active = (!lane_already_active).then(|| context.scheduler.enter_lane());
            if input_mode == InputMode::Raw && groups.is_empty() && batch::supports(&aggregates) {
                update_global_batch(&mut states, &aggregates, batch.batch(), &context)?;
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

            if input_mode == InputMode::Raw
                && try_apply_dense_dictionary_batch(
                    &key_encoder,
                    &encoded_groups,
                    &group_arrays,
                    &aggregates,
                    aggregate_arrays
                        .as_deref()
                        .expect("raw aggregate arrays were evaluated"),
                    batch.batch().num_rows(),
                    &mut group_index,
                    &mut states,
                    &mut reservation,
                    &mut workspace,
                )?
            {
                continue;
            }

            let mut group_cache = EncodedGroupIdCache::new();
            for row in 0..batch.batch().num_rows() {
                let borrowed_key = encoded_groups.borrowed_key(row);
                let index = if let Some(index) =
                    borrowed_key.and_then(|key| group_cache.get(key))
                {
                    index
                } else {
                    let mut owned_key = None;
                    let existing = if let Some(key) = borrowed_key {
                        group_index.get_encoded(key)
                    } else {
                        owned_key = Some(key_encoder.key(&encoded_groups, &group_arrays, row)?);
                        group_index.get(
                            owned_key
                                .as_ref()
                                .expect("non-row group key was materialized"),
                        )
                    };
                    let index = if let Some(index) = existing {
                        index
                    } else {
                        // Arrow row bytes are copied only for a new persistent
                        // group. Existing groups were resolved through the
                        // borrowed `&[u8]` lookup above.
                        let index_key = owned_key.unwrap_or_else(|| {
                            GroupKey::Encoded(
                                borrowed_key
                                    .expect("row-encoded group has borrowed bytes")
                                    .to_vec(),
                            )
                        });
                        let state_key = group_arrays
                            .iter()
                            .map(|array| cell(array, row))
                            .collect::<Result<Vec<_>>>()?;
                        let state = GroupState::new(state_key, &aggregates);
                        let bytes = estimate_group_bytes(&state)
                            .saturating_add(index_key.memory_size());
                        while reservation.try_grow(bytes).is_err() {
                            if states.is_empty() {
                                Err(spill::single_group_error(
                                    bytes,
                                    &context,
                                    reservation.pool().limit(),
                                ))?;
                            }
                            let spiller = spilled.get_or_insert_with(|| {
                                StateSpiller::new(
                                    &context,
                                    adaptive_spill_partitions(&context, reservation.size()),
                                )
                            });
                            let resident_bytes = spill_largest_partition(
                                &mut states,
                                &mut group_index,
                                &groups,
                                &aggregates,
                                Arc::clone(&partial_schema),
                                spiller,
                                &context,
                            )?;
                            // Victim removal compacts `states` and remaps every
                            // surviving index. No borrowed batch key may retain
                            // an id from the previous generation.
                            group_cache.clear();
                            reservation.try_resize(resident_bytes)?;
                        }
                        let index = states.len();
                        states.push(state);
                        let _ = group_index.insert(index_key, index)?;
                        index
                    };
                    // Cache only ids resolved by the persistent index. Failed
                    // reservations and failed insertions never become visible.
                    if let Some(key) = borrowed_key {
                        group_cache.insert(key, index);
                    }
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

        let mut spilled_partitions = if let Some(mut spiller) = spilled {
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
            let partitions = spiller.finish(&context)?;
            if let Some(admission) = partial_merge_admission.as_ref() {
                admission.mark_spilled();
            }
            Some(partitions)
        } else {
            None
        };
        if let Some(admission) = partial_merge_admission.as_ref() {
            admission.wait_ready(&context).await?;
            if admission.any_spilled() && spilled_partitions.is_none() {
                let mut spiller = StateSpiller::new(
                    &context,
                    adaptive_spill_partitions(&context, reservation.size()),
                );
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
                spilled_partitions = Some(spiller.finish(&context)?);
            }
            admission.wait_ready(&context).await?;
        }

        if let Some(partitions) = spilled_partitions {
            let partial_merge_permit = if let Some(admission) = partial_merge_admission.as_ref() {
                Some(admission.acquire_merge(&context).await?)
            } else {
                None
            };
            if matches!(output_mode, OutputMode::Final) {
                reservation = context
                    .memory
                    .child(
                        format!("aggregate-merge-{}", context.query_id),
                        aggregate_merge_state_limit(context.memory.limit()),
                    )
                    .reservation();
            } else if partial_merge_permit.is_some() {
                reservation = context
                    .memory
                    .child(
                        format!("aggregate-partial-merge-{}", context.query_id),
                        aggregate_partial_merge_state_limit(context.memory.limit()),
                    )
                    .reservation();
            }
            let mut pending = partitions
                .into_iter()
                .rev()
                .filter(|partition| !partition.files.is_empty())
                .map(PartitionTask::initial)
                .collect::<Vec<_>>();
            while let Some(task) = pop_largest_partition(&mut pending) {
                context.check_cancelled()?;
                match merge_partition(
                    &task.files,
                    &groups,
                    &aggregates,
                    &context,
                    &mut reservation,
                )? {
                    MergeOutcome::Merged(partition_states) => {
                        // merge_partition's hash index has been dropped. Keep
                        // charging the returned states, but release the index
                        // copy before output backpressure can overlap this
                        // partial merge with the downstream final aggregate.
                        reservation.try_resize(retained_group_state_bytes(&partition_states))?;
                        spill::remove_files(&context, &task.files)?;
                        let mut offset = 0;
                        while offset < partition_states.len() {
                            context.check_cancelled()?;
                            let chunk_len = output_chunk_len(
                                &partition_states[offset..],
                                batch_size,
                                schema.fields().len(),
                                aggregate_output_workspace_limit(context.memory.limit()),
                            );
                            let chunk = &partition_states[offset..offset + chunk_len];
                            yield build_output_envelope(
                                chunk,
                                &groups,
                                &aggregates,
                                Arc::clone(&schema),
                                output_mode,
                                &context,
                                reservation.size(),
                            ).await?;
                            offset += chunk_len;
                        }
                        reservation.try_resize(0)?;
                    }
                    MergeOutcome::Repartition => {
                        reservation.try_resize(0)?;
                        let next_depth =
                            task.next_depth(context.execution.max_repartition_depth)?;
                        let child_partitions = repartition_partition(
                            &task.files,
                            task.estimated_bytes(),
                            &groups,
                            &aggregates,
                            next_depth,
                            &context,
                        )?;
                        spill::remove_files(&context, &task.files)?;
                        for partition in child_partitions.into_iter().rev() {
                            if !partition.files.is_empty() {
                                pending.push(PartitionTask::child(partition, next_depth));
                            }
                        }
                    }
                }
            }
            drop(partial_merge_permit);
        } else {
            let mut offset = 0;
            while offset < states.len() {
                context.check_cancelled()?;
                let chunk_len = output_chunk_len(
                    &states[offset..],
                    batch_size,
                    schema.fields().len(),
                    aggregate_output_workspace_limit(context.memory.limit()),
                );
                let chunk = &states[offset..offset + chunk_len];
                yield build_output_envelope(
                    chunk,
                    &groups,
                    &aggregates,
                    Arc::clone(&schema),
                    output_mode,
                    &context,
                    reservation.size(),
                ).await?;
                offset += chunk_len;
            }
        }
    })
}

#[allow(clippy::too_many_arguments)]
fn try_apply_dense_dictionary_batch(
    key_encoder: &GroupKeyEncoder,
    encoded_groups: &key::EncodedGroupRows,
    group_arrays: &[arrow::array::ArrayRef],
    aggregates: &[AggregateExpr],
    aggregate_arrays: &[Option<arrow::array::ArrayRef>],
    rows: usize,
    group_index: &mut GroupIndex,
    states: &mut Vec<GroupState>,
    reservation: &mut MemoryReservation,
    workspace: &mut MemoryReservation,
) -> Result<bool> {
    let original_workspace = workspace.size();
    if workspace
        .try_grow(dense_dictionary::workspace_estimate(aggregates.len()))
        .is_err()
    {
        return Ok(false);
    }
    let batch = match DenseDictionaryBatch::try_new(
        encoded_groups,
        group_arrays,
        aggregates,
        aggregate_arrays,
        rows,
    ) {
        Ok(Some(batch)) => batch,
        Ok(None) => {
            workspace.try_resize(original_workspace)?;
            return Ok(false);
        }
        Err(error) => {
            workspace.try_resize(original_workspace)?;
            return Err(error);
        }
    };
    let dynamic_workspace = match batch.dynamic_workspace_estimate(encoded_groups, group_arrays) {
        Ok(bytes) => bytes,
        Err(error) => {
            drop(batch);
            workspace.try_resize(original_workspace)?;
            return Err(error);
        }
    };
    if workspace.try_grow(dynamic_workspace).is_err() {
        drop(batch);
        workspace.try_resize(original_workspace)?;
        return Ok(false);
    }

    let mut state_by_slot = vec![None; batch.representatives().len()];
    let mut pending_by_slot = vec![None; batch.representatives().len()];
    let mut pending = Vec::<(GroupKey, GroupState)>::new();
    let mut pending_bytes = 0usize;

    // Resolve each used dictionary slot exactly once. All keys, states and the
    // complete reservation are prepared before the persistent index changes.
    for (slot, representative) in batch.representatives().iter().enumerate() {
        let Some(row) = representative else {
            continue;
        };
        let borrowed_key = encoded_groups.borrowed_key(*row);
        let mut owned_key = None;
        let existing = if let Some(key) = borrowed_key {
            group_index.get_encoded(key)
        } else {
            owned_key = Some(key_encoder.key(encoded_groups, group_arrays, *row)?);
            group_index.get(
                owned_key
                    .as_ref()
                    .expect("non-row group key was materialized"),
            )
        };
        if let Some(index) = existing {
            state_by_slot[slot] = Some(index);
            continue;
        }
        let index_key = owned_key.unwrap_or_else(|| {
            GroupKey::Encoded(
                borrowed_key
                    .expect("row-encoded dictionary group has borrowed bytes")
                    .to_vec(),
            )
        });
        if let Some(index) = pending.iter().position(|(key, _)| key == &index_key) {
            pending_by_slot[slot] = Some(index);
            continue;
        }

        let state_key = group_arrays
            .iter()
            .map(|array| cell(array, *row))
            .collect::<Result<Vec<_>>>()?;
        let state = GroupState::new(state_key, aggregates);
        pending_bytes = pending_bytes
            .saturating_add(estimate_group_bytes(&state))
            .saturating_add(index_key.memory_size());
        pending_by_slot[slot] = Some(pending.len());
        pending.push((index_key, state));
    }

    if reservation.try_grow(pending_bytes).is_err() {
        drop(pending);
        drop(pending_by_slot);
        drop(state_by_slot);
        drop(batch);
        workspace.try_resize(original_workspace)?;
        return Ok(false);
    }

    let first_new_state = states.len();
    for (offset, (key, state)) in pending.into_iter().enumerate() {
        let index = first_new_state + offset;
        states.push(state);
        let _ = group_index.insert(key, index)?;
    }
    for (slot, pending_index) in pending_by_slot.into_iter().enumerate() {
        if let Some(pending_index) = pending_index {
            state_by_slot[slot] = Some(first_new_state + pending_index);
        }
    }
    batch.apply(&state_by_slot, states)?;
    Ok(true)
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

fn retained_group_state_bytes(states: &[GroupState]) -> usize {
    states
        .iter()
        .map(estimate_group_bytes)
        .fold(0usize, usize::saturating_add)
}

fn output_chunk_len(
    states: &[GroupState],
    max_rows: usize,
    output_columns: usize,
    workspace_limit: usize,
) -> usize {
    let mut bytes = output_columns.saturating_mul(512).saturating_add(1);
    let mut rows = 0;
    for state in states.iter().take(max_rows.max(1)) {
        let next = bytes.saturating_add(state.output_workspace_bytes(output_columns));
        if rows != 0 && next > workspace_limit {
            break;
        }
        bytes = next;
        rows += 1;
    }
    rows.max(1).min(states.len())
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

fn update_global_batch(
    states: &mut [GroupState],
    aggregates: &[AggregateExpr],
    input: &RecordBatch,
    context: &QueryContext,
) -> Result<()> {
    let workspace_estimate = aggregates
        .iter()
        .filter_map(|aggregate| aggregate.expr.as_ref())
        .map(|expression| {
            super::expr::projection_workspace_bytes(std::slice::from_ref(expression), input)
        })
        .fold(1usize, usize::saturating_add);
    let mut workspace = context.memory.try_reserve(workspace_estimate)?;
    let arrays = aggregates
        .iter()
        .map(|aggregate| {
            aggregate
                .expr
                .as_ref()
                .map(|expression| evaluate(expression, input))
                .transpose()
        })
        .collect::<Result<Vec<_>>>()?;
    workspace.try_resize(
        arrays
            .iter()
            .filter_map(Option::as_ref)
            .map(|array| derived_array_bytes(input, array))
            .fold(1usize, usize::saturating_add),
    )?;
    let state = states
        .first_mut()
        .ok_or_else(|| crate::Error::Internal("global aggregate state is missing".into()))?;
    batch::update(&mut state.aggregates, aggregates, &arrays, input.num_rows())
}

fn aggregate_state_limit(query_limit: usize, output_mode: OutputMode, lanes: usize) -> usize {
    // Resident input states coexist with active partition writers. Partial
    // lanes also coexist with each other, so each gets a smaller share.
    let divisor = if matches!(output_mode, OutputMode::Partial) {
        lanes.max(1).saturating_mul(2)
    } else {
        3
    };
    query_limit.checked_div(divisor).unwrap_or(0).max(1)
}

fn aggregate_merge_state_limit(query_limit: usize) -> usize {
    // Spill writers are closed before final partition merge. Keep the other
    // half for decoded IPC input, output materialization, and downstream work.
    query_limit.checked_div(2).unwrap_or(0).max(1)
}

fn aggregate_partial_merge_state_limit(query_limit: usize) -> usize {
    // Partial lanes close their writers before entering a query-wide
    // single-lane merge gate. Match the serial aggregate state budget while
    // retaining two thirds for sibling lanes, IPC input, and output batches.
    query_limit.checked_div(3).unwrap_or(0).max(1)
}

fn aggregate_output_workspace_limit(query_limit: usize) -> usize {
    // Partial and final aggregate states may overlap under backpressure. Keep
    // each output materialization small enough to make progress beside both.
    query_limit.checked_div(8).unwrap_or(0).max(1)
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
                    Some(
                        DataType::Int8
                        | DataType::Int16
                        | DataType::Int32
                        | DataType::Int64
                        | DataType::UInt8
                        | DataType::UInt16
                        | DataType::UInt32
                        | DataType::UInt64
                        | DataType::Decimal128(_, _),
                    ) => DataType::Binary,
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
