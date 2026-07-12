use std::{
    collections::{HashMap, HashSet},
    mem::size_of,
    sync::Arc,
};

use arrow::{datatypes::SchemaRef, record_batch::RecordBatch};

use crate::{
    Error, Result,
    runtime::{MemoryReservation, QueryContext},
    sql::{AggregateExpr, BoundExpr},
};

use super::{
    super::{GroupState, SPILL_PARTITIONS, StateSpiller, estimate_group_bytes, spill_states},
    spill::{DistinctKey, DistinctSpiller},
};

#[allow(clippy::too_many_arguments)]
pub(super) fn insert_group(
    key: Vec<super::super::CellValue>,
    groups: &[BoundExpr],
    aggregates: &[AggregateExpr],
    states: &mut Vec<GroupState>,
    index: &mut HashMap<Vec<super::super::CellValue>, usize>,
    memory: &mut MemoryReservation,
    schema: &SchemaRef,
    spiller: &mut Option<StateSpiller>,
    context: &QueryContext,
) -> Result<usize> {
    let state = GroupState::new(key.clone(), aggregates);
    let bytes = estimate_group_bytes(&state).saturating_add(index_key_bytes(&key));
    if memory.try_grow(bytes).is_err() {
        let spiller = spiller.get_or_insert_with(|| StateSpiller::new(context, SPILL_PARTITIONS));
        spill_states(
            states,
            index,
            groups,
            aggregates,
            Arc::clone(schema),
            spiller,
            context,
        )?;
        memory.try_resize(0)?;
        memory.try_grow(bytes).map_err(|_| {
            Error::ResourceExhausted(format!(
                "cannot reserve {bytes} bytes for one DISTINCT aggregate group (limit {})",
                memory.pool().limit()
            ))
        })?;
    }
    let position = states.len();
    states.push(state);
    index.insert(key, position);
    Ok(position)
}

pub(super) fn insert_distinct(
    key: DistinctKey,
    keys: &mut HashSet<DistinctKey>,
    memory: &mut MemoryReservation,
    spiller: &mut DistinctSpiller,
    context: &QueryContext,
) -> Result<()> {
    if keys.contains(&key) {
        return Ok(());
    }
    let bytes = key.memory_size();
    let mut candidate = memory.pool().try_reserve(bytes);
    if candidate.is_err() {
        if keys.is_empty() {
            return Err(Error::ResourceExhausted(format!(
                "one DISTINCT aggregate key requires {bytes} bytes (key budget {})",
                memory.pool().limit()
            )));
        }
        spiller.spill(std::mem::take(keys), context)?;
        memory.try_resize(0)?;
        candidate = memory.pool().try_reserve(bytes);
        candidate.as_ref().map_err(|_| {
            Error::ResourceExhausted(format!(
                "one DISTINCT aggregate key requires {bytes} bytes (key budget {})",
                memory.pool().limit()
            ))
        })?;
    }
    memory.absorb(candidate.expect("candidate reservation was checked above"))?;
    keys.insert(key);
    Ok(())
}

pub(super) fn apply_in_memory(
    keys: HashSet<DistinctKey>,
    index: &HashMap<Vec<super::super::CellValue>, usize>,
    states: &mut [GroupState],
    aggregates: &[AggregateExpr],
) -> Result<()> {
    for key in keys {
        let state = index
            .get(&key.group)
            .ok_or_else(|| Error::Internal("DISTINCT aggregate lost its input group".into()))?;
        states[*state].aggregates[key.aggregate]
            .update(&aggregates[key.aggregate], Some(key.value))?;
    }
    Ok(())
}

pub(super) fn contribute(
    keys: impl IntoIterator<Item = DistinctKey>,
    groups: &[BoundExpr],
    aggregates: &[AggregateExpr],
    schema: SchemaRef,
    spiller: &mut StateSpiller,
    context: &QueryContext,
    memory: &mut MemoryReservation,
) -> Result<()> {
    let mut states = Vec::<GroupState>::new();
    let mut index = HashMap::<Vec<super::super::CellValue>, usize>::new();
    for key in keys {
        context.check_cancelled()?;
        let position = if let Some(position) = index.get(&key.group) {
            *position
        } else {
            let state = GroupState::new(key.group.clone(), aggregates);
            let bytes = estimate_group_bytes(&state).saturating_add(index_key_bytes(&key.group));
            if memory.try_grow(bytes).is_err() {
                spill_states(
                    &mut states,
                    &mut index,
                    groups,
                    aggregates,
                    Arc::clone(&schema),
                    spiller,
                    context,
                )?;
                memory.try_resize(0)?;
                memory.try_grow(bytes).map_err(|_| {
                    Error::ResourceExhausted(format!(
                        "cannot reserve {bytes} bytes for one DISTINCT contribution group"
                    ))
                })?;
            }
            let position = states.len();
            states.push(state);
            index.insert(key.group, position);
            position
        };
        states[position].aggregates[key.aggregate]
            .update(&aggregates[key.aggregate], Some(key.value))?;
    }
    spill_states(
        &mut states,
        &mut index,
        groups,
        aggregates,
        schema,
        spiller,
        context,
    )?;
    memory.try_resize(0)?;
    Ok(())
}

pub(super) fn state_pool_limit(query_limit: usize) -> usize {
    query_limit.checked_div(4).unwrap_or(0).max(1)
}

pub(super) fn key_pool_limit(query_limit: usize) -> usize {
    query_limit
        .checked_div(8)
        .unwrap_or(0)
        .saturating_mul(3)
        .max(1)
}

pub(super) fn partition_count(query_limit: usize) -> usize {
    query_limit.checked_div(256 << 10).unwrap_or(0).clamp(8, 64)
}

pub(super) fn retained_arrays_bytes(
    input: &RecordBatch,
    groups: &[arrow::array::ArrayRef],
    aggregates: &[Option<arrow::array::ArrayRef>],
) -> usize {
    groups
        .iter()
        .chain(aggregates.iter().filter_map(Option::as_ref))
        .filter(|array| {
            !input
                .columns()
                .iter()
                .any(|column| Arc::ptr_eq(column, array))
        })
        .map(|array| array.get_array_memory_size())
        .fold(1usize, usize::saturating_add)
}

fn index_key_bytes(key: &[super::super::CellValue]) -> usize {
    key.len()
        .saturating_mul(size_of::<super::super::CellValue>())
        .saturating_add(
            key.iter()
                .map(|value| match value {
                    super::super::CellValue::Utf8(value) => value.capacity(),
                    super::super::CellValue::Binary(value) => value.capacity(),
                    _ => 0,
                })
                .sum::<usize>(),
        )
        .saturating_add(96)
}
