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

use super::super::spill::partition_for_key;
use super::{
    super::{
        GroupState, StateSpiller, adaptive_spill_partitions, estimate_group_bytes, spill_states,
    },
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
    while memory.try_grow(bytes).is_err() {
        if states.is_empty() {
            return Err(Error::ResourceExhausted(format!(
                "cannot reserve {bytes} bytes for one DISTINCT aggregate group (limit {})",
                memory.pool().limit()
            )));
        }
        let spiller = spiller.get_or_insert_with(|| {
            StateSpiller::new(context, adaptive_spill_partitions(context, memory.size()))
        });
        let resident_bytes = spill_largest_group_partition(
            states,
            index,
            groups,
            aggregates,
            Arc::clone(schema),
            spiller,
            context,
        )?;
        memory.try_resize(resident_bytes)?;
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
    let candidate = loop {
        if let Ok(candidate) = memory.pool().try_reserve(bytes) {
            break candidate;
        }
        if keys.is_empty() {
            return Err(Error::ResourceExhausted(format!(
                "one DISTINCT aggregate key requires {bytes} bytes (key budget {})",
                memory.pool().limit()
            )));
        }
        let resident_bytes = spiller.spill_largest_partition(keys, context)?;
        memory.try_resize(resident_bytes)?;
    };
    memory.absorb(candidate)?;
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
            while memory.try_grow(bytes).is_err() {
                if states.is_empty() {
                    return Err(Error::ResourceExhausted(format!(
                        "cannot reserve {bytes} bytes for one DISTINCT contribution group"
                    )));
                }
                let resident_bytes = spill_largest_group_partition(
                    &mut states,
                    &mut index,
                    groups,
                    aggregates,
                    Arc::clone(&schema),
                    spiller,
                    context,
                )?;
                memory.try_resize(resident_bytes)?;
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

#[allow(clippy::too_many_arguments)]
fn spill_largest_group_partition(
    states: &mut Vec<GroupState>,
    group_index: &mut HashMap<Vec<super::super::CellValue>, usize>,
    groups: &[BoundExpr],
    aggregates: &[AggregateExpr],
    schema: SchemaRef,
    spiller: &mut StateSpiller,
    context: &QueryContext,
) -> Result<usize> {
    let partitions = spiller.partition_count();
    let mut partition_bytes = vec![0usize; partitions];
    for state in states.iter() {
        let partition = partition_for_key(&state.key, partitions, 0);
        partition_bytes[partition] = partition_bytes[partition]
            .saturating_add(estimate_group_bytes(state))
            .saturating_add(index_key_bytes(&state.key));
    }
    let victim = partition_bytes
        .iter()
        .enumerate()
        .max_by_key(|(_, bytes)| *bytes)
        .map(|(partition, _)| partition)
        .ok_or_else(|| {
            Error::Internal("DISTINCT group victim selection has no partitions".into())
        })?;

    let survivor_count = states
        .iter()
        .filter(|state| partition_for_key(&state.key, partitions, 0) != victim)
        .count();
    let mut victim_states = Vec::new();
    let mut survivors = Vec::with_capacity(survivor_count);
    let mut remap = vec![usize::MAX; states.len()];
    for (old_index, state) in std::mem::take(states).into_iter().enumerate() {
        if partition_for_key(&state.key, partitions, 0) == victim {
            victim_states.push(state);
        } else {
            remap[old_index] = survivors.len();
            survivors.push(state);
        }
    }
    let mut survivor_index = HashMap::with_capacity(survivor_count);
    for (key, old_index) in std::mem::take(group_index) {
        let new_index = remap.get(old_index).copied().unwrap_or(usize::MAX);
        if new_index != usize::MAX {
            survivor_index.insert(key, new_index);
        }
    }
    *states = survivors;
    *group_index = survivor_index;

    let mut victim_index = HashMap::<u8, usize>::new();
    spill_states(
        &mut victim_states,
        &mut victim_index,
        groups,
        aggregates,
        schema,
        spiller,
        context,
    )?;
    Ok(resident_group_bytes(states))
}

fn resident_group_bytes(states: &[GroupState]) -> usize {
    states.iter().fold(0usize, |bytes, state| {
        bytes
            .saturating_add(estimate_group_bytes(state))
            .saturating_add(index_key_bytes(&state.key))
    })
}

#[cfg(test)]
mod tests;
