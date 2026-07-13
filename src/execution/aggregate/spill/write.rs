use std::{collections::HashMap, hash::Hash, mem::size_of, sync::Arc};

use arrow::{datatypes::SchemaRef, record_batch::RecordBatch};

use crate::{
    Error, Result,
    runtime::{MemoryReservation, QueryContext, QueryMetrics, SpillFile, SpillWriter},
    sql::{AggregateExpr, BoundExpr},
};

use super::{SpillPartition, estimate_index_key_bytes, partition_for_key};
use crate::execution::aggregate::{
    CellValue, GroupState, build_partial_batch, estimate_group_bytes, key::GroupKey,
};

// Policy charge used to rotate long aggregate streams after a bounded number
// of batches. StreamWriter does not retain per-batch footer blocks; this keeps
// later read/retry work bounded rather than accounting Arrow-owned memory.
const IPC_BATCH_METADATA_BYTES: usize = 256;
const SPILL_FILE_TARGET_BYTES: u64 = 256 * 1024 * 1024;

pub(in crate::execution::aggregate) struct StateSpiller {
    partitions: Vec<PartitionSink>,
    partition_bytes: Vec<u64>,
    partition_estimates: Vec<u64>,
    metrics: QueryMetrics,
}

struct PartitionSink {
    files: Vec<SpillFile>,
    writer: Option<SpillWriter>,
    metadata: MemoryReservation,
    uncompressed_bytes: u64,
}

impl StateSpiller {
    pub(in crate::execution::aggregate) fn new(context: &QueryContext, partitions: usize) -> Self {
        Self {
            partition_bytes: vec![0; partitions],
            partition_estimates: vec![0; partitions],
            metrics: context.metrics.clone(),
            partitions: (0..partitions)
                .map(|_| PartitionSink {
                    files: Vec::new(),
                    writer: None,
                    metadata: context.memory.reservation(),
                    uncompressed_bytes: 0,
                })
                .collect(),
        }
    }

    pub(in crate::execution::aggregate) fn finish(
        mut self,
        context: &QueryContext,
    ) -> Result<Vec<SpillPartition>> {
        self.close_writers(context)?;
        self.metrics.record_repartition(
            0,
            0,
            self.partition_bytes.iter().copied().max().unwrap_or(0),
        );
        Ok(self
            .partitions
            .into_iter()
            .zip(self.partition_estimates)
            .map(|(partition, estimated_bytes)| SpillPartition {
                files: partition.files,
                estimated_bytes,
            })
            .collect())
    }

    pub(in crate::execution::aggregate) fn partition_count(&self) -> usize {
        self.partitions.len()
    }

    pub(in crate::execution::aggregate) fn close_writers(
        &mut self,
        context: &QueryContext,
    ) -> Result<()> {
        for sink in &mut self.partitions {
            if let Some(writer) = sink.writer.take() {
                sink.files.push(writer.finish(1)?);
                sink.uncompressed_bytes = 0;
            }
            sink.metadata.try_resize(0)?;
        }
        context.check_spill_write_amplification(
            "HashAggregate",
            0,
            0,
            self.partition_bytes.iter().copied().max().unwrap_or(0),
        )?;
        Ok(())
    }

    pub(in crate::execution::aggregate) fn extend_files(
        &mut self,
        files: Vec<SpillPartition>,
    ) -> Result<()> {
        if files.len() != self.partitions.len() {
            return Err(Error::Internal(format!(
                "aggregate spill partition count changed from {} to {}",
                self.partitions.len(),
                files.len()
            )));
        }
        for ((partition, partition_estimate), files) in self
            .partitions
            .iter_mut()
            .zip(&mut self.partition_estimates)
            .zip(files)
        {
            partition.files.extend(files.files);
            *partition_estimate = partition_estimate.saturating_add(files.estimated_bytes);
        }
        Ok(())
    }
}

pub(in crate::execution::aggregate) fn spill_states<K>(
    states: &mut Vec<GroupState>,
    group_index: &mut HashMap<K, usize>,
    groups: &[BoundExpr],
    aggregates: &[AggregateExpr],
    schema: SchemaRef,
    spiller: &mut StateSpiller,
    context: &QueryContext,
) -> Result<()>
where
    K: Eq + Hash,
{
    if states.is_empty() {
        return Ok(());
    }
    // The caller releases its state reservation immediately after this
    // function returns. Drop the hash table allocation now as `clear()` keeps
    // both buckets and duplicated key allocations alive.
    *group_index = HashMap::new();
    states.sort_unstable_by_key(|state| partition_for_key(&state.key, spiller.partitions.len(), 0));

    let mut start = 0;
    while start < states.len() {
        context.check_cancelled()?;
        let partition = partition_for_key(&states[start].key, spiller.partitions.len(), 0);
        let mut end = start + 1;
        while end < states.len()
            && partition_for_key(&states[end].key, spiller.partitions.len(), 0) == partition
        {
            end += 1;
        }
        let max_partition_bytes = spiller.partition_bytes.iter().copied().max().unwrap_or(0);
        let other_pending_write_bytes = spiller
            .partitions
            .iter()
            .enumerate()
            .filter(|(index, _)| *index != partition)
            .filter_map(|(_, sink)| sink.writer.as_ref())
            .map(SpillWriter::pending_write_bytes)
            .fold(0u64, u64::saturating_add);
        let partition_bytes = &mut spiller.partition_bytes[partition];
        let partition_estimate = &mut spiller.partition_estimates[partition];
        write_partition(
            &states[start..end],
            partition,
            groups,
            aggregates,
            Arc::clone(&schema),
            &mut spiller.partitions[partition],
            partition_bytes,
            partition_estimate,
            max_partition_bytes,
            other_pending_write_bytes,
            context,
        )?;
        start = end;
    }
    // `Vec::clear()` would retain the allocation while the caller reports no
    // aggregate state memory. Replacing the vector makes that reservation
    // boundary match the actual allocation lifetime.
    *states = Vec::new();
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub(in crate::execution::aggregate) fn spill_largest_partition(
    states: &mut Vec<GroupState>,
    group_index: &mut HashMap<GroupKey, usize>,
    groups: &[BoundExpr],
    aggregates: &[AggregateExpr],
    schema: SchemaRef,
    spiller: &mut StateSpiller,
    context: &QueryContext,
) -> Result<usize> {
    if states.is_empty() {
        return Ok(0);
    }
    let partitions = spiller.partitions.len();
    let mut partition_bytes = vec![0usize; partitions];
    for state in states.iter() {
        let partition = partition_for_key(&state.key, partitions, 0);
        partition_bytes[partition] =
            partition_bytes[partition].saturating_add(estimate_group_bytes(state));
    }
    for (key, index) in group_index.iter() {
        if let Some(state) = states.get(*index) {
            let partition = partition_for_key(&state.key, partitions, 0);
            partition_bytes[partition] =
                partition_bytes[partition].saturating_add(key.memory_size());
        }
    }
    let victim = partition_bytes
        .iter()
        .enumerate()
        .max_by_key(|(_, bytes)| *bytes)
        .map(|(partition, _)| partition)
        .ok_or_else(|| Error::Internal("aggregate victim selection has no partitions".into()))?;

    let mut victim_states = Vec::new();
    let mut survivors = Vec::with_capacity(states.len());
    let mut remap = vec![usize::MAX; states.len()];
    for (old_index, state) in std::mem::take(states).into_iter().enumerate() {
        if partition_for_key(&state.key, partitions, 0) == victim {
            victim_states.push(state);
        } else {
            remap[old_index] = survivors.len();
            survivors.push(state);
        }
    }
    let mut survivor_index = HashMap::with_capacity(group_index.len());
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
    Ok(resident_state_bytes(states, group_index))
}

pub(in crate::execution::aggregate) fn resident_state_bytes(
    states: &[GroupState],
    group_index: &HashMap<GroupKey, usize>,
) -> usize {
    states
        .iter()
        .map(estimate_group_bytes)
        .fold(0usize, usize::saturating_add)
        .saturating_add(
            group_index
                .keys()
                .map(GroupKey::memory_size)
                .fold(0usize, usize::saturating_add),
        )
}

#[allow(clippy::too_many_arguments)]
fn write_partition(
    states: &[GroupState],
    partition: usize,
    groups: &[BoundExpr],
    aggregates: &[AggregateExpr],
    schema: SchemaRef,
    sink: &mut PartitionSink,
    partition_bytes: &mut u64,
    partition_estimate: &mut u64,
    max_partition_bytes: u64,
    other_pending_write_bytes: u64,
    context: &QueryContext,
) -> Result<()> {
    let mut offset = 0;

    while offset < states.len() {
        context.check_cancelled()?;
        rotate_writer_if_needed(sink, context)?;

        let mut rows = context.batch_size.max(1).min(states.len() - offset);
        let (batch, batch_memory) = loop {
            let chunk = &states[offset..offset + rows];
            if let Some(built) = try_build(chunk, groups, aggregates, Arc::clone(&schema), context)?
            {
                break built;
            }
            if rows == 1 {
                return Err(partial_batch_error(
                    estimate_build_bytes(chunk, schema.fields().len()),
                    context,
                ));
            }
            rows = rows.div_ceil(2);
        };

        if sink.writer.is_none() {
            sink.writer = Some(
                context
                    .spill
                    .writer(&format!("aggregate-p{partition}"), Arc::clone(&schema))?,
            );
        }
        let bytes = u64::try_from(batch.get_array_memory_size().max(1)).unwrap_or(u64::MAX);
        context.record_spill_logical_input_bytes(bytes);
        let merge_bytes = states[offset..offset + rows]
            .iter()
            .map(|state| {
                estimate_group_bytes(state).saturating_add(estimate_index_key_bytes(&state.key))
            })
            .fold(0usize, usize::saturating_add);
        let merge_bytes = u64::try_from(merge_bytes.max(1)).unwrap_or(u64::MAX);
        let projected_partition = partition_bytes.saturating_add(bytes);
        let projected_estimate = partition_estimate.saturating_add(merge_bytes);
        sink.writer
            .as_mut()
            .expect("aggregate spill writer was created above")
            .write_batch(&batch)?;
        *partition_bytes = projected_partition;
        *partition_estimate = projected_estimate;
        sink.uncompressed_bytes = sink.uncompressed_bytes.saturating_add(bytes);
        let pending_write_bytes = other_pending_write_bytes.saturating_add(
            sink.writer
                .as_ref()
                .expect("aggregate spill writer remains active")
                .pending_write_bytes(),
        );
        context.check_spill_write_amplification(
            "HashAggregate",
            pending_write_bytes,
            0,
            max_partition_bytes.max(projected_partition),
        )?;
        drop(batch_memory);
        offset += rows;
    }

    Ok(())
}

fn rotate_writer_if_needed(sink: &mut PartitionSink, context: &QueryContext) -> Result<()> {
    let limit = (context.memory.limit() / 16).max(IPC_BATCH_METADATA_BYTES);
    if sink.writer.is_some()
        && (sink.uncompressed_bytes >= SPILL_FILE_TARGET_BYTES
            || sink
                .metadata
                .size()
                .saturating_add(IPC_BATCH_METADATA_BYTES)
                > limit)
    {
        sink.files.push(
            sink.writer
                .take()
                .expect("aggregate spill writer was checked above")
                .finish(1)?,
        );
        sink.uncompressed_bytes = 0;
        sink.metadata.try_resize(0)?;
    }
    if sink.metadata.try_grow(IPC_BATCH_METADATA_BYTES).is_err() {
        if let Some(active) = sink.writer.take() {
            sink.files.push(active.finish(1)?);
            sink.uncompressed_bytes = 0;
            sink.metadata.try_resize(0)?;
        }
        sink.metadata
            .try_grow(IPC_BATCH_METADATA_BYTES)
            .map_err(|_| partial_batch_error(IPC_BATCH_METADATA_BYTES, context))?;
    }
    Ok(())
}

fn try_build(
    states: &[GroupState],
    groups: &[BoundExpr],
    aggregates: &[AggregateExpr],
    schema: SchemaRef,
    context: &QueryContext,
) -> Result<Option<(RecordBatch, MemoryReservation)>> {
    let estimate = estimate_build_bytes(states, schema.fields().len());
    let Ok(mut memory) = context.memory.try_reserve(estimate) else {
        return Ok(None);
    };
    let batch = build_partial_batch(states, groups, aggregates, schema)?;
    if memory
        .try_resize(batch.get_array_memory_size().max(1))
        .is_err()
    {
        return Ok(None);
    }
    Ok(Some((batch, memory)))
}

fn estimate_build_bytes(states: &[GroupState], columns: usize) -> usize {
    states
        .iter()
        .fold(0usize, |bytes, state| {
            bytes.saturating_add(estimate_group_bytes(state))
        })
        .saturating_mul(2)
        .saturating_add(
            states
                .len()
                .saturating_mul(columns)
                .saturating_mul(size_of::<CellValue>()),
        )
        .saturating_add(columns.saturating_mul(128))
        .max(1)
}

fn partial_batch_error(bytes: usize, context: &QueryContext) -> Error {
    Error::ResourceExhausted(format!(
        "aggregate spill requires at least {bytes} bytes to serialize one partial row \
         (query limit {} bytes, currently available {} bytes); increase the memory limit",
        context.memory.limit(),
        context.memory.available()
    ))
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, sync::Arc};

    use arrow::datatypes::{DataType, Field, Schema};

    use super::{StateSpiller, spill_largest_partition, spill_states};
    use crate::{
        execution::aggregate::{CellValue, GroupState, key::GroupKey, spill::partition_for_key},
        runtime::{MemoryPool, QueryContext},
        sql::{AggregateExpr, AggregateFunction, BoundExpr},
    };

    #[test]
    fn spilling_releases_state_and_index_backing_allocations() {
        let groups = vec![BoundExpr::column(0, DataType::Int64, "key")];
        let aggregates = vec![AggregateExpr {
            function: AggregateFunction::Count,
            expr: None,
            distinct: false,
            data_type: DataType::Int64,
            display_name: "count(*)".into(),
        }];
        let schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::Int64, false),
            Field::new("count", DataType::Int64, false),
        ]));
        let mut states = (0..64)
            .map(|key| GroupState::new(vec![CellValue::Int64(key)], &aggregates))
            .collect::<Vec<_>>();
        let mut group_index = states
            .iter()
            .enumerate()
            .map(|(index, state)| (state.key.clone(), index))
            .collect::<HashMap<_, _>>();
        assert!(states.capacity() > 0);
        assert!(group_index.capacity() > 0);

        let root = tempfile::tempdir().unwrap();
        let context = QueryContext::new(MemoryPool::new(1 << 20), root.path()).unwrap();
        let mut spiller = StateSpiller::new(&context, 32);
        spill_states(
            &mut states,
            &mut group_index,
            &groups,
            &aggregates,
            schema,
            &mut spiller,
            &context,
        )
        .unwrap();

        assert_eq!(states.capacity(), 0);
        assert_eq!(group_index.capacity(), 0);
        let files = spiller
            .finish(&context)
            .unwrap()
            .into_iter()
            .flat_map(|partition| partition.files)
            .collect::<Vec<_>>();
        for file in &files {
            context.spill.remove_file(file).unwrap();
        }
        assert_eq!(context.memory.used(), 0);
    }

    #[test]
    fn victim_spill_keeps_non_victim_partition_resident() {
        let groups = vec![BoundExpr::column(0, DataType::Int64, "key")];
        let aggregates = vec![AggregateExpr {
            function: AggregateFunction::Count,
            expr: None,
            distinct: false,
            data_type: DataType::Int64,
            display_name: "count(*)".into(),
        }];
        let schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::Int64, false),
            Field::new("count", DataType::Int64, false),
        ]));
        let mut by_partition = std::collections::HashMap::<usize, Vec<i64>>::new();
        for key in 0..10_000_i64 {
            let cells = vec![CellValue::Int64(key)];
            by_partition
                .entry(partition_for_key(&cells, 4, 0))
                .or_default()
                .push(key);
        }
        let mut partitions = by_partition.into_values().filter(|keys| keys.len() >= 3);
        let victim_keys = partitions.next().unwrap();
        let survivor_key = partitions.next().unwrap()[0];
        let keys = [victim_keys[0], victim_keys[1], victim_keys[2], survivor_key];
        let mut states = keys
            .iter()
            .map(|key| GroupState::new(vec![CellValue::Int64(*key)], &aggregates))
            .collect::<Vec<_>>();
        let mut group_index = keys
            .iter()
            .enumerate()
            .map(|(index, key)| (GroupKey::Cells(vec![CellValue::Int64(*key)]), index))
            .collect::<HashMap<_, _>>();

        let root = tempfile::tempdir().unwrap();
        let context = QueryContext::new(MemoryPool::new(4 << 20), root.path()).unwrap();
        let mut spiller = StateSpiller::new(&context, 4);
        let resident = spill_largest_partition(
            &mut states,
            &mut group_index,
            &groups,
            &aggregates,
            schema,
            &mut spiller,
            &context,
        )
        .unwrap();
        assert_eq!(states.len(), 1);
        assert_eq!(group_index.len(), 1);
        assert_eq!(states[0].key, vec![CellValue::Int64(survivor_key)]);
        assert!(resident > 0);

        let files = spiller.finish(&context).unwrap();
        assert!(context.metrics.snapshot().max_spill_partition_bytes > 0);
        assert_eq!(
            files
                .iter()
                .filter(|partition| !partition.files.is_empty())
                .count(),
            1
        );
        for partition in files {
            for file in partition.files {
                context.spill.remove_file(&file).unwrap();
            }
        }
    }
}
