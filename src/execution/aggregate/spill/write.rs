use std::{collections::HashMap, hash::Hash, mem::size_of, sync::Arc};

use arrow::{datatypes::SchemaRef, record_batch::RecordBatch};

use crate::{
    Error, Result,
    runtime::{MemoryReservation, QueryContext, SpillFile, SpillWriter},
    sql::{AggregateExpr, BoundExpr},
};

use super::partition_for_key;
use crate::execution::aggregate::{
    CellValue, GroupState, build_partial_batch, estimate_group_bytes,
};

// Policy charge used to rotate long aggregate streams after a bounded number
// of batches. StreamWriter does not retain per-batch footer blocks; this keeps
// later read/retry work bounded rather than accounting Arrow-owned memory.
const IPC_BATCH_METADATA_BYTES: usize = 256;

pub(in crate::execution::aggregate) struct StateSpiller {
    partitions: Vec<PartitionSink>,
}

struct PartitionSink {
    files: Vec<SpillFile>,
    writer: Option<SpillWriter>,
    metadata: MemoryReservation,
}

impl StateSpiller {
    pub(in crate::execution::aggregate) fn new(context: &QueryContext, partitions: usize) -> Self {
        Self {
            partitions: (0..partitions)
                .map(|_| PartitionSink {
                    files: Vec::new(),
                    writer: None,
                    metadata: context.memory.reservation(),
                })
                .collect(),
        }
    }

    pub(in crate::execution::aggregate) fn finish(mut self) -> Result<Vec<Vec<SpillFile>>> {
        for sink in &mut self.partitions {
            if let Some(writer) = sink.writer.take() {
                sink.files.push(writer.finish(1)?);
            }
            sink.metadata.try_resize(0)?;
        }
        Ok(self
            .partitions
            .into_iter()
            .map(|partition| partition.files)
            .collect())
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
        write_partition(
            &states[start..end],
            partition,
            groups,
            aggregates,
            Arc::clone(&schema),
            &mut spiller.partitions[partition],
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
fn write_partition(
    states: &[GroupState],
    partition: usize,
    groups: &[BoundExpr],
    aggregates: &[AggregateExpr],
    schema: SchemaRef,
    sink: &mut PartitionSink,
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
        sink.writer
            .as_mut()
            .expect("aggregate spill writer was created above")
            .write_batch(&batch)?;
        drop(batch_memory);
        offset += rows;
    }

    Ok(())
}

fn rotate_writer_if_needed(sink: &mut PartitionSink, context: &QueryContext) -> Result<()> {
    let limit = (context.memory.limit() / 16).max(IPC_BATCH_METADATA_BYTES);
    if sink.writer.is_some()
        && sink
            .metadata
            .size()
            .saturating_add(IPC_BATCH_METADATA_BYTES)
            > limit
    {
        sink.files.push(
            sink.writer
                .take()
                .expect("aggregate spill writer was checked above")
                .finish(1)?,
        );
        sink.metadata.try_resize(0)?;
    }
    if sink.metadata.try_grow(IPC_BATCH_METADATA_BYTES).is_err() {
        if let Some(active) = sink.writer.take() {
            sink.files.push(active.finish(1)?);
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

    use super::{StateSpiller, spill_states};
    use crate::{
        execution::aggregate::{CellValue, GroupState},
        runtime::{MemoryPool, QueryContext},
        sql::{AggregateExpr, AggregateFunction, BoundExpr},
    };

    #[test]
    fn spilling_releases_state_and_index_backing_allocations() {
        let groups = vec![BoundExpr::column(0, DataType::Int64, "key")];
        let aggregates = vec![AggregateExpr {
            function: AggregateFunction::Count,
            expr: None,
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
            .finish()
            .unwrap()
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();
        for file in &files {
            context.spill.remove_file(file).unwrap();
        }
        assert_eq!(context.memory.used(), 0);
    }
}
