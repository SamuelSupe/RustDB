use std::mem::size_of;

use arrow::{array::UInt32Array, compute::take, record_batch::RecordBatch};

use crate::{
    Error, Result,
    runtime::{QueryContext, SpillFile, SpillManager, SpillWriter},
    sql::{AggregateExpr, BoundExpr},
};

use super::{
    SPILL_PARTITIONS, SpillPartition, adaptive_spill_partitions, cap_repartition_partitions,
    estimate_index_key_bytes, group_key, partition_for_key,
};
use crate::execution::aggregate::{GroupState, estimate_group_bytes};

const SEED_STEP: u64 = 0x9e37_79b9_7f4a_7c15;
const MAX_REPARTITION_FILE_BYTES: usize = 256 * 1024 * 1024;

struct RepartitionSpiller {
    spill: SpillManager,
    depth: usize,
    target_bytes: usize,
    group_count: usize,
    aggregates: Vec<AggregateExpr>,
    partition_bytes: Vec<u64>,
    partition_estimates: Vec<u64>,
    partitions: Vec<PartitionSink>,
}

struct PartitionSink {
    files: Vec<SpillFile>,
    writer: Option<SpillWriter>,
    uncompressed_bytes: usize,
}

impl RepartitionSpiller {
    fn new(
        context: &QueryContext,
        depth: usize,
        partitions: usize,
        group_count: usize,
        aggregates: &[AggregateExpr],
    ) -> Self {
        let partitions = partitions.clamp(2, SPILL_PARTITIONS.saturating_mul(8));
        Self {
            spill: context.spill.clone(),
            depth,
            target_bytes: MAX_REPARTITION_FILE_BYTES,
            group_count,
            aggregates: aggregates.to_vec(),
            partition_bytes: vec![0; partitions],
            partition_estimates: vec![0; partitions],
            partitions: (0..partitions)
                .map(|_| PartitionSink {
                    files: Vec::new(),
                    writer: None,
                    uncompressed_bytes: 0,
                })
                .collect(),
        }
    }

    fn write(
        &mut self,
        partition: usize,
        batch: RecordBatch,
        context: &QueryContext,
    ) -> Result<()> {
        let bytes = batch.get_array_memory_size().max(1);
        let merge_bytes = merge_footprint(&batch, self.group_count, &self.aggregates)?;
        let merge_bytes = u64::try_from(merge_bytes).unwrap_or(u64::MAX);
        let bytes_u64 = u64::try_from(bytes).unwrap_or(u64::MAX);
        let projected_partition = self.partition_bytes[partition].saturating_add(bytes_u64);
        let projected_estimate = self.partition_estimates[partition].saturating_add(merge_bytes);
        let max_partition = self
            .partition_estimates
            .iter()
            .copied()
            .max()
            .unwrap_or(0)
            .max(projected_estimate);
        let sink = &self.partitions[partition];
        if sink.writer.is_some()
            && sink.uncompressed_bytes > 0
            && sink.uncompressed_bytes.saturating_add(bytes) > self.target_bytes
            && self.rotation_preserves_partition_slots()
        {
            self.finish_partition(partition)?;
        }
        let sink = &mut self.partitions[partition];
        if sink.writer.is_none() {
            sink.writer = Some(self.spill.writer(
                &format!("aggregate-r{}-p{partition}", self.depth),
                batch.schema(),
            )?);
        }
        sink.writer
            .as_mut()
            .expect("aggregate repartition writer was created above")
            .write_batch(&batch)?;
        sink.uncompressed_bytes = sink.uncompressed_bytes.saturating_add(bytes);
        self.partition_bytes[partition] = projected_partition;
        self.partition_estimates[partition] = projected_estimate;
        context.check_spill_write_amplification(
            "HashAggregate",
            self.pending_write_bytes(),
            self.depth,
            max_partition,
        )?;
        Ok(())
    }

    fn finish_partition(&mut self, partition: usize) -> Result<()> {
        let sink = &mut self.partitions[partition];
        if let Some(writer) = sink.writer.take() {
            sink.files.push(writer.finish(1)?);
        }
        sink.uncompressed_bytes = 0;
        Ok(())
    }

    fn rotation_preserves_partition_slots(&self) -> bool {
        let unmaterialized = self
            .partitions
            .iter()
            .filter(|sink| sink.writer.is_none() && sink.files.is_empty())
            .count();
        self.spill
            .active_file_count()
            .saturating_add(unmaterialized)
            < crate::runtime::MAX_ACTIVE_SPILL_FILES
    }

    fn pending_write_bytes(&self) -> u64 {
        self.partitions
            .iter()
            .filter_map(|sink| sink.writer.as_ref())
            .map(SpillWriter::pending_write_bytes)
            .fold(0u64, u64::saturating_add)
    }

    fn finish(mut self, context: &QueryContext) -> Result<Vec<SpillPartition>> {
        for partition in 0..self.partitions.len() {
            self.finish_partition(partition)?;
        }
        context.check_spill_write_amplification(
            "HashAggregate",
            0,
            self.depth,
            self.partition_estimates.iter().copied().max().unwrap_or(0),
        )?;
        context.metrics.record_repartition(
            self.partition_bytes
                .iter()
                .copied()
                .fold(0u64, u64::saturating_add),
            self.depth,
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
}

pub(in crate::execution::aggregate) fn repartition_partition(
    files: &[SpillFile],
    source_bytes: usize,
    groups: &[BoundExpr],
    aggregates: &[AggregateExpr],
    depth: usize,
    context: &QueryContext,
) -> Result<Vec<SpillPartition>> {
    context.spill.with_file_budget(|| {
        repartition_partition_locked(files, source_bytes, groups, aggregates, depth, context)
    })
}

fn repartition_partition_locked(
    files: &[SpillFile],
    source_bytes: usize,
    groups: &[BoundExpr],
    aggregates: &[AggregateExpr],
    depth: usize,
    context: &QueryContext,
) -> Result<Vec<SpillPartition>> {
    let desired = recursive_spill_partitions(context, source_bytes);
    let partitions =
        cap_repartition_partitions(context, desired, "HashAggregate", depth, source_bytes)?;
    let mut spiller = RepartitionSpiller::new(context, depth, partitions, groups.len(), aggregates);
    let seed = SEED_STEP.wrapping_mul(depth as u64);
    let workspace = context.memory.child(
        format!("aggregate-repartition-{}", context.query_id),
        repartition_workspace_limit(context.memory.limit()),
    );

    for source in files {
        for batch in context.spill.read_file(source)? {
            context.check_cancelled()?;
            let batch = batch?;
            // Slices keep the decoded IPC batch's buffers alive. Account the
            // complete batch until every slice has been repartitioned rather
            // than charging only the currently visible slice.
            let decoded_bytes = batch.get_array_memory_size().max(1);
            let _decoded_memory = workspace
                .try_reserve(decoded_bytes)
                .map_err(|_| batch_error(decoded_bytes, context))?;
            let mut offset = 0usize;
            while offset < batch.num_rows() {
                let mut row_count = context.batch_size.max(1).min(batch.num_rows() - offset);
                loop {
                    let slice = batch.slice(offset, row_count);
                    let indices_bytes = index_bytes(row_count);
                    let Ok(_index_memory) = workspace.try_reserve(indices_bytes) else {
                        if row_count == 1 {
                            return Err(index_error(indices_bytes, context));
                        }
                        row_count = row_count.div_ceil(2);
                        continue;
                    };
                    let rows = partition_rows(&slice, groups.len(), partitions, seed)?;
                    write_partition_rows(&slice, &rows, &mut spiller, context)?;
                    offset += row_count;
                    break;
                }
            }
        }
    }
    spiller.finish(context)
}

fn recursive_spill_partitions(context: &QueryContext, source_bytes: usize) -> usize {
    let lanes = context.scheduler.partitioning_lanes();
    let default_target = context
        .memory
        .limit()
        .checked_div(lanes.saturating_mul(2))
        .unwrap_or(0)
        .clamp(8 << 20, 64 << 20);
    let target = context
        .execution
        .spill_partition_target_bytes
        .unwrap_or(default_target)
        .max(1);
    let memory_pressure_floor = target
        .div_ceil(context.memory.limit().max(1))
        .checked_next_power_of_two()
        .unwrap_or(256)
        .clamp(2, 256);
    let memory_pressure_floor = if target > context.memory.limit() {
        memory_pressure_floor.max(SPILL_PARTITIONS)
    } else {
        memory_pressure_floor
    };
    adaptive_spill_partitions(context, source_bytes).max(memory_pressure_floor)
}

fn merge_footprint(
    batch: &RecordBatch,
    group_count: usize,
    aggregates: &[AggregateExpr],
) -> Result<usize> {
    let mut merge_bytes = 0usize;
    for row in 0..batch.num_rows() {
        let key = group_key(batch, row, group_count)?;
        let state = GroupState::new(key.clone(), aggregates);
        merge_bytes = merge_bytes.saturating_add(
            estimate_group_bytes(&state).saturating_add(estimate_index_key_bytes(&key)),
        );
    }
    Ok(merge_bytes.max(1))
}

fn write_partition_rows(
    batch: &RecordBatch,
    rows: &[(usize, u32)],
    spiller: &mut RepartitionSpiller,
    context: &QueryContext,
) -> Result<()> {
    let mut start = 0;
    while start < rows.len() {
        let partition = rows[start].0;
        let end = rows[start..].partition_point(|(candidate, _)| *candidate == partition) + start;
        write_rows(batch, partition, &rows[start..end], spiller, context)?;
        start = end;
    }
    Ok(())
}

fn partition_rows(
    batch: &RecordBatch,
    group_count: usize,
    partitions: usize,
    seed: u64,
) -> Result<Vec<(usize, u32)>> {
    let mut rows = Vec::with_capacity(batch.num_rows());
    for row in 0..batch.num_rows() {
        let partition = partition_for_key(&group_key(batch, row, group_count)?, partitions, seed);
        rows.push((
            partition,
            u32::try_from(row).map_err(|_| {
                Error::ResourceExhausted("aggregate spill batch exceeds UINT32_MAX rows".into())
            })?,
        ));
    }
    rows.sort_unstable();
    Ok(rows)
}

fn write_rows(
    batch: &RecordBatch,
    partition: usize,
    rows: &[(usize, u32)],
    spiller: &mut RepartitionSpiller,
    context: &QueryContext,
) -> Result<()> {
    if rows.len() == batch.num_rows() {
        spiller.write(partition, batch.clone(), context)?;
        return Ok(());
    }

    let indices = rows.iter().map(|(_, row)| *row).collect::<Vec<_>>();
    let estimate = output_estimate(batch, indices.len());
    if let Ok(mut memory) = context.memory.try_reserve(estimate) {
        let output = take_rows(batch, indices)?;
        let actual = output.get_array_memory_size().max(1);
        memory
            .try_resize(actual)
            .map_err(|_| output_error(estimate.max(actual), context))?;
        spiller.write(partition, output, context)?;
        return Ok(());
    }
    write_slices(batch, partition, rows, spiller, context)
}

fn write_slices(
    batch: &RecordBatch,
    partition: usize,
    rows: &[(usize, u32)],
    spiller: &mut RepartitionSpiller,
    context: &QueryContext,
) -> Result<()> {
    let mut start = 0;

    while start < rows.len() {
        let first = rows[start].1 as usize;
        let mut end = start + 1;
        while end < rows.len() && rows[end].1 == rows[end - 1].1.saturating_add(1) {
            end += 1;
        }
        spiller.write(partition, batch.slice(first, end - start), context)?;
        start = end;
    }
    Ok(())
}

fn take_rows(batch: &RecordBatch, indices: Vec<u32>) -> Result<RecordBatch> {
    let indices = UInt32Array::from(indices);
    let columns = batch
        .columns()
        .iter()
        .map(|column| take(column.as_ref(), &indices, None))
        .collect::<std::result::Result<Vec<_>, _>>()?;
    Ok(RecordBatch::try_new(batch.schema(), columns)?)
}

fn index_bytes(rows: usize) -> usize {
    rows.saturating_mul(size_of::<(usize, u32)>())
        .saturating_mul(2)
        + rows.saturating_mul(size_of::<u32>())
}

fn repartition_workspace_limit(query_limit: usize) -> usize {
    // Keep half the query budget free for active-file metadata and partition
    // output while source buffers and sorted row indices are resident.
    query_limit.checked_div(2).unwrap_or(0).max(1)
}

fn output_estimate(batch: &RecordBatch, rows: usize) -> usize {
    batch
        .get_array_memory_size()
        .saturating_add(rows.saturating_mul(batch.num_columns()).saturating_mul(8))
        .saturating_add(batch.num_columns().saturating_mul(256))
        .max(1)
}

fn batch_error(bytes: usize, context: &QueryContext) -> Error {
    resource_error("one IPC batch", bytes, context)
}

fn index_error(bytes: usize, context: &QueryContext) -> Error {
    resource_error("one batch of row indices", bytes, context)
}

fn output_error(bytes: usize, context: &QueryContext) -> Error {
    resource_error("one partition output batch", bytes, context)
}

fn resource_error(kind: &str, bytes: usize, context: &QueryContext) -> Error {
    Error::ResourceExhausted(format!(
        "aggregate spill repartition requires at least {bytes} bytes for {kind} \
         (query limit {} bytes, currently available {} bytes); reduce the spill batch size \
         or increase the memory limit",
        context.memory.limit(),
        context.memory.available()
    ))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::{
        array::{Array, Int64Array},
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    };

    use super::{RepartitionSpiller, merge_footprint, repartition_partition, write_slices};
    use crate::Error;
    use crate::execution::aggregate::spill::adaptive_spill_partitions;
    use crate::runtime::{MemoryPool, QueryContext};
    use crate::sql::BoundExpr;

    #[test]
    fn sparse_partition_rows_are_not_written_as_one_contiguous_slice() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            false,
        )]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(vec![0, 99, 2, 3]))],
        )
        .unwrap();
        let root = tempfile::tempdir().unwrap();
        let context = QueryContext::new(MemoryPool::new(1 << 20), root.path()).unwrap();
        let mut spiller = RepartitionSpiller::new(&context, 1, 32, 0, &[]);

        write_slices(&batch, 7, &[(7, 0), (7, 2), (7, 3)], &mut spiller, &context).unwrap();
        let mut partitions = spiller.finish(&context).unwrap();
        let metrics = context.metrics.snapshot();
        assert!(metrics.spill_repartition_bytes > 0);
        assert_eq!(metrics.max_repartition_depth, 1);
        assert!(metrics.max_spill_partition_bytes > 0);
        let files = partitions.remove(7).files;

        let mut actual = Vec::new();
        for file in &files {
            for batch in context.spill.read_file(file).unwrap() {
                let batch = batch.unwrap();
                let values = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                actual.extend((0..values.len()).map(|row| values.value(row)));
            }
            context.spill.remove_file(file).unwrap();
        }
        assert_eq!(actual, vec![0, 2, 3]);
        assert_eq!(context.memory.used(), 0);
    }

    #[test]
    fn recursive_fanout_uses_estimated_merge_footprint() {
        let schema = Arc::new(Schema::new(vec![Field::new("key", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from_iter_values(0..20_000))],
        )
        .unwrap();
        let source_bytes = merge_footprint(&batch, 1, &[]).unwrap();
        let root = tempfile::tempdir().unwrap();
        let mut context = QueryContext::new(MemoryPool::new(8 << 20), root.path()).unwrap();
        context.execution.spill_partition_target_bytes = Some(16 << 10);
        let file = context
            .spill
            .write_record_batches("aggregate-adaptive-source", schema, [batch])
            .unwrap();
        let aggregates = Vec::new();
        let groups = [BoundExpr::column(0, DataType::Int64, "key")];
        let expected = adaptive_spill_partitions(&context, source_bytes);

        let children = repartition_partition(
            std::slice::from_ref(&file),
            source_bytes,
            &groups,
            &aggregates,
            1,
            &context,
        )
        .unwrap();
        let metrics = context.metrics.snapshot();
        assert_eq!(children.len(), expected);
        assert!(children.len() > 2 && children.len() <= 256);
        assert!(children.len().is_power_of_two());
        assert!(metrics.spill_repartition_bytes > 0);
        assert_eq!(metrics.max_repartition_depth, 1);
        assert!(metrics.max_spill_partition_bytes > 0);

        context.spill.remove_file(&file).unwrap();
        for partition in &children {
            for file in &partition.files {
                context.spill.remove_file(file).unwrap();
            }
        }
        assert_eq!(context.memory.used(), 0);
    }

    #[test]
    fn decoded_ipc_batch_must_fit_the_repartition_workspace() {
        const MEMORY_LIMIT: usize = 128 << 10;
        let schema = Arc::new(Schema::new(vec![Field::new("key", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from_iter_values(0..12_000))],
        )
        .unwrap();
        let source_bytes = merge_footprint(&batch, 1, &[]).unwrap();
        assert!(batch.get_array_memory_size() > MEMORY_LIMIT / 2);

        let root = tempfile::tempdir().unwrap();
        let context = QueryContext::new(MemoryPool::new(MEMORY_LIMIT), root.path()).unwrap();
        let file = context
            .spill
            .write_record_batches("aggregate-repartition-source", schema, [batch])
            .unwrap();
        let error = repartition_partition(
            std::slice::from_ref(&file),
            source_bytes,
            &[BoundExpr::column(0, DataType::Int64, "key")],
            &[],
            1,
            &context,
        )
        .unwrap_err();
        assert!(matches!(
            error,
            Error::ResourceExhausted(message)
                if message.contains("one IPC batch")
                    && message.contains("query limit 131072 bytes")
        ));

        context.spill.remove_file(&file).unwrap();
        assert_eq!(context.memory.used(), 0);
    }
}
