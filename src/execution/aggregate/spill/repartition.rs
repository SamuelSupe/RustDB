use std::mem::size_of;

use arrow::{array::UInt32Array, compute::take, record_batch::RecordBatch};

use crate::{
    Error, Result,
    runtime::{QueryContext, SpillFile, SpillManager, SpillWriter},
    sql::BoundExpr,
};

use super::{SPILL_PARTITIONS, group_key, partition_for_key};

const SEED_STEP: u64 = 0x9e37_79b9_7f4a_7c15;
const MAX_REPARTITION_FILE_BYTES: usize = 8 * 1024 * 1024;

struct RepartitionSpiller {
    spill: SpillManager,
    depth: usize,
    target_bytes: usize,
    partitions: Vec<PartitionSink>,
}

struct PartitionSink {
    files: Vec<SpillFile>,
    writer: Option<SpillWriter>,
    uncompressed_bytes: usize,
}

impl RepartitionSpiller {
    fn new(context: &QueryContext, depth: usize) -> Self {
        Self {
            spill: context.spill.clone(),
            depth,
            target_bytes: MAX_REPARTITION_FILE_BYTES
                .min(context.memory.limit() / 8)
                .max(1),
            partitions: (0..SPILL_PARTITIONS)
                .map(|_| PartitionSink {
                    files: Vec::new(),
                    writer: None,
                    uncompressed_bytes: 0,
                })
                .collect(),
        }
    }

    fn write(&mut self, partition: usize, batch: RecordBatch) -> Result<()> {
        let bytes = batch.get_array_memory_size().max(1);
        let sink = &self.partitions[partition];
        if sink.writer.is_some()
            && sink.uncompressed_bytes > 0
            && sink.uncompressed_bytes.saturating_add(bytes) > self.target_bytes
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

    fn finish(mut self) -> Result<Vec<Vec<SpillFile>>> {
        for partition in 0..SPILL_PARTITIONS {
            self.finish_partition(partition)?;
        }
        Ok(self
            .partitions
            .into_iter()
            .map(|partition| partition.files)
            .collect())
    }
}

pub(in crate::execution::aggregate) fn repartition_partition(
    files: &[SpillFile],
    groups: &[BoundExpr],
    depth: usize,
    context: &QueryContext,
) -> Result<Vec<Vec<SpillFile>>> {
    let mut spiller = RepartitionSpiller::new(context, depth);
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
                    let rows = partition_rows(&slice, groups.len(), seed)?;
                    write_partition_rows(&slice, &rows, &mut spiller, context)?;
                    offset += row_count;
                    break;
                }
            }
        }
    }
    spiller.finish()
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

fn partition_rows(batch: &RecordBatch, group_count: usize, seed: u64) -> Result<Vec<(usize, u32)>> {
    let mut rows = Vec::with_capacity(batch.num_rows());
    for row in 0..batch.num_rows() {
        let partition =
            partition_for_key(&group_key(batch, row, group_count)?, SPILL_PARTITIONS, seed);
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
        spiller.write(partition, batch.clone())?;
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
        spiller.write(partition, output)?;
        return Ok(());
    }
    write_slices(batch, partition, rows, spiller)
}

fn write_slices(
    batch: &RecordBatch,
    partition: usize,
    rows: &[(usize, u32)],
    spiller: &mut RepartitionSpiller,
) -> Result<()> {
    let mut start = 0;

    while start < rows.len() {
        let first = rows[start].1 as usize;
        let mut end = start + 1;
        while end < rows.len() && rows[end].1 == rows[end - 1].1.saturating_add(1) {
            end += 1;
        }
        spiller.write(partition, batch.slice(first, end - start))?;
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

    use super::{RepartitionSpiller, repartition_partition, write_slices};
    use crate::Error;
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
        let mut spiller = RepartitionSpiller::new(&context, 1);

        write_slices(&batch, 7, &[(7, 0), (7, 2), (7, 3)], &mut spiller).unwrap();
        let mut partitions = spiller.finish().unwrap();
        let files = partitions.remove(7);

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
            context.spill.remove_file(file);
        }
        assert_eq!(actual, vec![0, 2, 3]);
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
        assert!(batch.get_array_memory_size() > MEMORY_LIMIT / 2);

        let root = tempfile::tempdir().unwrap();
        let context = QueryContext::new(MemoryPool::new(MEMORY_LIMIT), root.path()).unwrap();
        let file = context
            .spill
            .write_record_batches("aggregate-repartition-source", schema, [batch])
            .unwrap();
        let error = repartition_partition(
            std::slice::from_ref(&file),
            &[BoundExpr::column(0, DataType::Int64, "key")],
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

        context.spill.remove_file(&file);
        assert_eq!(context.memory.used(), 0);
    }
}
