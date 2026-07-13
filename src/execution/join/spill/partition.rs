use std::mem::size_of;

use arrow::{array::ArrayData, record_batch::RecordBatch};

use crate::{
    Error, Result,
    runtime::{MemoryPool, QueryContext, SpillFile, SpillManager, SpillWriter},
};

use super::super::CellValue;
use super::{BuildPartitionStats, PARTITIONS, build::compaction_reservation_bytes};

const MAX_SPILL_FILE_BYTES: usize = 256 * 1024 * 1024;

mod write;

pub(in crate::execution::join) use write::{Side, spill_batch_with_null_keys, spill_stream};
#[cfg(test)]
pub(in crate::execution::join) use write::{partition_for_key, spill_batch};

pub(in crate::execution::join) struct PartitionSpiller<'a> {
    context: &'a QueryContext,
    spill: SpillManager,
    memory: MemoryPool,
    label: String,
    target_bytes: usize,
    depth: usize,
    count_logical_input: bool,
    partition_bytes: Vec<u64>,
    partition_batches: Vec<u64>,
    max_partition_batch_bytes: Vec<u64>,
    partition_rows: Vec<u64>,
    key_columns: usize,
    partitions: Vec<PartitionSink>,
}

pub(in crate::execution::join) struct PartitionManifest {
    pub(in crate::execution::join) files: Vec<Vec<SpillFile>>,
    pub(in crate::execution::join) build: Vec<BuildPartitionStats>,
}

impl PartitionManifest {
    pub(in crate::execution::join) fn len(&self) -> usize {
        self.files.len()
    }
}

struct PartitionSink {
    files: Vec<SpillFile>,
    writer: Option<SpillWriter>,
    uncompressed_bytes: usize,
    schema_columns: usize,
}

impl<'a> PartitionSpiller<'a> {
    #[cfg(test)]
    pub(in crate::execution::join) fn new(
        context: &'a QueryContext,
        label: impl Into<String>,
    ) -> Self {
        Self::with_partitions(context, label, PARTITIONS)
    }

    pub(in crate::execution::join) fn with_partitions(
        context: &'a QueryContext,
        label: impl Into<String>,
        partitions: usize,
    ) -> Self {
        Self::with_policy(context, label, partitions, 0, true)
    }

    pub(in crate::execution::join) fn for_repartition(
        context: &'a QueryContext,
        label: impl Into<String>,
        partitions: usize,
        depth: usize,
    ) -> Self {
        Self::with_policy(context, label, partitions, depth, false)
    }

    fn with_policy(
        context: &'a QueryContext,
        label: impl Into<String>,
        partitions: usize,
        depth: usize,
        count_logical_input: bool,
    ) -> Self {
        let partitions = partitions.clamp(1, PARTITIONS);
        Self {
            context,
            spill: context.spill.clone(),
            memory: context.memory.clone(),
            label: label.into(),
            target_bytes: spill_file_target(context),
            depth,
            count_logical_input,
            partition_bytes: vec![0; partitions],
            partition_batches: vec![0; partitions],
            max_partition_batch_bytes: vec![0; partitions],
            partition_rows: vec![0; partitions],
            key_columns: 1,
            partitions: (0..partitions)
                .map(|_| PartitionSink {
                    files: Vec::new(),
                    writer: None,
                    uncompressed_bytes: 0,
                    schema_columns: 0,
                })
                .collect(),
        }
    }

    fn observe_key_columns(&mut self, columns: usize) {
        self.key_columns = self.key_columns.max(columns.max(1));
    }

    fn write(&mut self, partition: usize, batch: RecordBatch) -> Result<()> {
        let bytes = batch.get_array_memory_size().max(1);
        let logical_bytes = batch_logical_buffer_bytes(&batch).max(1);
        let bytes_u64 = u64::try_from(bytes).unwrap_or(u64::MAX);
        let logical_bytes_u64 = u64::try_from(logical_bytes).unwrap_or(u64::MAX);
        let projected_partition = self.partition_bytes[partition].saturating_add(logical_bytes_u64);
        let max_partition = self
            .partition_bytes
            .iter()
            .copied()
            .max()
            .unwrap_or(0)
            .max(projected_partition);
        let should_rotate = {
            let sink = &self.partitions[partition];
            sink.writer.is_some()
                && sink.uncompressed_bytes > 0
                && sink.uncompressed_bytes.saturating_add(bytes) > self.target_bytes
        };
        if should_rotate {
            self.finish_partition(partition)?;
        }

        if self.partitions[partition].writer.is_none() {
            self.open_writer(partition, batch.schema())?;
        }
        let sink = &mut self.partitions[partition];
        sink.schema_columns = sink.schema_columns.max(batch.num_columns());
        sink.writer
            .as_mut()
            .expect("partition writer was created above")
            .write_batch(&batch)?;
        sink.uncompressed_bytes = sink.uncompressed_bytes.saturating_add(bytes);
        self.partition_bytes[partition] = projected_partition;
        self.partition_batches[partition] = self.partition_batches[partition].saturating_add(1);
        self.max_partition_batch_bytes[partition] =
            self.max_partition_batch_bytes[partition].max(bytes_u64);
        self.partition_rows[partition] = self.partition_rows[partition]
            .saturating_add(u64::try_from(batch.num_rows()).unwrap_or(u64::MAX));
        self.context.check_spill_write_amplification(
            "HashJoin",
            self.pending_write_bytes(),
            self.depth,
            max_partition,
        )?;
        Ok(())
    }

    fn record_logical_input(&self, bytes: usize) {
        if self.count_logical_input {
            self.context
                .record_spill_logical_input_bytes(u64::try_from(bytes).unwrap_or(u64::MAX));
        }
    }

    fn open_writer(&mut self, partition: usize, schema: arrow::datatypes::SchemaRef) -> Result<()> {
        let label = format!("{}-p{partition}", self.label);
        loop {
            let headroom = self.spill.writer_headroom_bytes(&label, schema.as_ref());
            if self.memory.available() < headroom
                && self.finish_largest_open_partition(partition)?
            {
                continue;
            }
            match self.spill.writer(&label, schema.clone()) {
                Ok(writer) => {
                    self.partitions[partition].writer = Some(writer);
                    return Ok(());
                }
                Err(error @ Error::ResourceExhausted(_)) => {
                    if !self.finish_largest_open_partition(partition)? {
                        return Err(error);
                    }
                }
                Err(error) => return Err(error),
            }
        }
    }

    fn finish_largest_open_partition(&mut self, excluded: usize) -> Result<bool> {
        let victim = self
            .partitions
            .iter()
            .enumerate()
            .filter(|(partition, sink)| *partition != excluded && sink.writer.is_some())
            .max_by_key(|(_, sink)| sink.uncompressed_bytes)
            .map(|(partition, _)| partition);
        let Some(victim) = victim else {
            return Ok(false);
        };
        self.finish_partition(victim)?;
        Ok(true)
    }

    fn finish_partition(&mut self, partition: usize) -> Result<()> {
        let sink = &mut self.partitions[partition];
        if let Some(writer) = sink.writer.take() {
            // Rotated physical files must not inflate the logical partition metric.
            sink.files.push(writer.finish(0)?);
        }
        sink.uncompressed_bytes = 0;
        Ok(())
    }

    fn pending_write_bytes(&self) -> u64 {
        self.partitions
            .iter()
            .filter_map(|sink| sink.writer.as_ref())
            .map(SpillWriter::pending_write_bytes)
            .fold(0u64, u64::saturating_add)
    }

    pub(in crate::execution::join) fn finish(self) -> Result<Vec<Vec<SpillFile>>> {
        Ok(self.finish_manifest()?.files)
    }

    pub(in crate::execution::join) fn finish_manifest(mut self) -> Result<PartitionManifest> {
        for partition in 0..self.partitions.len() {
            self.finish_partition(partition)?;
        }
        self.context.check_spill_write_amplification(
            "HashJoin",
            0,
            self.depth,
            self.partition_bytes.iter().copied().max().unwrap_or(0),
        )?;
        let key_columns = self.key_columns;
        let file_counts = self
            .partitions
            .iter()
            .map(|partition| partition.files.len())
            .collect::<Vec<_>>();
        let schema_columns = self
            .partitions
            .iter()
            .map(|partition| partition.schema_columns)
            .collect::<Vec<_>>();
        let files = self
            .partitions
            .into_iter()
            .map(|partition| partition.files)
            .collect();
        let build = self
            .partition_bytes
            .into_iter()
            .zip(self.partition_batches)
            .zip(self.max_partition_batch_bytes)
            .zip(self.partition_rows)
            .zip(file_counts)
            .zip(schema_columns)
            .map(
                |(((((data_bytes, batches), max_batch_bytes), rows), files), columns)| {
                    let data = usize::try_from(data_bytes).unwrap_or(usize::MAX);
                    let batches = usize::try_from(batches).unwrap_or(usize::MAX);
                    let max_batch = usize::try_from(max_batch_bytes).unwrap_or(usize::MAX);
                    let rows_usize = usize::try_from(rows).unwrap_or(usize::MAX);
                    let hash = estimated_build_footprint(data_bytes, rows, key_columns);
                    let compaction =
                        compaction_reservation_bytes(data, batches, max_batch, files, columns);
                    BuildPartitionStats {
                        estimated_bytes: usize::try_from(hash)
                            .unwrap_or(usize::MAX)
                            .max(compaction),
                        data_bytes: data,
                        batches,
                        max_batch_bytes: max_batch,
                        rows: rows_usize,
                    }
                },
            )
            .collect();
        Ok(PartitionManifest { files, build })
    }
}

pub(in crate::execution::join) fn estimated_build_footprint(
    data_bytes: u64,
    rows: u64,
    key_columns: usize,
) -> u64 {
    // HashMap capacity can approach twice the row count. Rust also rounds the
    // heap allocations of small key/value vectors up to at least four values.
    let bucket_bytes = size_of::<Vec<CellValue>>()
        .saturating_add(size_of::<Vec<u32>>())
        .saturating_add(size_of::<u64>().saturating_mul(2));
    let key_capacity = key_columns
        .max(4)
        .checked_next_power_of_two()
        .unwrap_or(usize::MAX);
    let hash_and_key_bytes = bucket_bytes
        .saturating_mul(2)
        .saturating_add(size_of::<CellValue>().saturating_mul(key_capacity))
        .saturating_add(size_of::<u32>().saturating_mul(4));
    data_bytes
        .saturating_mul(2)
        .saturating_add(rows.saturating_mul(u64::try_from(hash_and_key_bytes).unwrap_or(u64::MAX)))
}

pub(in crate::execution::join) fn batch_logical_buffer_bytes(batch: &RecordBatch) -> usize {
    batch.columns().iter().fold(0usize, |bytes, array| {
        bytes.saturating_add(array_data_logical_buffer_bytes(&array.to_data()))
    })
}

fn array_data_logical_buffer_bytes(data: &ArrayData) -> usize {
    let buffers = data
        .buffers()
        .iter()
        .fold(0usize, |bytes, buffer| bytes.saturating_add(buffer.len()));
    let nulls = data.nulls().map(|nulls| nulls.buffer().len()).unwrap_or(0);
    data.child_data()
        .iter()
        .fold(buffers.saturating_add(nulls), |bytes, child| {
            bytes.saturating_add(array_data_logical_buffer_bytes(child))
        })
}

fn spill_file_target(_context: &QueryContext) -> usize {
    MAX_SPILL_FILE_BYTES
}

#[cfg(test)]
mod tests;
