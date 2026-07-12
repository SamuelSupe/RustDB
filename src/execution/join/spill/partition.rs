use std::{
    collections::hash_map::DefaultHasher,
    hash::{Hash, Hasher},
    mem::size_of,
};

use arrow::{
    array::{ArrayRef, UInt32Array},
    compute::take_record_batch,
    record_batch::RecordBatch,
};
use futures::StreamExt;

use crate::{
    Error, Result,
    runtime::{MemoryBatchStream, MemoryPool, QueryContext, SpillFile, SpillManager, SpillWriter},
    sql::{BoundExpr, JoinType},
};

use super::super::{CellValue, evaluate_keys, row_key};
use super::PARTITIONS;

const MAX_SPILL_FILE_BYTES: usize = 8 * 1024 * 1024;
const TAKE_ARRAY_METADATA_BYTES: usize = 256;

#[derive(Clone, Copy)]
pub(in crate::execution::join) enum Side {
    Left,
    Right,
}

pub(in crate::execution::join) struct PartitionSpiller {
    spill: SpillManager,
    memory: MemoryPool,
    label: String,
    target_bytes: usize,
    partitions: Vec<PartitionSink>,
}

struct PartitionSink {
    files: Vec<SpillFile>,
    writer: Option<SpillWriter>,
    uncompressed_bytes: usize,
}

impl PartitionSpiller {
    pub(in crate::execution::join) fn new(
        context: &QueryContext,
        label: impl Into<String>,
    ) -> Self {
        Self {
            spill: context.spill.clone(),
            memory: context.memory.clone(),
            label: label.into(),
            target_bytes: spill_file_target(context),
            partitions: (0..PARTITIONS)
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
        sink.writer
            .as_mut()
            .expect("partition writer was created above")
            .write_batch(&batch)?;
        sink.uncompressed_bytes = sink.uncompressed_bytes.saturating_add(bytes);
        Ok(())
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

    pub(in crate::execution::join) fn finish(mut self) -> Result<Vec<Vec<SpillFile>>> {
        for partition in 0..PARTITIONS {
            self.finish_partition(partition)?;
        }
        Ok(self
            .partitions
            .into_iter()
            .map(|partition| partition.files)
            .collect())
    }
}

pub(in crate::execution::join) async fn spill_stream(
    stream: &mut MemoryBatchStream,
    expressions: &[BoundExpr],
    side: Side,
    join_type: JoinType,
    context: &QueryContext,
    label: &str,
) -> Result<Vec<Vec<SpillFile>>> {
    let mut spiller = PartitionSpiller::new(context, label);
    while let Some(batch) = stream.next().await {
        context.check_cancelled()?;
        let (batch, memory) = batch?.into_parts();
        spill_batch(batch, expressions, side, join_type, &mut spiller, 0)?;
        drop(memory);
    }
    spiller.finish()
}

pub(in crate::execution::join) fn spill_batch(
    batch: RecordBatch,
    expressions: &[BoundExpr],
    side: Side,
    join_type: JoinType,
    spiller: &mut PartitionSpiller,
    seed: u64,
) -> Result<Vec<usize>> {
    let mut totals = [0usize; PARTITIONS];
    let mut offset = 0usize;
    let preferred_rows = spill_chunk_rows(&batch, spiller.target_bytes)
        .min(u32::MAX as usize)
        .max(1);
    while offset < batch.num_rows() {
        let mut rows = preferred_rows.min(batch.num_rows() - offset);
        loop {
            let required = spill_temporary_bytes(&batch, offset, rows)?;
            let copy_headroom = spiller.spill.write_copy_headroom_bytes();
            let unprotected_copy_headroom =
                copy_headroom.saturating_sub(spiller.memory.emergency_headroom());
            if required
                > spiller
                    .memory
                    .available()
                    .saturating_sub(unprotected_copy_headroom)
            {
                if rows > 1 {
                    rows /= 2;
                    continue;
                }
                return Err(Error::ResourceExhausted(format!(
                    "join spill cannot reserve the minimum one-row temporary state of \
                     {required} bytes while preserving {copy_headroom} bytes of spill I/O copy \
                     headroom (used {}, limit {})",
                    spiller.memory.used(),
                    spiller.memory.limit(),
                )));
            }
            match spiller.memory.try_reserve(required) {
                Ok(temporary) => {
                    if spiller.memory.available() < unprotected_copy_headroom {
                        drop(temporary);
                        if rows > 1 {
                            rows /= 2;
                            continue;
                        }
                        return Err(Error::ResourceExhausted(format!(
                            "join spill reserved the minimum one-row temporary state of \
                             {required} bytes but could not preserve {copy_headroom} bytes of \
                             spill I/O copy headroom (used {}, limit {})",
                            spiller.memory.used(),
                            spiller.memory.limit(),
                        )));
                    }
                    let counts = spill_batch_slice(
                        &batch,
                        offset,
                        rows,
                        expressions,
                        side,
                        join_type,
                        spiller,
                        seed,
                    )?;
                    drop(temporary);
                    for (total, count) in totals.iter_mut().zip(counts) {
                        *total = total.saturating_add(count);
                    }
                    offset += rows;
                    break;
                }
                Err(_error) if rows > 1 => rows /= 2,
                Err(error) => {
                    return Err(Error::ResourceExhausted(format!(
                        "join spill cannot reserve the minimum one-row temporary state of \
                         {required} bytes while preserving {copy_headroom} bytes of spill I/O \
                         copy headroom (used {}, limit {}): {error}",
                        spiller.memory.used(),
                        spiller.memory.limit(),
                    )));
                }
            }
        }
    }
    Ok(totals.to_vec())
}

#[allow(clippy::too_many_arguments)]
fn spill_batch_slice(
    batch: &RecordBatch,
    offset: usize,
    rows: usize,
    expressions: &[BoundExpr],
    side: Side,
    join_type: JoinType,
    spiller: &mut PartitionSpiller,
    seed: u64,
) -> Result<[usize; PARTITIONS]> {
    const SKIP: u8 = u8::MAX;

    let batch = batch.slice(offset, rows);
    let keys = evaluate_keys(expressions, &batch)?;
    let mut assignments = Vec::with_capacity(rows);
    let mut counts = [0usize; PARTITIONS];
    for row in 0..rows {
        let key = row_key(&keys, row)?;
        let partition = if key.iter().any(CellValue::is_null) {
            match side {
                Side::Right => {
                    assignments.push(SKIP);
                    continue;
                }
                Side::Left if matches!(join_type, JoinType::Inner | JoinType::Semi) => {
                    assignments.push(SKIP);
                    continue;
                }
                Side::Left => 0,
            }
        } else {
            partition_for_key(&key, seed)
        };
        counts[partition] = counts[partition].saturating_add(1);
        assignments.push(u8::try_from(partition).expect("join partition fits in u8"));
    }

    let mut indices: [Vec<u32>; PARTITIONS] =
        std::array::from_fn(|partition| Vec::with_capacity(counts[partition]));
    for (row, partition) in assignments.into_iter().enumerate() {
        if partition != SKIP {
            indices[usize::from(partition)].push(u32::try_from(row).map_err(|_| {
                Error::ResourceExhausted("join spill slice exceeds UINT32_MAX rows".into())
            })?);
        }
    }

    for (partition, indices) in indices.into_iter().enumerate() {
        if !indices.is_empty() {
            let partition_batch = take_record_batch(&batch, &UInt32Array::from(indices))?;
            spiller.write(partition, partition_batch)?;
        }
    }
    Ok(counts)
}

pub(in crate::execution::join) fn partition_for_key(key: &[CellValue], seed: u64) -> usize {
    let mut hasher = DefaultHasher::new();
    seed.hash(&mut hasher);
    key.hash(&mut hasher);
    (hasher.finish() as usize) % PARTITIONS
}

fn spill_file_target(context: &QueryContext) -> usize {
    MAX_SPILL_FILE_BYTES.min(context.memory.limit() / 8).max(1)
}

fn spill_chunk_rows(batch: &RecordBatch, target_bytes: usize) -> usize {
    if batch.num_rows() == 0 {
        return 1;
    }
    let bytes_per_row = batch
        .get_array_memory_size()
        .div_ceil(batch.num_rows())
        .max(1);
    let estimated_per_row = bytes_per_row.saturating_mul(2).saturating_add(256);
    (target_bytes / estimated_per_row)
        .max(1)
        .min(batch.num_rows())
}

fn spill_temporary_bytes(batch: &RecordBatch, offset: usize, rows: usize) -> Result<usize> {
    let logical_buffers = batch.columns().iter().try_fold(0usize, |bytes, column| {
        let data = column.to_data().slice(offset, rows);
        Ok::<_, arrow::error::ArrowError>(bytes.saturating_add(data.get_slice_memory_size()?))
    })?;
    let index_bytes = rows.saturating_mul(size_of::<u8>().saturating_add(size_of::<u32>()));
    let metadata = size_of::<RecordBatch>()
        .saturating_add(size_of::<UInt32Array>())
        .saturating_add(
            batch
                .num_columns()
                .saturating_mul(size_of::<ArrayRef>().saturating_add(TAKE_ARRAY_METADATA_BYTES)),
        );
    Ok(logical_buffers
        // One logical copy covers key buffers and one covers the Arrow take output.
        .saturating_mul(2)
        .saturating_add(index_bytes)
        .saturating_add(metadata)
        .max(1))
}
