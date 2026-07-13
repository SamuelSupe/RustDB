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
    runtime::{MemoryBatchStream, QueryContext, SpillFile},
    sql::{BoundExpr, JoinType},
};

use super::super::super::{CellValue, evaluate_keys, row_key};
#[cfg(test)]
use super::super::PARTITIONS;
use super::PartitionSpiller;

const TAKE_ARRAY_METADATA_BYTES: usize = 256;

#[derive(Clone, Copy)]
pub(in crate::execution::join) enum Side {
    Left,
    Right,
}

#[allow(clippy::too_many_arguments)]
pub(in crate::execution::join) async fn spill_stream(
    stream: &mut MemoryBatchStream,
    expressions: &[BoundExpr],
    side: Side,
    join_type: JoinType,
    null_equal_keys: bool,
    context: &QueryContext,
    label: &str,
    partitions: usize,
) -> Result<Vec<Vec<SpillFile>>> {
    let mut spiller = PartitionSpiller::with_partitions(context, label, partitions);
    while let Some(batch) = stream.next().await {
        context.check_cancelled()?;
        let (batch, memory) = batch?.into_parts();
        spill_batch_with_null_keys(
            batch,
            expressions,
            side,
            join_type,
            null_equal_keys,
            &mut spiller,
            0,
        )?;
        drop(memory);
    }
    spiller.finish()
}

pub(in crate::execution::join) fn spill_batch_with_null_keys(
    batch: RecordBatch,
    expressions: &[BoundExpr],
    side: Side,
    join_type: JoinType,
    null_equal_keys: bool,
    spiller: &mut PartitionSpiller<'_>,
    seed: u64,
) -> Result<Vec<usize>> {
    spiller.observe_key_columns(expressions.len());
    spiller.record_logical_input(batch.get_array_memory_size().max(1));
    let mut totals = vec![0usize; spiller.partitions.len()];
    let mut offset = 0usize;
    let preferred_rows = spill_chunk_rows(&batch, spiller.target_bytes)
        .min(u32::MAX as usize)
        .max(1);
    while offset < batch.num_rows() {
        let mut rows = preferred_rows.min(batch.num_rows() - offset);
        loop {
            let required = spill_temporary_bytes(&batch, offset, rows)?;
            let copy_headroom = spiller.spill.write_copy_headroom_bytes();
            let writer_headroom = if spiller
                .partitions
                .iter()
                .all(|partition| partition.writer.is_none())
            {
                spiller
                    .spill
                    .writer_headroom_bytes(&spiller.label, batch.schema().as_ref())
                    .max(copy_headroom)
            } else {
                copy_headroom
            };
            let unprotected_copy_headroom =
                writer_headroom.saturating_sub(spiller.memory.emergency_headroom());
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
                        null_equal_keys,
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
    Ok(totals)
}

#[cfg(test)]
pub(in crate::execution::join) fn spill_batch(
    batch: RecordBatch,
    expressions: &[BoundExpr],
    side: Side,
    join_type: JoinType,
    spiller: &mut PartitionSpiller<'_>,
    seed: u64,
) -> Result<Vec<usize>> {
    spill_batch_with_null_keys(batch, expressions, side, join_type, false, spiller, seed)
}

#[allow(clippy::too_many_arguments)]
fn spill_batch_slice(
    batch: &RecordBatch,
    offset: usize,
    rows: usize,
    expressions: &[BoundExpr],
    side: Side,
    join_type: JoinType,
    null_equal_keys: bool,
    spiller: &mut PartitionSpiller<'_>,
    seed: u64,
) -> Result<Vec<usize>> {
    let batch = batch.slice(offset, rows);
    let keys = evaluate_keys(expressions, &batch)?;
    let mut assignments = Vec::with_capacity(rows);
    let mut counts = vec![0usize; spiller.partitions.len()];
    for row in 0..rows {
        let key = row_key(&keys, row)?;
        let partition = if !null_equal_keys && key.iter().any(CellValue::is_null) {
            match side {
                Side::Right if !matches!(join_type, JoinType::Right | JoinType::Full) => {
                    assignments.push(None);
                    continue;
                }
                Side::Left
                    if matches!(
                        join_type,
                        JoinType::Inner | JoinType::Right | JoinType::Semi
                    ) =>
                {
                    assignments.push(None);
                    continue;
                }
                Side::Left | Side::Right => 0,
            }
        } else {
            partition_for_key_count(&key, seed, spiller.partitions.len())
        };
        counts[partition] = counts[partition].saturating_add(1);
        assignments.push(Some(partition));
    }

    let mut indices = counts
        .iter()
        .map(|count| Vec::<u32>::with_capacity(*count))
        .collect::<Vec<_>>();
    for (row, partition) in assignments.into_iter().enumerate() {
        if let Some(partition) = partition {
            indices[partition].push(u32::try_from(row).map_err(|_| {
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

#[cfg(test)]
pub(in crate::execution::join) fn partition_for_key(key: &[CellValue], seed: u64) -> usize {
    partition_for_key_count(key, seed, PARTITIONS)
}

fn partition_for_key_count(key: &[CellValue], seed: u64, partitions: usize) -> usize {
    let mut hasher = DefaultHasher::new();
    seed.hash(&mut hasher);
    key.hash(&mut hasher);
    (hasher.finish() as usize) % partitions
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
