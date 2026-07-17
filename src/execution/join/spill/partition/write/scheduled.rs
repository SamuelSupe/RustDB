use arrow::{array::UInt32Array, compute::take_record_batch, record_batch::RecordBatch};
use tokio_util::sync::CancellationToken;

use crate::{
    Error, Result,
    runtime::{MemoryReservation, QueryContext},
    sql::JoinType,
};

use super::super::super::super::{CellValue, evaluate_keys, row_key};
use super::super::PartitionSpiller;
use super::{Side, partition_for_key_count, spill_chunk_rows, spill_temporary_bytes};

#[allow(clippy::too_many_arguments)]
pub(in crate::execution::join) async fn spill_batch_with_null_keys_scheduled(
    batch: RecordBatch,
    expressions: &[crate::sql::BoundExpr],
    side: Side,
    join_type: JoinType,
    null_equal_keys: bool,
    spiller: &mut PartitionSpiller<'_>,
    seed: u64,
    cancellation: &CancellationToken,
    context: &QueryContext,
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
            let unprotected = writer_headroom.saturating_sub(spiller.memory.emergency_headroom());
            if required > spiller.memory.available().saturating_sub(unprotected) {
                if rows > 1 {
                    rows /= 2;
                    continue;
                }
                return minimum_memory_error(spiller, required, copy_headroom, None);
            }
            let mut temporary = match spiller.memory.try_reserve(required) {
                Ok(temporary) if spiller.memory.available() >= unprotected => temporary,
                Ok(temporary) => {
                    drop(temporary);
                    if rows > 1 {
                        rows /= 2;
                        continue;
                    }
                    return minimum_memory_error(spiller, required, copy_headroom, None);
                }
                Err(_) if rows > 1 => {
                    rows /= 2;
                    continue;
                }
                Err(error) => {
                    return minimum_memory_error(
                        spiller,
                        required,
                        copy_headroom,
                        Some(error.to_string()),
                    );
                }
            };

            let prepared = {
                let _permit = context
                    .acquire_compute_until_cancelled(cancellation)
                    .await?;
                let _active = context.scheduler.enter_lane();
                prepare_slice(
                    &batch,
                    offset,
                    rows,
                    expressions,
                    side,
                    join_type,
                    null_equal_keys,
                    seed,
                    spiller.partitions.len(),
                )?
            };
            let counts =
                write_prepared(prepared, spiller, cancellation, context, &mut temporary).await?;
            drop(temporary);
            for (total, count) in totals.iter_mut().zip(counts) {
                *total = total.saturating_add(count);
            }
            offset += rows;
            break;
        }
    }
    Ok(totals)
}

struct PreparedSlice {
    batch: RecordBatch,
    counts: Vec<usize>,
    indices: Vec<Vec<u32>>,
}

#[allow(clippy::too_many_arguments)]
fn prepare_slice(
    batch: &RecordBatch,
    offset: usize,
    rows: usize,
    expressions: &[crate::sql::BoundExpr],
    side: Side,
    join_type: JoinType,
    null_equal_keys: bool,
    seed: u64,
    partitions: usize,
) -> Result<PreparedSlice> {
    let batch = batch.slice(offset, rows);
    let keys = evaluate_keys(expressions, &batch)?;
    let mut assignments = Vec::with_capacity(rows);
    let mut counts = vec![0usize; partitions];
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
            partition_for_key_count(&key, seed, partitions)
        };
        counts[partition] = counts[partition].saturating_add(1);
        assignments.push(Some(partition));
    }
    let mut indices = counts
        .iter()
        .map(|count| Vec::with_capacity(*count))
        .collect::<Vec<Vec<u32>>>();
    for (row, partition) in assignments.into_iter().enumerate() {
        if let Some(partition) = partition {
            indices[partition].push(u32::try_from(row).map_err(|_| {
                Error::ResourceExhausted("join spill slice exceeds UINT32_MAX rows".into())
            })?);
        }
    }
    Ok(PreparedSlice {
        batch,
        counts,
        indices,
    })
}

async fn write_prepared(
    prepared: PreparedSlice,
    spiller: &mut PartitionSpiller<'_>,
    cancellation: &CancellationToken,
    context: &QueryContext,
    _temporary: &mut MemoryReservation,
) -> Result<Vec<usize>> {
    for (partition, indices) in prepared.indices.into_iter().enumerate() {
        if indices.is_empty() {
            continue;
        }
        let partition_batch = {
            let _permit = context
                .acquire_compute_until_cancelled(cancellation)
                .await?;
            let _active = context.scheduler.enter_lane();
            take_record_batch(&prepared.batch, &UInt32Array::from(indices))?
        };
        check_running(cancellation, context)?;
        spiller.write(partition, partition_batch)?;
    }
    Ok(prepared.counts)
}

fn check_running(cancellation: &CancellationToken, context: &QueryContext) -> Result<()> {
    if cancellation.is_cancelled() {
        Err(Error::Cancelled)
    } else {
        context.check_cancelled()
    }
}

fn minimum_memory_error(
    spiller: &PartitionSpiller<'_>,
    required: usize,
    copy_headroom: usize,
    detail: Option<String>,
) -> Result<Vec<usize>> {
    let detail = detail
        .map(|detail| format!(": {detail}"))
        .unwrap_or_default();
    Err(Error::ResourceExhausted(format!(
        "join spill cannot reserve the minimum one-row temporary state of {required} bytes while \
         preserving {copy_headroom} bytes of spill I/O copy headroom (used {}, limit {}){detail}",
        spiller.memory.used(),
        spiller.memory.limit(),
    )))
}
