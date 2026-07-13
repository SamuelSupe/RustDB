use std::{mem::size_of, sync::Arc};

use arrow::{array::ArrayRef, datatypes::SchemaRef, record_batch::RecordBatch};

use crate::{
    Result,
    runtime::{MemoryReservation, QueryContext, SpillFile},
};

use super::BuildPartitionStats;

const COMPACTION_FAN_IN: usize = 64;
const IPC_BLOCK_METADATA_BYTES: usize = 64;
const CONCAT_ARRAY_METADATA_BYTES: usize = 256;

pub(in crate::execution::join) enum BuildPartition {
    Loaded(RecordBatch),
    TooLarge { rows: usize },
}

pub(in crate::execution::join) fn load_build_partition(
    files: &[SpillFile],
    schema: &SchemaRef,
    context: &QueryContext,
    reservation: &mut MemoryReservation,
    stats: BuildPartitionStats,
) -> Result<BuildPartition> {
    // Manifest statistics replace the former measurement read. Reserve the
    // retained buffers, one concat output, and IPC block metadata up front.
    let required = compaction_reservation_bytes(
        stats.data_bytes,
        stats.batches,
        stats.max_batch_bytes,
        files.len(),
        schema.fields().len(),
    );
    if reservation.try_resize(required).is_err() {
        return Ok(BuildPartition::TooLarge { rows: stats.rows });
    }

    let mut batches = Vec::with_capacity(files.len());
    for file in files {
        if let Some(batch) = compact_spill_file(file, schema, context)? {
            batches.push(batch);
        }
    }
    let batch = compact_batches(batches, schema)?
        .unwrap_or_else(|| RecordBatch::new_empty(Arc::clone(schema)));
    if reservation
        .try_resize(batch.get_array_memory_size())
        .is_err()
    {
        drop(batch);
        reservation.try_resize(0)?;
        return Ok(BuildPartition::TooLarge { rows: stats.rows });
    }
    Ok(BuildPartition::Loaded(batch))
}

fn compaction_reservation_bytes(
    data_bytes: usize,
    batches: usize,
    max_batch_bytes: usize,
    files: usize,
    columns: usize,
) -> usize {
    let retained_metadata = size_of::<RecordBatch>()
        .saturating_add(
            columns
                .saturating_mul(size_of::<ArrayRef>().saturating_add(CONCAT_ARRAY_METADATA_BYTES)),
        )
        .saturating_mul(files.max(1));
    data_bytes
        .saturating_mul(2)
        .saturating_add(max_batch_bytes)
        .saturating_add(batches.saturating_mul(IPC_BLOCK_METADATA_BYTES))
        .saturating_add(retained_metadata)
}

fn compact_spill_file(
    file: &SpillFile,
    schema: &SchemaRef,
    context: &QueryContext,
) -> Result<Option<RecordBatch>> {
    let mut pending = Vec::with_capacity(COMPACTION_FAN_IN);
    let mut compacted = Vec::new();
    for batch in context.spill.read_file(file)? {
        pending.push(batch?);
        if pending.len() == COMPACTION_FAN_IN {
            compacted.push(
                compact_batches(std::mem::take(&mut pending), schema)?
                    .expect("a full compaction group is not empty"),
            );
            pending = Vec::with_capacity(COMPACTION_FAN_IN);
        }
    }
    if !pending.is_empty() {
        compacted.push(
            compact_batches(pending, schema)?.expect("a pending compaction group is not empty"),
        );
    }
    compact_batches(compacted, schema)
}

fn compact_batches(
    mut batches: Vec<RecordBatch>,
    schema: &SchemaRef,
) -> Result<Option<RecordBatch>> {
    if batches.is_empty() {
        return Ok(None);
    }
    while batches.len() > 1 {
        let mut next = Vec::with_capacity(batches.len().div_ceil(COMPACTION_FAN_IN));
        for group in batches.chunks(COMPACTION_FAN_IN) {
            next.push(if group.len() == 1 {
                group[0].clone()
            } else {
                arrow::compute::concat_batches(schema, group)?
            });
        }
        batches = next;
    }
    Ok(batches.pop())
}
