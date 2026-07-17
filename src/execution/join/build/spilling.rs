use arrow::record_batch::RecordBatch;
use futures::StreamExt;

use crate::{
    Result,
    runtime::{BatchEnvelope, MemoryBatchStream, MemoryReservation, QueryContext},
    sql::{BoundExpr, JoinType},
};

use super::super::spill::{self, Side};
use super::BuildOutcome;

// A streaming build starts spilling after only a bounded prefix is buffered.
// Reserve growth for the unseen suffix so common large scans do not begin
// with a tiny fanout and immediately rewrite the complete build side.
const STREAMING_BUILD_GROWTH_RESERVE: u64 = 4;

#[allow(clippy::too_many_arguments)]
pub(super) async fn streaming(
    right: &mut MemoryBatchStream,
    mut buffered: Vec<RecordBatch>,
    batch: BatchEnvelope,
    projected_bytes: usize,
    projected_rows: usize,
    keys: &[BoundExpr],
    join_type: JoinType,
    null_equal_keys: bool,
    context: &QueryContext,
    reservation: &mut MemoryReservation,
) -> Result<BuildOutcome> {
    let footprint = spill::estimated_build_footprint(
        u64::try_from(projected_bytes).unwrap_or(u64::MAX),
        u64::try_from(projected_rows).unwrap_or(u64::MAX),
        keys.len(),
    )
    .saturating_mul(STREAMING_BUILD_GROWTH_RESERVE);
    let partitions =
        spill::adaptive_partition_count(context, usize::try_from(footprint).unwrap_or(usize::MAX));
    let mut spiller = spill::PartitionSpiller::with_partitions(context, "join-right", partitions);
    for batch in buffered.drain(..) {
        let bytes = batch.get_array_memory_size();
        spill::spill_batch_with_null_keys(
            batch,
            keys,
            Side::Right,
            join_type,
            null_equal_keys,
            &mut spiller,
            0,
        )?;
        reservation.shrink(bytes);
    }
    envelope(batch, keys, join_type, null_equal_keys, &mut spiller)?;
    while let Some(batch) = right.next().await {
        envelope(batch?, keys, join_type, null_equal_keys, &mut spiller)?;
    }
    spiller.finish_manifest().map(BuildOutcome::Spilled)
}

fn envelope(
    batch: BatchEnvelope,
    keys: &[BoundExpr],
    join_type: JoinType,
    null_equal_keys: bool,
    spiller: &mut spill::PartitionSpiller<'_>,
) -> Result<()> {
    let (batch, memory) = batch.into_parts();
    spill::spill_batch_with_null_keys(
        batch,
        keys,
        Side::Right,
        join_type,
        null_equal_keys,
        spiller,
        0,
    )?;
    drop(memory);
    Ok(())
}

pub(super) fn batch(
    batch: RecordBatch,
    keys: &[BoundExpr],
    join_type: JoinType,
    null_equal_keys: bool,
    context: &QueryContext,
    reservation: &mut MemoryReservation,
) -> Result<spill::PartitionManifest> {
    reservation.try_resize(batch.get_array_memory_size())?;
    let footprint = spill::estimated_build_footprint(
        u64::try_from(spill::batch_logical_buffer_bytes(&batch)).unwrap_or(u64::MAX),
        u64::try_from(batch.num_rows()).unwrap_or(u64::MAX),
        keys.len(),
    );
    let partitions =
        spill::adaptive_partition_count(context, usize::try_from(footprint).unwrap_or(usize::MAX));
    let mut spiller = spill::PartitionSpiller::with_partitions(context, "join-right", partitions);
    spill::spill_batch_with_null_keys(
        batch,
        keys,
        Side::Right,
        join_type,
        null_equal_keys,
        &mut spiller,
        0,
    )?;
    reservation.try_resize(0)?;
    spiller.finish_manifest()
}
