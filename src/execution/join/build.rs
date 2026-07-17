use std::sync::Arc;

use arrow::{compute::concat_batches, datatypes::SchemaRef, record_batch::RecordBatch};
use futures::StreamExt;

use crate::{
    Result,
    runtime::{MemoryBatchStream, MemoryReservation, QueryContext},
    sql::{BoundExpr, JoinType},
};

use super::{
    EvaluatedKeys, GlobalMembershipState, JoinHashTable, condition::JoinPredicates, spill,
};

mod hash;
pub(super) mod multiplicity;
mod spilling;

pub(super) enum BuildOutcome {
    InMemory(Box<InMemoryBuild>),
    Spilled(spill::PartitionManifest),
}

pub(super) struct InMemoryBuild {
    batch: RecordBatch,
    hash_table: JoinHashTable,
    right_values: Option<EvaluatedKeys>,
    global_membership: Option<GlobalMembershipState>,
    matched_build: Option<super::BuildMatchTracker>,
}

impl InMemoryBuild {
    pub(super) fn hash_table(&self) -> &JoinHashTable {
        &self.hash_table
    }

    pub(super) fn into_parts(
        self,
    ) -> (
        RecordBatch,
        JoinHashTable,
        Option<EvaluatedKeys>,
        Option<GlobalMembershipState>,
        Option<super::BuildMatchTracker>,
    ) {
        (
            self.batch,
            self.hash_table,
            self.right_values,
            self.global_membership,
            self.matched_build,
        )
    }
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn build(
    right: &mut MemoryBatchStream,
    right_key_expressions: &[BoundExpr],
    predicates: &JoinPredicates,
    use_global_membership_hash: bool,
    left_width: usize,
    right_schema: &SchemaRef,
    join_type: JoinType,
    null_equal_keys: bool,
    context: &QueryContext,
    reservation: &mut MemoryReservation,
) -> Result<BuildOutcome> {
    let mut right_batches = Vec::new();
    let mut right_bytes = 0usize;
    let mut right_rows = 0usize;
    let build_buffer_limit = context.memory.limit().checked_div(4).unwrap_or(0).max(1);

    while let Some(batch) = right.next().await {
        context.check_cancelled()?;
        let batch = batch?;
        let bytes = batch.memory_size();
        let projected_bytes = right_bytes.saturating_add(bytes);
        let projected_rows = right_rows.saturating_add(batch.batch().num_rows());
        if projected_bytes > build_buffer_limit || reservation.try_grow(bytes).is_err() {
            reservation.shrink(right_bytes);
            return spilling::streaming(
                right,
                right_batches,
                batch,
                projected_bytes,
                projected_rows,
                right_key_expressions,
                join_type,
                null_equal_keys,
                context,
                reservation,
            )
            .await;
        }
        let (batch, batch_memory) = batch.into_parts();
        reservation.absorb(batch_memory)?;
        right_bytes = right_bytes.saturating_add(bytes);
        right_rows = projected_rows;
        right_batches.push(batch);
    }

    let right_batch = if right_batches.is_empty() {
        RecordBatch::new_empty(Arc::clone(right_schema))
    } else {
        let _permit = context.acquire_compute().await?;
        let _active = context.scheduler.enter_lane();
        concat_batches(right_schema, &right_batches)?
    };
    drop(right_batches);
    reservation.shrink(right_bytes);
    hash::finish(
        right_batch,
        right_key_expressions,
        predicates,
        use_global_membership_hash,
        left_width,
        join_type,
        null_equal_keys,
        context,
        reservation,
    )
    .await
}
