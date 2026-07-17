use tokio_util::sync::CancellationToken;

use crate::{Error, Result, runtime::BatchEnvelope, runtime::QueryContext, sql::BoundExpr};

use super::{
    PartitionSpiller, PartitionTask, Repartitioned, Side, finish_repartition,
    repartition_partition_count, seed_for_depth, spill_batch_with_null_keys_scheduled,
};

#[allow(clippy::too_many_arguments)]
pub(in crate::execution::join) async fn repartition_scheduled(
    task: &PartitionTask,
    left_expressions: &[BoundExpr],
    right_expressions: &[BoundExpr],
    join_type: crate::sql::JoinType,
    null_equal_keys: bool,
    next_depth: usize,
    context: &QueryContext,
    cancellation: &CancellationToken,
) -> Result<Option<Repartitioned>> {
    let Some(partitions) = repartition_partition_count(context, task.build.estimated_bytes) else {
        return Ok(None);
    };
    let seed = seed_for_depth(next_depth);
    let mut repartition_bytes = 0u64;
    let mut left_spiller = PartitionSpiller::for_repartition(
        context,
        format!("join-left-r{next_depth}"),
        partitions,
        next_depth,
    );
    for file in &task.left {
        for batch in context.spill.read_file(file)? {
            check_running(cancellation, context)?;
            let batch =
                BatchEnvelope::try_new(batch?, &context.memory, "join repartition left batch")?;
            repartition_bytes = repartition_bytes.saturating_add(
                u64::try_from(batch.batch().get_array_memory_size()).unwrap_or(u64::MAX),
            );
            let (batch, _memory) = batch.into_parts();
            spill_batch_with_null_keys_scheduled(
                batch,
                left_expressions,
                Side::Left,
                join_type,
                null_equal_keys,
                &mut left_spiller,
                seed,
                cancellation,
                context,
            )
            .await?;
        }
    }
    let left = left_spiller.finish()?;

    let mut right_spiller = PartitionSpiller::for_repartition(
        context,
        format!("join-right-r{next_depth}"),
        partitions,
        next_depth,
    );
    for file in &task.right {
        for batch in context.spill.read_file(file)? {
            check_running(cancellation, context)?;
            let batch =
                BatchEnvelope::try_new(batch?, &context.memory, "join repartition right batch")?;
            repartition_bytes = repartition_bytes.saturating_add(
                u64::try_from(batch.batch().get_array_memory_size()).unwrap_or(u64::MAX),
            );
            let (batch, _memory) = batch.into_parts();
            spill_batch_with_null_keys_scheduled(
                batch,
                right_expressions,
                Side::Right,
                join_type,
                null_equal_keys,
                &mut right_spiller,
                seed,
                cancellation,
                context,
            )
            .await?;
        }
    }
    let right = right_spiller.finish_manifest()?;
    finish_repartition(left, right, repartition_bytes, next_depth, context)
}

fn check_running(cancellation: &CancellationToken, context: &QueryContext) -> Result<()> {
    if cancellation.is_cancelled() {
        Err(Error::Cancelled)
    } else {
        context.check_cancelled()
    }
}
