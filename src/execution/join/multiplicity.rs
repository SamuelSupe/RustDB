use arrow::{datatypes::DataType, record_batch::RecordBatch};
use futures::StreamExt;
use tokio_util::sync::CancellationToken;

use crate::{
    Result,
    execution::aggregate::join_sink::JoinAggregateSink,
    runtime::{MemoryBatchStream, OperatorHandle, QueryContext},
    sql::{AggregateExpr, AggregateFunction, BoundExpr, ExprKind},
};

use super::{
    evaluate_keys_accounted, hash_table::CompositeMultiplicityTable, metrics::JoinPhaseMetrics,
};

pub(super) fn eligible(
    aggregates: &[AggregateExpr],
    left_width: usize,
    right_keys: &[BoundExpr],
) -> bool {
    super::build::multiplicity::supports(right_keys)
        && aggregates.iter().all(|aggregate| match aggregate.function {
            AggregateFunction::Count => !aggregate.distinct && aggregate.expr.is_none(),
            AggregateFunction::Sum if !aggregate.distinct => {
                aggregate.expr.as_ref().is_some_and(|expr| {
                    matches!(&expr.kind, ExprKind::Column(column) if *column < left_width)
                        && matches!(
                            expr.data_type,
                            DataType::Int8
                                | DataType::Int16
                                | DataType::Int32
                                | DataType::Int64
                                | DataType::UInt8
                                | DataType::UInt16
                                | DataType::UInt32
                                | DataType::UInt64
                                | DataType::Decimal128(_, _)
                        )
                })
            }
            _ => false,
        })
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn probe(
    left: &mut MemoryBatchStream,
    left_keys: &[BoundExpr],
    table: &CompositeMultiplicityTable,
    build_bytes: usize,
    sink: &mut JoinAggregateSink,
    operator: &OperatorHandle,
    context: &QueryContext,
    phases: &JoinPhaseMetrics,
) -> Result<()> {
    while let Some(batch) = left.next().await {
        context.check_cancelled()?;
        let batch = batch?;
        let rows = consume_batch(
            batch.batch(),
            batch.memory_size(),
            left_keys,
            table,
            build_bytes,
            sink,
            context,
            None,
            phases,
        )
        .await?;
        record(operator, context, rows);
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn consume_batch(
    batch: &RecordBatch,
    batch_bytes: usize,
    left_keys: &[BoundExpr],
    table: &CompositeMultiplicityTable,
    build_bytes: usize,
    sink: &mut JoinAggregateSink,
    context: &QueryContext,
    cancellation: Option<&CancellationToken>,
    phases: &JoinPhaseMetrics,
) -> Result<u64> {
    let keys = {
        let _permit = acquire_compute(context, cancellation).await?;
        let _active = context.scheduler.enter_lane();
        phases.measure_key_work(|| {
            evaluate_keys_accounted(left_keys, batch, context, "join multiplicity probe keys")
        })?
    };
    let estimate = table.probe_workspace_bytes(&keys)?;
    let held_bytes = build_bytes
        .saturating_add(batch_bytes)
        .saturating_add(keys.memory_size())
        .saturating_add(sink.memory_size());
    let workspace = reserve_probe_memory(context, estimate, held_bytes, cancellation).await?;
    let probe = {
        let _permit = acquire_compute(context, cancellation).await?;
        let _active = context.scheduler.enter_lane();
        phases.measure_key_work(|| table.encode_probe(&keys, workspace))?
    };
    let mut matched = 0u64;
    let chunk_size = context.batch_size.max(1);
    for start in (0..batch.num_rows()).step_by(chunk_size) {
        if cancellation.is_some_and(CancellationToken::is_cancelled) {
            return Err(crate::Error::Cancelled);
        }
        let end = start.saturating_add(chunk_size).min(batch.num_rows());
        let rows = {
            let _permit = acquire_compute(context, cancellation).await?;
            let _active = context.scheduler.enter_lane();
            sink.consume_probe_multiplicity_range(
                batch,
                start..end,
                |row| table.count(&probe, &keys, row),
                context,
            )?
        };
        matched = matched.checked_add(rows).ok_or_else(|| {
            crate::Error::ResourceExhausted("join candidate count overflowed UINT64".into())
        })?;
    }
    context.metrics.observe_memory(context.memory.used());
    Ok(matched)
}

async fn acquire_compute(
    context: &QueryContext,
    cancellation: Option<&CancellationToken>,
) -> Result<crate::runtime::GlobalComputePermit> {
    match cancellation {
        Some(cancellation) => context.acquire_compute_until_cancelled(cancellation).await,
        None => context.acquire_compute().await,
    }
}

async fn reserve_probe_memory(
    context: &QueryContext,
    bytes: usize,
    held_bytes: usize,
    cancellation: Option<&CancellationToken>,
) -> Result<crate::runtime::MemoryReservation> {
    let reserve = context.reserve_memory_while_holding(
        bytes,
        held_bytes,
        "join multiplicity probe row encoding",
    );
    match cancellation {
        Some(cancellation) => {
            if cancellation.is_cancelled() {
                return Err(crate::Error::Cancelled);
            }
            context.check_cancelled()?;
            context.memory.try_reserve(bytes).map_err(|error| {
                crate::Error::ResourceExhausted(format!(
                    "parallel join multiplicity probe cannot reserve {bytes} workspace bytes while retaining {held_bytes} bytes; reduce compute lanes or increase query memory: {error}"
                ))
            })
        }
        None => reserve.await,
    }
}

pub(super) fn record(operator: &OperatorHandle, context: &QueryContext, rows: u64) {
    if rows == 0 {
        return;
    }
    context.metrics.add_join_candidates(rows);
    operator.record_output(rows, 0);
}

#[cfg(test)]
#[path = "multiplicity/tests.rs"]
mod tests;
