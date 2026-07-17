use std::{sync::Arc, time::Instant};

use arrow::datatypes::SchemaRef;
use futures::StreamExt;
use tokio::sync::{Mutex, mpsc};
use tokio_util::sync::CancellationToken;

use crate::{
    Error, Result,
    execution::aggregate::join_sink::{JoinAggregateSink, SelectionTarget},
    runtime::{MemoryBatchStream, OperatorHandle, QueryContext, boxed_memory_batch_stream},
    sql::{AggregateExpr, BoundExpr, JoinType},
};

use super::{
    FrozenBuild, ProbeCursor, check_running, evaluate_keys_accounted, evaluate_optional_values,
    optional_array, optional_memory,
};
use crate::execution::join::{condition::JoinPredicates, output::JoinEmission};

struct LaneDone {
    lane: usize,
    sink: JoinAggregateSink,
}

#[allow(clippy::too_many_arguments)]
pub(in crate::execution::join) fn probe_global_aggregate(
    left: MemoryBatchStream,
    left_keys: Vec<BoundExpr>,
    build: FrozenBuild,
    predicates: JoinPredicates,
    join_schema: SchemaRef,
    aggregates: Vec<AggregateExpr>,
    aggregate_schema: SchemaRef,
    join_operator: OperatorHandle,
    context: Arc<QueryContext>,
    batch_size: usize,
) -> MemoryBatchStream {
    boxed_memory_batch_stream(async_stream::try_stream! {
        validate(&build, &predicates)?;
        let lanes = context.scheduler.configured_lanes();
        let input = Arc::new(Mutex::new(left));
        let build = Arc::new(build);
        let cancellation = CancellationToken::new();
        let _cancel_on_drop = super::CancelOnDrop(cancellation.clone());
        let (sender, mut receiver) = mpsc::channel(lanes.max(1));

        for lane in 0..lanes {
            context.tasks.spawn("hash-join-aggregate-probe-lane", run_lane(
                lane,
                Arc::clone(&input),
                sender.clone(),
                cancellation.clone(),
                left_keys.clone(),
                Arc::clone(&build),
                predicates.clone(),
                Arc::clone(&join_schema),
                aggregates.clone(),
                Arc::clone(&aggregate_schema),
                join_operator.clone(),
                Arc::clone(&context),
                batch_size.max(1),
            ))?;
        }
        drop(input);

        let mut partials = Vec::with_capacity(lanes);
        while partials.len() < lanes {
            let message = tokio::select! {
                biased;
                _ = context.control.cancelled() => Err(context
                    .check_cancelled()
                    .expect_err("cancelled query has a terminal error")),
                message = receiver.recv() => message.ok_or_else(|| Error::Execution(format!(
                    "parallel join aggregate stopped after {} of {lanes} lanes completed",
                    partials.len(),
                ))),
            }?;
            partials.push(message);
        }
        // Output reservation must not wait while the coordinator still owns
        // the immutable build-side reservation.
        drop(build);
        partials.sort_unstable_by_key(|partial| partial.lane);
        let mut partials = partials.into_iter();
        let mut merged = partials
            .next()
            .expect("parallel join aggregate always starts at least one lane")
            .sink;
        for partial in partials {
            merged.merge(partial.sink)?;
        }
        yield merged.finish(&context).await?;
        drop(sender);
    })
}

fn validate(build: &FrozenBuild, predicates: &JoinPredicates) -> Result<()> {
    if !predicates.is_simple_equality()
        || build.global_membership.is_some()
        || build.null_equal_keys
        || build.matched_build.is_some()
    {
        return Err(Error::Internal(
            "unsupported join reached the parallel global aggregate sink".into(),
        ));
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn run_lane(
    lane: usize,
    input: Arc<Mutex<MemoryBatchStream>>,
    sender: mpsc::Sender<LaneDone>,
    cancellation: CancellationToken,
    left_key_expressions: Vec<BoundExpr>,
    build: Arc<FrozenBuild>,
    predicates: JoinPredicates,
    join_schema: SchemaRef,
    aggregates: Vec<AggregateExpr>,
    aggregate_schema: SchemaRef,
    join_operator: OperatorHandle,
    context: Arc<QueryContext>,
    batch_size: usize,
) -> Result<()> {
    let sink = run_lane_inner(
        input,
        &cancellation,
        &left_key_expressions,
        &build,
        &predicates,
        &join_schema,
        aggregates,
        aggregate_schema,
        &join_operator,
        &context,
        batch_size,
    )
    .await?;
    drop(build);
    send_done(&sender, LaneDone { lane, sink }, &cancellation, &context).await
}

#[allow(clippy::too_many_arguments)]
async fn run_lane_inner(
    input: Arc<Mutex<MemoryBatchStream>>,
    cancellation: &CancellationToken,
    left_key_expressions: &[BoundExpr],
    build: &FrozenBuild,
    predicates: &JoinPredicates,
    join_schema: &SchemaRef,
    aggregates: Vec<AggregateExpr>,
    aggregate_schema: SchemaRef,
    join_operator: &OperatorHandle,
    context: &QueryContext,
    batch_size: usize,
) -> Result<JoinAggregateSink> {
    let mut sink = JoinAggregateSink::try_new(aggregates, aggregate_schema, join_schema, context)?;
    loop {
        check_running(cancellation, context)?;
        let next = {
            let mut input = input.lock().await;
            tokio::select! {
                _ = cancellation.cancelled() => return Err(Error::Cancelled),
                _ = context.control.cancelled() => return Err(Error::Cancelled),
                next = input.next() => next,
            }
        };
        let Some(left_batch) = next else {
            return Ok(sink);
        };
        let left_batch = left_batch?;
        let (left_values, left_keys, fixed_rows) = {
            let _permit = context
                .acquire_compute_until_cancelled(cancellation)
                .await?;
            let _active = context.scheduler.enter_lane();
            let left_values = evaluate_optional_values(
                predicates.left_value(),
                left_batch.batch(),
                context,
                "parallel join aggregate membership value",
            )?;
            let left_keys = evaluate_keys_accounted(
                left_key_expressions,
                left_batch.batch(),
                context,
                "parallel join aggregate probe keys",
            )?;
            let rows = build
                .hash_table
                .fixed_probe(&left_keys)?
                .map(|probe| {
                    sink.consume_fixed_matches(
                        left_batch.batch(),
                        &build.batch,
                        |row| probe.lookup(row),
                        context,
                    )
                })
                .transpose()?;
            (left_values, left_keys, rows)
        };
        if let Some(rows) = fixed_rows {
            record_direct(context, join_operator, rows);
            continue;
        }
        let mut cursor = ProbeCursor::new(
            left_batch.batch(),
            &build.batch,
            &left_keys,
            &build.hash_table,
            predicates,
            optional_array(&left_values),
            optional_array(&build.right_values),
            None,
            false,
            None,
            JoinType::Inner,
            Arc::clone(join_schema),
            batch_size,
            build
                .memory_size()
                .saturating_add(left_batch.memory_size())
                .saturating_add(left_keys.memory_size())
                .saturating_add(optional_memory(&left_values)),
        );
        let mut target = SelectionTarget::new(&mut sink);
        loop {
            match cursor.next_output(&mut target, context).await? {
                JoinEmission::Consumed { rows } => {
                    join_operator.record_output(u64::try_from(rows).unwrap_or(u64::MAX), 0)
                }
                JoinEmission::Exhausted => break,
                JoinEmission::Batch(_) => {
                    return Err(Error::Internal(
                        "selection-aware join aggregate materialized an output batch".into(),
                    ));
                }
            }
        }
    }
}

fn record_direct(context: &QueryContext, operator: &OperatorHandle, rows: usize) {
    if rows == 0 {
        return;
    }
    let rows = u64::try_from(rows).unwrap_or(u64::MAX);
    context.metrics.add_join_candidates(rows);
    operator.record_output(rows, 0);
}

async fn send_done(
    sender: &mpsc::Sender<LaneDone>,
    message: LaneDone,
    cancellation: &CancellationToken,
    context: &QueryContext,
) -> Result<()> {
    let started = Instant::now();
    let result = tokio::select! {
        _ = cancellation.cancelled() => return Err(Error::Cancelled),
        _ = context.control.cancelled() => return Err(Error::Cancelled),
        result = sender.send(message) => result,
    };
    context.scheduler.record_wait(started.elapsed());
    result.map_err(|_| Error::Cancelled)
}

#[cfg(test)]
#[path = "aggregate/tests.rs"]
mod tests;
