use std::{sync::Arc, time::Instant};

use arrow::datatypes::SchemaRef;
use tokio::sync::{Mutex, mpsc};
use tokio_util::sync::CancellationToken;

use crate::{
    Error, Result,
    execution::aggregate::join_sink::JoinAggregateSink,
    runtime::{
        MemoryBatchStream, MemoryReservation, OperatorHandle, QueryContext,
        boxed_memory_batch_stream,
    },
    sql::{AggregateExpr, BoundExpr},
};

use super::{CancelOnDrop, check_running, morsel::MorselInput};
use crate::execution::join::{
    hash_table::CompositeMultiplicityTable,
    metrics::JoinPhaseMetrics,
    multiplicity::{consume_batch, record},
};

struct FrozenBuild {
    table: CompositeMultiplicityTable,
    memory: MemoryReservation,
}

impl FrozenBuild {
    fn memory_size(&self) -> usize {
        self.memory.size()
    }
}

struct LaneDone {
    lane: usize,
    sink: JoinAggregateSink,
}

#[allow(clippy::too_many_arguments)]
pub(in crate::execution::join) fn probe_global_multiplicity(
    left: MemoryBatchStream,
    left_keys: Vec<BoundExpr>,
    table: CompositeMultiplicityTable,
    memory: MemoryReservation,
    join_schema: SchemaRef,
    aggregates: Vec<AggregateExpr>,
    aggregate_schema: SchemaRef,
    operator: OperatorHandle,
    context: Arc<QueryContext>,
    phases: JoinPhaseMetrics,
) -> MemoryBatchStream {
    boxed_memory_batch_stream(async_stream::try_stream! {
        let lanes = context.scheduler.configured_lanes();
        let input = Arc::new(Mutex::new(MorselInput::new(left)));
        let build = Arc::new(FrozenBuild { table, memory });
        let cancellation = CancellationToken::new();
        let _cancel_on_drop = CancelOnDrop(cancellation.clone());
        let (sender, mut receiver) = mpsc::channel(lanes.max(1));

        for lane in 0..lanes {
            context.tasks.spawn("join-multiplicity-probe-lane", run_lane(
                lane,
                Arc::clone(&input),
                sender.clone(),
                cancellation.clone(),
                left_keys.clone(),
                Arc::clone(&build),
                Arc::clone(&join_schema),
                aggregates.clone(),
                Arc::clone(&aggregate_schema),
                operator.clone(),
                Arc::clone(&context),
                phases.clone(),
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
                    "parallel join multiplicity stopped after {} of {lanes} lanes completed",
                    partials.len(),
                ))),
            }?;
            partials.push(message);
        }
        drop(build);
        partials.sort_unstable_by_key(|partial| partial.lane);
        let mut partials = partials.into_iter();
        let mut merged = partials
            .next()
            .expect("parallel join multiplicity always starts at least one lane")
            .sink;
        for partial in partials {
            merged.merge(partial.sink)?;
        }
        yield merged.finish(&context).await?;
        drop(sender);
    })
}

#[allow(clippy::too_many_arguments)]
async fn run_lane(
    lane: usize,
    input: Arc<Mutex<MorselInput>>,
    sender: mpsc::Sender<LaneDone>,
    cancellation: CancellationToken,
    left_keys: Vec<BoundExpr>,
    build: Arc<FrozenBuild>,
    join_schema: SchemaRef,
    aggregates: Vec<AggregateExpr>,
    aggregate_schema: SchemaRef,
    operator: OperatorHandle,
    context: Arc<QueryContext>,
    phases: JoinPhaseMetrics,
) -> Result<()> {
    let sink = run_lane_inner(
        input,
        &cancellation,
        &left_keys,
        &build,
        &join_schema,
        aggregates,
        aggregate_schema,
        &operator,
        &context,
        &phases,
    )
    .await?;
    drop(build);
    send_done(&sender, LaneDone { lane, sink }, &cancellation, &context).await
}

#[allow(clippy::too_many_arguments)]
async fn run_lane_inner(
    input: Arc<Mutex<MorselInput>>,
    cancellation: &CancellationToken,
    left_keys: &[BoundExpr],
    build: &FrozenBuild,
    join_schema: &SchemaRef,
    aggregates: Vec<AggregateExpr>,
    aggregate_schema: SchemaRef,
    operator: &OperatorHandle,
    context: &QueryContext,
    phases: &JoinPhaseMetrics,
) -> Result<JoinAggregateSink> {
    let mut sink = JoinAggregateSink::try_new(aggregates, aggregate_schema, join_schema, context)?;
    loop {
        check_running(cancellation, context)?;
        let next = {
            let mut input = input.lock().await;
            input
                .next(context.batch_size.max(1), cancellation, context)
                .await?
        };
        let Some(morsel) = next else {
            return Ok(sink);
        };
        if context.scheduler.configured_lanes() > 1 {
            // Ready mutex/semaphore futures do not necessarily yield. Give
            // sibling lanes a chance to claim other slices of the same source
            // batch before this lane starts synchronous hash work.
            tokio::task::yield_now().await;
        }
        let rows = consume_batch(
            morsel.batch(),
            morsel.retained_bytes(),
            left_keys,
            &build.table,
            build.memory_size(),
            &mut sink,
            context,
            Some(cancellation),
            phases,
        )
        .await?;
        record(operator, context, rows);
    }
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
