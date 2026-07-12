use std::{collections::VecDeque, sync::Arc, time::Instant};

use futures::StreamExt;
use tokio::sync::{Mutex, mpsc};
use tokio_util::sync::CancellationToken;

use crate::{
    Result,
    datasource::{ScanRequest, ScanTask},
    runtime::{BatchEnvelope, MemoryBatchStream, QueryContext, boxed_memory_batch_stream},
    sql::BoundExpr,
};

use super::{FusedPipeline, PipelineOperator, compact};
use crate::execution::{expr, scan};

enum LaneMessage {
    Batch(BatchEnvelope),
    Done,
}

struct LanePlan {
    operators: Vec<PipelineOperator>,
    compact_filters: Option<Vec<BoundExpr>>,
    projection: Option<Vec<usize>>,
    scan_schema: arrow::datatypes::SchemaRef,
}

pub(super) fn execute(pipeline: FusedPipeline, context: Arc<QueryContext>) -> MemoryBatchStream {
    boxed_memory_batch_stream(async_stream::try_stream! {
        let mut request = ScanRequest::new(context.batch_size);
        request.projection = pipeline.scan.projection.clone();
        request.predicate = pipeline
            .scan
            .pushed_filter
            .as_ref()
            .and_then(scan::to_scan_predicate);
        request.limit = pipeline.scan.limit;

        let target = context.scheduler.configured_lanes();
        let tasks = pipeline
            .scan
            .provider
            .scan_tasks(request, Arc::clone(&context), target)
            .await?;
        if tasks.is_empty() {
            return;
        }

        let lanes = context.scheduler.lanes_for(tasks.len());
        let compact_filters = compact_filters(&pipeline.operators, pipeline.scan.projection.as_deref())?;
        let lane_plan = Arc::new(LanePlan {
            operators: pipeline.operators,
            compact_filters,
            projection: pipeline.scan.projection,
            scan_schema: pipeline.scan.schema,
        });
        let pending = Arc::new(Mutex::new(VecDeque::from(tasks)));
        let cancellation = CancellationToken::new();
        let _cancel_on_drop = CancelOnDrop(cancellation.clone());
        // Every queued envelope owns a memory reservation, so the global
        // budget remains authoritative while one slot per lane avoids
        // serializing independent scan workers behind a single sender.
        let (sender, mut receiver) = mpsc::channel(lanes);

        for _ in 0..lanes {
            context.tasks.spawn("scan-pipeline-lane", run_lane(
                Arc::clone(&pending),
                sender.clone(),
                cancellation.clone(),
                Arc::clone(&lane_plan),
                Arc::clone(&context),
            ))?;
        }
        // Keep the coordinator sender alive until every lane reports Done.
        // A worker sender is dropped during panic unwinding before TaskGroup
        // records the panic; closing the channel here would race that record
        // and could expose the generic stopped error instead.

        let mut completed = 0;
        while completed < lanes {
            let message: Result<Option<LaneMessage>> = tokio::select! {
                biased;
                _ = context.control.cancelled() => Err(context
                    .check_cancelled()
                    .expect_err("cancelled query has a terminal error")),
                message = receiver.recv() => Ok(message),
            };
            let message = message?;
            match message {
                Some(LaneMessage::Batch(batch)) => yield batch,
                Some(LaneMessage::Done) => completed += 1,
                None => {
                    cancellation.cancel();
                    Err(crate::Error::Execution(format!(
                        "scan pipeline stopped after {completed} of {lanes} lanes completed"
                    )))?;
                }
            }
        }
        drop(sender);
    })
}

async fn run_lane(
    pending: Arc<Mutex<VecDeque<ScanTask>>>,
    sender: mpsc::Sender<LaneMessage>,
    cancellation: CancellationToken,
    plan: Arc<LanePlan>,
    context: Arc<QueryContext>,
) -> Result<()> {
    run_lane_inner(pending, &sender, &cancellation, &plan, &context).await?;
    sender
        .send(LaneMessage::Done)
        .await
        .map_err(|_| crate::Error::Cancelled)
}

async fn run_lane_inner(
    pending: Arc<Mutex<VecDeque<ScanTask>>>,
    sender: &mpsc::Sender<LaneMessage>,
    cancellation: &CancellationToken,
    plan: &LanePlan,
    context: &QueryContext,
) -> Result<()> {
    loop {
        check_running(cancellation, context)?;
        let Some(task) = pending.lock().await.pop_front() else {
            return Ok(());
        };
        tracing::trace!(scan_task = task.id(), "starting scan task");
        let mut input = task.into_stream();
        loop {
            // A lane waiting for downstream channel capacity is not active
            // compute work.  Hold the metric guard while polling/decoding and
            // running fused kernels, then release it before the bounded send.
            let active = context.scheduler.enter_lane();
            let next = input.next().await;
            let Some(batch) = next else {
                drop(active);
                break;
            };
            check_running(cancellation, context)?;
            let mut batch = batch?;
            let mut emit = true;
            if let Some(filters) = plan.compact_filters.as_deref() {
                for predicate in filters {
                    let workspace = context
                        .reserve_memory_while_holding(
                            expr::filter_workspace_bytes(predicate, batch.batch()),
                            batch.memory_size(),
                            "compact pipeline filter workspace",
                        )
                        .await?;
                    let filtered = expr::filter(predicate, batch.batch())?;
                    if filtered.num_rows() == 0 {
                        emit = false;
                        break;
                    }
                    batch = batch.replace_with_reservation(
                        filtered,
                        workspace,
                        "compact pipeline filter",
                    )?;
                }
            }
            if !emit {
                drop(active);
                continue;
            }
            if let Some(projection) = plan
                .projection
                .as_deref()
                .filter(|projection| !projection.is_empty())
            {
                let workspace = context
                    .reserve_memory_while_holding(
                        crate::runtime::estimate_schema_batch_bytes(
                            plan.scan_schema.as_ref(),
                            batch.num_rows(),
                        ),
                        batch.memory_size(),
                        "scan projection expansion workspace",
                    )
                    .await?;
                let expanded =
                    scan::expand_projection(batch.batch().clone(), &plan.scan_schema, projection)?;
                batch = batch.replace_with_reservation(
                    expanded,
                    workspace,
                    "scan projection expansion",
                )?;
            }
            for operator in plan
                .operators
                .iter()
                .filter(|_| plan.compact_filters.is_none())
            {
                match operator {
                    PipelineOperator::Filter(predicate) => {
                        let workspace = context
                            .reserve_memory_while_holding(
                                expr::filter_workspace_bytes(predicate, batch.batch()),
                                batch.memory_size(),
                                "pipeline filter workspace",
                            )
                            .await?;
                        let filtered = expr::filter(predicate, batch.batch())?;
                        if filtered.num_rows() == 0 {
                            emit = false;
                            break;
                        }
                        batch = batch.replace_with_reservation(
                            filtered,
                            workspace,
                            "pipeline filter",
                        )?;
                    }
                    PipelineOperator::Projection {
                        expressions,
                        schema,
                    } => {
                        let workspace = context
                            .reserve_memory_while_holding(
                                expr::projection_workspace_bytes(expressions, batch.batch()),
                                batch.memory_size(),
                                "pipeline projection workspace",
                            )
                            .await?;
                        let projected =
                            expr::project(expressions, Arc::clone(schema), batch.batch())?;
                        batch = batch.replace_with_reservation(
                            projected,
                            workspace,
                            "pipeline projection",
                        )?;
                    }
                }
            }
            drop(active);
            if emit {
                send_batch(sender, batch, cancellation, context).await?;
            }
        }
    }
}

fn compact_filters(
    operators: &[PipelineOperator],
    projection: Option<&[usize]>,
) -> Result<Option<Vec<BoundExpr>>> {
    let Some(projection) = projection.filter(|projection| !projection.is_empty()) else {
        return Ok(None);
    };
    if !operators
        .iter()
        .all(|operator| matches!(operator, PipelineOperator::Filter(_)))
    {
        return Ok(None);
    }
    operators
        .iter()
        .map(|operator| {
            let PipelineOperator::Filter(predicate) = operator else {
                unreachable!("all operators were checked as filters")
            };
            compact::remap_filter(predicate, projection)
        })
        .collect::<Result<Vec<_>>>()
        .map(Some)
}

async fn send_batch(
    sender: &mpsc::Sender<LaneMessage>,
    batch: BatchEnvelope,
    cancellation: &CancellationToken,
    context: &QueryContext,
) -> Result<()> {
    let started = Instant::now();
    let result = tokio::select! {
        _ = cancellation.cancelled() => return Ok(()),
        _ = context.control.cancelled() => return Err(crate::Error::Cancelled),
        result = sender.send(LaneMessage::Batch(batch)) => result,
    };
    context.scheduler.record_wait(started.elapsed());
    result.map_err(|_| crate::Error::Cancelled)
}

fn check_running(cancellation: &CancellationToken, context: &QueryContext) -> Result<()> {
    if cancellation.is_cancelled() {
        Err(crate::Error::Cancelled)
    } else {
        context.check_cancelled()
    }
}

struct CancelOnDrop(CancellationToken);

impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        self.0.cancel();
    }
}
