use std::{collections::VecDeque, sync::Arc, time::Instant};

use futures::StreamExt;
use tokio::sync::{Mutex, mpsc};
use tokio_util::sync::CancellationToken;

use crate::{
    Result,
    datasource::{PredicateGuarantee, ScanRequest, ScanTask},
    runtime::{BatchEnvelope, MemoryBatchStream, QueryContext, boxed_memory_batch_stream},
};

use super::{
    FusedPipeline, PipelineOperator, compact, dictionary::GroupDictionaryPlan,
    profile::PipelineProfile,
};
use crate::execution::{expr, scan};

enum LaneMessage {
    Batch(BatchEnvelope),
    Done,
}

struct LanePlan {
    operators: Vec<PipelineOperator>,
    compact: Option<compact::CompactPlan>,
    projection: Option<Vec<usize>>,
    scan_schema: arrow::datatypes::SchemaRef,
    dictionary_outputs: Vec<usize>,
    profile: Arc<PipelineProfile>,
}

pub(super) fn execute(
    pipeline: FusedPipeline,
    dictionaries: GroupDictionaryPlan,
    decode_batch_size: Option<usize>,
    context: Arc<QueryContext>,
    parent_id: Option<u64>,
) -> MemoryBatchStream {
    boxed_memory_batch_stream(async_stream::try_stream! {
        let preserve_dictionaries = dictionaries.enabled();
        let profile = Arc::new(PipelineProfile::new(&context, &pipeline, parent_id));
        let mut request = ScanRequest::new(context.batch_size);
        request.projection = pipeline.scan.projection.clone();
        request.predicate = pipeline.scan.exact_filter.clone().or_else(|| {
            pipeline
                .scan
                .pushed_filter
                .as_ref()
                .and_then(scan::to_scan_predicate)
        });
        if pipeline.scan.exact_filter.is_some() {
            request.predicate_guarantee = PredicateGuarantee::Exact;
        }
        request.limit = pipeline.scan.limit;
        request.dictionary_columns = dictionaries.scan_columns;

        // Validate the fused remap before a provider allocates scan tasks or
        // starts source work.
        let compact = compact::plan(&pipeline.operators, pipeline.scan.projection.as_deref())?;
        if preserve_dictionaries && compact.is_none() {
            Err(crate::Error::Internal(
                "dictionary scan hint requires a compact terminal projection".into(),
            ))?;
        }
        if decode_batch_size.is_some()
            && pipeline.scan.exact_filter.is_none()
            && pipeline.scan.pushed_filter.is_none()
            && !pipeline
                .operators
                .iter()
                .any(|operator| matches!(operator, PipelineOperator::Filter(_)))
        {
            request.decode_batch_size = decode_batch_size;
        }

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
        let lane_plan = Arc::new(LanePlan {
            operators: pipeline.operators,
            compact,
            projection: pipeline.scan.projection,
            scan_schema: pipeline.scan.schema,
            dictionary_outputs: dictionaries.output_columns,
            profile,
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
            // Source polling may wait for object I/O. Fused kernels acquire an
            // engine-wide compute slot only after their memory workspace is ready.
            let scan_started = Instant::now();
            let next = input.next().await;
            let scan_wait = scan_started.elapsed();
            let Some(batch) = next else {
                break;
            };
            check_running(cancellation, context)?;
            let mut batch = batch?;
            plan.profile.record_scan(batch.batch(), scan_wait);
            let mut emit = true;
            if let Some(compact) = plan.compact.as_ref() {
                let filters = &compact.filters;
                for (operator, predicate) in filters.iter().enumerate() {
                    plan.profile.record_input(operator, batch.batch());
                    let started = Instant::now();
                    let workspace = context
                        .reserve_memory_while_holding(
                            expr::filter_workspace_bytes(predicate, batch.batch()),
                            batch.memory_size(),
                            "compact pipeline filter workspace",
                        )
                        .await?;
                    let filtered = run_compute(context, cancellation, || {
                        expr::filter(predicate, batch.batch())
                    })
                    .await?;
                    if filtered.num_rows() == 0 {
                        plan.profile
                            .record_output(operator, None, started.elapsed());
                        emit = false;
                        break;
                    }
                    batch = batch.replace_with_reservation(
                        filtered,
                        workspace,
                        "compact pipeline filter",
                    )?;
                    plan.profile
                        .record_output(operator, Some(batch.batch()), started.elapsed());
                }
                if emit && let Some(projection) = compact.projection.as_ref() {
                    let operator = filters.len();
                    plan.profile.record_input(operator, batch.batch());
                    let started = Instant::now();
                    let workspace = context
                        .reserve_memory_while_holding(
                            expr::projection_workspace_bytes(
                                &projection.expressions,
                                batch.batch(),
                            ),
                            batch.memory_size(),
                            "compact pipeline projection workspace",
                        )
                        .await?;
                    let projected = run_compute(context, cancellation, || {
                        if !plan.dictionary_outputs.is_empty() {
                            expr::project_preserving_dictionaries(
                                &projection.expressions,
                                Arc::clone(&projection.schema),
                                batch.batch(),
                                &plan.dictionary_outputs,
                            )
                        } else {
                            expr::project(
                                &projection.expressions,
                                Arc::clone(&projection.schema),
                                batch.batch(),
                            )
                        }
                    })
                    .await?;
                    batch = batch.replace_with_reservation(
                        projected,
                        workspace,
                        "compact pipeline projection",
                    )?;
                    plan.profile
                        .record_output(operator, Some(batch.batch()), started.elapsed());
                }
            }
            if !emit {
                continue;
            }
            if plan
                .compact
                .as_ref()
                .is_none_or(|compact| compact.projection.is_none())
                && let Some(projection) = plan
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
                let expanded = run_compute(context, cancellation, || {
                    scan::expand_projection(batch.batch().clone(), &plan.scan_schema, projection)
                })
                .await?;
                batch = batch.replace_with_reservation(
                    expanded,
                    workspace,
                    "scan projection expansion",
                )?;
            }
            for (operator_index, operator) in plan
                .operators
                .iter()
                .filter(|_| plan.compact.is_none())
                .enumerate()
            {
                plan.profile.record_input(operator_index, batch.batch());
                let started = Instant::now();
                match operator {
                    PipelineOperator::Filter(predicate) => {
                        let workspace = context
                            .reserve_memory_while_holding(
                                expr::filter_workspace_bytes(predicate, batch.batch()),
                                batch.memory_size(),
                                "pipeline filter workspace",
                            )
                            .await?;
                        let filtered = run_compute(context, cancellation, || {
                            expr::filter(predicate, batch.batch())
                        })
                        .await?;
                        if filtered.num_rows() == 0 {
                            plan.profile
                                .record_output(operator_index, None, started.elapsed());
                            emit = false;
                            break;
                        }
                        batch = batch.replace_with_reservation(
                            filtered,
                            workspace,
                            "pipeline filter",
                        )?;
                        plan.profile.record_output(
                            operator_index,
                            Some(batch.batch()),
                            started.elapsed(),
                        );
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
                        let projected = run_compute(context, cancellation, || {
                            expr::project(expressions, Arc::clone(schema), batch.batch())
                        })
                        .await?;
                        batch = batch.replace_with_reservation(
                            projected,
                            workspace,
                            "pipeline projection",
                        )?;
                        plan.profile.record_output(
                            operator_index,
                            Some(batch.batch()),
                            started.elapsed(),
                        );
                    }
                }
            }
            if emit {
                send_batch(sender, batch, cancellation, context).await?;
            }
        }
    }
}

async fn run_compute<T>(
    context: &QueryContext,
    cancellation: &CancellationToken,
    operation: impl FnOnce() -> Result<T>,
) -> Result<T> {
    let _permit = tokio::select! {
        _ = cancellation.cancelled() => return Err(crate::Error::Cancelled),
        permit = context.acquire_compute() => permit?,
    };
    check_running(cancellation, context)?;
    let _active = context.scheduler.enter_lane();
    operation()
}

async fn send_batch(
    sender: &mpsc::Sender<LaneMessage>,
    batch: BatchEnvelope,
    cancellation: &CancellationToken,
    context: &QueryContext,
) -> Result<()> {
    if cancellation.is_cancelled() {
        return Ok(());
    }
    context.check_cancelled()?;
    let started = Instant::now();
    let result = match sender.try_send(LaneMessage::Batch(batch)) {
        Ok(()) => Ok(()),
        Err(mpsc::error::TrySendError::Full(message)) => {
            let backpressure_started = Instant::now();
            let result = tokio::select! {
                _ = cancellation.cancelled() => Ok(()),
                _ = context.control.cancelled() => Err(crate::Error::Cancelled),
                result = sender.send(message) => result.map_err(|_| crate::Error::Cancelled),
            };
            context
                .metrics
                .record_scan_pipeline_output_queue_wait(backpressure_started.elapsed());
            result
        }
        Err(mpsc::error::TrySendError::Closed(_)) => Err(crate::Error::Cancelled),
    };
    context.scheduler.record_wait(started.elapsed());
    result
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
