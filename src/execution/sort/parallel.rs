mod task;

use std::sync::Arc;

use futures::StreamExt;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::{
    Error, Result,
    runtime::{
        BatchEnvelope, MemoryBatchStream, MemoryPool, QueryContext, boxed_memory_batch_stream,
    },
    sql::SortExpr,
};

use super::{
    MERGE_FAN_IN, MemoryRun, MergeIterator, MergeRun, RunCleanup, available_workspace,
    compact_pending_runs, compact_runs, estimate_sort_bytes, make_converter, reserve_workspace,
    rows_within_limit, sort_batches, sort_state_limit, spill_run,
};
use task::{CancelOnDrop, drain_runs, receive_run, spawn_run};

const MIN_PARALLEL_MEMORY: usize = 64 << 20;

pub(super) fn is_supported(context: &QueryContext) -> bool {
    context.scheduler.configured_lanes() > 1 && context.memory.limit() >= MIN_PARALLEL_MEMORY
}

pub(super) fn sort(
    mut input: MemoryBatchStream,
    expressions: Vec<SortExpr>,
    fetch: Option<usize>,
    schema: arrow::datatypes::SchemaRef,
    context: Arc<QueryContext>,
    batch_size: usize,
) -> MemoryBatchStream {
    boxed_memory_batch_stream(async_stream::try_stream! {
        if expressions.is_empty() {
            Err(Error::InvalidArgument("ORDER BY requires at least one expression".into()))?;
        }
        if fetch == Some(0) {
            return;
        }

        let lanes = context.scheduler.configured_lanes();
        let batch_size = batch_size.max(1);
        let spill_headroom = context
            .spill
            .writer_headroom_bytes("sort-merge", schema.as_ref());
        let sort_pool = context.memory.child(
            format!("parallel-sort-{}", context.query_id),
            sort_state_limit(context.memory.limit()),
        );
        let cancellation = CancellationToken::new();
        let _cancel_on_drop = CancelOnDrop::new(cancellation.clone());
        let (sender, mut receiver) = mpsc::channel(lanes.max(1));
        let mut cleanup = RunCleanup::new(context.spill.clone());
        let mut memory_runs = Vec::<MemoryRun>::new();
        let mut spill_batch_rows = batch_size;
        let mut outstanding = 0usize;

        while let Some(batch) = input.next().await {
            context.check_cancelled()?;
            let batch = batch?;
            if batch.num_rows() == 0 {
                continue;
            }
            let estimate = estimate_sort_bytes(batch.batch(), expressions.len());
            let mut batch = Some(batch);
            loop {
                let mut workspace = sort_pool.reservation();
                let reserved = estimate <= sort_pool.limit()
                    && reserve_workspace(
                        &mut workspace,
                        estimate,
                        spill_headroom,
                        &context,
                    )
                    .is_ok();
                if reserved {
                    spawn_run(
                        batch.take().expect("parallel sort batch is available"),
                        workspace,
                        sender.clone(),
                        cancellation.clone(),
                        expressions.clone(),
                        fetch,
                        Arc::clone(&schema),
                        Arc::clone(&context),
                    )?;
                    outstanding += 1;
                    break;
                }

                if outstanding > 0 {
                    let generated = receive_run(&mut receiver, &cancellation, &context).await?;
                    outstanding -= 1;
                    memory_runs.push(generated);
                    continue;
                }

                if !memory_runs.is_empty() {
                    spill_memory_run(
                        memory_runs.remove(0),
                        &schema,
                        &context,
                        &sort_pool,
                        batch_size,
                        &mut cleanup,
                        &mut spill_batch_rows,
                    )?;
                    continue;
                }

                spill_sliced_batch(
                    batch.take().expect("parallel sort batch is available"),
                    &expressions,
                    fetch,
                    &schema,
                    &context,
                    &sort_pool,
                    spill_headroom,
                    batch_size,
                    &mut cleanup,
                    &mut spill_batch_rows,
                )?;
                break;
            }

            if outstanding >= lanes {
                let generated = receive_run(&mut receiver, &cancellation, &context).await?;
                outstanding -= 1;
                memory_runs.push(generated);
            }
        }
        drain_runs(
            &mut receiver,
            &cancellation,
            &context,
            &mut outstanding,
            &mut memory_runs,
        ).await?;
        // A panicking run task drops its sender before TaskGroup records the
        // panic. Retain the coordinator sender through the drain so the
        // cancellation branch returns that first failure instead of a generic
        // closed-channel error.
        drop(sender);
        // Keep a pure-memory sort pure while bounding its final fan-in. Small
        // sorted blocks are coalesced pairwise under workspace reservations;
        // if that cannot fit, the existing external-run path takes over.
        while cleanup.is_empty() && memory_runs.len() > MERGE_FAN_IN {
            let right = memory_runs.pop().expect("two memory runs are available");
            let left = memory_runs.pop().expect("two memory runs are available");
            match try_merge_memory_runs(
                left,
                right,
                &expressions,
                fetch,
                &schema,
                &context,
                &sort_pool,
                spill_headroom,
            )? {
                Ok(run) => memory_runs.push(run),
                Err((left, right)) => {
                    spill_memory_run(
                        left,
                        &schema,
                        &context,
                        &sort_pool,
                        batch_size,
                        &mut cleanup,
                        &mut spill_batch_rows,
                    )?;
                    memory_runs.push(right);
                }
            }
        }
        if !cleanup.is_empty() {
            while let Some(run) = memory_runs.pop() {
                spill_memory_run(
                    run,
                    &schema,
                    &context,
                    &sort_pool,
                    batch_size,
                    &mut cleanup,
                    &mut spill_batch_rows,
                )?;
            }
        }
        compact_pending_runs(
            &mut cleanup,
            &expressions,
            fetch,
            &schema,
            &context,
            &sort_pool,
            spill_batch_rows,
        )?;
        if cleanup.is_empty() && memory_runs.is_empty() {
            return;
        }

        let spill_runs = if cleanup.is_empty() {
            Vec::new()
        } else {
            compact_runs(
                cleanup.files(),
                &mut cleanup,
                &expressions,
                fetch,
                &schema,
                &context,
                &sort_pool,
                spill_batch_rows,
            )?
        };
        let mut runs = memory_runs
            .into_iter()
            .map(MergeRun::Memory)
            .collect::<Vec<_>>();
        runs.extend(spill_runs.into_iter().map(MergeRun::Spill));
        let mut merge = MergeIterator::new_mixed(
            runs,
            expressions,
            fetch,
            Arc::clone(&schema),
            Arc::clone(&context),
            context.memory.reservation(),
            batch_size,
        )?;
        while let Some(batch) = merge.next_envelope().await? {
            context.check_cancelled()?;
            yield batch;
        }
    })
}

#[allow(clippy::too_many_arguments)]
fn try_merge_memory_runs(
    left: MemoryRun,
    right: MemoryRun,
    expressions: &[SortExpr],
    fetch: Option<usize>,
    schema: &arrow::datatypes::SchemaRef,
    context: &Arc<QueryContext>,
    sort_pool: &MemoryPool,
    spill_headroom: usize,
) -> Result<std::result::Result<MemoryRun, (MemoryRun, MemoryRun)>> {
    let (left_batch, left_memory) = left.into_parts();
    let (right_batch, right_memory) = right.into_parts();
    let estimate = estimate_sort_bytes(&left_batch, expressions.len())
        .saturating_add(estimate_sort_bytes(&right_batch, expressions.len()));
    let mut workspace = sort_pool.reservation();
    if reserve_workspace(&mut workspace, estimate, spill_headroom, context).is_err() {
        return Ok(Err((
            MemoryRun::new(left_batch, left_memory),
            MemoryRun::new(right_batch, right_memory),
        )));
    }
    let converter = make_converter(expressions)?;
    let sorted = sort_batches(
        &[left_batch, right_batch],
        expressions,
        &converter,
        fetch,
        schema,
    )?;
    drop(left_memory);
    drop(right_memory);
    workspace.try_resize(sorted.get_array_memory_size())?;
    context.metrics.observe_memory(context.memory.used());
    Ok(Ok(MemoryRun::new(sorted, workspace)))
}

#[allow(clippy::too_many_arguments)]
fn spill_memory_run(
    run: MemoryRun,
    schema: &arrow::datatypes::SchemaRef,
    context: &Arc<QueryContext>,
    sort_pool: &MemoryPool,
    batch_size: usize,
    cleanup: &mut RunCleanup,
    spill_batch_rows: &mut usize,
) -> Result<()> {
    let (batch, memory) = run.into_parts();
    let (file, rows) =
        super::run::spill_sorted_run(&batch, schema, context, batch_size, sort_pool.limit())?;
    cleanup.add(file);
    *spill_batch_rows = (*spill_batch_rows).min(rows);
    drop(batch);
    drop(memory);
    // Other in-memory lane runs can still occupy the sort pool. Compact only
    // after they have all been released or spilled; otherwise the merge
    // cursor can fail despite the query having a valid external-sort path.
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn spill_sliced_batch(
    input: BatchEnvelope,
    expressions: &[SortExpr],
    fetch: Option<usize>,
    schema: &arrow::datatypes::SchemaRef,
    context: &Arc<QueryContext>,
    sort_pool: &MemoryPool,
    spill_headroom: usize,
    batch_size: usize,
    cleanup: &mut RunCleanup,
    spill_batch_rows: &mut usize,
) -> Result<()> {
    let converter = make_converter(expressions)?;
    let (batch, input_memory) = input.into_parts();
    let mut offset = 0usize;
    while offset < batch.num_rows() {
        context.check_cancelled()?;
        let rows_per_run = rows_within_limit(
            &batch,
            expressions.len(),
            available_workspace(sort_pool, spill_headroom, context),
        )?;
        let length = rows_per_run.min(batch.num_rows() - offset);
        let slice = batch.slice(offset, length);
        let estimate = super::estimate_sort_rows(&batch, expressions.len(), length);
        let mut workspace = sort_pool.reservation();
        reserve_workspace(&mut workspace, estimate, spill_headroom, context)?;
        let (file, rows) = spill_run(
            &[slice],
            expressions,
            &converter,
            fetch,
            schema,
            context,
            batch_size,
            sort_pool.limit(),
        )?;
        cleanup.add(file);
        *spill_batch_rows = (*spill_batch_rows).min(rows);
        drop(workspace);
        compact_pending_runs(
            cleanup,
            expressions,
            fetch,
            schema,
            context,
            sort_pool,
            *spill_batch_rows,
        )?;
        offset += length;
    }
    drop(input_memory);
    Ok(())
}
