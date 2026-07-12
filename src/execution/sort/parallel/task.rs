use std::{sync::Arc, time::Instant};

use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::{
    Error, Result,
    runtime::{BatchEnvelope, MemoryReservation, QueryContext},
    sql::SortExpr,
};

use super::super::{MemoryRun, make_converter, sort_batches};

#[allow(clippy::too_many_arguments)]
pub(super) fn spawn_run(
    input: BatchEnvelope,
    workspace: MemoryReservation,
    sender: mpsc::Sender<Result<MemoryRun>>,
    cancellation: CancellationToken,
    expressions: Vec<SortExpr>,
    fetch: Option<usize>,
    schema: arrow::datatypes::SchemaRef,
    context: Arc<QueryContext>,
) -> Result<()> {
    let tasks = context.tasks.clone();
    tasks.spawn("sort-run-lane", async move {
        let generated = {
            let _active = context.scheduler.enter_lane();
            context.check_cancelled()?;
            let converter = make_converter(&expressions)?;
            let (batch, input_memory) = input.into_parts();
            let sorted = sort_batches(&[batch], &expressions, &converter, fetch, &schema)?;
            drop(input_memory);
            let sorted_bytes = sorted.get_array_memory_size();
            let mut workspace = workspace;
            workspace.try_resize(sorted_bytes)?;
            context.metrics.observe_memory(context.memory.used());
            MemoryRun::new(sorted, workspace)
        };
        let started = Instant::now();
        let result = tokio::select! {
            _ = cancellation.cancelled() => Err(Error::Cancelled),
            _ = context.control.cancelled() => Err(context
                .check_cancelled()
                .expect_err("cancelled query has a terminal error")),
            result = sender.send(Ok(generated)) => result.map_err(|_| Error::Cancelled),
        };
        context.scheduler.record_wait(started.elapsed());
        result
    })
}

pub(super) async fn receive_run(
    receiver: &mut mpsc::Receiver<Result<MemoryRun>>,
    cancellation: &CancellationToken,
    context: &QueryContext,
) -> Result<MemoryRun> {
    let started = Instant::now();
    let result = tokio::select! {
        biased;
        _ = context.control.cancelled() => Err(context
            .check_cancelled()
            .expect_err("cancelled query has a terminal error")),
        _ = cancellation.cancelled() => Err(Error::Cancelled),
        generated = receiver.recv() => generated.unwrap_or_else(|| {
            Err(Error::Execution("parallel sort lane stopped before returning its run".into()))
        }),
    };
    context.scheduler.record_wait(started.elapsed());
    result
}

pub(super) async fn drain_runs(
    receiver: &mut mpsc::Receiver<Result<MemoryRun>>,
    cancellation: &CancellationToken,
    context: &QueryContext,
    outstanding: &mut usize,
    memory_runs: &mut Vec<MemoryRun>,
) -> Result<()> {
    while *outstanding > 0 {
        let generated = receive_run(receiver, cancellation, context).await?;
        *outstanding -= 1;
        memory_runs.push(generated);
    }
    Ok(())
}

pub(super) struct CancelOnDrop(CancellationToken);

impl CancelOnDrop {
    pub(super) fn new(cancellation: CancellationToken) -> Self {
        Self(cancellation)
    }
}

impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        self.0.cancel();
    }
}
