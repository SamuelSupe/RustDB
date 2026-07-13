use std::time::Duration;

use arrow::record_batch::RecordBatch;

use crate::runtime::{OperatorHandle, QueryContext};

use super::{FusedPipeline, PipelineOperator};

pub(super) struct PipelineProfile {
    scan: OperatorHandle,
    operators: Vec<OperatorHandle>,
}

impl PipelineProfile {
    pub(super) fn new(
        context: &QueryContext,
        pipeline: &FusedPipeline,
        parent_id: Option<u64>,
    ) -> Self {
        let mut parent = parent_id;
        let mut operators = pipeline
            .operators
            .iter()
            .rev()
            .map(|operator| {
                let handle = context.metrics.register_operator(name(operator), parent);
                parent = Some(handle.id());
                handle
            })
            .collect::<Vec<_>>();
        operators.reverse();
        let scan = context.metrics.register_operator("Scan", parent);
        Self { scan, operators }
    }

    pub(super) fn record_scan(&self, batch: &RecordBatch, wait: Duration) {
        let rows = rows(batch);
        self.scan.record_input(rows);
        self.scan.record_wait(wait);
        self.scan.record_elapsed(wait);
        self.scan.record_output(rows, bytes(batch));
    }

    pub(super) fn record_input(&self, operator: usize, batch: &RecordBatch) {
        self.operators[operator].record_input(rows(batch));
    }

    pub(super) fn record_output(
        &self,
        operator: usize,
        batch: Option<&RecordBatch>,
        elapsed: Duration,
    ) {
        let handle = &self.operators[operator];
        handle.record_elapsed(elapsed);
        if let Some(batch) = batch {
            handle.record_output(rows(batch), bytes(batch));
        }
    }
}

fn name(operator: &PipelineOperator) -> &'static str {
    match operator {
        PipelineOperator::Filter(_) => "Filter",
        PipelineOperator::Projection { .. } => "Projection",
    }
}

fn rows(batch: &RecordBatch) -> u64 {
    u64::try_from(batch.num_rows()).unwrap_or(u64::MAX)
}

fn bytes(batch: &RecordBatch) -> u64 {
    u64::try_from(batch.get_array_memory_size()).unwrap_or(u64::MAX)
}
