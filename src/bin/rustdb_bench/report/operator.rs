use rustdb::OperatorMetricsSnapshot;
use serde::Serialize;

#[derive(Debug, Serialize)]
pub(crate) struct OperatorReport {
    id: u64,
    parent_id: Option<u64>,
    name: String,
    input_rows: u64,
    input_batches: u64,
    output_rows: u64,
    output_batches: u64,
    output_bytes: u64,
    elapsed_ms: f64,
    wait_ms: f64,
}

impl From<&OperatorMetricsSnapshot> for OperatorReport {
    fn from(metrics: &OperatorMetricsSnapshot) -> Self {
        Self {
            id: metrics.id,
            parent_id: metrics.parent_id,
            name: metrics.name.clone(),
            input_rows: metrics.input_rows,
            input_batches: metrics.input_batches,
            output_rows: metrics.output_rows,
            output_batches: metrics.output_batches,
            output_bytes: metrics.output_bytes,
            elapsed_ms: metrics.elapsed.as_secs_f64() * 1_000.0,
            wait_ms: metrics.wait.as_secs_f64() * 1_000.0,
        }
    }
}
