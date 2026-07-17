use std::time::Instant;

use futures::StreamExt;
use rustdb::{QueryResult, Result};

use super::checksum::TypedChecksum;

pub(super) struct OutputSummary {
    pub(super) rows: u64,
    pub(super) batches: u64,
    pub(super) first_batch_ms: Option<f64>,
    pub(super) checksum: String,
}

pub(super) async fn consume(result: &mut QueryResult, started: Instant) -> Result<OutputSummary> {
    let mut rows = 0_u64;
    let mut batches = 0_u64;
    let mut first_batch_ms = None;
    let mut checksum = TypedChecksum::new(result.schema().as_ref())?;

    while let Some(batch) = result.stream().next().await {
        let batch = batch?;
        first_batch_ms.get_or_insert_with(|| started.elapsed().as_secs_f64() * 1_000.0);
        rows = rows.checked_add(batch.num_rows() as u64).ok_or_else(|| {
            rustdb::Error::Execution("benchmark result row count overflowed u64".to_owned())
        })?;
        batches = batches.checked_add(1).ok_or_else(|| {
            rustdb::Error::Execution("benchmark result batch count overflowed u64".to_owned())
        })?;
        checksum.update_batch(&batch)?;
    }

    Ok(OutputSummary {
        rows,
        batches,
        first_batch_ms,
        checksum: checksum.finish(),
    })
}
