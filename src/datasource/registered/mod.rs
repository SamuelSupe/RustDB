mod csv;
mod csv_mapping;
mod parquet;
mod parquet_mapping;

use std::sync::atomic::{AtomicU64, Ordering};

pub(crate) use csv::RegisteredCsvTable;
pub(crate) use parquet::RegisteredParquetTable;

static NEXT_PROVIDER_ID: AtomicU64 = AtomicU64::new(1);

fn next_provider_id() -> u64 {
    NEXT_PROVIDER_ID.fetch_add(1, Ordering::Relaxed)
}
