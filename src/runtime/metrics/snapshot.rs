use std::time::Duration;

use super::OperatorMetricsSnapshot;

#[derive(Clone, Debug, Default, Eq, PartialEq)]
#[non_exhaustive]
pub struct QueryMetricsSnapshot {
    pub elapsed: Duration,
    pub rows_scanned: u64,
    pub rows_returned: u64,
    pub batches_scanned: u64,
    pub batches_returned: u64,
    pub bytes_scanned: u64,
    pub bytes_returned: u64,
    pub discovered_files: u64,
    pub files_pruned: u64,
    pub row_groups_pruned: u64,
    pub parquet_page_index_bytes_read: u64,
    pub parquet_bloom_filter_bytes_read: u64,
    pub parquet_pages_pruned: u64,
    pub parquet_page_rows_pruned: u64,
    pub parquet_bloom_row_groups_pruned: u64,
    pub parquet_pruning_budget_skips: u64,
    pub s3_requests: u64,
    pub s3_bytes_transferred: u64,
    /// Bytes currently reserved by this query's memory pool.
    pub current_memory_bytes: u64,
    pub peak_memory_bytes: u64,
    pub peak_active_lanes: u64,
    pub scheduler_wait: Duration,
    pub spill_bytes: u64,
    pub spill_partitions: u64,
    pub spill_read_bytes: u64,
    pub spill_write_bytes: u64,
    pub spill_logical_input_bytes: u64,
    /// Cumulative physical writes divided by logical Spill input, scaled by
    /// 1,000,000. A value of 1,000,000 means 1.0x; zero means no logical input.
    pub spill_write_amplification_millionths: u64,
    pub spill_files: u64,
    pub spill_quota_rejections: u64,
    pub active_spill_bytes: u64,
    pub peak_active_spill_bytes: u64,
    pub active_spill_files: u64,
    pub peak_active_spill_files: u64,
    pub spill_repartition_bytes: u64,
    pub max_repartition_depth: u64,
    pub max_spill_partition_bytes: u64,
    pub join_candidate_pairs: u64,
    pub join_short_circuits: u64,
    pub runtime_filter_hits: u64,
    pub csv_source_bytes: u64,
    pub csv_decompressed_bytes: u64,
    pub csv_morsels: u64,
    pub peak_csv_parser_lanes: u64,
    pub metadata_cache_hits: u64,
    pub metadata_cache_misses: u64,
    pub metadata_singleflight_wait: Duration,
    pub cancel_to_quiesce: Duration,
    pub operators: Vec<OperatorMetricsSnapshot>,
}

impl QueryMetricsSnapshot {
    /// Returns cumulative physical Spill writes per logical input byte.
    pub fn spill_write_amplification(&self) -> Option<f64> {
        (self.spill_logical_input_bytes != 0)
            .then(|| self.spill_write_amplification_millionths as f64 / 1_000_000.0)
    }
}
