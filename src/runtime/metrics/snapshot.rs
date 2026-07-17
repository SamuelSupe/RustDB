use std::time::Duration;

use super::OperatorMetricsSnapshot;

#[derive(Clone, Debug, Default, Eq, PartialEq)]
#[non_exhaustive]
pub struct QueryMetricsSnapshot {
    /// Time from query-context creation through completion (or the current
    /// snapshot). It excludes engine admission and top-level parsing performed
    /// by `Session::execute`; use the two dedicated fields below when those
    /// phases are relevant to end-to-end latency.
    pub elapsed: Duration,
    /// Time waiting for the engine-wide concurrent-query permit. Query
    /// `elapsed` starts after this wait, so benchmark wall time should remain
    /// the end-to-end latency source.
    pub query_admission_wait: Duration,
    /// SQL parsing measured outside table-function preparation. This includes
    /// top-level `Session::execute` parsing and any retained view source parsed
    /// inside a command. Executing an already parsed `PreparedStatement` does
    /// not parse SQL here and therefore reports zero.
    pub sql_parse_time: Duration,
    /// AST table-function discovery and provider construction before binding.
    pub table_function_prepare_time: Duration,
    /// SQL name resolution and logical-plan binding.
    pub bind_time: Duration,
    /// Query snapshot capture and provider-specific preparation.
    pub provider_prepare_time: Duration,
    /// Logical optimizer wall time.
    pub optimize_time: Duration,
    /// Native integrity checks nested inside `provider_prepare_time`.
    pub native_verification_time: Duration,
    /// Native segments that required full SHA-256 verification in this query.
    pub native_full_verification_segments: u64,
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
    /// Arrow Parquet readers built after row-group pruning and chunking.
    pub parquet_reader_builds: u64,
    /// Local Parquet descriptors opened by this query after snapshot capture.
    pub parquet_local_file_opens: u64,
    /// File-columns decoded as Decimal64 before lossless public widening.
    pub parquet_narrow_decimal_columns: u64,
    /// Physical Native predicate-sidecar bytes read by the query.
    pub native_predicate_sidecar_bytes_read: u64,
    /// Rows evaluated directly from Native predicate-sidecar encodings.
    pub native_predicate_sidecar_rows_evaluated: u64,
    /// Rows retained by Native predicate-sidecar evaluation.
    pub native_predicate_sidecar_rows_selected: u64,
    /// Row groups whose complete exact predicate bypassed Arrow RowFilter.
    pub native_predicate_sidecar_exact_bypasses: u64,
    /// Row groups whose complete projection was decoded from the Native
    /// predicate sidecar without constructing an Arrow Parquet reader.
    pub native_predicate_sidecar_full_projection_bypasses: u64,
    /// Output rows materialized directly from Native predicate sidecars.
    pub native_predicate_sidecar_full_projection_rows: u64,
    /// Row groups in full-projection candidates that safely declined direct
    /// projection and continued through predicate-only or Parquet execution.
    pub native_predicate_sidecar_full_projection_fallback_row_groups: u64,
    /// Sidecar opportunities that safely fell back to the Parquet path.
    pub native_predicate_sidecar_fallbacks: u64,
    pub s3_requests: u64,
    pub s3_bytes_transferred: u64,
    /// Bytes currently reserved by this query's memory pool.
    pub current_memory_bytes: u64,
    pub peak_memory_bytes: u64,
    pub peak_active_lanes: u64,
    pub scheduler_wait: Duration,
    /// Cumulative time queued for an engine-wide compute permit.
    pub compute_permit_wait: Duration,
    /// Legacy mixed time blocked by full bounded execution queues. Prefer the
    /// typed wait counters below for attribution.
    pub queue_backpressure_wait: Duration,
    /// Subset of queue backpressure spent publishing framed CSV morsels.
    pub csv_morsel_queue_wait: Duration,
    /// Subset of queue backpressure spent publishing scan-pipeline output.
    pub scan_pipeline_output_queue_wait: Duration,
    /// Subset of queue backpressure spent dispatching aggregate lane input.
    pub aggregate_lane_dispatch_queue_wait: Duration,
    /// Subset of queue backpressure spent publishing partial aggregates.
    pub aggregate_partial_output_queue_wait: Duration,
    /// Cumulative time spent in execution barriers. Parallel lane totals can
    /// exceed wall-clock query time.
    pub barrier_wait: Duration,
    /// Cumulative wall time of Parquet range reads across all readers. Parallel
    /// requests overlap, so this is an attribution total rather than query wall
    /// time.
    pub parquet_range_read_time: Duration,
    /// Physical Parquet range bytes returned by local or object-store reads.
    pub parquet_range_bytes_read: u64,
    /// Cumulative CPU time spent polling Arrow's asynchronous Parquet decoder.
    /// I/O waits return `Pending` and do not hold a compute permit.
    pub parquet_decode_compute_time: Duration,
    /// Cumulative wait for compute permits specifically requested by Parquet
    /// reader construction and decoder polling.
    pub parquet_decode_compute_permit_wait: Duration,
    pub parquet_decode_polls: u64,
    pub parquet_decode_pending_polls: u64,
    /// Subset of decoder compute spent evaluating pushed Arrow row predicates.
    pub parquet_row_filter_compute_time: Duration,
    pub parquet_row_filter_evaluations: u64,
    pub parquet_row_filter_input_rows: u64,
    /// Cumulative post-decode schema/Hive alignment time.
    pub parquet_alignment_time: Duration,
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
    /// Cumulative time awaiting raw CSV source reads.
    pub csv_source_io_time: Duration,
    /// Cumulative quote-aware CSV record framing time.
    pub csv_framing_time: Duration,
    /// Cumulative Arrow CSV decode and flush compute time.
    pub csv_decode_compute_time: Duration,
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
