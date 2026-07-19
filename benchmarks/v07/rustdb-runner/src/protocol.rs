use serde::{Deserialize, Serialize};

use crate::{source::SourceFile, worker_resources::WorkerResources};

pub(crate) const CHECKSUM_MODE: &str = "multiset-sha256-v2";
pub(crate) const CHECKSUM_BACKEND: &str = "rust-sha256-v2";

#[derive(Debug, Deserialize)]
#[serde(tag = "command", rename_all = "snake_case")]
pub(crate) enum Command {
    Setup {
        setup_id: String,
        storage_track: String,
        source_sha256: String,
        source_bytes: u64,
        source_files: Vec<SourceFile>,
        statements_sha256: String,
        statements: Vec<String>,
        max_storage_bytes: u64,
    },
    Run {
        run_id: String,
        sql: String,
        storage_track: String,
        engine_order: usize,
        setup_id: Option<String>,
    },
    Shutdown,
}

#[derive(Debug, Serialize)]
pub(crate) struct SetupResponse {
    pub(crate) kind: &'static str,
    pub(crate) engine: &'static str,
    pub(crate) setup_id: String,
    pub(crate) complete: bool,
    pub(crate) load_elapsed_ms: f64,
    pub(crate) rss_baseline_bytes: u64,
    pub(crate) peak_rss_bytes: u64,
    pub(crate) storage_baseline_bytes: u64,
    pub(crate) storage_peak_bytes: u64,
    pub(crate) storage_final_bytes: u64,
    pub(crate) table_count: usize,
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct CacheState {
    pub(crate) os_page_cache: String,
    pub(crate) metadata_cache: String,
    pub(crate) external_file_cache: String,
}

#[derive(Debug, Serialize)]
pub(crate) struct Hello {
    pub(crate) kind: &'static str,
    pub(crate) engine: &'static str,
    pub(crate) version: &'static str,
    pub(crate) build_id: String,
    pub(crate) threads: usize,
    pub(crate) memory_limit_bytes: usize,
    pub(crate) concurrency: usize,
    pub(crate) batch_size: usize,
    pub(crate) cache_state: CacheState,
    pub(crate) worker_resources: WorkerResources,
}

#[derive(Debug, Serialize)]
pub(crate) struct QueryRun {
    pub(crate) query_slot: usize,
    pub(crate) harness_query_id: String,
    pub(crate) start_offset_ms: f64,
    pub(crate) finish_offset_ms: f64,
    pub(crate) elapsed_ms: f64,
    /// Time until `Session::execute` returns. This is a RustDB-internal eager
    /// preparation boundary and is not a cross-engine latency metric.
    pub(crate) execute_return_ms: f64,
    pub(crate) ttfb_ms: f64,
    pub(crate) rows: u64,
    pub(crate) batches: u64,
    pub(crate) checksum: String,
    pub(crate) checksum_mode: &'static str,
    pub(crate) checksum_backend: &'static str,
    pub(crate) checksum_compute_ms: f64,
    pub(crate) complete: bool,
    pub(crate) discovered_files: u64,
    pub(crate) scanned_rows: u64,
    pub(crate) scanned_bytes: u64,
    pub(crate) parquet_reader_builds: u64,
    pub(crate) parquet_local_file_opens: u64,
    pub(crate) parquet_narrow_decimal_columns: u64,
    pub(crate) parquet_range_bytes_read: u64,
    pub(crate) parquet_range_read_time_ms: f64,
    pub(crate) parquet_decode_compute_time_ms: f64,
    pub(crate) parquet_decode_compute_permit_wait_ms: f64,
    pub(crate) parquet_decode_polls: u64,
    pub(crate) parquet_decode_pending_polls: u64,
    pub(crate) parquet_row_filter_compute_time_ms: f64,
    pub(crate) parquet_row_filter_evaluations: u64,
    pub(crate) parquet_row_filter_input_rows: u64,
    pub(crate) parquet_alignment_time_ms: f64,
    pub(crate) native_predicate_sidecar_bytes_read: u64,
    pub(crate) native_predicate_sidecar_rows_evaluated: u64,
    pub(crate) native_predicate_sidecar_rows_selected: u64,
    pub(crate) native_predicate_sidecar_exact_bypasses: u64,
    pub(crate) native_predicate_sidecar_full_projection_bypasses: u64,
    pub(crate) native_predicate_sidecar_full_projection_rows: u64,
    pub(crate) native_predicate_sidecar_full_projection_fallback_row_groups: u64,
    pub(crate) native_predicate_sidecar_fallbacks: u64,
    pub(crate) csv_source_bytes: u64,
    pub(crate) csv_decompressed_bytes: u64,
    pub(crate) csv_morsels: u64,
    pub(crate) peak_csv_parser_lanes: u64,
    pub(crate) current_reservation_bytes: u64,
    pub(crate) peak_reservation_bytes: u64,
    pub(crate) peak_active_lanes: u64,
    pub(crate) scheduler_wait_ms: f64,
    pub(crate) compute_permit_wait_ms: f64,
    pub(crate) queue_backpressure_wait_ms: f64,
    pub(crate) csv_morsel_queue_wait_ms: f64,
    pub(crate) scan_pipeline_output_queue_wait_ms: f64,
    pub(crate) aggregate_lane_dispatch_queue_wait_ms: f64,
    pub(crate) aggregate_partial_output_queue_wait_ms: f64,
    pub(crate) barrier_wait_ms: f64,
    pub(crate) query_admission_wait_ms: f64,
    pub(crate) sql_parse_time_ms: f64,
    pub(crate) table_function_prepare_time_ms: f64,
    pub(crate) bind_time_ms: f64,
    pub(crate) provider_prepare_time_ms: f64,
    pub(crate) optimize_time_ms: f64,
    pub(crate) native_verification_time_ms: f64,
    pub(crate) native_full_verification_segments: u64,
    pub(crate) csv_source_io_time_ms: f64,
    pub(crate) csv_framing_time_ms: f64,
    pub(crate) csv_decode_compute_time_ms: f64,
    pub(crate) spill_read_bytes: u64,
    pub(crate) spill_write_bytes: u64,
    pub(crate) join_candidate_pairs: u64,
    pub(crate) operators: Vec<OperatorRun>,
}

#[derive(Debug, Serialize)]
pub(crate) struct OperatorRun {
    pub(crate) id: u64,
    pub(crate) parent_id: Option<u64>,
    pub(crate) name: String,
    pub(crate) input_rows: u64,
    pub(crate) input_batches: u64,
    pub(crate) output_rows: u64,
    pub(crate) output_batches: u64,
    pub(crate) output_bytes: u64,
    pub(crate) elapsed_ms: f64,
    pub(crate) wait_ms: f64,
}

#[derive(Debug, Serialize)]
pub(crate) struct RunResponse {
    pub(crate) kind: &'static str,
    pub(crate) run_id: String,
    pub(crate) engine: &'static str,
    pub(crate) version: &'static str,
    pub(crate) build_id: String,
    pub(crate) threads: usize,
    pub(crate) memory_limit_bytes: usize,
    pub(crate) concurrency: usize,
    pub(crate) batch_size: usize,
    pub(crate) cache_state: CacheState,
    pub(crate) worker_resources: WorkerResources,
    pub(crate) storage_track: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) setup_id: Option<String>,
    pub(crate) engine_order: usize,
    pub(crate) group_elapsed_ms: f64,
    pub(crate) rss_baseline_bytes: u64,
    pub(crate) peak_rss_bytes: u64,
    pub(crate) engine_root_current_reservation_bytes: u64,
    pub(crate) engine_root_lifetime_peak_reservation_bytes: u64,
    pub(crate) engine_root_memory_limit_bytes: u64,
    pub(crate) start_skew_ms: f64,
    pub(crate) throughput_queries_per_second: f64,
    pub(crate) queries: Vec<QueryRun>,
}

#[derive(Debug, Serialize)]
pub(crate) struct ErrorResponse {
    pub(crate) kind: &'static str,
    pub(crate) message: String,
}
