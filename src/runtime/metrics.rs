use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use parking_lot::RwLock;

use super::MemoryPool;

mod amplification;
mod operator;
mod phase;
mod snapshot;
mod timing;
pub(crate) use operator::OperatorHandle;
pub use operator::OperatorMetricsSnapshot;
use operator::OperatorRegistry;
pub use snapshot::QueryMetricsSnapshot;

#[derive(Clone, Debug)]
pub struct QueryMetrics {
    inner: Arc<Inner>,
}

#[derive(Debug)]
struct Inner {
    started_at: Instant,
    memory_pool: RwLock<Option<MemoryPool>>,
    elapsed_ns: AtomicU64,
    rows_scanned: AtomicU64,
    rows_returned: AtomicU64,
    batches_scanned: AtomicU64,
    batches_returned: AtomicU64,
    bytes_scanned: AtomicU64,
    bytes_returned: AtomicU64,
    output_sealed: AtomicBool,
    discovered_files: AtomicU64,
    files_pruned: AtomicU64,
    row_groups_pruned: AtomicU64,
    parquet_page_index_bytes_read: AtomicU64,
    parquet_bloom_filter_bytes_read: AtomicU64,
    parquet_pages_pruned: AtomicU64,
    parquet_page_rows_pruned: AtomicU64,
    parquet_bloom_row_groups_pruned: AtomicU64,
    parquet_pruning_budget_skips: AtomicU64,
    parquet_reader_builds: AtomicU64,
    parquet_local_file_opens: AtomicU64,
    parquet_narrow_decimal_columns: AtomicU64,
    native_predicate_sidecar_bytes_read: AtomicU64,
    native_predicate_sidecar_rows_evaluated: AtomicU64,
    native_predicate_sidecar_rows_selected: AtomicU64,
    native_predicate_sidecar_exact_bypasses: AtomicU64,
    native_predicate_sidecar_full_projection_bypasses: AtomicU64,
    native_predicate_sidecar_full_projection_rows: AtomicU64,
    native_predicate_sidecar_full_projection_fallback_row_groups: AtomicU64,
    native_predicate_sidecar_fallbacks: AtomicU64,
    s3_requests: AtomicU64,
    s3_bytes_transferred: AtomicU64,
    peak_memory_bytes: AtomicU64,
    peak_active_lanes: AtomicU64,
    scheduler_wait_ns: AtomicU64,
    phase: phase::PhaseMetrics,
    timing: timing::TimingMetrics,
    spill_bytes: AtomicU64,
    spill_partitions: AtomicU64,
    spill_read_bytes: AtomicU64,
    spill_write_bytes: AtomicU64,
    spill_logical_input_bytes: AtomicU64,
    spill_files: AtomicU64,
    spill_quota_rejections: AtomicU64,
    active_spill_bytes: AtomicU64,
    peak_active_spill_bytes: AtomicU64,
    active_spill_files: AtomicU64,
    peak_active_spill_files: AtomicU64,
    spill_repartition_bytes: AtomicU64,
    max_repartition_depth: AtomicU64,
    max_spill_partition_bytes: AtomicU64,
    join_candidate_pairs: AtomicU64,
    join_short_circuits: AtomicU64,
    runtime_filter_hits: AtomicU64,
    csv_source_bytes: AtomicU64,
    csv_decompressed_bytes: AtomicU64,
    csv_morsels: AtomicU64,
    active_csv_parser_lanes: AtomicU64,
    peak_csv_parser_lanes: AtomicU64,
    metadata_cache_hits: AtomicU64,
    metadata_cache_misses: AtomicU64,
    metadata_singleflight_wait_ns: AtomicU64,
    cancel_to_quiesce_ns: AtomicU64,
    operators: OperatorRegistry,
}

impl QueryMetrics {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Inner {
                started_at: Instant::now(),
                memory_pool: RwLock::new(None),
                elapsed_ns: AtomicU64::new(0),
                rows_scanned: AtomicU64::new(0),
                rows_returned: AtomicU64::new(0),
                batches_scanned: AtomicU64::new(0),
                batches_returned: AtomicU64::new(0),
                bytes_scanned: AtomicU64::new(0),
                bytes_returned: AtomicU64::new(0),
                output_sealed: AtomicBool::new(false),
                discovered_files: AtomicU64::new(0),
                files_pruned: AtomicU64::new(0),
                row_groups_pruned: AtomicU64::new(0),
                parquet_page_index_bytes_read: AtomicU64::new(0),
                parquet_bloom_filter_bytes_read: AtomicU64::new(0),
                parquet_pages_pruned: AtomicU64::new(0),
                parquet_page_rows_pruned: AtomicU64::new(0),
                parquet_bloom_row_groups_pruned: AtomicU64::new(0),
                parquet_pruning_budget_skips: AtomicU64::new(0),
                parquet_reader_builds: AtomicU64::new(0),
                parquet_local_file_opens: AtomicU64::new(0),
                parquet_narrow_decimal_columns: AtomicU64::new(0),
                native_predicate_sidecar_bytes_read: AtomicU64::new(0),
                native_predicate_sidecar_rows_evaluated: AtomicU64::new(0),
                native_predicate_sidecar_rows_selected: AtomicU64::new(0),
                native_predicate_sidecar_exact_bypasses: AtomicU64::new(0),
                native_predicate_sidecar_full_projection_bypasses: AtomicU64::new(0),
                native_predicate_sidecar_full_projection_rows: AtomicU64::new(0),
                native_predicate_sidecar_full_projection_fallback_row_groups: AtomicU64::new(0),
                native_predicate_sidecar_fallbacks: AtomicU64::new(0),
                s3_requests: AtomicU64::new(0),
                s3_bytes_transferred: AtomicU64::new(0),
                peak_memory_bytes: AtomicU64::new(0),
                peak_active_lanes: AtomicU64::new(0),
                scheduler_wait_ns: AtomicU64::new(0),
                phase: phase::PhaseMetrics::default(),
                timing: timing::TimingMetrics::default(),
                spill_bytes: AtomicU64::new(0),
                spill_partitions: AtomicU64::new(0),
                spill_read_bytes: AtomicU64::new(0),
                spill_write_bytes: AtomicU64::new(0),
                spill_logical_input_bytes: AtomicU64::new(0),
                spill_files: AtomicU64::new(0),
                spill_quota_rejections: AtomicU64::new(0),
                active_spill_bytes: AtomicU64::new(0),
                peak_active_spill_bytes: AtomicU64::new(0),
                active_spill_files: AtomicU64::new(0),
                peak_active_spill_files: AtomicU64::new(0),
                spill_repartition_bytes: AtomicU64::new(0),
                max_repartition_depth: AtomicU64::new(0),
                max_spill_partition_bytes: AtomicU64::new(0),
                join_candidate_pairs: AtomicU64::new(0),
                join_short_circuits: AtomicU64::new(0),
                runtime_filter_hits: AtomicU64::new(0),
                csv_source_bytes: AtomicU64::new(0),
                csv_decompressed_bytes: AtomicU64::new(0),
                csv_morsels: AtomicU64::new(0),
                active_csv_parser_lanes: AtomicU64::new(0),
                peak_csv_parser_lanes: AtomicU64::new(0),
                metadata_cache_hits: AtomicU64::new(0),
                metadata_cache_misses: AtomicU64::new(0),
                metadata_singleflight_wait_ns: AtomicU64::new(0),
                cancel_to_quiesce_ns: AtomicU64::new(0),
                operators: OperatorRegistry::default(),
            }),
        }
    }

    pub fn with_memory_pool(memory_pool: MemoryPool) -> Self {
        let metrics = Self::new();
        metrics.bind_memory_pool(memory_pool);
        metrics
    }

    pub fn bind_memory_pool(&self, memory_pool: MemoryPool) {
        *self.inner.memory_pool.write() = Some(memory_pool);
    }

    pub fn finish(&self) {
        let elapsed = duration_ns(self.inner.started_at.elapsed());
        let _ = self.inner.elapsed_ns.compare_exchange(
            0,
            elapsed.max(1),
            Ordering::Relaxed,
            Ordering::Relaxed,
        );
    }

    pub fn elapsed(&self) -> Duration {
        let finished = self.inner.elapsed_ns.load(Ordering::Relaxed);
        if finished == 0 {
            self.inner.started_at.elapsed()
        } else {
            Duration::from_nanos(finished)
        }
    }

    pub fn record_scan(&self, rows: u64, batches: u64, bytes: u64) {
        add(&self.inner.rows_scanned, rows);
        add(&self.inner.batches_scanned, batches);
        add(&self.inner.bytes_scanned, bytes);
    }

    pub fn record_output(&self, rows: u64, batches: u64, bytes: u64) {
        if self.inner.output_sealed.load(Ordering::Acquire) {
            return;
        }
        add(&self.inner.rows_returned, rows);
        add(&self.inner.batches_returned, batches);
        add(&self.inner.bytes_returned, bytes);
    }

    pub(crate) fn seal_output(&self) {
        self.inner.output_sealed.store(true, Ordering::Release);
    }

    pub fn add_files_pruned(&self, count: u64) {
        add(&self.inner.files_pruned, count);
    }

    pub(crate) fn add_discovered_files(&self, count: u64) {
        add(&self.inner.discovered_files, count);
    }

    pub fn add_row_groups_pruned(&self, count: u64) {
        add(&self.inner.row_groups_pruned, count);
    }

    pub(crate) fn add_parquet_page_index_bytes_read(&self, bytes: u64) {
        add(&self.inner.parquet_page_index_bytes_read, bytes);
    }

    pub(crate) fn add_parquet_bloom_filter_bytes_read(&self, bytes: u64) {
        add(&self.inner.parquet_bloom_filter_bytes_read, bytes);
    }

    pub(crate) fn add_parquet_pages_pruned(&self, pages: u64) {
        add(&self.inner.parquet_pages_pruned, pages);
    }

    pub(crate) fn add_parquet_page_rows_pruned(&self, rows: u64) {
        add(&self.inner.parquet_page_rows_pruned, rows);
    }

    pub(crate) fn add_parquet_bloom_row_groups_pruned(&self, row_groups: u64) {
        add(&self.inner.parquet_bloom_row_groups_pruned, row_groups);
    }

    pub(crate) fn add_parquet_pruning_budget_skip(&self) {
        add(&self.inner.parquet_pruning_budget_skips, 1);
    }

    pub(crate) fn add_parquet_reader_build(&self) {
        add(&self.inner.parquet_reader_builds, 1);
    }

    pub(crate) fn add_parquet_local_file_open(&self) {
        add(&self.inner.parquet_local_file_opens, 1);
    }

    pub(crate) fn add_parquet_narrow_decimal_columns(&self, columns: u64) {
        add(&self.inner.parquet_narrow_decimal_columns, columns);
    }

    pub(crate) fn record_native_predicate_sidecar_read(&self, bytes: u64) {
        add(&self.inner.native_predicate_sidecar_bytes_read, bytes);
    }

    pub(crate) fn record_native_predicate_sidecar_selection(
        &self,
        evaluated_rows: u64,
        selected_rows: u64,
    ) {
        add(
            &self.inner.native_predicate_sidecar_rows_evaluated,
            evaluated_rows,
        );
        add(
            &self.inner.native_predicate_sidecar_rows_selected,
            selected_rows,
        );
    }

    pub(crate) fn add_native_predicate_sidecar_exact_bypass(&self) {
        add(&self.inner.native_predicate_sidecar_exact_bypasses, 1);
    }

    pub(crate) fn record_native_predicate_sidecar_full_projection(&self, rows: u64) {
        add(
            &self.inner.native_predicate_sidecar_full_projection_bypasses,
            1,
        );
        add(
            &self.inner.native_predicate_sidecar_full_projection_rows,
            rows,
        );
    }

    pub(crate) fn add_native_predicate_sidecar_full_projection_fallback_row_groups(
        &self,
        row_groups: u64,
    ) {
        add(
            &self
                .inner
                .native_predicate_sidecar_full_projection_fallback_row_groups,
            row_groups,
        );
    }

    pub(crate) fn add_native_predicate_sidecar_fallback(&self) {
        add(&self.inner.native_predicate_sidecar_fallbacks, 1);
    }

    pub fn add_s3_requests(&self, count: u64) {
        add(&self.inner.s3_requests, count);
    }

    pub fn add_s3_bytes_transferred(&self, bytes: u64) {
        add(&self.inner.s3_bytes_transferred, bytes);
    }

    /// Records a completed S3 GET and the response body bytes it transferred.
    pub fn record_s3_get(&self, bytes: u64) {
        add(&self.inner.s3_requests, 1);
        add(&self.inner.s3_bytes_transferred, bytes);
    }

    pub fn observe_memory(&self, bytes: usize) {
        set_max(&self.inner.peak_memory_bytes, usize_to_u64(bytes));
    }

    pub(crate) fn observe_active_lanes(&self, lanes: usize) {
        set_max(&self.inner.peak_active_lanes, usize_to_u64(lanes));
    }

    pub(crate) fn record_scheduler_wait(&self, duration: Duration) {
        add(&self.inner.scheduler_wait_ns, duration_ns(duration));
    }

    pub fn record_spill(&self, bytes: u64, partitions: u64) {
        add(&self.inner.spill_bytes, bytes);
        add(&self.inner.spill_partitions, partitions);
    }

    pub(crate) fn add_spill_read_bytes(&self, bytes: u64) {
        add(&self.inner.spill_read_bytes, bytes);
    }

    pub(crate) fn add_spill_write_bytes(&self, bytes: u64) {
        add(&self.inner.spill_write_bytes, bytes);
        let active = add_and_load(&self.inner.active_spill_bytes, bytes);
        set_max(&self.inner.peak_active_spill_bytes, active);
    }

    #[allow(dead_code)]
    pub(crate) fn add_spill_logical_input_bytes(&self, bytes: u64) {
        add(&self.inner.spill_logical_input_bytes, bytes);
    }

    #[allow(dead_code)]
    pub(crate) fn spill_write_totals(&self) -> (u64, u64) {
        (
            load(&self.inner.spill_logical_input_bytes),
            load(&self.inner.spill_write_bytes),
        )
    }

    #[allow(dead_code)]
    pub(crate) fn projected_spill_write_amplification_exceeds(
        &self,
        unaccounted_write_bytes: u64,
        limit: f64,
    ) -> (bool, u64, u64) {
        let (logical_bytes, write_bytes) = self.spill_write_totals();
        let projected_write_bytes = write_bytes.saturating_add(unaccounted_write_bytes);
        (
            amplification::exceeds(logical_bytes, projected_write_bytes, limit),
            logical_bytes,
            projected_write_bytes,
        )
    }

    pub(crate) fn add_spill_file(&self) {
        add(&self.inner.spill_files, 1);
        let active = add_and_load(&self.inner.active_spill_files, 1);
        set_max(&self.inner.peak_active_spill_files, active);
    }

    pub(crate) fn add_spill_quota_rejection(&self) {
        add(&self.inner.spill_quota_rejections, 1);
    }

    pub(crate) fn remove_active_spill(&self, bytes: u64, files: u64) {
        subtract(&self.inner.active_spill_bytes, bytes);
        subtract(&self.inner.active_spill_files, files);
    }

    pub(crate) fn clear_active_spill(&self) {
        self.inner.active_spill_bytes.store(0, Ordering::Relaxed);
        self.inner.active_spill_files.store(0, Ordering::Relaxed);
    }

    pub(crate) fn record_repartition(&self, bytes: u64, depth: usize, largest: u64) {
        add(&self.inner.spill_repartition_bytes, bytes);
        set_max(&self.inner.max_repartition_depth, usize_to_u64(depth));
        set_max(&self.inner.max_spill_partition_bytes, largest);
    }

    pub(crate) fn add_join_candidates(&self, pairs: u64) {
        add(&self.inner.join_candidate_pairs, pairs);
    }

    pub(crate) fn add_join_short_circuits(&self, rows: u64) {
        add(&self.inner.join_short_circuits, rows);
    }

    pub(crate) fn record_runtime_filter(&self) {
        add(&self.inner.runtime_filter_hits, 1);
    }

    pub(crate) fn add_csv_source_bytes(&self, bytes: u64) {
        add(&self.inner.csv_source_bytes, bytes);
    }

    pub(crate) fn add_csv_decompressed_bytes(&self, bytes: u64) {
        add(&self.inner.csv_decompressed_bytes, bytes);
    }

    pub(crate) fn add_csv_morsels(&self, morsels: u64) {
        add(&self.inner.csv_morsels, morsels);
    }

    #[cfg(test)]
    pub(crate) fn observe_csv_parser_lanes(&self, lanes: usize) {
        set_max(&self.inner.peak_csv_parser_lanes, usize_to_u64(lanes));
    }

    pub(crate) fn enter_csv_parser_lane(&self) -> CsvParserLane {
        let active = self
            .inner
            .active_csv_parser_lanes
            .fetch_add(1, Ordering::AcqRel)
            .saturating_add(1);
        set_max(&self.inner.peak_csv_parser_lanes, active);
        CsvParserLane {
            metrics: self.clone(),
        }
    }

    #[cfg(test)]
    pub(crate) fn active_csv_parser_lanes(&self) -> u64 {
        load(&self.inner.active_csv_parser_lanes)
    }

    pub(crate) fn record_metadata_cache_hit(&self) {
        add(&self.inner.metadata_cache_hits, 1);
    }

    pub(crate) fn record_metadata_cache_miss(&self) {
        add(&self.inner.metadata_cache_misses, 1);
    }

    pub(crate) fn record_metadata_singleflight_wait(&self, wait: Duration) {
        add(&self.inner.metadata_singleflight_wait_ns, duration_ns(wait));
    }

    pub(crate) fn record_cancel_to_quiesce(&self, elapsed: Duration) {
        set_max(&self.inner.cancel_to_quiesce_ns, duration_ns(elapsed));
    }

    pub(crate) fn register_operator(
        &self,
        name: &'static str,
        parent_id: Option<u64>,
    ) -> OperatorHandle {
        self.inner.operators.register(name, parent_id)
    }

    pub fn snapshot(&self) -> QueryMetricsSnapshot {
        let (pool_used, pool_peak) = self
            .inner
            .memory_pool
            .read()
            .as_ref()
            .map(|pool| (usize_to_u64(pool.used()), usize_to_u64(pool.peak())))
            .unwrap_or_default();
        let spill_logical_input_bytes = load(&self.inner.spill_logical_input_bytes);
        let spill_write_bytes = load(&self.inner.spill_write_bytes);
        let timing = self.inner.timing.snapshot();
        let phase = self.inner.phase.snapshot();
        QueryMetricsSnapshot {
            elapsed: self.elapsed(),
            query_admission_wait: phase.query_admission_wait,
            sql_parse_time: phase.sql_parse_time,
            table_function_prepare_time: phase.table_function_prepare_time,
            bind_time: phase.bind_time,
            provider_prepare_time: phase.provider_prepare_time,
            optimize_time: phase.optimize_time,
            native_verification_time: phase.native_verification_time,
            native_full_verification_segments: phase.native_full_verification_segments,
            rows_scanned: load(&self.inner.rows_scanned),
            rows_returned: load(&self.inner.rows_returned),
            batches_scanned: load(&self.inner.batches_scanned),
            batches_returned: load(&self.inner.batches_returned),
            bytes_scanned: load(&self.inner.bytes_scanned),
            bytes_returned: load(&self.inner.bytes_returned),
            discovered_files: load(&self.inner.discovered_files),
            files_pruned: load(&self.inner.files_pruned),
            row_groups_pruned: load(&self.inner.row_groups_pruned),
            parquet_page_index_bytes_read: load(&self.inner.parquet_page_index_bytes_read),
            parquet_bloom_filter_bytes_read: load(&self.inner.parquet_bloom_filter_bytes_read),
            parquet_pages_pruned: load(&self.inner.parquet_pages_pruned),
            parquet_page_rows_pruned: load(&self.inner.parquet_page_rows_pruned),
            parquet_bloom_row_groups_pruned: load(&self.inner.parquet_bloom_row_groups_pruned),
            parquet_pruning_budget_skips: load(&self.inner.parquet_pruning_budget_skips),
            parquet_reader_builds: load(&self.inner.parquet_reader_builds),
            parquet_local_file_opens: load(&self.inner.parquet_local_file_opens),
            parquet_narrow_decimal_columns: load(&self.inner.parquet_narrow_decimal_columns),
            native_predicate_sidecar_bytes_read: load(
                &self.inner.native_predicate_sidecar_bytes_read,
            ),
            native_predicate_sidecar_rows_evaluated: load(
                &self.inner.native_predicate_sidecar_rows_evaluated,
            ),
            native_predicate_sidecar_rows_selected: load(
                &self.inner.native_predicate_sidecar_rows_selected,
            ),
            native_predicate_sidecar_exact_bypasses: load(
                &self.inner.native_predicate_sidecar_exact_bypasses,
            ),
            native_predicate_sidecar_full_projection_bypasses: load(
                &self.inner.native_predicate_sidecar_full_projection_bypasses,
            ),
            native_predicate_sidecar_full_projection_rows: load(
                &self.inner.native_predicate_sidecar_full_projection_rows,
            ),
            native_predicate_sidecar_full_projection_fallback_row_groups: load(
                &self
                    .inner
                    .native_predicate_sidecar_full_projection_fallback_row_groups,
            ),
            native_predicate_sidecar_fallbacks: load(
                &self.inner.native_predicate_sidecar_fallbacks,
            ),
            s3_requests: load(&self.inner.s3_requests),
            s3_bytes_transferred: load(&self.inner.s3_bytes_transferred),
            current_memory_bytes: pool_used,
            peak_memory_bytes: load(&self.inner.peak_memory_bytes).max(pool_peak),
            peak_active_lanes: load(&self.inner.peak_active_lanes),
            scheduler_wait: Duration::from_nanos(load(&self.inner.scheduler_wait_ns)),
            compute_permit_wait: timing.compute_permit_wait,
            queue_backpressure_wait: timing.queue_backpressure_wait,
            csv_morsel_queue_wait: timing.csv_morsel_queue_wait,
            scan_pipeline_output_queue_wait: timing.scan_pipeline_output_queue_wait,
            aggregate_lane_dispatch_queue_wait: timing.aggregate_lane_dispatch_queue_wait,
            aggregate_partial_output_queue_wait: timing.aggregate_partial_output_queue_wait,
            barrier_wait: timing.barrier_wait,
            parquet_range_read_time: timing.parquet_range_read_time,
            parquet_range_bytes_read: timing.parquet_range_bytes_read,
            parquet_decode_compute_time: timing.parquet_decode_compute_time,
            parquet_decode_compute_permit_wait: timing.parquet_decode_compute_permit_wait,
            parquet_decode_polls: timing.parquet_decode_polls,
            parquet_decode_pending_polls: timing.parquet_decode_pending_polls,
            parquet_row_filter_compute_time: timing.parquet_row_filter_compute_time,
            parquet_row_filter_evaluations: timing.parquet_row_filter_evaluations,
            parquet_row_filter_input_rows: timing.parquet_row_filter_input_rows,
            parquet_alignment_time: timing.parquet_alignment_time,
            spill_bytes: load(&self.inner.spill_bytes),
            spill_partitions: load(&self.inner.spill_partitions),
            spill_read_bytes: load(&self.inner.spill_read_bytes),
            spill_write_bytes,
            spill_logical_input_bytes,
            spill_write_amplification_millionths: amplification::millionths(
                spill_logical_input_bytes,
                spill_write_bytes,
            ),
            spill_files: load(&self.inner.spill_files),
            spill_quota_rejections: load(&self.inner.spill_quota_rejections),
            active_spill_bytes: load(&self.inner.active_spill_bytes),
            peak_active_spill_bytes: load(&self.inner.peak_active_spill_bytes),
            active_spill_files: load(&self.inner.active_spill_files),
            peak_active_spill_files: load(&self.inner.peak_active_spill_files),
            spill_repartition_bytes: load(&self.inner.spill_repartition_bytes),
            max_repartition_depth: load(&self.inner.max_repartition_depth),
            max_spill_partition_bytes: load(&self.inner.max_spill_partition_bytes),
            join_candidate_pairs: load(&self.inner.join_candidate_pairs),
            join_short_circuits: load(&self.inner.join_short_circuits),
            runtime_filter_hits: load(&self.inner.runtime_filter_hits),
            csv_source_bytes: load(&self.inner.csv_source_bytes),
            csv_decompressed_bytes: load(&self.inner.csv_decompressed_bytes),
            csv_morsels: load(&self.inner.csv_morsels),
            peak_csv_parser_lanes: load(&self.inner.peak_csv_parser_lanes),
            csv_source_io_time: timing.csv_source_io_time,
            csv_framing_time: timing.csv_framing_time,
            csv_decode_compute_time: timing.csv_decode_compute_time,
            metadata_cache_hits: load(&self.inner.metadata_cache_hits),
            metadata_cache_misses: load(&self.inner.metadata_cache_misses),
            metadata_singleflight_wait: Duration::from_nanos(load(
                &self.inner.metadata_singleflight_wait_ns,
            )),
            cancel_to_quiesce: Duration::from_nanos(load(&self.inner.cancel_to_quiesce_ns)),
            operators: self.inner.operators.snapshot(),
        }
    }
}

pub(crate) struct CsvParserLane {
    metrics: QueryMetrics,
}

impl Drop for CsvParserLane {
    fn drop(&mut self) {
        self.metrics
            .inner
            .active_csv_parser_lanes
            .fetch_sub(1, Ordering::AcqRel);
    }
}

impl Default for QueryMetrics {
    fn default() -> Self {
        Self::new()
    }
}

fn add(counter: &AtomicU64, value: u64) {
    let _ = counter.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
        Some(current.saturating_add(value))
    });
}

fn add_and_load(counter: &AtomicU64, value: u64) -> u64 {
    let mut observed = 0;
    let _ = counter.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
        observed = current.saturating_add(value);
        Some(observed)
    });
    observed
}

fn subtract(counter: &AtomicU64, value: u64) {
    let _ = counter.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
        Some(current.saturating_sub(value))
    });
}

fn set_max(counter: &AtomicU64, value: u64) {
    let _ = counter.fetch_max(value, Ordering::Relaxed);
}

fn load(counter: &AtomicU64) -> u64 {
    counter.load(Ordering::Relaxed)
}

fn duration_ns(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}

fn usize_to_u64(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests;
