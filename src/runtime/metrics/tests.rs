use std::time::Duration;

use super::{MemoryPool, QueryMetrics};

#[test]
fn records_metrics_queue_edge_waits_and_high_watermarks() {
    let metrics = QueryMetrics::new();
    metrics.record_scan(10, 1, 100);
    metrics.record_output(4, 1, 20);
    metrics.seal_output();
    metrics.record_output(100, 10, 200);
    metrics.add_discovered_files(7);
    metrics.observe_memory(80);
    metrics.observe_active_lanes(3);
    metrics.record_scheduler_wait(Duration::from_micros(25));
    metrics.record_compute_permit_wait(Duration::from_micros(3));
    metrics.record_queue_backpressure_wait(Duration::from_micros(5));
    metrics.record_csv_morsel_queue_wait(Duration::from_micros(7));
    metrics.record_scan_pipeline_output_queue_wait(Duration::from_micros(11));
    metrics.record_aggregate_lane_dispatch_queue_wait(Duration::from_micros(13));
    metrics.record_aggregate_partial_output_queue_wait(Duration::from_micros(17));
    metrics.record_barrier_wait(Duration::from_micros(7));
    metrics.record_parquet_range_read(4_096, Duration::from_micros(19));
    metrics.record_parquet_decode_activity(Duration::from_micros(25), 2, 1);
    metrics.record_parquet_decode_compute_permit_wait(Duration::from_micros(7));
    metrics.record_parquet_row_filter_compute(8_192, Duration::from_micros(11));
    metrics.record_parquet_alignment_time(Duration::from_micros(3));
    metrics.record_query_admission_wait(Duration::from_micros(13));
    metrics.record_sql_parse_time(Duration::from_micros(17));
    metrics.record_table_function_prepare_time(Duration::from_micros(19));
    metrics.record_bind_time(Duration::from_micros(23));
    metrics.record_provider_prepare_time(Duration::from_micros(29));
    metrics.record_optimize_time(Duration::from_micros(31));
    metrics.record_native_verification_time(Duration::from_micros(11));
    metrics.add_native_full_verification_segments(2);
    metrics.record_spill(512, 2);
    metrics.add_spill_logical_input_bytes(200);
    metrics.add_spill_write_bytes(400);
    metrics.add_spill_read_bytes(300);
    metrics.add_spill_file();
    metrics.add_spill_quota_rejection();
    metrics.record_repartition(200, 2, 150);
    metrics.add_join_candidates(12);
    metrics.add_join_short_circuits(3);
    metrics.record_runtime_filter();
    metrics.add_csv_source_bytes(90);
    metrics.add_csv_decompressed_bytes(180);
    metrics.add_csv_morsels(2);
    metrics.observe_csv_parser_lanes(4);
    metrics.record_csv_source_io_time(Duration::from_micros(11));
    metrics.record_csv_framing_time(Duration::from_micros(13));
    metrics.record_csv_decode_compute_time(Duration::from_micros(17));
    metrics.record_metadata_cache_hit();
    metrics.record_metadata_cache_miss();
    metrics.record_metadata_singleflight_wait(Duration::from_millis(2));
    metrics.record_cancel_to_quiesce(Duration::from_millis(3));
    let parent = metrics.register_operator("Projection", None);
    let child = metrics.register_operator("Scan", Some(parent.id()));
    child.record_output(10, 80);
    child.finish(Duration::from_millis(4));
    parent.record_output(4, 32);
    parent.finish(Duration::from_millis(5));
    metrics.add_s3_requests(1);
    metrics.record_s3_get(123);
    metrics.add_parquet_page_index_bytes_read(41);
    metrics.add_parquet_bloom_filter_bytes_read(42);
    metrics.add_parquet_pages_pruned(3);
    metrics.add_parquet_page_rows_pruned(30);
    metrics.add_parquet_bloom_row_groups_pruned(2);
    metrics.add_parquet_pruning_budget_skip();
    metrics.add_parquet_reader_build();
    metrics.add_parquet_local_file_open();
    metrics.add_parquet_narrow_decimal_columns(3);
    metrics.record_native_predicate_sidecar_read(512);
    metrics.record_native_predicate_sidecar_selection(100, 7);
    metrics.add_native_predicate_sidecar_exact_bypass();
    metrics.record_native_predicate_sidecar_full_projection(7);
    metrics.add_native_predicate_sidecar_full_projection_fallback_row_groups(3);
    metrics.add_native_predicate_sidecar_fallback();
    metrics.finish();

    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.rows_scanned, 10);
    assert_eq!(snapshot.rows_returned, 4);
    assert_eq!(snapshot.discovered_files, 7);
    assert_eq!(snapshot.peak_memory_bytes, 80);
    assert_eq!(snapshot.peak_active_lanes, 3);
    assert_eq!(snapshot.scheduler_wait, Duration::from_micros(25));
    assert_eq!(snapshot.compute_permit_wait, Duration::from_micros(3));
    assert_eq!(snapshot.queue_backpressure_wait, Duration::from_micros(53));
    assert_eq!(snapshot.csv_morsel_queue_wait, Duration::from_micros(7));
    assert_eq!(
        snapshot.scan_pipeline_output_queue_wait,
        Duration::from_micros(11)
    );
    assert_eq!(
        snapshot.aggregate_lane_dispatch_queue_wait,
        Duration::from_micros(13)
    );
    assert_eq!(
        snapshot.aggregate_partial_output_queue_wait,
        Duration::from_micros(17)
    );
    assert_eq!(snapshot.barrier_wait, Duration::from_micros(7));
    assert_eq!(snapshot.parquet_range_read_time, Duration::from_micros(19));
    assert_eq!(snapshot.parquet_range_bytes_read, 4_096);
    assert_eq!(
        snapshot.parquet_decode_compute_time,
        Duration::from_micros(25)
    );
    assert_eq!(
        snapshot.parquet_decode_compute_permit_wait,
        Duration::from_micros(7)
    );
    assert_eq!(snapshot.parquet_decode_polls, 2);
    assert_eq!(snapshot.parquet_decode_pending_polls, 1);
    assert_eq!(
        snapshot.parquet_row_filter_compute_time,
        Duration::from_micros(11)
    );
    assert_eq!(snapshot.parquet_row_filter_evaluations, 1);
    assert_eq!(snapshot.parquet_row_filter_input_rows, 8_192);
    assert_eq!(snapshot.parquet_alignment_time, Duration::from_micros(3));
    assert_eq!(snapshot.query_admission_wait, Duration::from_micros(13));
    assert_eq!(snapshot.sql_parse_time, Duration::from_micros(17));
    assert_eq!(
        snapshot.table_function_prepare_time,
        Duration::from_micros(19)
    );
    assert_eq!(snapshot.bind_time, Duration::from_micros(23));
    assert_eq!(snapshot.provider_prepare_time, Duration::from_micros(29));
    assert_eq!(snapshot.optimize_time, Duration::from_micros(31));
    assert_eq!(snapshot.native_verification_time, Duration::from_micros(11));
    assert_eq!(snapshot.native_full_verification_segments, 2);
    assert_eq!(snapshot.spill_bytes, 512);
    assert_eq!(snapshot.spill_partitions, 2);
    assert_eq!(snapshot.spill_write_bytes, 400);
    assert_eq!(snapshot.spill_logical_input_bytes, 200);
    assert_eq!(snapshot.spill_write_amplification_millionths, 2_000_000);
    assert_eq!(snapshot.spill_write_amplification(), Some(2.0));
    assert_eq!(snapshot.active_spill_bytes, 400);
    assert_eq!(snapshot.peak_active_spill_bytes, 400);
    assert_eq!(snapshot.spill_read_bytes, 300);
    assert_eq!(snapshot.spill_files, 1);
    assert_eq!(snapshot.active_spill_files, 1);
    assert_eq!(snapshot.peak_active_spill_files, 1);
    assert_eq!(snapshot.spill_repartition_bytes, 200);
    assert_eq!(snapshot.max_repartition_depth, 2);
    assert_eq!(snapshot.max_spill_partition_bytes, 150);
    assert_eq!(snapshot.join_candidate_pairs, 12);
    assert_eq!(snapshot.join_short_circuits, 3);
    assert_eq!(snapshot.runtime_filter_hits, 1);
    assert_eq!(snapshot.csv_source_bytes, 90);
    assert_eq!(snapshot.csv_decompressed_bytes, 180);
    assert_eq!(snapshot.csv_morsels, 2);
    assert_eq!(snapshot.peak_csv_parser_lanes, 4);
    assert_eq!(snapshot.csv_source_io_time, Duration::from_micros(11));
    assert_eq!(snapshot.csv_framing_time, Duration::from_micros(13));
    assert_eq!(snapshot.csv_decode_compute_time, Duration::from_micros(17));
    assert_eq!(snapshot.metadata_cache_hits, 1);
    assert_eq!(snapshot.metadata_cache_misses, 1);
    assert_eq!(
        snapshot.metadata_singleflight_wait,
        Duration::from_millis(2)
    );
    assert_eq!(snapshot.cancel_to_quiesce, Duration::from_millis(3));
    assert_eq!(snapshot.operators.len(), 2);
    assert_eq!(snapshot.operators[0].input_rows, 10);
    assert_eq!(snapshot.operators[0].output_rows, 4);
    assert_eq!(snapshot.operators[1].output_rows, 10);
    assert_eq!(snapshot.spill_quota_rejections, 1);
    assert_eq!(snapshot.s3_requests, 2);
    assert_eq!(snapshot.s3_bytes_transferred, 123);
    assert_eq!(snapshot.parquet_page_index_bytes_read, 41);
    assert_eq!(snapshot.parquet_bloom_filter_bytes_read, 42);
    assert_eq!(snapshot.parquet_pages_pruned, 3);
    assert_eq!(snapshot.parquet_page_rows_pruned, 30);
    assert_eq!(snapshot.parquet_bloom_row_groups_pruned, 2);
    assert_eq!(snapshot.parquet_pruning_budget_skips, 1);
    assert_eq!(snapshot.parquet_reader_builds, 1);
    assert_eq!(snapshot.parquet_local_file_opens, 1);
    assert_eq!(snapshot.parquet_narrow_decimal_columns, 3);
    assert_eq!(snapshot.native_predicate_sidecar_bytes_read, 512);
    assert_eq!(snapshot.native_predicate_sidecar_rows_evaluated, 100);
    assert_eq!(snapshot.native_predicate_sidecar_rows_selected, 7);
    assert_eq!(snapshot.native_predicate_sidecar_exact_bypasses, 1);
    assert_eq!(
        snapshot.native_predicate_sidecar_full_projection_bypasses,
        1
    );
    assert_eq!(snapshot.native_predicate_sidecar_full_projection_rows, 7);
    assert_eq!(
        snapshot.native_predicate_sidecar_full_projection_fallback_row_groups,
        3
    );
    assert_eq!(snapshot.native_predicate_sidecar_fallbacks, 1);
    assert!(!snapshot.elapsed.is_zero());
}

#[test]
fn snapshot_reports_current_and_peak_pool_memory() {
    let pool = MemoryPool::new(1_024);
    let metrics = QueryMetrics::with_memory_pool(pool.clone());
    let reservation = pool.try_reserve(128).unwrap();

    let active = metrics.snapshot();
    assert_eq!(active.current_memory_bytes, 128);
    assert_eq!(active.peak_memory_bytes, 128);

    drop(reservation);
    let released = metrics.snapshot();
    assert_eq!(released.current_memory_bytes, 0);
    assert_eq!(released.peak_memory_bytes, 128);
}
