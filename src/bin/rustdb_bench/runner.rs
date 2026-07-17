use std::{path::Path, time::Duration, time::Instant};

use rustdb::{Engine, Error, QueryMetricsSnapshot, Result, Session};

use super::{
    args::Args,
    checksum::ALGORITHM,
    consume::consume,
    report::{
        BenchmarkReport, BuildReport, ConfigReport, OperatorReport, RunReport, environment,
        executable_sha256, percentile,
    },
    rss::RssSampler,
};

pub(crate) async fn run(args: Args) -> Result<()> {
    args.validate()?;
    let binary_sha256 = executable_sha256()?;
    let sql = tokio::fs::read_to_string(&args.query)
        .await
        .map_err(|error| Error::io(Some(args.query.clone()), error))?;
    let config = args.engine_config();
    let report_config = ConfigReport::new(&config, args.rss_sample_interval_ms);
    let temp_dir = config.spill.directory.clone();
    let memory_limit = config.memory_limit;
    let session = Engine::new(config)?.session();
    let mut expected_checksum = None;

    for _ in 0..args.warmup {
        let warmup = run_once(
            &session,
            &sql,
            &temp_dir,
            memory_limit,
            args.require_spill,
            args.rss_sample_interval_ms,
        )
        .await?;
        validate_checksum(&mut expected_checksum, &warmup)?;
    }

    let mut runs = Vec::with_capacity(args.iterations);
    for _ in 0..args.iterations {
        let report = run_once(
            &session,
            &sql,
            &temp_dir,
            memory_limit,
            args.require_spill,
            args.rss_sample_interval_ms,
        )
        .await?;
        validate_checksum(&mut expected_checksum, &report)?;
        runs.push(report);
    }

    let result_checksum_sha256 = expected_checksum.ok_or_else(|| {
        Error::Internal("benchmark completed without a result checksum".to_owned())
    })?;
    let mut elapsed: Vec<_> = runs.iter().map(|run| run.elapsed_ms).collect();
    elapsed.sort_by(f64::total_cmp);
    let report = BenchmarkReport {
        engine_version: env!("CARGO_PKG_VERSION"),
        build_id: args.build_id,
        binary_sha256,
        build: BuildReport {
            cargo_profile: args.build_profile,
            rustflags: args.build_rustflags,
            rustc_version: args.rustc_version,
        },
        query_file: args.query.display().to_string(),
        warmup: args.warmup,
        iterations: args.iterations,
        config: report_config,
        environment: environment(args.cpu_model),
        checksum_algorithm: ALGORITHM,
        result_checksum_sha256,
        p50_ms: percentile(&elapsed, 0.50),
        p95_ms: percentile(&elapsed, 0.95),
        runs,
    };
    println!(
        "{}",
        serde_json::to_string_pretty(&report).map_err(|error| {
            Error::Internal(format!("cannot encode benchmark report: {error}"))
        })?
    );
    Ok(())
}

async fn run_once(
    session: &Session,
    sql: &str,
    temp_dir: &Path,
    memory_limit: usize,
    require_spill: bool,
    rss_sample_interval_ms: u64,
) -> Result<RunReport> {
    let rss_sampler = RssSampler::start(Duration::from_millis(rss_sample_interval_ms));
    let started = Instant::now();
    let mut result = session.execute(sql).await?;
    let query_id = result.query_id();
    let query_dir = temp_dir.join(format!("query-{query_id}"));
    let metrics = result.metrics();
    let output = consume(&mut result, started).await?;
    let elapsed_ms = started.elapsed().as_secs_f64() * 1_000.0;
    drop(result);
    let metrics = metrics.snapshot();
    let rss = rss_sampler.finish()?;

    if metrics.peak_memory_bytes > u64::try_from(memory_limit).unwrap_or(u64::MAX) {
        return Err(Error::ResourceExhausted(format!(
            "query {query_id} reserved {} bytes above the configured {} byte limit",
            metrics.peak_memory_bytes, memory_limit
        )));
    }
    if require_spill && metrics.spill_bytes == 0 {
        return Err(Error::Execution(format!(
            "query {query_id} completed without spilling while --require-spill was set"
        )));
    }
    let spill_cleaned = terminal_resources_clean(&metrics, &query_dir);
    if !spill_cleaned {
        return Err(Error::Execution(format!(
            "query {query_id} did not release terminal resources: current memory {} bytes, active Spill {} bytes in {} files, spill directory exists={}",
            metrics.current_memory_bytes,
            metrics.active_spill_bytes,
            metrics.active_spill_files,
            query_dir.exists(),
        )));
    }

    Ok(RunReport {
        query_id: query_id.to_string(),
        elapsed_ms,
        first_batch_ms: output.first_batch_ms,
        rows: output.rows,
        batches: output.batches,
        result_checksum_sha256: output.checksum,
        rows_per_second: if elapsed_ms == 0.0 {
            0.0
        } else {
            output.rows as f64 * 1_000.0 / elapsed_ms
        },
        scanned_rows: metrics.rows_scanned,
        scanned_bytes: metrics.bytes_scanned,
        current_memory_bytes: metrics.current_memory_bytes,
        peak_memory_bytes: metrics.peak_memory_bytes,
        rss_bytes_after: rss.after_bytes,
        engine_current_reservation_bytes: metrics.current_memory_bytes,
        engine_peak_reservation_bytes: metrics.peak_memory_bytes,
        process_rss_before_bytes: rss.before_bytes,
        process_peak_rss_bytes: rss.peak_bytes,
        process_rss_after_bytes: rss.after_bytes,
        process_rss_samples: rss.samples,
        peak_active_lanes: metrics.peak_active_lanes,
        scheduler_wait_ms: metrics.scheduler_wait.as_secs_f64() * 1_000.0,
        spill_bytes: metrics.spill_bytes,
        spill_read_bytes: metrics.spill_read_bytes,
        spill_write_bytes: metrics.spill_write_bytes,
        spill_logical_input_bytes: metrics.spill_logical_input_bytes,
        spill_write_amplification_millionths: metrics.spill_write_amplification_millionths,
        spill_files: metrics.spill_files,
        active_spill_bytes: metrics.active_spill_bytes,
        peak_active_spill_bytes: metrics.peak_active_spill_bytes,
        active_spill_files: metrics.active_spill_files,
        peak_active_spill_files: metrics.peak_active_spill_files,
        spill_repartition_bytes: metrics.spill_repartition_bytes,
        max_repartition_depth: metrics.max_repartition_depth,
        max_spill_partition_bytes: metrics.max_spill_partition_bytes,
        spill_quota_rejections: metrics.spill_quota_rejections,
        spill_partitions: metrics.spill_partitions,
        join_candidate_pairs: metrics.join_candidate_pairs,
        join_short_circuits: metrics.join_short_circuits,
        runtime_filter_hits: metrics.runtime_filter_hits,
        csv_source_bytes: metrics.csv_source_bytes,
        csv_decompressed_bytes: metrics.csv_decompressed_bytes,
        csv_morsels: metrics.csv_morsels,
        peak_csv_parser_lanes: metrics.peak_csv_parser_lanes,
        metadata_cache_hits: metrics.metadata_cache_hits,
        metadata_cache_misses: metrics.metadata_cache_misses,
        metadata_singleflight_wait_ms: metrics.metadata_singleflight_wait.as_secs_f64() * 1_000.0,
        cancel_to_quiesce_ms: metrics.cancel_to_quiesce.as_secs_f64() * 1_000.0,
        parquet_page_index_bytes_read: metrics.parquet_page_index_bytes_read,
        parquet_bloom_filter_bytes_read: metrics.parquet_bloom_filter_bytes_read,
        parquet_pages_pruned: metrics.parquet_pages_pruned,
        parquet_page_rows_pruned: metrics.parquet_page_rows_pruned,
        parquet_bloom_row_groups_pruned: metrics.parquet_bloom_row_groups_pruned,
        parquet_pruning_budget_skips: metrics.parquet_pruning_budget_skips,
        s3_requests: metrics.s3_requests,
        s3_bytes_transferred: metrics.s3_bytes_transferred,
        operators: metrics.operators.iter().map(OperatorReport::from).collect(),
        spill_cleaned,
    })
}

fn validate_checksum(expected: &mut Option<String>, report: &RunReport) -> Result<()> {
    if let Some(expected) = expected {
        if expected != &report.result_checksum_sha256 {
            return Err(Error::Execution(format!(
                "query {} returned checksum {} instead of {}",
                report.query_id, report.result_checksum_sha256, expected
            )));
        }
    } else {
        *expected = Some(report.result_checksum_sha256.clone());
    }
    Ok(())
}

fn terminal_resources_clean(metrics: &QueryMetricsSnapshot, query_dir: &Path) -> bool {
    metrics.current_memory_bytes == 0
        && metrics.active_spill_bytes == 0
        && metrics.active_spill_files == 0
        && !query_dir.exists()
}

#[cfg(test)]
mod tests {
    use rustdb::{Engine, EngineConfig, QueryMetricsSnapshot};

    use super::{run_once, terminal_resources_clean};

    #[test]
    fn terminal_resource_check_requires_every_counter_and_directory_to_be_clear() {
        let directory = tempfile::tempdir().unwrap();
        let query_dir = directory.path().join("query-test");
        let mut metrics = QueryMetricsSnapshot::default();
        assert!(terminal_resources_clean(&metrics, &query_dir));

        metrics.current_memory_bytes = 1;
        assert!(!terminal_resources_clean(&metrics, &query_dir));
        metrics.current_memory_bytes = 0;
        metrics.active_spill_bytes = 1;
        assert!(!terminal_resources_clean(&metrics, &query_dir));
        metrics.active_spill_bytes = 0;
        metrics.active_spill_files = 1;
        assert!(!terminal_resources_clean(&metrics, &query_dir));
        metrics.active_spill_files = 0;
        std::fs::create_dir(&query_dir).unwrap();
        assert!(!terminal_resources_clean(&metrics, &query_dir));
    }

    #[tokio::test]
    async fn completed_run_reports_checksum_and_released_resources() {
        let directory = tempfile::tempdir().unwrap();
        let mut config = EngineConfig::default();
        config.memory_limit = 8 << 20;
        config.spill.directory = directory.path().to_path_buf();
        let session = Engine::new(config).unwrap().session();

        let report = run_once(&session, "SELECT 1", directory.path(), 8 << 20, false, 1)
            .await
            .unwrap();

        assert_eq!(report.result_checksum_sha256.len(), 64);
        assert_eq!(report.current_memory_bytes, 0);
        assert_eq!(report.engine_current_reservation_bytes, 0);
        assert_eq!(report.active_spill_bytes, 0);
        assert_eq!(report.active_spill_files, 0);
        assert!(report.spill_cleaned);
    }
}
