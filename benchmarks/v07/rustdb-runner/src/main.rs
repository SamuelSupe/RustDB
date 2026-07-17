#![forbid(unsafe_code)]

mod args;
mod build_id;
mod checksum;
mod disk;
mod protocol;
mod rss;
mod setup;
mod source;
mod start_gate;
mod worker_resources;

use std::{
    io::{self, BufRead, Write},
    sync::Arc,
    time::{Duration, Instant},
};

use args::Args;
use checksum::MultisetChecksum;
use clap::Parser;
use futures::{StreamExt, future::join_all};
use protocol::{
    CHECKSUM_BACKEND, CHECKSUM_MODE, CacheState, Command, ErrorResponse, Hello, OperatorRun,
    QueryRun, RunResponse,
};
use rss::RssSampler;
use rustdb::{Engine, EngineConfig, Session};
use setup::{SetupRequest, SetupState};
use start_gate::QueryStartGate;
use worker_resources::WorkerResources;

fn main() {
    if let Err(error) = run() {
        let _ = write_json(&ErrorResponse {
            kind: "error",
            message: error,
        });
        std::process::exit(1);
    }
}

fn run() -> Result<(), String> {
    let args = Args::parse();
    validate_args(&args)?;
    build_id::verify(&args.build_id)?;
    let worker_resources = WorkerResources::collect()?;
    let mut config = EngineConfig::default();
    config.compute_threads = args.threads;
    config.memory_limit = args.memory_limit;
    config.max_concurrent_queries = args.concurrency;
    config.batch_size = args.batch_size;
    config.metadata_cache_bytes = args.metadata_cache_bytes;
    config.spill.directory = args.spill_directory.clone();
    let engine = match args.database.as_ref() {
        Some(path) => Engine::open(path, config),
        None => Engine::new(config),
    }
    .map_err(|error| error.to_string())?;
    let session = engine.session();
    let mut setup_state = SetupState::load(args.database.as_deref(), &session)?;
    let cache_state = cache_state(args.metadata_cache_bytes);
    write_json(&Hello {
        kind: "hello",
        engine: "rustdb",
        version: env!("RUSTDB_ENGINE_VERSION"),
        build_id: args.build_id.clone(),
        threads: args.threads,
        memory_limit_bytes: args.memory_limit,
        concurrency: args.concurrency,
        batch_size: args.batch_size,
        cache_state: cache_state.clone(),
        worker_resources: worker_resources.clone(),
    })?;

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(args.concurrency)
        .enable_all()
        .build()
        .map_err(|error| format!("cannot create Tokio runtime: {error}"))?;
    for line in io::stdin().lock().lines() {
        let line = line.map_err(|error| format!("cannot read command: {error}"))?;
        if line.trim().is_empty() {
            continue;
        }
        let command: Command = serde_json::from_str(&line)
            .map_err(|error| format!("invalid worker command: {error}"))?;
        match command {
            Command::Shutdown => return Ok(()),
            Command::Setup {
                setup_id,
                storage_track,
                source_sha256,
                source_bytes,
                source_files,
                statements_sha256,
                statements,
                max_storage_bytes,
            } => {
                let response = runtime.block_on(setup_state.execute(
                    &session,
                    &args.spill_directory,
                    args.memory_limit,
                    SetupRequest {
                        setup_id,
                        storage_track,
                        source_sha256,
                        source_bytes,
                        source_files,
                        statements_sha256,
                        statements,
                        max_storage_bytes,
                    },
                ))?;
                write_json(&response)?;
            }
            Command::Run {
                run_id,
                sql,
                storage_track,
                engine_order,
                setup_id,
            } => {
                let response_setup_id =
                    setup_state.run_setup_id(&session, &storage_track, setup_id.as_deref())?;
                let response = runtime.block_on(run_group(
                    &engine,
                    &session,
                    &args,
                    cache_state.clone(),
                    &worker_resources,
                    run_id,
                    sql,
                    storage_track,
                    response_setup_id,
                    engine_order,
                ))?;
                write_json(&response)?;
            }
        }
    }
    Ok(())
}

fn validate_args(args: &Args) -> Result<(), String> {
    if args.threads == 0 || args.memory_limit == 0 || args.concurrency == 0 || args.batch_size == 0
    {
        return Err(
            "threads, memory-limit, concurrency and batch-size must be positive".to_owned(),
        );
    }
    if args.build_id.len() != 64
        || !args
            .build_id
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err("build-id must be a lowercase SHA-256".to_owned());
    }
    Ok(())
}

fn cache_state(metadata_cache_bytes: usize) -> CacheState {
    CacheState {
        os_page_cache: "warm-uncontrolled".to_owned(),
        metadata_cache: if metadata_cache_bytes == 0 {
            "disabled".to_owned()
        } else {
            "enabled".to_owned()
        },
        external_file_cache: "not-applicable".to_owned(),
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_group(
    engine: &Engine,
    session: &Session,
    args: &Args,
    cache_state: CacheState,
    startup_worker_resources: &WorkerResources,
    run_id: String,
    sql: String,
    storage_track: String,
    setup_id: Option<String>,
    engine_order: usize,
) -> Result<RunResponse, String> {
    let gate = Arc::new(QueryStartGate::try_new(args.concurrency)?);
    let tasks = (0..args.concurrency)
        .map(|query_slot| {
            tokio::spawn(run_query(
                session.clone(),
                sql.clone(),
                Arc::clone(&gate),
                query_slot,
                format!("{run_id}:query-{query_slot}"),
            ))
        })
        .collect::<Vec<_>>();
    gate.wait_until_ready().await;
    let sampler = RssSampler::start();
    let group_started = gate.release().await?;
    let queries = join_all(tasks)
        .await
        .into_iter()
        .map(|result| {
            result
                .map_err(|error| format!("query task failed: {error}"))
                .and_then(|query| query)
        })
        .collect::<Result<Vec<_>, _>>()?;
    let group_elapsed_ms = group_started.elapsed().as_secs_f64() * 1_000.0;
    let (rss_baseline_bytes, peak_rss_bytes) = sampler.stop();
    let engine_memory = engine.memory_snapshot();
    let worker_resources = WorkerResources::collect()?;
    if worker_resources != *startup_worker_resources {
        return Err("worker resource constraints changed after hello".to_owned());
    }
    let checksum = queries
        .first()
        .map(|query| query.checksum.as_str())
        .ok_or_else(|| "run group produced no queries".to_owned())?;
    if queries.iter().any(|query| query.checksum != checksum) {
        return Err("concurrent executions returned different checksums".to_owned());
    }
    Ok(RunResponse {
        kind: "run",
        run_id,
        engine: "rustdb",
        version: env!("RUSTDB_ENGINE_VERSION"),
        build_id: args.build_id.clone(),
        threads: args.threads,
        memory_limit_bytes: args.memory_limit,
        concurrency: args.concurrency,
        batch_size: args.batch_size,
        cache_state,
        worker_resources,
        storage_track,
        setup_id,
        engine_order,
        group_elapsed_ms,
        rss_baseline_bytes,
        peak_rss_bytes,
        engine_root_current_reservation_bytes: u64::try_from(engine_memory.current_bytes)
            .map_err(|_| "engine root current reservation does not fit u64".to_owned())?,
        engine_root_lifetime_peak_reservation_bytes: u64::try_from(
            engine_memory.lifetime_peak_bytes,
        )
        .map_err(|_| "engine root lifetime peak reservation does not fit u64".to_owned())?,
        engine_root_memory_limit_bytes: u64::try_from(engine_memory.limit_bytes)
            .map_err(|_| "engine root memory limit does not fit u64".to_owned())?,
        start_skew_ms: start_skew_ms(&queries),
        throughput_queries_per_second: args.concurrency as f64 * 1_000.0 / group_elapsed_ms,
        queries,
    })
}

async fn run_query(
    session: Session,
    sql: String,
    gate: Arc<QueryStartGate>,
    query_slot: usize,
    harness_query_id: String,
) -> Result<QueryRun, String> {
    let (group_started, started) = gate.wait_for_start().await?;
    let start_offset_ms = started.duration_since(group_started).as_secs_f64() * 1_000.0;
    let mut result = session
        .execute(&sql)
        .await
        .map_err(|error| error.to_string())?;
    let execute_return_ms = started.elapsed().as_secs_f64() * 1_000.0;
    let metrics = result.metrics();
    let mut rows = 0_u64;
    let mut batches = 0_u64;
    let mut ttfb_ms = None;
    let mut checksum = MultisetChecksum::new();
    let mut checksum_compute = Duration::ZERO;
    while let Some(batch) = result.stream().next().await {
        let batch = batch.map_err(|error| error.to_string())?;
        ttfb_ms.get_or_insert_with(|| started.elapsed().as_secs_f64() * 1_000.0);
        rows = rows.saturating_add(batch.num_rows() as u64);
        batches = batches.saturating_add(1);
        let checksum_started = Instant::now();
        checksum.update_batch(&batch)?;
        checksum_compute = checksum_compute.saturating_add(checksum_started.elapsed());
    }
    let checksum_started = Instant::now();
    let checksum = checksum.finish();
    checksum_compute = checksum_compute.saturating_add(checksum_started.elapsed());
    let finished = Instant::now();
    let elapsed_ms = finished.duration_since(started).as_secs_f64() * 1_000.0;
    let finish_offset_ms = finished.duration_since(group_started).as_secs_f64() * 1_000.0;
    drop(result);
    let metrics = metrics.snapshot();
    Ok(QueryRun {
        query_slot,
        harness_query_id,
        start_offset_ms,
        finish_offset_ms,
        elapsed_ms,
        execute_return_ms,
        ttfb_ms: ttfb_ms.unwrap_or(elapsed_ms),
        rows,
        batches,
        checksum,
        checksum_mode: CHECKSUM_MODE,
        checksum_backend: CHECKSUM_BACKEND,
        checksum_compute_ms: checksum_compute.as_secs_f64() * 1_000.0,
        complete: true,
        scanned_rows: metrics.rows_scanned,
        scanned_bytes: metrics.bytes_scanned,
        parquet_reader_builds: metrics.parquet_reader_builds,
        parquet_local_file_opens: metrics.parquet_local_file_opens,
        parquet_narrow_decimal_columns: metrics.parquet_narrow_decimal_columns,
        parquet_range_bytes_read: metrics.parquet_range_bytes_read,
        parquet_range_read_time_ms: metrics.parquet_range_read_time.as_secs_f64() * 1_000.0,
        parquet_decode_compute_time_ms: metrics.parquet_decode_compute_time.as_secs_f64() * 1_000.0,
        parquet_decode_compute_permit_wait_ms: metrics
            .parquet_decode_compute_permit_wait
            .as_secs_f64()
            * 1_000.0,
        parquet_decode_polls: metrics.parquet_decode_polls,
        parquet_decode_pending_polls: metrics.parquet_decode_pending_polls,
        parquet_row_filter_compute_time_ms: metrics.parquet_row_filter_compute_time.as_secs_f64()
            * 1_000.0,
        parquet_row_filter_evaluations: metrics.parquet_row_filter_evaluations,
        parquet_row_filter_input_rows: metrics.parquet_row_filter_input_rows,
        parquet_alignment_time_ms: metrics.parquet_alignment_time.as_secs_f64() * 1_000.0,
        native_predicate_sidecar_bytes_read: metrics.native_predicate_sidecar_bytes_read,
        native_predicate_sidecar_rows_evaluated: metrics.native_predicate_sidecar_rows_evaluated,
        native_predicate_sidecar_rows_selected: metrics.native_predicate_sidecar_rows_selected,
        native_predicate_sidecar_exact_bypasses: metrics.native_predicate_sidecar_exact_bypasses,
        native_predicate_sidecar_full_projection_bypasses: metrics
            .native_predicate_sidecar_full_projection_bypasses,
        native_predicate_sidecar_full_projection_rows: metrics
            .native_predicate_sidecar_full_projection_rows,
        native_predicate_sidecar_full_projection_fallback_row_groups: metrics
            .native_predicate_sidecar_full_projection_fallback_row_groups,
        native_predicate_sidecar_fallbacks: metrics.native_predicate_sidecar_fallbacks,
        csv_source_bytes: metrics.csv_source_bytes,
        csv_decompressed_bytes: metrics.csv_decompressed_bytes,
        csv_morsels: metrics.csv_morsels,
        peak_csv_parser_lanes: metrics.peak_csv_parser_lanes,
        current_reservation_bytes: metrics.current_memory_bytes,
        peak_reservation_bytes: metrics.peak_memory_bytes,
        peak_active_lanes: metrics.peak_active_lanes,
        scheduler_wait_ms: metrics.scheduler_wait.as_secs_f64() * 1_000.0,
        compute_permit_wait_ms: metrics.compute_permit_wait.as_secs_f64() * 1_000.0,
        queue_backpressure_wait_ms: metrics.queue_backpressure_wait.as_secs_f64() * 1_000.0,
        csv_morsel_queue_wait_ms: metrics.csv_morsel_queue_wait.as_secs_f64() * 1_000.0,
        scan_pipeline_output_queue_wait_ms: metrics.scan_pipeline_output_queue_wait.as_secs_f64()
            * 1_000.0,
        aggregate_lane_dispatch_queue_wait_ms: metrics
            .aggregate_lane_dispatch_queue_wait
            .as_secs_f64()
            * 1_000.0,
        aggregate_partial_output_queue_wait_ms: metrics
            .aggregate_partial_output_queue_wait
            .as_secs_f64()
            * 1_000.0,
        barrier_wait_ms: metrics.barrier_wait.as_secs_f64() * 1_000.0,
        query_admission_wait_ms: metrics.query_admission_wait.as_secs_f64() * 1_000.0,
        sql_parse_time_ms: metrics.sql_parse_time.as_secs_f64() * 1_000.0,
        table_function_prepare_time_ms: metrics.table_function_prepare_time.as_secs_f64() * 1_000.0,
        bind_time_ms: metrics.bind_time.as_secs_f64() * 1_000.0,
        provider_prepare_time_ms: metrics.provider_prepare_time.as_secs_f64() * 1_000.0,
        optimize_time_ms: metrics.optimize_time.as_secs_f64() * 1_000.0,
        native_verification_time_ms: metrics.native_verification_time.as_secs_f64() * 1_000.0,
        native_full_verification_segments: metrics.native_full_verification_segments,
        csv_source_io_time_ms: metrics.csv_source_io_time.as_secs_f64() * 1_000.0,
        csv_framing_time_ms: metrics.csv_framing_time.as_secs_f64() * 1_000.0,
        csv_decode_compute_time_ms: metrics.csv_decode_compute_time.as_secs_f64() * 1_000.0,
        spill_read_bytes: metrics.spill_read_bytes,
        spill_write_bytes: metrics.spill_write_bytes,
        join_candidate_pairs: metrics.join_candidate_pairs,
        operators: metrics
            .operators
            .into_iter()
            .map(|operator| OperatorRun {
                id: operator.id,
                parent_id: operator.parent_id,
                name: operator.name,
                input_rows: operator.input_rows,
                input_batches: operator.input_batches,
                output_rows: operator.output_rows,
                output_batches: operator.output_batches,
                output_bytes: operator.output_bytes,
                elapsed_ms: operator.elapsed.as_secs_f64() * 1_000.0,
                wait_ms: operator.wait.as_secs_f64() * 1_000.0,
            })
            .collect(),
    })
}

fn start_skew_ms(queries: &[QueryRun]) -> f64 {
    let minimum = queries
        .iter()
        .map(|query| query.start_offset_ms)
        .reduce(f64::min)
        .unwrap_or(0.0);
    let maximum = queries
        .iter()
        .map(|query| query.start_offset_ms)
        .reduce(f64::max)
        .unwrap_or(minimum);
    maximum - minimum
}

fn write_json(value: &impl serde::Serialize) -> Result<(), String> {
    let mut stdout = io::stdout().lock();
    serde_json::to_writer(&mut stdout, value)
        .map_err(|error| format!("cannot encode worker response: {error}"))?;
    stdout
        .write_all(b"\n")
        .and_then(|_| stdout.flush())
        .map_err(|error| format!("cannot write worker response: {error}"))
}
