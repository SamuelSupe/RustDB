use std::{hint::black_box, path::PathBuf, time::Instant};

use clap::Parser;
use futures::StreamExt;
use rustdb::{Engine, EngineConfig, Error, Result};
use serde::Serialize;
use sysinfo::{Pid, ProcessesToUpdate, System};

#[derive(Debug, Parser)]
#[command(about = "Run a repeatable RustDB SQL benchmark and emit JSON")]
struct Args {
    /// SQL file containing exactly one query. File functions make the dataset explicit.
    #[arg(long)]
    query: PathBuf,

    #[arg(long, default_value_t = 1)]
    warmup: usize,

    #[arg(long, default_value_t = 5)]
    iterations: usize,

    /// Engine memory limit in bytes.
    #[arg(long)]
    memory_limit: Option<usize>,

    #[arg(long)]
    threads: Option<usize>,

    #[arg(long)]
    batch_size: Option<usize>,

    #[arg(long)]
    io_concurrency: Option<usize>,

    /// Parquet metadata cache size in bytes; use zero for uncached runs.
    #[arg(long)]
    metadata_cache_bytes: Option<usize>,

    #[arg(long)]
    temp_dir: Option<PathBuf>,

    #[arg(long)]
    s3_endpoint: Option<String>,

    #[arg(long)]
    s3_region: Option<String>,

    #[arg(long)]
    s3_path_style: bool,

    #[arg(long)]
    s3_allow_http: bool,
}

#[derive(Debug, Serialize)]
struct BenchmarkReport {
    query_file: String,
    warmup: usize,
    iterations: usize,
    p50_ms: f64,
    p95_ms: f64,
    runs: Vec<RunReport>,
}

#[derive(Debug, Serialize)]
struct RunReport {
    elapsed_ms: f64,
    first_batch_ms: Option<f64>,
    rows: u64,
    batches: u64,
    rows_per_second: f64,
    scanned_rows: u64,
    scanned_bytes: u64,
    peak_memory_bytes: u64,
    rss_bytes_after: Option<u64>,
    spill_bytes: u64,
    spill_partitions: u64,
    s3_requests: u64,
    s3_bytes_transferred: u64,
}

fn main() {
    let args = Args::parse();
    let runtime = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(error) => {
            eprintln!("error: failed to create runtime: {error}");
            std::process::exit(1);
        }
    };
    if let Err(error) = runtime.block_on(run(args)) {
        eprintln!("error: {error}");
        std::process::exit(1);
    }
}

async fn run(args: Args) -> Result<()> {
    if args.iterations == 0 {
        return Err(Error::InvalidArgument(
            "iterations must be greater than zero".to_owned(),
        ));
    }
    let sql = tokio::fs::read_to_string(&args.query)
        .await
        .map_err(|error| Error::io(Some(args.query.clone()), error))?;
    let mut config = EngineConfig::default();
    if let Some(memory_limit) = args.memory_limit {
        config.memory_limit = memory_limit;
    }
    if let Some(threads) = args.threads {
        config.compute_threads = threads;
    }
    if let Some(batch_size) = args.batch_size {
        config.batch_size = batch_size;
    }
    if let Some(io_concurrency) = args.io_concurrency {
        config.io_concurrency = io_concurrency;
    }
    if let Some(metadata_cache_bytes) = args.metadata_cache_bytes {
        config.metadata_cache_bytes = metadata_cache_bytes;
    }
    if let Some(temp_dir) = args.temp_dir {
        config.temp_dir = temp_dir;
    }
    config.s3.endpoint = args.s3_endpoint;
    config.s3.region = args.s3_region;
    config.s3.force_path_style = args.s3_path_style;
    config.s3.allow_http = args.s3_allow_http;
    let session = Engine::new(config)?.session();

    for _ in 0..args.warmup {
        black_box(run_once(&session, &sql).await?);
    }
    let mut runs = Vec::with_capacity(args.iterations);
    for _ in 0..args.iterations {
        runs.push(run_once(&session, &sql).await?);
    }
    let mut elapsed: Vec<_> = runs.iter().map(|run| run.elapsed_ms).collect();
    elapsed.sort_by(f64::total_cmp);
    let report = BenchmarkReport {
        query_file: args.query.display().to_string(),
        warmup: args.warmup,
        iterations: args.iterations,
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

async fn run_once(session: &rustdb::Session, sql: &str) -> Result<RunReport> {
    let started = Instant::now();
    let mut result = session.execute(sql).await?;
    let metrics = result.metrics();
    let mut rows = 0_u64;
    let mut batches = 0_u64;
    let mut first_batch_ms = None;
    while let Some(batch) = result.stream().next().await {
        let batch = batch?;
        first_batch_ms.get_or_insert_with(|| started.elapsed().as_secs_f64() * 1_000.0);
        rows = rows.saturating_add(u64::try_from(batch.num_rows()).unwrap_or(u64::MAX));
        batches = batches.saturating_add(1);
        black_box(batch);
    }
    let elapsed_ms = started.elapsed().as_secs_f64() * 1_000.0;
    let metrics = metrics.snapshot();
    Ok(RunReport {
        elapsed_ms,
        first_batch_ms,
        rows,
        batches,
        rows_per_second: if elapsed_ms == 0.0 {
            0.0
        } else {
            rows as f64 * 1_000.0 / elapsed_ms
        },
        scanned_rows: metrics.rows_scanned,
        scanned_bytes: metrics.bytes_scanned,
        peak_memory_bytes: metrics.peak_memory_bytes,
        rss_bytes_after: current_rss_bytes(),
        spill_bytes: metrics.spill_bytes,
        spill_partitions: metrics.spill_partitions,
        s3_requests: metrics.s3_requests,
        s3_bytes_transferred: metrics.s3_bytes_transferred,
    })
}

fn current_rss_bytes() -> Option<u64> {
    let pid = Pid::from_u32(std::process::id());
    let mut system = System::new();
    system.refresh_processes(ProcessesToUpdate::Some(&[pid]), false);
    system.process(pid).map(sysinfo::Process::memory)
}

fn percentile(sorted: &[f64], percentile: f64) -> f64 {
    let index = ((sorted.len() - 1) as f64 * percentile).ceil() as usize;
    sorted[index.min(sorted.len() - 1)]
}

#[cfg(test)]
mod tests {
    use super::percentile;

    #[test]
    fn percentile_uses_nearest_rank() {
        assert_eq!(percentile(&[1.0, 2.0, 3.0, 4.0], 0.50), 3.0);
        assert_eq!(percentile(&[1.0, 2.0, 3.0, 4.0], 0.95), 4.0);
    }
}
