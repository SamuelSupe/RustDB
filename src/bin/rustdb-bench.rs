use std::{fs::File, hint::black_box, io::Read, path::PathBuf, time::Instant};

use clap::Parser;
use futures::StreamExt;
use rustdb::{Engine, EngineConfig, Error, Result};
use serde::Serialize;
use sha2::{Digest, Sha256};
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

    #[arg(long = "spill-directory", alias = "temp-dir")]
    spill_directory: Option<PathBuf>,

    #[arg(long)]
    s3_endpoint: Option<String>,

    #[arg(long)]
    s3_region: Option<String>,

    #[arg(long)]
    s3_path_style: bool,

    #[arg(long)]
    s3_allow_http: bool,

    /// Fail the run unless every measured query spills at least one byte.
    #[arg(long)]
    require_spill: bool,

    /// Source/build identifier recorded in the JSON report.
    #[arg(long, default_value = "unrecorded")]
    build_id: String,

    /// Cargo profile used to compile this benchmark executable.
    #[arg(long, default_value = "unrecorded")]
    build_profile: String,

    /// RUSTFLAGS used to compile this benchmark executable.
    #[arg(long, default_value = "unrecorded", allow_hyphen_values = true)]
    build_rustflags: String,

    /// Rust compiler version used to compile this benchmark executable.
    #[arg(long, default_value = "unrecorded")]
    rustc_version: String,

    /// Host CPU model supplied by the benchmark harness.
    #[arg(long)]
    cpu_model: Option<String>,
}

#[derive(Debug, Serialize)]
struct BenchmarkReport {
    engine_version: &'static str,
    build_id: String,
    binary_sha256: String,
    build: BuildReport,
    query_file: String,
    warmup: usize,
    iterations: usize,
    config: ConfigReport,
    environment: EnvironmentReport,
    p50_ms: f64,
    p95_ms: f64,
    runs: Vec<RunReport>,
}

#[derive(Debug, Serialize)]
struct BuildReport {
    cargo_profile: String,
    rustflags: String,
    rustc_version: String,
}

#[derive(Debug, Serialize)]
struct ConfigReport {
    memory_limit_bytes: usize,
    compute_threads: usize,
    batch_size: usize,
    io_concurrency: usize,
    metadata_cache_bytes: usize,
}

#[derive(Debug, Serialize)]
struct EnvironmentReport {
    os: &'static str,
    arch: &'static str,
    cpu_model: String,
    logical_cpus: usize,
    total_memory_bytes: u64,
}

#[derive(Debug, Serialize)]
struct RunReport {
    query_id: String,
    elapsed_ms: f64,
    first_batch_ms: Option<f64>,
    rows: u64,
    batches: u64,
    rows_per_second: f64,
    scanned_rows: u64,
    scanned_bytes: u64,
    peak_memory_bytes: u64,
    peak_active_lanes: u64,
    scheduler_wait_ms: f64,
    rss_bytes_after: Option<u64>,
    spill_bytes: u64,
    spill_read_bytes: u64,
    spill_write_bytes: u64,
    spill_files: u64,
    spill_quota_rejections: u64,
    spill_partitions: u64,
    parquet_page_index_bytes_read: u64,
    parquet_bloom_filter_bytes_read: u64,
    parquet_pages_pruned: u64,
    parquet_page_rows_pruned: u64,
    parquet_bloom_row_groups_pruned: u64,
    parquet_pruning_budget_skips: u64,
    s3_requests: u64,
    s3_bytes_transferred: u64,
    spill_cleaned: bool,
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
    let binary_sha256 = executable_sha256()?;
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
    if let Some(directory) = args.spill_directory {
        config.spill.directory = directory;
    }
    config.s3.endpoint = args.s3_endpoint;
    config.s3.region = args.s3_region;
    config.s3.force_path_style = args.s3_path_style;
    config.s3.allow_http = args.s3_allow_http;
    let report_config = ConfigReport {
        memory_limit_bytes: config.memory_limit,
        compute_threads: config.compute_threads,
        batch_size: config.batch_size,
        io_concurrency: config.io_concurrency,
        metadata_cache_bytes: config.metadata_cache_bytes,
    };
    let temp_dir = config.spill.directory.clone();
    let memory_limit = config.memory_limit;
    let session = Engine::new(config)?.session();

    for _ in 0..args.warmup {
        black_box(run_once(&session, &sql, &temp_dir, memory_limit, args.require_spill).await?);
    }
    let mut runs = Vec::with_capacity(args.iterations);
    for _ in 0..args.iterations {
        runs.push(run_once(&session, &sql, &temp_dir, memory_limit, args.require_spill).await?);
    }
    let mut elapsed: Vec<_> = runs.iter().map(|run| run.elapsed_ms).collect();
    elapsed.sort_by(f64::total_cmp);
    let environment = environment_report(args.cpu_model);
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
        environment,
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

fn executable_sha256() -> Result<String> {
    let path = std::env::current_exe().map_err(|error| Error::io(None, error))?;
    file_sha256(&path)
}

fn file_sha256(path: &std::path::Path) -> Result<String> {
    let mut executable =
        File::open(path).map_err(|error| Error::io(Some(path.to_path_buf()), error))?;
    let mut digest = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let bytes = executable
            .read(&mut buffer)
            .map_err(|error| Error::io(Some(path.to_path_buf()), error))?;
        if bytes == 0 {
            break;
        }
        digest.update(&buffer[..bytes]);
    }
    Ok(format!("{:x}", digest.finalize()))
}

async fn run_once(
    session: &rustdb::Session,
    sql: &str,
    temp_dir: &std::path::Path,
    memory_limit: usize,
    require_spill: bool,
) -> Result<RunReport> {
    let started = Instant::now();
    let mut result = session.execute(sql).await?;
    let query_id = result.query_id();
    let query_dir = temp_dir.join(format!("query-{query_id}"));
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
    drop(result);
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
    if query_dir.exists() {
        return Err(Error::Execution(format!(
            "query {query_id} left spill directory {} behind",
            query_dir.display()
        )));
    }
    Ok(RunReport {
        query_id: query_id.to_string(),
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
        peak_active_lanes: metrics.peak_active_lanes,
        scheduler_wait_ms: metrics.scheduler_wait.as_secs_f64() * 1_000.0,
        rss_bytes_after: current_rss_bytes(),
        spill_bytes: metrics.spill_bytes,
        spill_read_bytes: metrics.spill_read_bytes,
        spill_write_bytes: metrics.spill_write_bytes,
        spill_files: metrics.spill_files,
        spill_quota_rejections: metrics.spill_quota_rejections,
        spill_partitions: metrics.spill_partitions,
        parquet_page_index_bytes_read: metrics.parquet_page_index_bytes_read,
        parquet_bloom_filter_bytes_read: metrics.parquet_bloom_filter_bytes_read,
        parquet_pages_pruned: metrics.parquet_pages_pruned,
        parquet_page_rows_pruned: metrics.parquet_page_rows_pruned,
        parquet_bloom_row_groups_pruned: metrics.parquet_bloom_row_groups_pruned,
        parquet_pruning_budget_skips: metrics.parquet_pruning_budget_skips,
        s3_requests: metrics.s3_requests,
        s3_bytes_transferred: metrics.s3_bytes_transferred,
        spill_cleaned: true,
    })
}

fn environment_report(cpu_model: Option<String>) -> EnvironmentReport {
    let system = System::new_all();
    EnvironmentReport {
        os: std::env::consts::OS,
        arch: std::env::consts::ARCH,
        cpu_model: cpu_model
            .filter(|model| !model.trim().is_empty())
            .or_else(|| {
                system
                    .cpus()
                    .first()
                    .map(|cpu| cpu.brand().to_owned())
                    .filter(|model| !model.trim().is_empty())
            })
            .unwrap_or_else(|| "unknown".to_owned()),
        logical_cpus: std::thread::available_parallelism()
            .map(usize::from)
            .unwrap_or(1),
        total_memory_bytes: system.total_memory(),
    }
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
    use super::{file_sha256, percentile};

    #[test]
    fn percentile_uses_nearest_rank() {
        assert_eq!(percentile(&[1.0, 2.0, 3.0, 4.0], 0.50), 3.0);
        assert_eq!(percentile(&[1.0, 2.0, 3.0, 4.0], 0.95), 4.0);
    }

    #[test]
    fn file_digest_is_lowercase_sha256() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("payload");
        std::fs::write(&path, b"abc").unwrap();
        assert_eq!(
            file_sha256(&path).unwrap(),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
    }
}
