#[path = "rustdb/args.rs"]
mod args;
#[path = "rustdb/output.rs"]
mod output;
#[path = "rustdb/repl.rs"]
mod repl;
#[path = "rustdb/sql_input.rs"]
mod sql_input;

use clap::Parser;

use args::Args;
use rustdb::{Engine, EngineConfig, Error, Result};

fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "warn".into()),
        )
        .with_writer(std::io::stderr)
        .init();

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
    if let Some(metadata_cache) = args.metadata_cache {
        config.metadata_cache_bytes = metadata_cache;
    }
    if let Some(bytes) = args.csv_target_morsel_bytes {
        config.csv_scan.target_morsel_bytes = bytes;
    }
    if args.no_csv_parallel_single_file {
        config.csv_scan.parallel_single_file = false;
    }
    if let Some(mode) = args.parquet_page_index {
        config.parquet_scan.page_index = mode.into();
    }
    if let Some(mode) = args.parquet_bloom_filter {
        config.parquet_scan.bloom_filter = mode.into();
    }
    if let Some(bytes) = args.parquet_pruning_metadata {
        config.parquet_scan.max_pruning_metadata_bytes = bytes;
    }
    if let Some(max_concurrent_queries) = args.max_concurrent_queries {
        config.max_concurrent_queries = max_concurrent_queries;
    }
    if let Some(directory) = args.spill_directory {
        config.spill.directory = directory;
    }
    if let Some(limit) = args.spill_engine_limit {
        config.spill.engine_limit_bytes = Some(u64::try_from(limit).unwrap_or(u64::MAX));
    }
    if let Some(limit) = args.spill_query_limit {
        config.spill.query_limit_bytes = Some(u64::try_from(limit).unwrap_or(u64::MAX));
    }
    if let Some(bytes) = args.spill_min_free_bytes {
        config.spill.min_free_bytes = u64::try_from(bytes).unwrap_or(u64::MAX);
    }
    if let Some(threads) = args.spill_io_threads {
        config.spill.io_threads = threads;
    }
    if let Some(bytes) = args.spill_partition_target_bytes {
        config.execution.spill_partition_target_bytes = Some(bytes);
    }
    if let Some(depth) = args.max_repartition_depth {
        config.execution.max_repartition_depth = depth;
    }
    if let Some(amplification) = args.max_spill_write_amplification {
        config.execution.max_spill_write_amplification = Some(amplification);
    }
    if let Some(bytes) = args.runtime_filter_bytes {
        config.execution.runtime_filter_bytes = bytes;
    }
    config.s3.region = args.s3_region;
    config.s3.endpoint = args.s3_endpoint;
    config.s3.force_path_style = args.s3_path_style;
    config.s3.allow_http = args.s3_allow_http;
    config.s3.anonymous = args.s3_anonymous;

    let session = Engine::new(config)?.session();
    let csv_null = args.csv_null.as_deref();
    match (args.command, args.file) {
        (Some(sql), None) => {
            execute_statements(&session, &sql, args.format, csv_null, args.metrics).await
        }
        (None, Some(path)) => {
            let sql = tokio::fs::read_to_string(&path)
                .await
                .map_err(|error| Error::io(Some(path), error))?;
            execute_statements(&session, &sql, args.format, csv_null, args.metrics).await
        }
        (None, None) => repl::run(&session, args.format, csv_null, args.metrics).await,
        (Some(_), Some(_)) => unreachable!("clap rejects conflicting arguments"),
    }
}

pub(crate) async fn execute_statements(
    session: &rustdb::Session,
    sql: &str,
    format: args::OutputFormat,
    csv_null: Option<&str>,
    metrics: bool,
) -> Result<()> {
    let statements = sql_input::parse_statements(sql)?;
    if statements.is_empty() {
        return Err(Error::InvalidArgument("SQL input is empty".to_owned()));
    }
    for statement in statements {
        repl::execute(session, &statement, format, csv_null, metrics).await?;
    }
    Ok(())
}
