#[path = "rustdb/args.rs"]
mod args;
#[path = "rustdb/output.rs"]
mod output;
#[path = "rustdb/repl.rs"]
mod repl;

use clap::Parser;
use sqlparser::{dialect::DuckDbDialect, parser::Parser as SqlParser};

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
    config.s3.region = args.s3_region;
    config.s3.endpoint = args.s3_endpoint;
    config.s3.force_path_style = args.s3_path_style;
    config.s3.allow_http = args.s3_allow_http;
    config.s3.anonymous = args.s3_anonymous;

    let session = Engine::new(config)?.session();
    match (args.command, args.file) {
        (Some(sql), None) => execute_statements(&session, &sql, args.format, args.metrics).await,
        (None, Some(path)) => {
            let sql = tokio::fs::read_to_string(&path)
                .await
                .map_err(|error| Error::io(Some(path), error))?;
            execute_statements(&session, &sql, args.format, args.metrics).await
        }
        (None, None) => repl::run(&session, args.format, args.metrics).await,
        (Some(_), Some(_)) => unreachable!("clap rejects conflicting arguments"),
    }
}

pub(crate) async fn execute_statements(
    session: &rustdb::Session,
    sql: &str,
    format: args::OutputFormat,
    metrics: bool,
) -> Result<()> {
    let statements = SqlParser::parse_sql(&DuckDbDialect {}, sql)?;
    if statements.is_empty() {
        return Err(Error::InvalidArgument("SQL input is empty".to_owned()));
    }
    for statement in statements {
        repl::execute(session, &statement.to_string(), format, metrics).await?;
    }
    Ok(())
}
