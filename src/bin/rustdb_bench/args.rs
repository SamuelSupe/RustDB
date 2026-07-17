use std::path::PathBuf;

use clap::Parser;
use rustdb::{EngineConfig, Error, Result};

#[derive(Debug, Parser)]
#[command(about = "Run a repeatable RustDB SQL benchmark and emit JSON")]
pub(crate) struct Args {
    /// SQL file containing exactly one query. File functions make the dataset explicit.
    #[arg(long)]
    pub(super) query: PathBuf,

    #[arg(long, default_value_t = 1)]
    pub(super) warmup: usize,

    #[arg(long, default_value_t = 5)]
    pub(super) iterations: usize,

    /// Engine memory limit in bytes.
    #[arg(long)]
    pub(super) memory_limit: Option<usize>,

    #[arg(long)]
    pub(super) threads: Option<usize>,

    #[arg(long)]
    pub(super) batch_size: Option<usize>,

    #[arg(long)]
    pub(super) io_concurrency: Option<usize>,

    /// Parquet metadata cache size in bytes; use zero for uncached runs.
    #[arg(long)]
    pub(super) metadata_cache_bytes: Option<usize>,

    #[arg(long = "spill-directory", alias = "temp-dir")]
    pub(super) spill_directory: Option<PathBuf>,

    #[arg(long)]
    pub(super) s3_endpoint: Option<String>,

    #[arg(long)]
    pub(super) s3_region: Option<String>,

    #[arg(long)]
    pub(super) s3_path_style: bool,

    #[arg(long)]
    pub(super) s3_allow_http: bool,

    /// Fail the run unless every measured query spills at least one byte.
    #[arg(long)]
    pub(super) require_spill: bool,

    /// Process RSS sampling interval. Shorter intervals add more measurement overhead.
    #[arg(long, default_value_t = 10)]
    pub(super) rss_sample_interval_ms: u64,

    /// Source/build identifier recorded in the JSON report.
    #[arg(long, default_value = "unrecorded")]
    pub(super) build_id: String,

    /// Cargo profile used to compile this benchmark executable.
    #[arg(long, default_value = "unrecorded")]
    pub(super) build_profile: String,

    /// RUSTFLAGS used to compile this benchmark executable.
    #[arg(long, default_value = "unrecorded", allow_hyphen_values = true)]
    pub(super) build_rustflags: String,

    /// Rust compiler version used to compile this benchmark executable.
    #[arg(long, default_value = "unrecorded")]
    pub(super) rustc_version: String,

    /// Host CPU model supplied by the benchmark harness.
    #[arg(long)]
    pub(super) cpu_model: Option<String>,
}

impl Args {
    pub(super) fn validate(&self) -> Result<()> {
        if self.iterations == 0 {
            return Err(Error::InvalidArgument(
                "iterations must be greater than zero".to_owned(),
            ));
        }
        if self.rss_sample_interval_ms == 0 {
            return Err(Error::InvalidArgument(
                "rss-sample-interval-ms must be greater than zero".to_owned(),
            ));
        }
        Ok(())
    }

    pub(super) fn engine_config(&self) -> EngineConfig {
        let mut config = EngineConfig::default();
        if let Some(memory_limit) = self.memory_limit {
            config.memory_limit = memory_limit;
        }
        if let Some(threads) = self.threads {
            config.compute_threads = threads;
        }
        if let Some(batch_size) = self.batch_size {
            config.batch_size = batch_size;
        }
        if let Some(io_concurrency) = self.io_concurrency {
            config.io_concurrency = io_concurrency;
        }
        if let Some(metadata_cache_bytes) = self.metadata_cache_bytes {
            config.metadata_cache_bytes = metadata_cache_bytes;
        }
        if let Some(directory) = &self.spill_directory {
            config.spill.directory = directory.clone();
        }
        config.s3.endpoint.clone_from(&self.s3_endpoint);
        config.s3.region.clone_from(&self.s3_region);
        config.s3.force_path_style = self.s3_path_style;
        config.s3.allow_http = self.s3_allow_http;
        config
    }
}
