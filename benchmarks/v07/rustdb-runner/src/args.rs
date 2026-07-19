use std::path::PathBuf;

use clap::Parser;

#[derive(Debug, Parser)]
#[command(about = "Long-lived RustDB worker for the v0.7 comparison harness")]
pub(crate) struct Args {
    #[arg(long, default_value_t = 4)]
    pub(crate) threads: usize,

    #[arg(long, default_value_t = 2_147_483_648)]
    pub(crate) memory_limit: usize,

    #[arg(long, default_value_t = 1)]
    pub(crate) concurrency: usize,

    #[arg(long, default_value_t = 8192)]
    pub(crate) batch_size: usize,

    #[arg(long, default_value_t = 0)]
    pub(crate) metadata_cache_bytes: usize,

    #[arg(long)]
    pub(crate) spill_directory: PathBuf,

    #[arg(long)]
    pub(crate) build_id: String,

    #[arg(long)]
    pub(crate) database: Option<PathBuf>,

    #[arg(long)]
    pub(crate) s3_endpoint: Option<String>,

    #[arg(long)]
    pub(crate) s3_region: Option<String>,

    #[arg(long)]
    pub(crate) s3_path_style: bool,

    #[arg(long)]
    pub(crate) s3_allow_http: bool,
}
