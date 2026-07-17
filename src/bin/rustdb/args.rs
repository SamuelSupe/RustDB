use std::path::PathBuf;

use clap::{Parser, ValueEnum};

pub const HELP_ZH: &str = include_str!("help.zh-CN.txt");

#[derive(Clone, Copy, Debug, Default, ValueEnum)]
pub enum OutputFormat {
    #[default]
    Table,
    Csv,
    Jsonl,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
pub enum PruningModeArg {
    Auto,
    Disabled,
}

impl From<PruningModeArg> for rustdb::ParquetPruningMode {
    fn from(value: PruningModeArg) -> Self {
        match value {
            PruningModeArg::Auto => Self::Auto,
            PruningModeArg::Disabled => Self::Disabled,
        }
    }
}

#[derive(Debug, Parser)]
#[command(
    name = "rustdb",
    version,
    about = "Query local and S3 CSV/Parquet files"
)]
pub struct Args {
    /// Print complete Simplified Chinese help and exit.
    #[arg(long)]
    pub help_zh: bool,

    /// Execute a SQL statement and exit.
    #[arg(short = 'c', long, conflicts_with = "file")]
    pub command: Option<String>,

    /// Execute SQL from a file and exit.
    #[arg(short = 'f', long, conflicts_with = "command")]
    pub file: Option<PathBuf>,

    /// Result rendering format.
    #[arg(long, value_enum, default_value_t)]
    pub format: OutputFormat,

    /// Text written for SQL NULL in CSV output (empty by default).
    #[arg(long)]
    pub csv_null: Option<String>,

    /// Query memory limit, for example 512MiB, 2GiB, or a byte count.
    #[arg(long, value_parser = parse_bytes)]
    pub memory_limit: Option<usize>,

    /// Number of compute worker threads.
    #[arg(long)]
    pub threads: Option<usize>,

    /// Rows targeted per Arrow execution batch.
    #[arg(long)]
    pub batch_size: Option<usize>,

    /// Maximum concurrent object-store/file morsels per scan.
    #[arg(long)]
    pub io_concurrency: Option<usize>,

    /// Parquet metadata cache limit, for example 64MiB or 0 to disable.
    #[arg(long, value_parser = parse_bytes)]
    pub metadata_cache: Option<usize>,

    /// Target decompressed bytes per record-aligned CSV parser morsel.
    #[arg(long, value_parser = parse_bytes)]
    pub csv_target_morsel_bytes: Option<usize>,

    /// Disable parallel parsing of record-aligned morsels from one CSV object.
    #[arg(long)]
    pub no_csv_parallel_single_file: bool,

    /// Parquet page-index pruning policy.
    #[arg(long, value_enum)]
    pub parquet_page_index: Option<PruningModeArg>,

    /// Parquet Bloom-filter pruning policy.
    #[arg(long, value_enum)]
    pub parquet_bloom_filter: Option<PruningModeArg>,

    /// Query-level upper bound for optional Parquet pruning metadata.
    #[arg(long, value_parser = parse_bytes)]
    pub parquet_pruning_metadata: Option<usize>,

    /// Maximum admitted queries for embedded or scripted concurrent use.
    #[arg(long)]
    pub max_concurrent_queries: Option<usize>,

    /// Directory used for query spill files.
    #[arg(long = "spill-directory", alias = "temp-dir")]
    pub spill_directory: Option<PathBuf>,

    /// Maximum Spill bytes retained by all active queries.
    #[arg(long, value_parser = parse_bytes)]
    pub spill_engine_limit: Option<usize>,

    /// Maximum Spill bytes retained by one query.
    #[arg(long, value_parser = parse_bytes)]
    pub spill_query_limit: Option<usize>,

    /// Minimum free bytes that Spill must leave on its filesystem.
    #[arg(long, value_parser = parse_bytes)]
    pub spill_min_free_bytes: Option<usize>,

    /// Number of dedicated blocking Spill I/O threads.
    #[arg(long)]
    pub spill_io_threads: Option<usize>,

    /// Target logical bytes per adaptive Spill hash partition.
    #[arg(long, value_parser = parse_bytes)]
    pub spill_partition_target_bytes: Option<usize>,

    /// Maximum recursive hash repartition depth; zero goes directly to fallback.
    #[arg(long)]
    pub max_repartition_depth: Option<usize>,

    /// Optional maximum cumulative Spill write amplification.
    #[arg(long)]
    pub max_spill_write_amplification: Option<f64>,

    /// Memory budget for Join runtime filters; zero disables them.
    #[arg(long, value_parser = parse_bytes)]
    pub runtime_filter_bytes: Option<usize>,

    /// AWS region override.
    #[arg(long)]
    pub s3_region: Option<String>,

    /// S3-compatible endpoint override.
    #[arg(long)]
    pub s3_endpoint: Option<String>,

    /// Force path-style S3 requests.
    #[arg(long)]
    pub s3_path_style: bool,

    /// Allow a plain HTTP S3 endpoint. Intended for local development only.
    #[arg(long)]
    pub s3_allow_http: bool,

    /// Do not sign S3 requests.
    #[arg(long)]
    pub s3_anonymous: bool,

    /// Print execution metrics after each query.
    #[arg(long)]
    pub metrics: bool,
}

fn parse_bytes(input: &str) -> Result<usize, String> {
    let input = input.trim();
    if input.is_empty() {
        return Err("memory size is empty".to_owned());
    }
    let split = input
        .find(|character: char| !character.is_ascii_digit())
        .unwrap_or(input.len());
    let (number, unit) = input.split_at(split);
    let number = number
        .parse::<u128>()
        .map_err(|_| format!("invalid memory size: {input}"))?;
    let multiplier = match unit.trim().to_ascii_lowercase().as_str() {
        "" | "b" => 1_u128,
        "k" | "kb" => 1_000,
        "m" | "mb" => 1_000_000,
        "g" | "gb" => 1_000_000_000,
        "kib" => 1 << 10,
        "mib" => 1 << 20,
        "gib" => 1 << 30,
        _ => return Err(format!("unsupported memory unit in: {input}")),
    };
    usize::try_from(number.saturating_mul(multiplier))
        .map_err(|_| format!("memory size is too large: {input}"))
}

#[cfg(test)]
mod tests {
    use clap::Parser;

    use super::{Args, HELP_ZH, parse_bytes};

    #[test]
    fn parses_binary_and_decimal_sizes() {
        assert_eq!(parse_bytes("64MiB").unwrap(), 64 * 1024 * 1024);
        assert_eq!(parse_bytes("2GB").unwrap(), 2_000_000_000);
        assert!(parse_bytes("12watts").is_err());
    }

    #[test]
    fn accepts_chinese_help_switch() {
        let args = Args::try_parse_from(["rustdb", "--help-zh"]).unwrap();
        assert!(args.help_zh);
        assert!(HELP_ZH.contains("使用方法"));
        assert!(HELP_ZH.contains("--s3-endpoint"));
    }
}
