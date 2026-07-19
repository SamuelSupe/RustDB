use std::{net::SocketAddr, path::PathBuf};

use clap::{Args as ClapArgs, Subcommand, ValueEnum};

use super::args::OutputFormat;

#[derive(Debug, Subcommand)]
pub enum Operation {
    /// Explicitly migrate a v0.7 Native database to the WAL-enabled v0.8 format.
    Migrate { database: PathBuf },
    /// Create a verified Native backup in a local directory or S3 prefix.
    Backup {
        database: PathBuf,
        destination: String,
    },
    /// Restore a verified local or S3 Native backup into a new database.
    Restore { backup: String, database: PathBuf },
    /// Serve one Native database through the read-only HTTPS shell protocol.
    Serve(Box<ServeArgs>),
    /// Connect the CLI to a named remote HTTPS shell profile.
    Shell(ShellArgs),
    /// Import, export, or rotate HTTPS shell connection material.
    Profile {
        #[command(subcommand)]
        command: ProfileOperation,
    },
    /// Manage persistent server-side CSV and Parquet source registrations.
    Datasource {
        #[command(subcommand)]
        command: DatasourceOperation,
    },
}

#[derive(Debug, ClapArgs)]
pub struct ServeArgs {
    #[arg(long)]
    pub database: PathBuf,
    /// Optional TOML service configuration. Defaults to ./rustdb.toml when present.
    #[arg(long)]
    pub config: Option<PathBuf>,
    #[arg(long)]
    pub listen: Option<SocketAddr>,
    #[arg(long)]
    pub advertise_url: Option<String>,
    #[arg(long)]
    pub state_root: Option<PathBuf>,
    #[arg(long)]
    pub result_directory: Option<PathBuf>,
    #[arg(long)]
    pub result_ttl_secs: Option<u64>,
    #[arg(long, value_parser = super::args::parse_bytes)]
    pub result_global_limit: Option<usize>,
    #[arg(long, value_parser = super::args::parse_bytes)]
    pub result_query_limit: Option<usize>,
    #[arg(long)]
    pub max_running: Option<usize>,
    #[arg(long)]
    pub max_queued: Option<usize>,
    #[arg(long)]
    pub max_query_time_secs: Option<u64>,
    #[arg(long, value_parser = super::args::parse_bytes)]
    pub memory_limit: Option<usize>,
    #[arg(long)]
    pub threads: Option<usize>,
    #[arg(long)]
    pub s3_region: Option<String>,
    #[arg(long)]
    pub s3_endpoint: Option<String>,
    #[arg(long)]
    pub s3_path_style: bool,
    #[arg(long)]
    pub s3_allow_http: bool,
    #[arg(long)]
    pub s3_anonymous: bool,
}

#[derive(Debug, ClapArgs)]
pub struct ShellArgs {
    #[arg(long)]
    pub profile: String,
    #[arg(short = 'c', long, conflicts_with = "file")]
    pub command: Option<String>,
    #[arg(short = 'f', long, conflicts_with = "command")]
    pub file: Option<PathBuf>,
    #[arg(long, value_enum, default_value_t)]
    pub format: OutputFormat,
    #[arg(long)]
    pub csv_null: Option<String>,
    #[arg(long)]
    pub metrics: bool,
    /// Request a query timeout shorter than the server maximum.
    #[arg(long)]
    pub timeout_ms: Option<u64>,
}

#[derive(Debug, Subcommand)]
pub enum ProfileOperation {
    /// Import an offline connection bundle as a named profile.
    Import {
        bundle: PathBuf,
        #[arg(long)]
        name: String,
        #[arg(long)]
        profile_root: Option<PathBuf>,
    },
    /// Export the current server-generated connection bundle.
    Export {
        #[arg(long)]
        database: PathBuf,
        #[arg(long)]
        output: PathBuf,
        #[arg(long)]
        state_root: Option<PathBuf>,
    },
    /// Rotate the single server Bearer Token while the server is stopped.
    RotateToken {
        #[arg(long)]
        database: PathBuf,
        #[arg(long)]
        state_root: Option<PathBuf>,
    },
}

#[derive(Debug, Subcommand)]
pub enum DatasourceOperation {
    AddCsv {
        #[arg(long)]
        database: PathBuf,
        #[arg(long)]
        name: String,
        #[arg(long = "location", required = true)]
        locations: Vec<String>,
        #[arg(long, value_enum, default_value_t)]
        header: CsvHeaderArg,
        #[arg(long, value_enum, default_value_t)]
        compression: CsvCompressionArg,
        #[arg(long, default_value = ",")]
        delimiter: char,
    },
    AddParquet {
        #[arg(long)]
        database: PathBuf,
        #[arg(long)]
        name: String,
        #[arg(long = "location", required = true)]
        locations: Vec<String>,
        #[arg(long, value_enum, default_value_t)]
        schema_mode: ParquetSchemaModeArg,
        #[arg(long)]
        hive_partitioning: bool,
    },
    List {
        #[arg(long)]
        database: PathBuf,
    },
    Refresh {
        #[arg(long)]
        database: PathBuf,
        #[arg(long)]
        name: String,
    },
    Remove {
        #[arg(long)]
        database: PathBuf,
        #[arg(long)]
        name: String,
    },
}

#[derive(Clone, Copy, Debug, Default, ValueEnum)]
pub enum CsvHeaderArg {
    #[default]
    Auto,
    Present,
    Absent,
}

#[derive(Clone, Copy, Debug, Default, ValueEnum)]
pub enum CsvCompressionArg {
    #[default]
    Auto,
    None,
    Gzip,
    Zstd,
}

#[derive(Clone, Copy, Debug, Default, ValueEnum)]
pub enum ParquetSchemaModeArg {
    #[default]
    Strict,
    Union,
    SafeWidening,
}
