use std::{net::SocketAddr, path::PathBuf};

use clap::{Args as ClapArgs, Subcommand, ValueEnum};

use super::args::OutputFormat;

#[derive(Debug, Subcommand)]
pub enum Operation {
    /// Validate a versioned RustDB service configuration without starting a server.
    Config {
        #[command(subcommand)]
        command: ConfigOperation,
    },
    /// Inspect a Native database without opening or modifying it.
    Native {
        #[command(subcommand)]
        command: NativeOperation,
    },
    /// Validate that a Native database uses the current Beta format epoch.
    Migrate { database: PathBuf },
    /// Idempotently import CSV or Parquet into a new persistent Native table.
    Import(ImportArgs),
    /// Create a verified Native plus HTTP-control-state bundle.
    Backup {
        database: PathBuf,
        destination: String,
        /// HTTP service state root. Defaults to the platform RustDB state root.
        #[arg(long)]
        state_root: Option<PathBuf>,
    },
    /// Validate a local or S3 service backup without restoring it.
    BackupCheck {
        backup: String,
        /// Emit the validation summary as JSON.
        #[arg(long)]
        json: bool,
    },
    /// Restore a verified service backup into fresh database and state targets.
    Restore {
        backup: String,
        database: PathBuf,
        /// Fresh HTTP service state root. Defaults to the platform RustDB state root.
        #[arg(long)]
        state_root: Option<PathBuf>,
    },
    /// Produce a redacted, versioned local diagnostics report.
    Diagnostics {
        #[arg(long)]
        database: PathBuf,
        /// Atomically write a private JSON file instead of stdout.
        #[arg(long)]
        output: Option<PathBuf>,
    },
    /// Serve one Native database through the read-only HTTPS shell protocol.
    Serve(Box<ServeArgs>),
    /// Check, repair, or administer local HTTP service state.
    Service {
        #[command(subcommand)]
        command: ServiceOperation,
    },
    /// Connect the CLI to a named remote HTTPS shell profile.
    Shell(ShellArgs),
    /// Import, export, or rotate HTTPS shell connection material.
    Profile {
        #[command(subcommand)]
        command: ProfileOperation,
    },
    /// Manage local HTTP principals while the server is stopped.
    Principal {
        #[command(subcommand)]
        command: PrincipalOperation,
    },
    /// Rotate or revoke local HTTP credentials while the server is stopped.
    Token {
        #[command(subcommand)]
        command: TokenOperation,
    },
    /// Manage persistent server-side CSV and Parquet source registrations.
    Datasource {
        #[command(subcommand)]
        command: DatasourceOperation,
    },
}

#[derive(Debug, ClapArgs)]
pub struct ImportArgs {
    /// Persistent Native database directory.
    #[arg(long)]
    pub database: PathBuf,
    /// New Native table name. Existing catalog objects are never overwritten.
    #[arg(long)]
    pub table: String,
    /// Local, file://, or s3:// source URI or pattern.
    #[arg(long)]
    pub location: String,
    #[arg(long, value_enum)]
    pub format: NativeImportFormatArg,
    /// Durable idempotency key. Reusing it with different inputs is rejected.
    #[arg(long)]
    pub import_id: String,
    #[arg(long, value_enum, default_value_t)]
    pub header: CsvHeaderArg,
    #[arg(long, value_parser = parse_ascii_byte, default_value = ",")]
    pub delimiter: u8,
    #[arg(long, value_enum, default_value_t)]
    pub compression: CsvCompressionArg,
    /// Emit the durable receipt as JSON.
    #[arg(long)]
    pub json: bool,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
pub enum NativeImportFormatArg {
    Csv,
    Parquet,
}

fn parse_ascii_byte(value: &str) -> Result<u8, String> {
    let bytes = value.as_bytes();
    if bytes.len() != 1 || !bytes[0].is_ascii() {
        return Err("delimiter must be exactly one ASCII byte".to_owned());
    }
    Ok(bytes[0])
}

#[derive(Debug, Subcommand)]
pub enum NativeOperation {
    /// Verify marker, catalog, manifests, segments, sidecars, and delete vectors.
    Check {
        database: PathBuf,
        /// Emit one structured JSON report.
        #[arg(long)]
        json: bool,
    },
    /// Plan a conservative repair; no bytes are changed without --apply.
    Repair {
        database: PathBuf,
        /// Apply the revalidated plan after creating a metadata backup.
        #[arg(long)]
        apply: bool,
        /// Emit one structured JSON plan or report.
        #[arg(long)]
        json: bool,
    },
}

#[derive(Debug, Subcommand)]
pub enum ConfigOperation {
    /// Parse and validate a service configuration. Defaults to ./rustdb.toml.
    Validate {
        #[arg(value_name = "PATH")]
        path: Option<PathBuf>,
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
    pub query_memory_limit: Option<usize>,
    #[arg(long, value_parser = super::args::parse_bytes)]
    pub query_spill_limit: Option<usize>,
    #[arg(long, value_parser = super::args::parse_bytes)]
    pub query_result_limit: Option<usize>,
    #[arg(long)]
    pub principal_max_running: Option<usize>,
    #[arg(long)]
    pub principal_max_queued: Option<usize>,
    #[arg(long, value_parser = super::args::parse_bytes)]
    pub principal_memory_limit: Option<usize>,
    #[arg(long, value_parser = super::args::parse_bytes)]
    pub principal_spill_limit: Option<usize>,
    #[arg(long, value_parser = super::args::parse_bytes)]
    pub principal_result_limit: Option<usize>,
    /// Weighted-fair share for every principal. Roles do not affect it.
    #[arg(long)]
    pub principal_weight: Option<u32>,
    #[arg(long, value_parser = super::args::parse_bytes)]
    pub memory_limit: Option<usize>,
    #[arg(long)]
    pub threads: Option<usize>,
    #[arg(long, value_parser = super::args::parse_bytes)]
    pub spill_engine_limit: Option<usize>,
    #[arg(long, value_parser = super::args::parse_bytes)]
    pub spill_query_limit: Option<usize>,
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
    /// Fixed thread count for blocking HTTP service-state and result I/O.
    #[arg(long)]
    pub service_io_threads: Option<usize>,
    /// Local-only administration socket. Defaults inside the service state directory.
    #[arg(long)]
    pub admin_socket: Option<PathBuf>,
    /// Interval for checking and hot-reloading near-expiry TLS leaves.
    #[arg(long)]
    pub tls_renew_interval_secs: Option<u64>,
    /// Explicitly disable HTTP authentication (development only).
    #[arg(long)]
    pub no_auth: bool,
}

#[derive(Debug, Clone, ClapArgs)]
pub struct ServiceStateArgs {
    #[arg(long)]
    pub database: PathBuf,
    #[arg(long)]
    pub state_root: Option<PathBuf>,
    #[arg(long)]
    pub result_directory: Option<PathBuf>,
}

#[derive(Debug, Clone, ClapArgs)]
pub struct ServiceAdminArgs {
    #[arg(long)]
    pub database: PathBuf,
    #[arg(long)]
    pub state_root: Option<PathBuf>,
    #[arg(long)]
    pub admin_socket: Option<PathBuf>,
}

#[derive(Debug, Subcommand)]
pub enum ServiceOperation {
    /// Read-only verification of principals, query journal, and stored results.
    Check {
        #[command(flatten)]
        paths: ServiceStateArgs,
        #[arg(long)]
        json: bool,
    },
    /// Conservatively repair marker-owned service artifacts.
    Repair {
        #[command(flatten)]
        paths: ServiceStateArgs,
        /// Apply repairs. Without this flag the command is read-only.
        #[arg(long)]
        apply: bool,
        #[arg(long)]
        json: bool,
    },
    /// Read local server and query counts.
    Status {
        #[command(flatten)]
        server: ServiceAdminArgs,
    },
    /// Atomically reload the durable principal/token directory.
    ReloadTokens {
        #[command(flatten)]
        server: ServiceAdminArgs,
    },
    /// Add an overlapping token and reload it into the running server.
    RotateToken {
        #[command(flatten)]
        server: ServiceAdminArgs,
        #[arg(long)]
        principal: String,
    },
    /// Revoke one token and reload the running server.
    RevokeToken {
        #[command(flatten)]
        server: ServiceAdminArgs,
        #[arg(long)]
        token_id: String,
    },
    /// Request bounded graceful shutdown through the local socket.
    Shutdown {
        #[command(flatten)]
        server: ServiceAdminArgs,
    },
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
        /// Export this active credential instead of the current Admin profile token.
        #[arg(long)]
        token_id: Option<String>,
        /// Override the URL recorded in the last managed server profile.
        #[arg(long)]
        server_url: Option<String>,
        #[arg(long)]
        state_root: Option<PathBuf>,
    },
}

#[derive(Debug, Subcommand)]
pub enum PrincipalOperation {
    /// List principals without exposing credential digests or clear-text tokens.
    List {
        #[arg(long)]
        database: PathBuf,
        #[arg(long)]
        state_root: Option<PathBuf>,
    },
    /// Create an enabled principal and its first local profile token.
    Create {
        #[arg(long)]
        database: PathBuf,
        #[arg(long)]
        id: String,
        #[arg(long, value_enum, default_value_t)]
        role: RoleArg,
        #[arg(long)]
        state_root: Option<PathBuf>,
    },
    /// Enable or disable a principal. The final enabled admin is protected.
    SetEnabled {
        #[arg(long)]
        database: PathBuf,
        #[arg(long)]
        id: String,
        #[arg(long, value_enum)]
        state: PrincipalStateArg,
        #[arg(long)]
        state_root: Option<PathBuf>,
    },
    /// Change a principal role. The final enabled admin is protected.
    SetRole {
        #[arg(long)]
        database: PathBuf,
        #[arg(long)]
        id: String,
        #[arg(long, value_enum)]
        role: RoleArg,
        #[arg(long)]
        state_root: Option<PathBuf>,
    },
}

#[derive(Debug, Subcommand)]
pub enum TokenOperation {
    /// List token ids and lifecycle state without exposing credential material.
    List {
        #[arg(long)]
        database: PathBuf,
        /// Restrict output to one principal.
        #[arg(long)]
        principal: Option<String>,
        #[arg(long)]
        state_root: Option<PathBuf>,
    },
    /// Add an overlapping token for interruption-free rotation.
    Rotate {
        #[arg(long)]
        database: PathBuf,
        #[arg(long)]
        principal: String,
        #[arg(long)]
        state_root: Option<PathBuf>,
    },
    /// Revoke one token id and remove its local clear-text profile token.
    Revoke {
        #[arg(long)]
        database: PathBuf,
        #[arg(long)]
        token_id: String,
        #[arg(long)]
        state_root: Option<PathBuf>,
    },
}

#[derive(Clone, Copy, Debug, Default, ValueEnum)]
pub enum RoleArg {
    #[default]
    Query,
    Admin,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
pub enum PrincipalStateArg {
    Enabled,
    Disabled,
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
