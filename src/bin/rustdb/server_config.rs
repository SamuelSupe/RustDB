use std::{fs, net::SocketAddr, path::PathBuf, time::Duration};

use rustdb::{EngineConfig, Error, Result, http_shell::HttpServerConfig};
use serde::Deserialize;

use super::{
    args::{Args, parse_bytes},
    config,
    operations::ServeArgs,
};

#[derive(Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct FileConfig {
    server: FileServer,
    engine: FileEngine,
}

#[derive(Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct FileServer {
    listen: Option<String>,
    advertise_url: Option<String>,
    state_root: Option<PathBuf>,
    result_directory: Option<PathBuf>,
    result_ttl_secs: Option<u64>,
    result_global_limit: Option<String>,
    result_query_limit: Option<String>,
    max_running: Option<usize>,
    max_queued: Option<usize>,
    max_query_time_secs: Option<u64>,
}

#[derive(Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct FileEngine {
    memory_limit: Option<String>,
    threads: Option<usize>,
    s3_region: Option<String>,
    s3_endpoint: Option<String>,
    s3_path_style: Option<bool>,
    s3_allow_http: Option<bool>,
    s3_anonymous: Option<bool>,
}

pub(super) fn load(args: &ServeArgs, global: &Args) -> Result<(EngineConfig, HttpServerConfig)> {
    let file = load_file(args.config.as_ref())?;
    let mut server = HttpServerConfig::default();
    let mut engine = EngineConfig::default();

    apply_file(&mut server, &mut engine, file)?;
    apply_environment(&mut server, &mut engine)?;
    config::apply_engine_args(&mut engine, global);
    apply_cli(&mut server, &mut engine, args);
    validate(&server, &engine)?;
    engine.max_concurrent_queries = server.query.max_running;
    Ok((engine, server))
}

fn load_file(explicit: Option<&PathBuf>) -> Result<FileConfig> {
    let path = explicit.cloned().or_else(|| {
        PathBuf::from("rustdb.toml")
            .exists()
            .then(|| PathBuf::from("rustdb.toml"))
    });
    let Some(path) = path else {
        return Ok(FileConfig::default());
    };
    let contents = fs::read_to_string(&path).map_err(|error| Error::io(path.clone(), error))?;
    toml::from_str(&contents).map_err(|error| {
        Error::InvalidArgument(format!(
            "invalid service config {}: {error}",
            path.display()
        ))
    })
}

fn apply_file(
    server: &mut HttpServerConfig,
    engine: &mut EngineConfig,
    file: FileConfig,
) -> Result<()> {
    if let Some(value) = file.server.listen {
        server.listen = parse_address("server.listen", &value)?;
    }
    assign_some(&mut server.advertise_url, file.server.advertise_url);
    assign_some(&mut server.result_directory, file.server.result_directory);
    if let Some(value) = file.server.result_ttl_secs {
        server.result_ttl = Duration::from_secs(value);
    }
    if let Some(value) = file.server.result_global_limit {
        server.result_global_limit_bytes =
            Some(parse_size_u64("server.result_global_limit", &value)?);
    }
    if let Some(value) = file.server.result_query_limit {
        server.result_query_limit_bytes =
            Some(parse_size_u64("server.result_query_limit", &value)?);
    }
    if let Some(value) = file.server.state_root {
        server.state_root = value;
    }
    assign(&mut server.query.max_running, file.server.max_running);
    assign(&mut server.query.max_queued, file.server.max_queued);
    if let Some(value) = file.server.max_query_time_secs {
        server.query.max_query_time = Duration::from_secs(value);
    }
    if let Some(value) = file.engine.memory_limit {
        engine.memory_limit = parse_size("engine.memory_limit", &value)?;
    }
    assign(&mut engine.compute_threads, file.engine.threads);
    assign_some(&mut engine.s3.region, file.engine.s3_region);
    assign_some(&mut engine.s3.endpoint, file.engine.s3_endpoint);
    assign(&mut engine.s3.force_path_style, file.engine.s3_path_style);
    assign(&mut engine.s3.allow_http, file.engine.s3_allow_http);
    assign(&mut engine.s3.anonymous, file.engine.s3_anonymous);
    Ok(())
}

fn apply_environment(server: &mut HttpServerConfig, engine: &mut EngineConfig) -> Result<()> {
    if let Some(value) = environment("RUSTDB_LISTEN")? {
        server.listen = parse_address("RUSTDB_LISTEN", &value)?;
    }
    assign_some(
        &mut server.advertise_url,
        environment("RUSTDB_ADVERTISE_URL")?,
    );
    assign_some(
        &mut server.result_directory,
        environment("RUSTDB_RESULT_DIRECTORY")?.map(PathBuf::from),
    );
    if let Some(value) = environment("RUSTDB_RESULT_TTL_SECS")? {
        server.result_ttl = Duration::from_secs(parse_number("RUSTDB_RESULT_TTL_SECS", &value)?);
    }
    if let Some(value) = environment("RUSTDB_RESULT_GLOBAL_LIMIT")? {
        server.result_global_limit_bytes =
            Some(parse_size_u64("RUSTDB_RESULT_GLOBAL_LIMIT", &value)?);
    }
    if let Some(value) = environment("RUSTDB_RESULT_QUERY_LIMIT")? {
        server.result_query_limit_bytes =
            Some(parse_size_u64("RUSTDB_RESULT_QUERY_LIMIT", &value)?);
    }
    if let Some(value) = environment("RUSTDB_STATE_ROOT")? {
        server.state_root = PathBuf::from(value);
    }
    if let Some(value) = environment("RUSTDB_HTTP_MAX_RUNNING")? {
        server.query.max_running = parse_number("RUSTDB_HTTP_MAX_RUNNING", &value)?;
    }
    if let Some(value) = environment("RUSTDB_HTTP_MAX_QUEUED")? {
        server.query.max_queued = parse_number("RUSTDB_HTTP_MAX_QUEUED", &value)?;
    }
    if let Some(value) = environment("RUSTDB_HTTP_MAX_QUERY_TIME_SECS")? {
        server.query.max_query_time =
            Duration::from_secs(parse_number("RUSTDB_HTTP_MAX_QUERY_TIME_SECS", &value)?);
    }
    if let Some(value) = environment("RUSTDB_MEMORY_LIMIT")? {
        engine.memory_limit = parse_size("RUSTDB_MEMORY_LIMIT", &value)?;
    }
    if let Some(value) = environment("RUSTDB_THREADS")? {
        engine.compute_threads = parse_number("RUSTDB_THREADS", &value)?;
    }
    assign_some(&mut engine.s3.region, environment("RUSTDB_S3_REGION")?);
    assign_some(&mut engine.s3.endpoint, environment("RUSTDB_S3_ENDPOINT")?);
    if let Some(value) = environment("RUSTDB_S3_PATH_STYLE")? {
        engine.s3.force_path_style = parse_bool("RUSTDB_S3_PATH_STYLE", &value)?;
    }
    if let Some(value) = environment("RUSTDB_S3_ALLOW_HTTP")? {
        engine.s3.allow_http = parse_bool("RUSTDB_S3_ALLOW_HTTP", &value)?;
    }
    if let Some(value) = environment("RUSTDB_S3_ANONYMOUS")? {
        engine.s3.anonymous = parse_bool("RUSTDB_S3_ANONYMOUS", &value)?;
    }
    Ok(())
}

fn apply_cli(server: &mut HttpServerConfig, engine: &mut EngineConfig, args: &ServeArgs) {
    assign(&mut server.listen, args.listen);
    assign_some(&mut server.advertise_url, args.advertise_url.clone());
    assign(&mut server.state_root, args.state_root.clone());
    assign_some(&mut server.result_directory, args.result_directory.clone());
    if let Some(value) = args.result_ttl_secs {
        server.result_ttl = Duration::from_secs(value);
    }
    if let Some(value) = args.result_global_limit {
        server.result_global_limit_bytes = Some(u64::try_from(value).unwrap_or(u64::MAX));
    }
    if let Some(value) = args.result_query_limit {
        server.result_query_limit_bytes = Some(u64::try_from(value).unwrap_or(u64::MAX));
    }
    assign(&mut server.query.max_running, args.max_running);
    assign(&mut server.query.max_queued, args.max_queued);
    if let Some(value) = args.max_query_time_secs {
        server.query.max_query_time = Duration::from_secs(value);
    }
    assign(&mut engine.memory_limit, args.memory_limit);
    assign(&mut engine.compute_threads, args.threads);
    assign_some(&mut engine.s3.region, args.s3_region.clone());
    assign_some(&mut engine.s3.endpoint, args.s3_endpoint.clone());
    if args.s3_path_style {
        engine.s3.force_path_style = true;
    }
    if args.s3_allow_http {
        engine.s3.allow_http = true;
    }
    if args.s3_anonymous {
        engine.s3.anonymous = true;
    }
}

fn validate(server: &HttpServerConfig, engine: &EngineConfig) -> Result<()> {
    if server.query.max_running == 0
        || server.query.max_queued == 0
        || server.query.max_query_time.is_zero()
        || server.result_ttl.is_zero()
        || engine.memory_limit == 0
        || engine.compute_threads == 0
    {
        return Err(Error::InvalidArgument(
            "server limits, memory, thread count, and timeout must be positive".into(),
        ));
    }
    Ok(())
}

fn environment(name: &str) -> Result<Option<String>> {
    match std::env::var(name) {
        Ok(value) => Ok(Some(value)),
        Err(std::env::VarError::NotPresent) => Ok(None),
        Err(error) => Err(Error::InvalidArgument(format!(
            "environment variable {name} is invalid: {error}"
        ))),
    }
}

fn parse_address(name: &str, value: &str) -> Result<SocketAddr> {
    value
        .parse()
        .map_err(|error| Error::InvalidArgument(format!("invalid {name}: {error}")))
}

fn parse_size(name: &str, value: &str) -> Result<usize> {
    parse_bytes(value).map_err(|error| Error::InvalidArgument(format!("invalid {name}: {error}")))
}

fn parse_size_u64(name: &str, value: &str) -> Result<u64> {
    parse_size(name, value).map(|value| u64::try_from(value).unwrap_or(u64::MAX))
}

fn parse_number<T>(name: &str, value: &str) -> Result<T>
where
    T: std::str::FromStr,
    T::Err: std::fmt::Display,
{
    value
        .parse()
        .map_err(|error| Error::InvalidArgument(format!("invalid {name}: {error}")))
}

fn parse_bool(name: &str, value: &str) -> Result<bool> {
    match value.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Ok(true),
        "0" | "false" | "no" | "off" => Ok(false),
        _ => Err(Error::InvalidArgument(format!(
            "invalid {name}: expected true or false"
        ))),
    }
}

fn assign<T>(target: &mut T, value: Option<T>) {
    if let Some(value) = value {
        *target = value;
    }
}

fn assign_some<T>(target: &mut Option<T>, value: Option<T>) {
    if let Some(value) = value {
        *target = Some(value);
    }
}
