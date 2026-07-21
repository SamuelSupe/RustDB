use std::{net::SocketAddr, path::PathBuf, time::Duration};

use rustdb::{EngineConfig, Error, Result, http_shell::HttpServerConfig};

use super::{FileConfig, ServeArgs};
use crate::args::parse_bytes;

pub(super) fn apply_file(
    server: &mut HttpServerConfig,
    engine: &mut EngineConfig,
    file: FileConfig,
) -> Result<()> {
    assign_query_file_limits(server, &file.server)?;
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
    assign(
        &mut server.rss_guard.warning_ratio,
        file.server.rss_warning_ratio,
    );
    assign(&mut server.rss_guard.high_ratio, file.server.rss_high_ratio);
    assign(
        &mut server.rss_guard.critical_ratio,
        file.server.rss_critical_ratio,
    );
    if let Some(value) = file.server.rss_sample_interval_ms {
        server.rss_sample_interval = Duration::from_millis(value);
    }
    assign(
        &mut server.service_io_threads,
        file.server.service_io_threads,
    );
    assign_some(&mut server.admin_socket, file.server.admin_socket);
    if let Some(value) = file.server.tls_renew_interval_secs {
        server.tls_renew_interval = Duration::from_secs(value);
    }
    assign(&mut server.no_auth, file.server.no_auth);
    if let Some(value) = file.engine.memory_limit {
        engine.memory_limit = parse_size("engine.memory_limit", &value)?;
    }
    assign(&mut engine.compute_threads, file.engine.threads);
    if let Some(value) = file.engine.spill_engine_limit {
        engine.spill.engine_limit_bytes =
            Some(parse_size_u64("engine.spill_engine_limit", &value)?);
    }
    if let Some(value) = file.engine.spill_query_limit {
        engine.spill.query_limit_bytes = Some(parse_size_u64("engine.spill_query_limit", &value)?);
    }
    if let Some(value) = file.engine.native_min_free_bytes {
        engine.native_storage.min_free_bytes =
            parse_size_u64("engine.native_min_free_bytes", &value)?;
    }
    assign(
        &mut engine.native_storage.min_free_ratio,
        file.engine.native_min_free_ratio,
    );
    assign_some(&mut engine.s3.region, file.engine.s3_region);
    assign_some(&mut engine.s3.endpoint, file.engine.s3_endpoint);
    assign(&mut engine.s3.force_path_style, file.engine.s3_path_style);
    assign(&mut engine.s3.allow_http, file.engine.s3_allow_http);
    assign(&mut engine.s3.anonymous, file.engine.s3_anonymous);
    Ok(())
}

pub(super) fn apply_environment(
    server: &mut HttpServerConfig,
    engine: &mut EngineConfig,
) -> Result<()> {
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
    apply_query_environment(server)?;
    if let Some(value) = environment("RUSTDB_NO_AUTH")? {
        server.no_auth = parse_bool("RUSTDB_NO_AUTH", &value)?;
    }
    if let Some(value) = environment("RUSTDB_SERVICE_IO_THREADS")? {
        server.service_io_threads = parse_number("RUSTDB_SERVICE_IO_THREADS", &value)?;
    }
    assign_some(
        &mut server.admin_socket,
        environment("RUSTDB_ADMIN_SOCKET")?.map(PathBuf::from),
    );
    if let Some(value) = environment("RUSTDB_TLS_RENEW_INTERVAL_SECS")? {
        server.tls_renew_interval =
            Duration::from_secs(parse_number("RUSTDB_TLS_RENEW_INTERVAL_SECS", &value)?);
    }
    if let Some(value) = environment("RUSTDB_MEMORY_LIMIT")? {
        engine.memory_limit = parse_size("RUSTDB_MEMORY_LIMIT", &value)?;
    }
    if let Some(value) = environment("RUSTDB_THREADS")? {
        engine.compute_threads = parse_number("RUSTDB_THREADS", &value)?;
    }
    if let Some(value) = environment("RUSTDB_SPILL_ENGINE_LIMIT")? {
        engine.spill.engine_limit_bytes =
            Some(parse_size_u64("RUSTDB_SPILL_ENGINE_LIMIT", &value)?);
    }
    if let Some(value) = environment("RUSTDB_SPILL_QUERY_LIMIT")? {
        engine.spill.query_limit_bytes = Some(parse_size_u64("RUSTDB_SPILL_QUERY_LIMIT", &value)?);
    }
    if let Some(value) = environment("RUSTDB_NATIVE_MIN_FREE_BYTES")? {
        engine.native_storage.min_free_bytes =
            parse_size_u64("RUSTDB_NATIVE_MIN_FREE_BYTES", &value)?;
    }
    if let Some(value) = environment("RUSTDB_NATIVE_MIN_FREE_RATIO")? {
        engine.native_storage.min_free_ratio =
            parse_number("RUSTDB_NATIVE_MIN_FREE_RATIO", &value)?;
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

pub(super) fn apply_cli(
    server: &mut HttpServerConfig,
    engine: &mut EngineConfig,
    args: &ServeArgs,
) {
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
    assign_u64_size(
        &mut server.query.query_memory_limit_bytes,
        args.query_memory_limit,
    );
    assign_u64_size(
        &mut server.query.query_spill_limit_bytes,
        args.query_spill_limit,
    );
    assign_u64_size(
        &mut server.query.query_result_limit_bytes,
        args.query_result_limit,
    );
    assign(
        &mut server.query.principal_max_running,
        args.principal_max_running,
    );
    assign(
        &mut server.query.principal_max_queued,
        args.principal_max_queued,
    );
    assign_u64_size(
        &mut server.query.principal_memory_limit_bytes,
        args.principal_memory_limit,
    );
    assign_u64_size(
        &mut server.query.principal_spill_limit_bytes,
        args.principal_spill_limit,
    );
    assign_u64_size(
        &mut server.query.principal_result_limit_bytes,
        args.principal_result_limit,
    );
    assign(&mut server.query.principal_weight, args.principal_weight);
    assign(&mut engine.memory_limit, args.memory_limit);
    assign(&mut engine.compute_threads, args.threads);
    assign_optional_u64_size(
        &mut engine.spill.engine_limit_bytes,
        args.spill_engine_limit,
    );
    assign_optional_u64_size(&mut engine.spill.query_limit_bytes, args.spill_query_limit);
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
    assign(&mut server.service_io_threads, args.service_io_threads);
    assign_some(&mut server.admin_socket, args.admin_socket.clone());
    if let Some(value) = args.tls_renew_interval_secs {
        server.tls_renew_interval = Duration::from_secs(value);
    }
    if args.no_auth {
        server.no_auth = true;
    }
}

fn assign_query_file_limits(
    server: &mut HttpServerConfig,
    file: &super::file::FileServer,
) -> Result<()> {
    if let Some(value) = &file.query_memory_limit {
        server.query.query_memory_limit_bytes = parse_size_u64("server.query_memory_limit", value)?;
    }
    if let Some(value) = &file.query_spill_limit {
        server.query.query_spill_limit_bytes = parse_size_u64("server.query_spill_limit", value)?;
    }
    if let Some(value) = &file.query_result_limit {
        server.query.query_result_limit_bytes = parse_size_u64("server.query_result_limit", value)?;
    }
    assign(
        &mut server.query.principal_max_running,
        file.principal_max_running,
    );
    assign(
        &mut server.query.principal_max_queued,
        file.principal_max_queued,
    );
    if let Some(value) = &file.principal_memory_limit {
        server.query.principal_memory_limit_bytes =
            parse_size_u64("server.principal_memory_limit", value)?;
    }
    if let Some(value) = &file.principal_spill_limit {
        server.query.principal_spill_limit_bytes =
            parse_size_u64("server.principal_spill_limit", value)?;
    }
    if let Some(value) = &file.principal_result_limit {
        server.query.principal_result_limit_bytes =
            parse_size_u64("server.principal_result_limit", value)?;
    }
    assign(&mut server.query.principal_weight, file.principal_weight);
    Ok(())
}

fn apply_query_environment(server: &mut HttpServerConfig) -> Result<()> {
    for (name, target) in [
        (
            "RUSTDB_HTTP_QUERY_MEMORY_LIMIT",
            &mut server.query.query_memory_limit_bytes,
        ),
        (
            "RUSTDB_HTTP_QUERY_SPILL_LIMIT",
            &mut server.query.query_spill_limit_bytes,
        ),
        (
            "RUSTDB_HTTP_QUERY_RESULT_LIMIT",
            &mut server.query.query_result_limit_bytes,
        ),
        (
            "RUSTDB_HTTP_PRINCIPAL_MEMORY_LIMIT",
            &mut server.query.principal_memory_limit_bytes,
        ),
        (
            "RUSTDB_HTTP_PRINCIPAL_SPILL_LIMIT",
            &mut server.query.principal_spill_limit_bytes,
        ),
        (
            "RUSTDB_HTTP_PRINCIPAL_RESULT_LIMIT",
            &mut server.query.principal_result_limit_bytes,
        ),
    ] {
        if let Some(value) = environment(name)? {
            *target = parse_size_u64(name, &value)?;
        }
    }
    if let Some(value) = environment("RUSTDB_HTTP_PRINCIPAL_MAX_RUNNING")? {
        server.query.principal_max_running =
            parse_number("RUSTDB_HTTP_PRINCIPAL_MAX_RUNNING", &value)?;
    }
    if let Some(value) = environment("RUSTDB_HTTP_PRINCIPAL_MAX_QUEUED")? {
        server.query.principal_max_queued =
            parse_number("RUSTDB_HTTP_PRINCIPAL_MAX_QUEUED", &value)?;
    }
    if let Some(value) = environment("RUSTDB_HTTP_PRINCIPAL_WEIGHT")? {
        server.query.principal_weight = parse_number("RUSTDB_HTTP_PRINCIPAL_WEIGHT", &value)?;
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

fn assign_u64_size(target: &mut u64, value: Option<usize>) {
    if let Some(value) = value {
        *target = u64::try_from(value).unwrap_or(u64::MAX);
    }
}

fn assign_optional_u64_size(target: &mut Option<u64>, value: Option<usize>) {
    if let Some(value) = value {
        *target = Some(u64::try_from(value).unwrap_or(u64::MAX));
    }
}
