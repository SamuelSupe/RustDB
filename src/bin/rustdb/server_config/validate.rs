use rustdb::http_shell::security::ServerEndpoint;
use rustdb::{EngineConfig, Error, Result, http_shell::HttpServerConfig};

pub(super) fn all(server: &HttpServerConfig, engine: &EngineConfig) -> Result<()> {
    if server.state_root.as_os_str().is_empty() {
        return Err(Error::InvalidArgument(
            "server.state_root must not be empty".into(),
        ));
    }
    if server.query.max_running == 0 {
        return Err(Error::InvalidArgument(
            "server.max_running must be greater than zero".into(),
        ));
    }
    if server.query.max_queued == 0 {
        return Err(Error::InvalidArgument(
            "server.max_queued must be greater than zero".into(),
        ));
    }
    if server.query.max_query_time.is_zero() {
        return Err(Error::InvalidArgument(
            "server.max_query_time_secs must be greater than zero".into(),
        ));
    }
    validate_query_resources(server, engine)?;
    if server.result_ttl.is_zero() {
        return Err(Error::InvalidArgument(
            "server.result_ttl_secs must be greater than zero".into(),
        ));
    }
    validate_result_limits(server)?;
    let _ = ServerEndpoint::resolve(server.listen, server.advertise_url.as_deref())?;

    engine.validate()
}

fn validate_query_resources(server: &HttpServerConfig, engine: &EngineConfig) -> Result<()> {
    let query = &server.query;
    if query.principal_max_running == 0 || query.principal_max_queued == 0 {
        return Err(Error::InvalidArgument(
            "server principal running and queue limits must be greater than zero".into(),
        ));
    }
    if query.principal_weight == 0 || query.principal_weight > 1_024 {
        return Err(Error::InvalidArgument(
            "server.principal_weight must be between 1 and 1024".into(),
        ));
    }
    let query_resources = [
        query.query_memory_limit_bytes,
        query.query_spill_limit_bytes,
        query.query_result_limit_bytes,
    ];
    let principal_resources = [
        query.principal_memory_limit_bytes,
        query.principal_spill_limit_bytes,
        query.principal_result_limit_bytes,
    ];
    if query_resources.contains(&0) || principal_resources.contains(&0) {
        return Err(Error::InvalidArgument(
            "server query and principal resource limits must be greater than zero".into(),
        ));
    }
    if query_resources
        .iter()
        .zip(principal_resources)
        .any(|(query, principal)| *query > principal)
    {
        return Err(Error::InvalidArgument(
            "server per-query resource limits must fit within per-principal limits".into(),
        ));
    }
    if query.query_memory_limit_bytes > u64::try_from(engine.memory_limit).unwrap_or(u64::MAX) {
        return Err(Error::InvalidArgument(
            "server.query_memory_limit must not exceed engine.memory_limit".into(),
        ));
    }
    if engine
        .spill
        .query_limit_bytes
        .is_some_and(|limit| query.query_spill_limit_bytes > limit)
    {
        return Err(Error::InvalidArgument(
            "server.query_spill_limit must not exceed engine.spill_query_limit".into(),
        ));
    }
    if server
        .result_query_limit_bytes
        .is_some_and(|limit| query.query_result_limit_bytes > limit)
        || server
            .result_global_limit_bytes
            .is_some_and(|limit| query.query_result_limit_bytes > limit)
    {
        return Err(Error::InvalidArgument(
            "server.query_result_limit must fit within configured result hard limits".into(),
        ));
    }
    Ok(())
}

fn validate_result_limits(server: &HttpServerConfig) -> Result<()> {
    if matches!(server.result_global_limit_bytes, Some(0)) {
        return Err(Error::InvalidArgument(
            "server.result_global_limit must be greater than zero when configured".into(),
        ));
    }
    if matches!(server.result_query_limit_bytes, Some(0)) {
        return Err(Error::InvalidArgument(
            "server.result_query_limit must be greater than zero when configured".into(),
        ));
    }
    if let (Some(global), Some(query)) = (
        server.result_global_limit_bytes,
        server.result_query_limit_bytes,
    ) && query > global
    {
        return Err(Error::InvalidArgument(format!(
            "server.result_query_limit ({query}) must not exceed server.result_global_limit ({global})"
        )));
    }
    Ok(())
}
