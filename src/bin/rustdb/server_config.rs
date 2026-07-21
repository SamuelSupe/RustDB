use std::path::{Path, PathBuf};

use rustdb::{EngineConfig, Result, http_shell::HttpServerConfig};

use super::{args::Args, config, operations::ServeArgs};

#[path = "server_config/file.rs"]
mod file;
#[path = "server_config/merge.rs"]
mod merge;
#[path = "server_config/validate.rs"]
mod validate;

use file::FileConfig;

pub(super) const CONFIG_SCHEMA_VERSION: u32 = 2;

pub(super) fn load(args: &ServeArgs, global: &Args) -> Result<(EngineConfig, HttpServerConfig)> {
    let file = FileConfig::load_optional(args.config.as_deref())?;
    resolve(file, global, Some(args))
}

pub(super) fn validate_path(path: Option<&Path>, global: &Args) -> Result<PathBuf> {
    let path = path.unwrap_or_else(|| Path::new("rustdb.toml"));
    let file = FileConfig::load_required(path)?;
    let _ = resolve(file, global, None)?;
    Ok(path.to_path_buf())
}

fn resolve(
    file: FileConfig,
    global: &Args,
    serve: Option<&ServeArgs>,
) -> Result<(EngineConfig, HttpServerConfig)> {
    let mut server = HttpServerConfig::default();
    let mut engine = EngineConfig::default();

    merge::apply_file(&mut server, &mut engine, file)?;
    merge::apply_environment(&mut server, &mut engine)?;
    config::apply_engine_args(&mut engine, global);
    if let Some(args) = serve {
        merge::apply_cli(&mut server, &mut engine, args);
    }
    let spill_query_limit = engine
        .spill
        .query_limit_bytes
        .unwrap_or(server.query.query_spill_limit_bytes);
    engine.spill.query_limit_bytes = Some(spill_query_limit);
    engine.spill.engine_limit_bytes = Some(engine.spill.engine_limit_bytes.unwrap_or_else(|| {
        spill_query_limit
            .saturating_mul(u64::try_from(server.query.max_running).unwrap_or(u64::MAX))
    }));
    validate::all(&server, &engine)?;
    engine.max_concurrent_queries = server.query.max_running;
    Ok((engine, server))
}
