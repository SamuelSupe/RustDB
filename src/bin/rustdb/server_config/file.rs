use std::{fs, path::Path};

use rustdb::{Error, Result};
use serde::Deserialize;

use super::CONFIG_SCHEMA_VERSION;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct FileConfig {
    schema_version: Option<u32>,
    #[serde(default)]
    pub(super) server: FileServer,
    #[serde(default)]
    pub(super) engine: FileEngine,
}

#[derive(Deserialize)]
struct VersionProbe {
    schema_version: Option<u32>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub(super) struct FileServer {
    pub(super) listen: Option<String>,
    pub(super) advertise_url: Option<String>,
    pub(super) state_root: Option<std::path::PathBuf>,
    pub(super) result_directory: Option<std::path::PathBuf>,
    pub(super) result_ttl_secs: Option<u64>,
    pub(super) result_global_limit: Option<String>,
    pub(super) result_query_limit: Option<String>,
    pub(super) max_running: Option<usize>,
    pub(super) max_queued: Option<usize>,
    pub(super) max_query_time_secs: Option<u64>,
    pub(super) query_memory_limit: Option<String>,
    pub(super) query_spill_limit: Option<String>,
    pub(super) query_result_limit: Option<String>,
    pub(super) principal_max_running: Option<usize>,
    pub(super) principal_max_queued: Option<usize>,
    pub(super) principal_memory_limit: Option<String>,
    pub(super) principal_spill_limit: Option<String>,
    pub(super) principal_result_limit: Option<String>,
    pub(super) principal_weight: Option<u32>,
    pub(super) no_auth: Option<bool>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub(super) struct FileEngine {
    pub(super) memory_limit: Option<String>,
    pub(super) threads: Option<usize>,
    pub(super) spill_engine_limit: Option<String>,
    pub(super) spill_query_limit: Option<String>,
    pub(super) native_min_free_bytes: Option<String>,
    pub(super) native_min_free_ratio: Option<f64>,
    pub(super) s3_region: Option<String>,
    pub(super) s3_endpoint: Option<String>,
    pub(super) s3_path_style: Option<bool>,
    pub(super) s3_allow_http: Option<bool>,
    pub(super) s3_anonymous: Option<bool>,
}

impl FileConfig {
    pub(super) fn load_optional(explicit: Option<&Path>) -> Result<Self> {
        match explicit {
            Some(path) => Self::load_required(path),
            None => {
                let path = Path::new("rustdb.toml");
                if path.exists() {
                    Self::load_required(path)
                } else {
                    Ok(Self::defaults())
                }
            }
        }
    }

    pub(super) fn load_required(path: &Path) -> Result<Self> {
        let contents =
            fs::read_to_string(path).map_err(|error| Error::io(path.to_path_buf(), error))?;
        let version: VersionProbe = parse(path, &contents)?;
        check_version(path, version.schema_version)?;
        let config: Self = toml::from_str(&contents).map_err(|error| {
            Error::InvalidArgument(format!(
                "invalid service config {}: {error}",
                path.display()
            ))
        })?;
        debug_assert_eq!(config.schema_version, Some(CONFIG_SCHEMA_VERSION));
        Ok(config)
    }

    fn defaults() -> Self {
        Self {
            schema_version: Some(CONFIG_SCHEMA_VERSION),
            server: FileServer::default(),
            engine: FileEngine::default(),
        }
    }
}

fn parse<'a, T>(path: &Path, contents: &'a str) -> Result<T>
where
    T: Deserialize<'a>,
{
    toml::from_str(contents).map_err(|error| {
        Error::InvalidArgument(format!(
            "invalid service config {}: {error}",
            path.display()
        ))
    })
}

fn check_version(path: &Path, version: Option<u32>) -> Result<()> {
    match version {
        Some(CONFIG_SCHEMA_VERSION) => Ok(()),
        Some(version) => Err(Error::InvalidArgument(format!(
            "invalid service config {}: unsupported schema_version {version}; expected {CONFIG_SCHEMA_VERSION}",
            path.display()
        ))),
        None => Err(Error::InvalidArgument(format!(
            "invalid service config {}: top-level schema_version = {CONFIG_SCHEMA_VERSION} is required",
            path.display()
        ))),
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use super::FileConfig;
    use rustdb::Error;

    #[test]
    fn requires_the_current_top_level_schema_version() {
        let directory = tempfile::tempdir().unwrap();
        let missing = directory.path().join("missing.toml");
        fs::write(&missing, "[server]\nmax_running = 1\n").unwrap();
        let error = FileConfig::load_required(&missing).unwrap_err();
        assert!(matches!(error, Error::InvalidArgument(_)));
        assert!(error.to_string().contains("schema_version = 1 is required"));

        let unknown = directory.path().join("unknown.toml");
        fs::write(&unknown, "schema_version = 2\n").unwrap();
        let error = FileConfig::load_required(&unknown).unwrap_err();
        assert!(matches!(error, Error::InvalidArgument(_)));
        assert!(error.to_string().contains("unsupported schema_version 2"));
    }

    #[test]
    fn accepts_the_packaged_example() {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("packaging/config/rustdb.example.toml");
        FileConfig::load_required(&path).unwrap();
    }
}
