use std::{
    path::PathBuf,
    sync::Arc,
    time::{Duration, SystemTime},
};

use arrow::datatypes::SchemaRef;

use crate::{Error, ParquetSchemaMode, Result};

mod builder;
pub use builder::EngineConfigBuilder;

const DEFAULT_MIN_FREE_BYTES: u64 = 1024 * 1024 * 1024;
const DEFAULT_PARQUET_PRUNING_METADATA_BYTES: usize = 64 * 1024 * 1024;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[non_exhaustive]
pub enum ParquetPruningMode {
    #[default]
    Auto,
    Disabled,
}

#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct ParquetScanConfig {
    pub page_index: ParquetPruningMode,
    pub bloom_filter: ParquetPruningMode,
    pub max_pruning_metadata_bytes: usize,
}

impl Default for ParquetScanConfig {
    fn default() -> Self {
        Self {
            page_index: ParquetPruningMode::Auto,
            bloom_filter: ParquetPruningMode::Auto,
            max_pruning_metadata_bytes: DEFAULT_PARQUET_PRUNING_METADATA_BYTES,
        }
    }
}

#[derive(Clone, Debug)]
pub struct SpillConfig {
    pub directory: PathBuf,
    pub engine_limit_bytes: Option<u64>,
    pub query_limit_bytes: Option<u64>,
    pub min_free_ratio: f64,
    pub min_free_bytes: u64,
    pub orphan_ttl: Duration,
    pub io_threads: usize,
}

impl SpillConfig {
    pub fn validate(&self) -> Result<()> {
        if self.directory.as_os_str().is_empty() {
            return Err(Error::InvalidArgument(
                "spill.directory must not be empty".to_owned(),
            ));
        }
        if matches!(self.engine_limit_bytes, Some(0)) {
            return Err(Error::InvalidArgument(
                "spill.engine_limit_bytes must be greater than zero when configured".to_owned(),
            ));
        }
        if matches!(self.query_limit_bytes, Some(0)) {
            return Err(Error::InvalidArgument(
                "spill.query_limit_bytes must be greater than zero when configured".to_owned(),
            ));
        }
        if let (Some(engine), Some(query)) = (self.engine_limit_bytes, self.query_limit_bytes)
            && query > engine
        {
            return Err(Error::InvalidArgument(format!(
                "spill.query_limit_bytes ({query}) must not exceed spill.engine_limit_bytes ({engine})"
            )));
        }
        if !self.min_free_ratio.is_finite() || !(0.0..1.0).contains(&self.min_free_ratio) {
            return Err(Error::InvalidArgument(
                "spill.min_free_ratio must be finite and in the range [0, 1)".to_owned(),
            ));
        }
        if self.orphan_ttl.is_zero() {
            return Err(Error::InvalidArgument(
                "spill.orphan_ttl must be greater than zero".to_owned(),
            ));
        }
        if self.io_threads == 0 {
            return Err(Error::InvalidArgument(
                "spill.io_threads must be greater than zero".to_owned(),
            ));
        }
        if SystemTime::UNIX_EPOCH
            .checked_add(self.orphan_ttl)
            .is_none()
        {
            return Err(Error::InvalidArgument(
                "spill.orphan_ttl is too large".to_owned(),
            ));
        }
        Ok(())
    }
}

impl Default for SpillConfig {
    fn default() -> Self {
        Self {
            directory: std::env::temp_dir().join("rustdb-spill"),
            engine_limit_bytes: None,
            query_limit_bytes: None,
            min_free_ratio: 0.10,
            min_free_bytes: DEFAULT_MIN_FREE_BYTES,
            orphan_ttl: Duration::from_secs(24 * 60 * 60),
            io_threads: 2,
        }
    }
}

#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct EngineConfig {
    pub memory_limit: usize,
    pub batch_size: usize,
    pub compute_threads: usize,
    pub io_concurrency: usize,
    pub max_concurrent_queries: usize,
    pub metadata_cache_bytes: usize,
    pub parquet_scan: ParquetScanConfig,
    pub s3: S3Config,
    pub spill: SpillConfig,
}

impl Default for EngineConfig {
    fn default() -> Self {
        let mut system = sysinfo::System::new();
        system.refresh_memory();
        let detected = usize::try_from(system.total_memory()).unwrap_or(usize::MAX);
        let memory_limit = detected.saturating_mul(7) / 10;
        let compute_threads = std::thread::available_parallelism()
            .map(usize::from)
            .unwrap_or(1);

        let spill = SpillConfig::default();
        Self {
            memory_limit: memory_limit.max(64 * 1024 * 1024),
            batch_size: 8_192,
            compute_threads,
            io_concurrency: 32,
            max_concurrent_queries: 1,
            metadata_cache_bytes: 64 * 1024 * 1024,
            parquet_scan: ParquetScanConfig::default(),
            s3: S3Config::default(),
            spill,
        }
    }
}

impl EngineConfig {
    pub fn builder() -> EngineConfigBuilder {
        EngineConfigBuilder::default()
    }
}

#[derive(Clone, Default)]
pub struct S3Config {
    pub region: Option<String>,
    pub endpoint: Option<String>,
    pub force_path_style: bool,
    pub anonymous: bool,
    pub allow_http: bool,
    pub credential_provider: Option<object_store::aws::AwsCredentialProvider>,
}

impl std::fmt::Debug for S3Config {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("S3Config")
            .field("region", &self.region)
            .field("endpoint", &self.endpoint.as_ref().map(|_| "<configured>"))
            .field("force_path_style", &self.force_path_style)
            .field("anonymous", &self.anonymous)
            .field("allow_http", &self.allow_http)
            .field(
                "credential_provider",
                &self.credential_provider.as_ref().map(|_| "<configured>"),
            )
            .finish()
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum CsvHeader {
    #[default]
    Auto,
    Present,
    Absent,
}

#[derive(Clone, Debug)]
pub struct CsvOptions {
    pub schema: Option<SchemaRef>,
    pub header: CsvHeader,
    pub delimiter: u8,
    pub quote: u8,
    pub escape: Option<u8>,
    pub sample_size: usize,
}

impl Default for CsvOptions {
    fn default() -> Self {
        Self {
            schema: None,
            header: CsvHeader::Auto,
            delimiter: b',',
            quote: b'"',
            escape: None,
            sample_size: 10_000,
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct ParquetOptions {
    pub schema: Option<Arc<arrow::datatypes::Schema>>,
    pub union_by_name: bool,
    pub schema_mode: ParquetSchemaMode,
    pub hive_partitioning: bool,
}

impl ParquetOptions {
    pub(crate) fn effective_schema_mode(&self) -> Result<ParquetSchemaMode> {
        match (self.union_by_name, self.schema_mode) {
            (false, mode) => Ok(mode),
            (true, ParquetSchemaMode::Strict) => Ok(ParquetSchemaMode::UnionByName),
            (true, _) => Err(Error::InvalidArgument(
                "union_by_name and schema_mode cannot both be configured".to_owned(),
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{path::PathBuf, time::Duration};

    use super::{ParquetPruningMode, ParquetScanConfig, S3Config, SpillConfig};
    use crate::Error;

    #[test]
    fn debug_output_redacts_configured_endpoint() {
        let config = S3Config {
            endpoint: Some("https://user-secret.example.test".into()),
            ..S3Config::default()
        };
        let debug = format!("{config:?}");
        assert!(debug.contains("<configured>"));
        assert!(!debug.contains("user-secret"));
    }

    #[test]
    fn parquet_pruning_defaults_to_auto_with_a_sixty_four_mib_cap() {
        let config = ParquetScanConfig::default();
        assert_eq!(config.page_index, ParquetPruningMode::Auto);
        assert_eq!(config.bloom_filter, ParquetPruningMode::Auto);
        assert_eq!(config.max_pruning_metadata_bytes, 64 * 1024 * 1024);
    }

    #[test]
    fn spill_defaults_apply_the_safety_policy() {
        let config = SpillConfig::default();
        assert_eq!(config.min_free_ratio, 0.10);
        assert_eq!(config.min_free_bytes, 1024 * 1024 * 1024);
        assert_eq!(config.orphan_ttl, Duration::from_secs(24 * 60 * 60));
        assert_eq!(config.io_threads, 2);
        assert_eq!(config.engine_limit_bytes, None);
        assert_eq!(config.query_limit_bytes, None);
        config.validate().expect("default config must be valid");
    }

    #[test]
    fn spill_validation_rejects_invalid_limits_and_safety_values() {
        let cases = [
            SpillConfig {
                directory: PathBuf::new(),
                ..SpillConfig::default()
            },
            SpillConfig {
                engine_limit_bytes: Some(0),
                ..SpillConfig::default()
            },
            SpillConfig {
                query_limit_bytes: Some(0),
                ..SpillConfig::default()
            },
            SpillConfig {
                engine_limit_bytes: Some(10),
                query_limit_bytes: Some(11),
                ..SpillConfig::default()
            },
            SpillConfig {
                min_free_ratio: f64::NAN,
                ..SpillConfig::default()
            },
            SpillConfig {
                min_free_ratio: -0.1,
                ..SpillConfig::default()
            },
            SpillConfig {
                min_free_ratio: 1.0,
                ..SpillConfig::default()
            },
            SpillConfig {
                orphan_ttl: Duration::ZERO,
                ..SpillConfig::default()
            },
            SpillConfig {
                io_threads: 0,
                ..SpillConfig::default()
            },
        ];

        for config in cases {
            assert!(matches!(config.validate(), Err(Error::InvalidArgument(_))));
        }
    }
}
