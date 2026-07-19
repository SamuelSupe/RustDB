use std::{
    collections::BTreeMap,
    path::PathBuf,
    sync::Arc,
    time::{Duration, SystemTime},
};

use arrow::datatypes::SchemaRef;

use crate::{Error, ParquetSchemaMode, Result};

mod builder;
mod validate;
pub use builder::{CsvOptionsBuilder, EngineConfigBuilder};

const DEFAULT_MIN_FREE_BYTES: u64 = 1024 * 1024 * 1024;
const DEFAULT_PARQUET_PRUNING_METADATA_BYTES: usize = 64 * 1024 * 1024;
const DEFAULT_CSV_MORSEL_BYTES: usize = 8 * 1024 * 1024;
const DEFAULT_RUNTIME_FILTER_BYTES: usize = 8 * 1024 * 1024;

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
#[non_exhaustive]
pub struct CsvScanConfig {
    pub target_morsel_bytes: usize,
    pub parallel_single_file: bool,
}

impl CsvScanConfig {
    pub fn validate(&self) -> Result<()> {
        if self.target_morsel_bytes == 0 {
            return Err(Error::InvalidArgument(
                "csv_scan.target_morsel_bytes must be greater than zero".to_owned(),
            ));
        }
        Ok(())
    }
}

impl Default for CsvScanConfig {
    fn default() -> Self {
        Self {
            target_morsel_bytes: DEFAULT_CSV_MORSEL_BYTES,
            parallel_single_file: true,
        }
    }
}

#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct ExecutionConfig {
    pub spill_partition_target_bytes: Option<usize>,
    pub max_repartition_depth: usize,
    pub max_spill_write_amplification: Option<f64>,
    pub runtime_filter_bytes: usize,
}

impl ExecutionConfig {
    pub fn validate(&self) -> Result<()> {
        if matches!(self.spill_partition_target_bytes, Some(0)) {
            return Err(Error::InvalidArgument(
                "execution.spill_partition_target_bytes must be greater than zero when configured"
                    .to_owned(),
            ));
        }
        if let Some(limit) = self.max_spill_write_amplification
            && (!limit.is_finite() || limit < 1.0)
        {
            return Err(Error::InvalidArgument(
                "execution.max_spill_write_amplification must be finite and at least 1.0"
                    .to_owned(),
            ));
        }
        Ok(())
    }
}

impl Default for ExecutionConfig {
    fn default() -> Self {
        Self {
            spill_partition_target_bytes: None,
            max_repartition_depth: 2,
            max_spill_write_amplification: None,
            runtime_filter_bytes: DEFAULT_RUNTIME_FILTER_BYTES,
        }
    }
}

#[derive(Clone, Debug)]
#[non_exhaustive]
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

/// Hard limits for persistent Native storage.
///
/// The engine limit covers the complete database directory and commit
/// publication headroom. Table limits cover physical snapshots owned by the
/// logical table, including retained versions.
///
/// Limits are process configuration: they are applied by `Engine::open` and
/// are not persisted in the database directory.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct NativeStorageConfig {
    pub engine_limit_bytes: Option<u64>,
    pub default_table_limit_bytes: Option<u64>,
    pub table_limit_bytes: BTreeMap<String, u64>,
    /// Minimum fraction of the filesystem kept free for non-RustDB activity.
    pub min_free_ratio: f64,
    /// Absolute filesystem reserve; the larger ratio/byte reserve wins.
    pub min_free_bytes: u64,
}

impl NativeStorageConfig {
    pub fn validate(&self) -> Result<()> {
        validate_optional_limit("native_storage.engine_limit_bytes", self.engine_limit_bytes)?;
        validate_optional_limit(
            "native_storage.default_table_limit_bytes",
            self.default_table_limit_bytes,
        )?;
        if !self.min_free_ratio.is_finite() || !(0.0..1.0).contains(&self.min_free_ratio) {
            return Err(Error::InvalidArgument(
                "native_storage.min_free_ratio must be finite and in the range [0, 1)".to_owned(),
            ));
        }
        if let (Some(engine), Some(table)) =
            (self.engine_limit_bytes, self.default_table_limit_bytes)
            && table > engine
        {
            return Err(Error::InvalidArgument(format!(
                "native_storage.default_table_limit_bytes ({table}) must not exceed native_storage.engine_limit_bytes ({engine})"
            )));
        }
        let mut canonical_names = std::collections::BTreeSet::new();
        for (name, &limit) in &self.table_limit_bytes {
            let canonical = Self::canonical_table_name(name);
            if name.is_empty()
                || name.len() > 255
                || *name != name.to_ascii_lowercase()
                || canonical.is_none()
            {
                return Err(Error::InvalidArgument(format!(
                    "native_storage.table_limit_bytes key '{name}' must be a normalized table or schema.table name of at most 255 UTF-8 bytes"
                )));
            }
            if !canonical_names.insert(canonical.expect("validated canonical name")) {
                return Err(Error::InvalidArgument(format!(
                    "native_storage.table_limit_bytes key '{name}' duplicates another default-schema table limit"
                )));
            }
            if limit == 0 {
                return Err(Error::InvalidArgument(format!(
                    "native_storage.table_limit_bytes['{name}'] must be greater than zero"
                )));
            }
            if let Some(engine) = self.engine_limit_bytes
                && limit > engine
            {
                return Err(Error::InvalidArgument(format!(
                    "native_storage.table_limit_bytes['{name}'] ({limit}) must not exceed native_storage.engine_limit_bytes ({engine})"
                )));
            }
        }
        Ok(())
    }

    pub(crate) fn table_limit(&self, name: &str) -> Option<u64> {
        let canonical = Self::canonical_table_name(name)?;
        self.table_limit_bytes
            .get(&canonical)
            .or_else(|| {
                (!canonical.contains('.'))
                    .then(|| self.table_limit_bytes.get(&format!("main.{canonical}")))
                    .flatten()
            })
            .copied()
            .or(self.default_table_limit_bytes)
    }

    pub(crate) fn canonical_table_name(name: &str) -> Option<String> {
        let normalized = name.to_ascii_lowercase();
        let parts = normalized.split('.').collect::<Vec<_>>();
        match parts.as_slice() {
            [table] if !table.is_empty() && !table.contains('\0') => Some((*table).to_owned()),
            ["main", table] if !table.is_empty() && !table.contains('\0') => {
                Some((*table).to_owned())
            }
            [schema, table]
                if !schema.is_empty()
                    && !table.is_empty()
                    && !schema.contains('\0')
                    && !table.contains('\0') =>
            {
                Some(format!("{schema}.{table}"))
            }
            _ => None,
        }
    }
}

impl Default for NativeStorageConfig {
    fn default() -> Self {
        Self {
            engine_limit_bytes: None,
            default_table_limit_bytes: None,
            table_limit_bytes: BTreeMap::new(),
            min_free_ratio: 0.10,
            min_free_bytes: DEFAULT_MIN_FREE_BYTES,
        }
    }
}

fn validate_optional_limit(name: &str, value: Option<u64>) -> Result<()> {
    if matches!(value, Some(0)) {
        return Err(Error::InvalidArgument(format!(
            "{name} must be greater than zero when configured"
        )));
    }
    Ok(())
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
    pub csv_scan: CsvScanConfig,
    pub execution: ExecutionConfig,
    pub s3: S3Config,
    pub spill: SpillConfig,
    pub native_storage: NativeStorageConfig,
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
            csv_scan: CsvScanConfig::default(),
            execution: ExecutionConfig::default(),
            s3: S3Config::default(),
            spill,
            native_storage: NativeStorageConfig::default(),
        }
    }
}

impl EngineConfig {
    pub fn builder() -> EngineConfigBuilder {
        EngineConfigBuilder::default()
    }
}

#[derive(Clone, Default)]
#[non_exhaustive]
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

impl S3Config {
    #[must_use]
    pub fn region(mut self, region: impl Into<String>) -> Self {
        self.region = Some(region.into());
        self
    }

    #[must_use]
    pub fn endpoint(mut self, endpoint: impl Into<String>) -> Self {
        self.endpoint = Some(endpoint.into());
        self
    }

    #[must_use]
    pub fn force_path_style(mut self, enabled: bool) -> Self {
        self.force_path_style = enabled;
        self
    }

    #[must_use]
    pub fn anonymous(mut self, enabled: bool) -> Self {
        self.anonymous = enabled;
        self
    }

    #[must_use]
    pub fn allow_http(mut self, enabled: bool) -> Self {
        self.allow_http = enabled;
        self
    }

    #[must_use]
    pub fn credential_provider(
        mut self,
        provider: object_store::aws::AwsCredentialProvider,
    ) -> Self {
        self.credential_provider = Some(provider);
        self
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[non_exhaustive]
pub enum CsvHeader {
    #[default]
    Auto,
    Present,
    Absent,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[non_exhaustive]
pub enum CsvCompression {
    #[default]
    Auto,
    None,
    Gzip,
    Zstd,
}

#[derive(Clone, Debug, PartialEq)]
#[non_exhaustive]
pub struct CsvOptions {
    pub schema: Option<SchemaRef>,
    pub header: CsvHeader,
    pub delimiter: u8,
    pub quote: u8,
    pub escape: Option<u8>,
    pub sample_size: usize,
    pub compression: CsvCompression,
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
            compression: CsvCompression::Auto,
        }
    }
}

impl CsvOptions {
    pub fn builder() -> CsvOptionsBuilder {
        CsvOptionsBuilder::default()
    }
}

#[derive(Clone, Debug, Default, PartialEq)]
#[non_exhaustive]
pub struct ParquetOptions {
    pub schema: Option<Arc<arrow::datatypes::Schema>>,
    pub union_by_name: bool,
    pub schema_mode: ParquetSchemaMode,
    pub hive_partitioning: bool,
}

impl ParquetOptions {
    #[must_use]
    pub fn schema(mut self, schema: Arc<arrow::datatypes::Schema>) -> Self {
        self.schema = Some(schema);
        self
    }

    #[must_use]
    pub fn union_by_name(mut self, enabled: bool) -> Self {
        self.union_by_name = enabled;
        self
    }

    #[must_use]
    pub fn schema_mode(mut self, mode: ParquetSchemaMode) -> Self {
        self.schema_mode = mode;
        self
    }

    #[must_use]
    pub fn hive_partitioning(mut self, enabled: bool) -> Self {
        self.hive_partitioning = enabled;
        self
    }

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
    use std::{collections::BTreeMap, path::PathBuf, time::Duration};

    use super::{
        CsvScanConfig, ExecutionConfig, NativeStorageConfig, ParquetPruningMode, ParquetScanConfig,
        S3Config, SpillConfig,
    };
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
    fn csv_and_execution_defaults_are_bounded() {
        let csv = CsvScanConfig::default();
        assert_eq!(csv.target_morsel_bytes, 8 * 1024 * 1024);
        assert!(csv.parallel_single_file);
        csv.validate().unwrap();

        let execution = ExecutionConfig::default();
        assert_eq!(execution.spill_partition_target_bytes, None);
        assert_eq!(execution.max_repartition_depth, 2);
        assert_eq!(execution.max_spill_write_amplification, None);
        assert_eq!(execution.runtime_filter_bytes, 8 * 1024 * 1024);
        execution.validate().unwrap();
    }

    #[test]
    fn csv_and_execution_validation_reject_zero_or_invalid_values() {
        let csv = CsvScanConfig {
            target_morsel_bytes: 0,
            ..CsvScanConfig::default()
        };
        assert!(matches!(csv.validate(), Err(Error::InvalidArgument(_))));

        let mut execution = ExecutionConfig {
            max_repartition_depth: 0,
            runtime_filter_bytes: 0,
            ..ExecutionConfig::default()
        };
        execution.validate().unwrap();
        execution.max_spill_write_amplification = Some(f64::NAN);
        assert!(matches!(
            execution.validate(),
            Err(Error::InvalidArgument(_))
        ));
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

    #[test]
    fn native_storage_validation_rejects_zero_and_incoherent_limits() {
        for config in [
            NativeStorageConfig {
                engine_limit_bytes: Some(0),
                ..NativeStorageConfig::default()
            },
            NativeStorageConfig {
                engine_limit_bytes: Some(10),
                default_table_limit_bytes: Some(11),
                ..NativeStorageConfig::default()
            },
            NativeStorageConfig {
                table_limit_bytes: BTreeMap::from([("Events".to_owned(), 1)]),
                ..NativeStorageConfig::default()
            },
            NativeStorageConfig {
                engine_limit_bytes: Some(10),
                table_limit_bytes: BTreeMap::from([("events".to_owned(), 11)]),
                ..NativeStorageConfig::default()
            },
            NativeStorageConfig {
                min_free_ratio: f64::NAN,
                ..NativeStorageConfig::default()
            },
            NativeStorageConfig {
                min_free_ratio: 1.0,
                ..NativeStorageConfig::default()
            },
        ] {
            assert!(matches!(config.validate(), Err(Error::InvalidArgument(_))));
        }
    }
}
