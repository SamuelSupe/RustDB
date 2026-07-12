use std::path::PathBuf;

use super::{EngineConfig, ParquetPruningMode, ParquetScanConfig, S3Config, SpillConfig};

#[derive(Clone, Debug)]
pub struct EngineConfigBuilder {
    config: EngineConfig,
}

impl EngineConfigBuilder {
    pub(crate) fn new() -> Self {
        Self {
            config: EngineConfig::default(),
        }
    }

    #[must_use]
    pub fn memory_limit(mut self, bytes: usize) -> Self {
        self.config.memory_limit = bytes;
        self
    }

    #[must_use]
    pub fn batch_size(mut self, rows: usize) -> Self {
        self.config.batch_size = rows;
        self
    }

    #[must_use]
    pub fn compute_threads(mut self, threads: usize) -> Self {
        self.config.compute_threads = threads;
        self
    }

    #[must_use]
    pub fn io_concurrency(mut self, concurrency: usize) -> Self {
        self.config.io_concurrency = concurrency;
        self
    }

    #[must_use]
    pub fn max_concurrent_queries(mut self, queries: usize) -> Self {
        self.config.max_concurrent_queries = queries;
        self
    }

    #[must_use]
    pub fn metadata_cache_bytes(mut self, bytes: usize) -> Self {
        self.config.metadata_cache_bytes = bytes;
        self
    }

    #[must_use]
    pub fn parquet_scan(mut self, parquet_scan: ParquetScanConfig) -> Self {
        self.config.parquet_scan = parquet_scan;
        self
    }

    #[must_use]
    pub fn parquet_page_index(mut self, mode: ParquetPruningMode) -> Self {
        self.config.parquet_scan.page_index = mode;
        self
    }

    #[must_use]
    pub fn parquet_bloom_filter(mut self, mode: ParquetPruningMode) -> Self {
        self.config.parquet_scan.bloom_filter = mode;
        self
    }

    #[must_use]
    pub fn parquet_pruning_metadata_bytes(mut self, bytes: usize) -> Self {
        self.config.parquet_scan.max_pruning_metadata_bytes = bytes;
        self
    }

    #[must_use]
    pub fn s3(mut self, s3: S3Config) -> Self {
        self.config.s3 = s3;
        self
    }

    #[must_use]
    pub fn spill(mut self, spill: SpillConfig) -> Self {
        self.config.spill = spill;
        self
    }

    #[must_use]
    pub fn spill_directory(mut self, directory: impl Into<PathBuf>) -> Self {
        let directory = directory.into();
        self.config.spill.directory = directory;
        self
    }

    pub fn build(self) -> EngineConfig {
        self.config
    }
}

impl Default for EngineConfigBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::EngineConfig;
    use crate::ParquetPruningMode;

    #[test]
    fn builder_overrides_resource_settings() {
        let config = EngineConfig::builder()
            .memory_limit(128 << 20)
            .batch_size(4_096)
            .compute_threads(4)
            .parquet_page_index(ParquetPruningMode::Disabled)
            .parquet_bloom_filter(ParquetPruningMode::Disabled)
            .parquet_pruning_metadata_bytes(2 << 20)
            .spill_directory("/tmp/rustdb-builder-spill")
            .build();
        assert_eq!(config.memory_limit, 128 << 20);
        assert_eq!(config.batch_size, 4_096);
        assert_eq!(config.compute_threads, 4);
        assert_eq!(config.parquet_scan.page_index, ParquetPruningMode::Disabled);
        assert_eq!(
            config.parquet_scan.bloom_filter,
            ParquetPruningMode::Disabled
        );
        assert_eq!(config.parquet_scan.max_pruning_metadata_bytes, 2 << 20);
        assert_eq!(
            config.spill.directory,
            PathBuf::from("/tmp/rustdb-builder-spill")
        );
    }
}
