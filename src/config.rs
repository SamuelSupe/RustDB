use std::{path::PathBuf, sync::Arc};

use arrow::datatypes::SchemaRef;

#[derive(Clone, Debug)]
pub struct EngineConfig {
    pub memory_limit: usize,
    pub temp_dir: PathBuf,
    pub batch_size: usize,
    pub compute_threads: usize,
    pub io_concurrency: usize,
    pub max_concurrent_queries: usize,
    pub metadata_cache_bytes: usize,
    pub s3: S3Config,
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

        Self {
            memory_limit: memory_limit.max(64 * 1024 * 1024),
            temp_dir: std::env::temp_dir().join("rustdb-spill"),
            batch_size: 8_192,
            compute_threads,
            io_concurrency: 32,
            max_concurrent_queries: 1,
            metadata_cache_bytes: 64 * 1024 * 1024,
            s3: S3Config::default(),
        }
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
    pub hive_partitioning: bool,
}

#[cfg(test)]
mod tests {
    use super::S3Config;

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
}
