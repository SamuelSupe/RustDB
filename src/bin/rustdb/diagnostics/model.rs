use std::collections::BTreeMap;

use chrono::{SecondsFormat, Utc};
use rustdb::{EngineConfig, NativeCheckIssue, NativeCheckReport, ParquetPruningMode};
use serde::Serialize;

use super::storage::StorageSummary;

pub(super) const SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Serialize)]
pub(super) struct DiagnosticsDocument {
    schema_version: u32,
    generated_at: String,
    build: BuildSummary,
    database_path_sha256: String,
    native_check: NativeCheckSummary,
    storage: StorageSummary,
    runtime: RuntimeSummary,
}

impl DiagnosticsDocument {
    pub(super) fn new(
        database_path_sha256: String,
        native_check: &NativeCheckReport,
        storage: StorageSummary,
        config: &EngineConfig,
    ) -> Self {
        Self {
            schema_version: SCHEMA_VERSION,
            generated_at: Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true),
            build: BuildSummary::current(),
            database_path_sha256,
            native_check: NativeCheckSummary::from_report(native_check),
            storage,
            runtime: RuntimeSummary::from_config(config),
        }
    }
}

#[derive(Debug, Serialize)]
struct BuildSummary {
    rustdb_version: &'static str,
    target_arch: &'static str,
    target_os: &'static str,
    target_family: &'static str,
}

impl BuildSummary {
    fn current() -> Self {
        Self {
            rustdb_version: env!("CARGO_PKG_VERSION"),
            target_arch: std::env::consts::ARCH,
            target_os: std::env::consts::OS,
            target_family: std::env::consts::FAMILY,
        }
    }
}

#[derive(Debug, Serialize)]
struct NativeCheckSummary {
    ok: bool,
    format_version: Option<u32>,
    catalog_generation: Option<u64>,
    checked_tables: u64,
    checked_snapshots: u64,
    checked_files: u64,
    checked_bytes: u64,
    warning_count: usize,
    error_count: usize,
    warning_codes: Vec<IssueCodeCount>,
    error_codes: Vec<IssueCodeCount>,
}

impl NativeCheckSummary {
    fn from_report(report: &NativeCheckReport) -> Self {
        Self {
            ok: report.is_ok(),
            format_version: report.format_version(),
            catalog_generation: report.catalog_generation(),
            checked_tables: report.checked_tables(),
            checked_snapshots: report.checked_snapshots(),
            checked_files: report.checked_files(),
            checked_bytes: report.checked_bytes(),
            warning_count: report.warnings().len(),
            error_count: report.errors().len(),
            warning_codes: count_issue_codes(report.warnings()),
            error_codes: count_issue_codes(report.errors()),
        }
    }
}

#[derive(Debug, Serialize)]
struct IssueCodeCount {
    code: String,
    count: u64,
}

fn count_issue_codes(issues: &[NativeCheckIssue]) -> Vec<IssueCodeCount> {
    let mut counts = BTreeMap::<&str, u64>::new();
    for issue in issues {
        let count = counts.entry(issue.code()).or_default();
        *count = count.saturating_add(1);
    }
    counts
        .into_iter()
        .map(|(code, count)| IssueCodeCount {
            code: code.to_owned(),
            count,
        })
        .collect()
}

#[derive(Debug, Serialize)]
struct RuntimeSummary {
    memory_limit_bytes: u64,
    batch_size: usize,
    compute_threads: usize,
    io_concurrency: usize,
    max_concurrent_queries: usize,
    metadata_cache_bytes: u64,
    csv_target_morsel_bytes: u64,
    csv_parallel_single_file: bool,
    parquet_page_index: &'static str,
    parquet_bloom_filter: &'static str,
    parquet_max_pruning_metadata_bytes: u64,
    spill_engine_limit_bytes: Option<u64>,
    spill_query_limit_bytes: Option<u64>,
    spill_min_free_bytes: u64,
    spill_io_threads: usize,
    max_repartition_depth: usize,
    runtime_filter_bytes: u64,
    native_engine_limit_bytes: Option<u64>,
    native_default_table_limit_bytes: Option<u64>,
    native_table_limit_count: usize,
    native_min_free_bytes: u64,
    s3_region_configured: bool,
    s3_endpoint_configured: bool,
    s3_credential_provider_configured: bool,
    s3_force_path_style: bool,
    s3_anonymous: bool,
    s3_allow_http: bool,
}

impl RuntimeSummary {
    fn from_config(config: &EngineConfig) -> Self {
        Self {
            memory_limit_bytes: as_u64(config.memory_limit),
            batch_size: config.batch_size,
            compute_threads: config.compute_threads,
            io_concurrency: config.io_concurrency,
            max_concurrent_queries: config.max_concurrent_queries,
            metadata_cache_bytes: as_u64(config.metadata_cache_bytes),
            csv_target_morsel_bytes: as_u64(config.csv_scan.target_morsel_bytes),
            csv_parallel_single_file: config.csv_scan.parallel_single_file,
            parquet_page_index: pruning_mode(config.parquet_scan.page_index),
            parquet_bloom_filter: pruning_mode(config.parquet_scan.bloom_filter),
            parquet_max_pruning_metadata_bytes: as_u64(
                config.parquet_scan.max_pruning_metadata_bytes,
            ),
            spill_engine_limit_bytes: config.spill.engine_limit_bytes,
            spill_query_limit_bytes: config.spill.query_limit_bytes,
            spill_min_free_bytes: config.spill.min_free_bytes,
            spill_io_threads: config.spill.io_threads,
            max_repartition_depth: config.execution.max_repartition_depth,
            runtime_filter_bytes: as_u64(config.execution.runtime_filter_bytes),
            native_engine_limit_bytes: config.native_storage.engine_limit_bytes,
            native_default_table_limit_bytes: config.native_storage.default_table_limit_bytes,
            native_table_limit_count: config.native_storage.table_limit_bytes.len(),
            native_min_free_bytes: config.native_storage.min_free_bytes,
            s3_region_configured: config.s3.region.is_some(),
            s3_endpoint_configured: config.s3.endpoint.is_some(),
            s3_credential_provider_configured: config.s3.credential_provider.is_some(),
            s3_force_path_style: config.s3.force_path_style,
            s3_anonymous: config.s3.anonymous,
            s3_allow_http: config.s3.allow_http,
        }
    }
}

fn pruning_mode(mode: ParquetPruningMode) -> &'static str {
    match mode {
        ParquetPruningMode::Auto => "auto",
        ParquetPruningMode::Disabled => "disabled",
        _ => "unknown",
    }
}

fn as_u64(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use rustdb::EngineConfig;

    use super::RuntimeSummary;

    #[test]
    fn runtime_summary_omits_sensitive_values_and_paths() {
        let mut config = EngineConfig::default();
        config.s3.region = Some("secret-region-name".into());
        config.s3.endpoint = Some("https://user:password@example.invalid".into());
        config.spill.directory = "/private/secret/spill".into();
        config
            .native_storage
            .table_limit_bytes
            .insert("secret_table".into(), 1024);
        let encoded = serde_json::to_string(&RuntimeSummary::from_config(&config)).unwrap();
        for secret in ["secret-region-name", "password", "/private", "secret_table"] {
            assert!(!encoded.contains(secret));
        }
        assert!(encoded.contains("s3_endpoint_configured"));
        assert!(encoded.contains("native_table_limit_count"));
    }
}
