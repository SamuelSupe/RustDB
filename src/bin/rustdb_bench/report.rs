use std::{fs::File, io::Read, path::Path};

use rustdb::{EngineConfig, Error, Result};
use serde::Serialize;
use sha2::{Digest, Sha256};
use sysinfo::System;

mod operator;

pub(super) use operator::OperatorReport;

#[derive(Debug, Serialize)]
pub(super) struct BenchmarkReport {
    pub(super) engine_version: &'static str,
    pub(super) build_id: String,
    pub(super) binary_sha256: String,
    pub(super) build: BuildReport,
    pub(super) query_file: String,
    pub(super) warmup: usize,
    pub(super) iterations: usize,
    pub(super) config: ConfigReport,
    pub(super) environment: EnvironmentReport,
    pub(super) checksum_algorithm: &'static str,
    pub(super) result_checksum_sha256: String,
    pub(super) p50_ms: f64,
    pub(super) p95_ms: f64,
    pub(super) runs: Vec<RunReport>,
}

#[derive(Debug, Serialize)]
pub(super) struct BuildReport {
    pub(super) cargo_profile: String,
    pub(super) rustflags: String,
    pub(super) rustc_version: String,
}

#[derive(Debug, Serialize)]
pub(super) struct ConfigReport {
    memory_limit_bytes: usize,
    compute_threads: usize,
    batch_size: usize,
    io_concurrency: usize,
    metadata_cache_bytes: usize,
    csv_parallel_single_file: bool,
    csv_target_morsel_bytes: usize,
    spill_partition_target_bytes: Option<usize>,
    max_repartition_depth: usize,
    max_spill_write_amplification: Option<f64>,
    runtime_filter_bytes: usize,
    rss_sample_interval_ms: u64,
}

impl ConfigReport {
    pub(super) fn new(config: &EngineConfig, rss_sample_interval_ms: u64) -> Self {
        Self {
            memory_limit_bytes: config.memory_limit,
            compute_threads: config.compute_threads,
            batch_size: config.batch_size,
            io_concurrency: config.io_concurrency,
            metadata_cache_bytes: config.metadata_cache_bytes,
            csv_parallel_single_file: config.csv_scan.parallel_single_file,
            csv_target_morsel_bytes: config.csv_scan.target_morsel_bytes,
            spill_partition_target_bytes: config.execution.spill_partition_target_bytes,
            max_repartition_depth: config.execution.max_repartition_depth,
            max_spill_write_amplification: config.execution.max_spill_write_amplification,
            runtime_filter_bytes: config.execution.runtime_filter_bytes,
            rss_sample_interval_ms,
        }
    }
}

#[derive(Debug, Serialize)]
pub(super) struct EnvironmentReport {
    os: &'static str,
    arch: &'static str,
    cpu_model: String,
    logical_cpus: usize,
    total_memory_bytes: u64,
}

#[derive(Debug, Serialize)]
pub(super) struct RunReport {
    pub(super) query_id: String,
    pub(super) elapsed_ms: f64,
    pub(super) first_batch_ms: Option<f64>,
    pub(super) rows: u64,
    pub(super) batches: u64,
    pub(super) result_checksum_sha256: String,
    pub(super) rows_per_second: f64,
    pub(super) scanned_rows: u64,
    pub(super) scanned_bytes: u64,

    // Compatibility aliases retained for existing benchmark readers.
    pub(super) current_memory_bytes: u64,
    pub(super) peak_memory_bytes: u64,
    pub(super) rss_bytes_after: Option<u64>,

    pub(super) engine_current_reservation_bytes: u64,
    pub(super) engine_peak_reservation_bytes: u64,
    pub(super) process_rss_before_bytes: Option<u64>,
    pub(super) process_peak_rss_bytes: Option<u64>,
    pub(super) process_rss_after_bytes: Option<u64>,
    pub(super) process_rss_samples: u64,

    pub(super) peak_active_lanes: u64,
    pub(super) scheduler_wait_ms: f64,
    pub(super) spill_bytes: u64,
    pub(super) spill_read_bytes: u64,
    pub(super) spill_write_bytes: u64,
    pub(super) spill_logical_input_bytes: u64,
    pub(super) spill_write_amplification_millionths: u64,
    pub(super) spill_files: u64,
    pub(super) active_spill_bytes: u64,
    pub(super) peak_active_spill_bytes: u64,
    pub(super) active_spill_files: u64,
    pub(super) peak_active_spill_files: u64,
    pub(super) spill_repartition_bytes: u64,
    pub(super) max_repartition_depth: u64,
    pub(super) max_spill_partition_bytes: u64,
    pub(super) spill_quota_rejections: u64,
    pub(super) spill_partitions: u64,
    pub(super) join_candidate_pairs: u64,
    pub(super) join_short_circuits: u64,
    pub(super) runtime_filter_hits: u64,
    pub(super) csv_source_bytes: u64,
    pub(super) csv_decompressed_bytes: u64,
    pub(super) csv_morsels: u64,
    pub(super) peak_csv_parser_lanes: u64,
    pub(super) metadata_cache_hits: u64,
    pub(super) metadata_cache_misses: u64,
    pub(super) metadata_singleflight_wait_ms: f64,
    pub(super) cancel_to_quiesce_ms: f64,
    pub(super) parquet_page_index_bytes_read: u64,
    pub(super) parquet_bloom_filter_bytes_read: u64,
    pub(super) parquet_pages_pruned: u64,
    pub(super) parquet_page_rows_pruned: u64,
    pub(super) parquet_bloom_row_groups_pruned: u64,
    pub(super) parquet_pruning_budget_skips: u64,
    pub(super) s3_requests: u64,
    pub(super) s3_bytes_transferred: u64,
    pub(super) operators: Vec<OperatorReport>,
    pub(super) spill_cleaned: bool,
}

pub(super) fn environment(cpu_model: Option<String>) -> EnvironmentReport {
    let system = System::new_all();
    EnvironmentReport {
        os: std::env::consts::OS,
        arch: std::env::consts::ARCH,
        cpu_model: cpu_model
            .filter(|model| !model.trim().is_empty())
            .or_else(|| {
                system
                    .cpus()
                    .first()
                    .map(|cpu| cpu.brand().to_owned())
                    .filter(|model| !model.trim().is_empty())
            })
            .unwrap_or_else(|| "unknown".to_owned()),
        logical_cpus: std::thread::available_parallelism()
            .map(usize::from)
            .unwrap_or(1),
        total_memory_bytes: system.total_memory(),
    }
}

pub(super) fn executable_sha256() -> Result<String> {
    let path = std::env::current_exe().map_err(|error| Error::io(None, error))?;
    file_sha256(&path)
}

fn file_sha256(path: &Path) -> Result<String> {
    let mut file = File::open(path).map_err(|error| Error::io(Some(path.to_path_buf()), error))?;
    let mut digest = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let bytes = file
            .read(&mut buffer)
            .map_err(|error| Error::io(Some(path.to_path_buf()), error))?;
        if bytes == 0 {
            break;
        }
        digest.update(&buffer[..bytes]);
    }
    Ok(format!("{:x}", digest.finalize()))
}

pub(super) fn percentile(sorted: &[f64], percentile: f64) -> f64 {
    let index = ((sorted.len() - 1) as f64 * percentile).ceil() as usize;
    sorted[index.min(sorted.len() - 1)]
}

#[cfg(test)]
mod tests {
    use super::{file_sha256, percentile};

    #[test]
    fn percentile_uses_nearest_rank() {
        assert_eq!(percentile(&[1.0, 2.0, 3.0, 4.0], 0.50), 3.0);
        assert_eq!(percentile(&[1.0, 2.0, 3.0, 4.0], 0.95), 4.0);
    }

    #[test]
    fn file_digest_is_lowercase_sha256() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("payload");
        std::fs::write(&path, b"abc").unwrap();
        assert_eq!(
            file_sha256(&path).unwrap(),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
    }
}
