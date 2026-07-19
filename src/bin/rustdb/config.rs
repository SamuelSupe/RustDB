use rustdb::EngineConfig;

use super::args::Args;

pub(super) fn engine_config(args: &Args) -> EngineConfig {
    let mut config = EngineConfig::default();
    apply_engine_args(&mut config, args);
    config
}

pub(super) fn apply_engine_args(config: &mut EngineConfig, args: &Args) {
    if let Some(value) = args.memory_limit {
        config.memory_limit = value;
    }
    if let Some(value) = args.native_engine_limit {
        config.native_storage.engine_limit_bytes = Some(as_u64(value));
    }
    if let Some(value) = args.native_default_table_limit {
        config.native_storage.default_table_limit_bytes = Some(as_u64(value));
    }
    for limit in &args.native_table_limits {
        config
            .native_storage
            .table_limit_bytes
            .insert(limit.name.clone(), as_u64(limit.bytes));
    }
    if let Some(value) = args.threads {
        config.compute_threads = value;
    }
    if let Some(value) = args.batch_size {
        config.batch_size = value;
    }
    if let Some(value) = args.io_concurrency {
        config.io_concurrency = value;
    }
    if let Some(value) = args.metadata_cache {
        config.metadata_cache_bytes = value;
    }
    if let Some(value) = args.csv_target_morsel_bytes {
        config.csv_scan.target_morsel_bytes = value;
    }
    if args.no_csv_parallel_single_file {
        config.csv_scan.parallel_single_file = false;
    }
    if let Some(value) = args.parquet_page_index {
        config.parquet_scan.page_index = value.into();
    }
    if let Some(value) = args.parquet_bloom_filter {
        config.parquet_scan.bloom_filter = value.into();
    }
    if let Some(value) = args.parquet_pruning_metadata {
        config.parquet_scan.max_pruning_metadata_bytes = value;
    }
    if let Some(value) = args.max_concurrent_queries {
        config.max_concurrent_queries = value;
    }
    if let Some(value) = &args.spill_directory {
        config.spill.directory = value.clone();
    }
    if let Some(value) = args.spill_engine_limit {
        config.spill.engine_limit_bytes = Some(as_u64(value));
    }
    if let Some(value) = args.spill_query_limit {
        config.spill.query_limit_bytes = Some(as_u64(value));
    }
    if let Some(value) = args.spill_min_free_bytes {
        config.spill.min_free_bytes = as_u64(value);
    }
    if let Some(value) = args.spill_io_threads {
        config.spill.io_threads = value;
    }
    if let Some(value) = args.spill_partition_target_bytes {
        config.execution.spill_partition_target_bytes = Some(value);
    }
    if let Some(value) = args.max_repartition_depth {
        config.execution.max_repartition_depth = value;
    }
    if let Some(value) = args.max_spill_write_amplification {
        config.execution.max_spill_write_amplification = Some(value);
    }
    if let Some(value) = args.runtime_filter_bytes {
        config.execution.runtime_filter_bytes = value;
    }
    if args.s3_region.is_some() {
        config.s3.region = args.s3_region.clone();
    }
    if args.s3_endpoint.is_some() {
        config.s3.endpoint = args.s3_endpoint.clone();
    }
    if args.s3_path_style {
        config.s3.force_path_style = true;
    }
    if args.s3_allow_http {
        config.s3.allow_http = true;
    }
    if args.s3_anonymous {
        config.s3.anonymous = true;
    }
}

fn as_u64(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}
