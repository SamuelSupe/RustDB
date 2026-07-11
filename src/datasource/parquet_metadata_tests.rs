use std::{fs::File, sync::Arc};

use arrow::{
    array::Int64Array,
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use parquet::arrow::ArrowWriter;
use tempfile::{TempDir, tempdir};

use super::{estimated_metadata_bytes, load_parquet_metadata, registration_metadata_limit};
use crate::{
    EngineConfig, Error, S3Config,
    datasource::MetadataCache,
    runtime::{MemoryPool, QueryContext},
    storage::{LocationResolver, ObjectSource},
};

#[test]
fn footer_estimate_includes_expansion_and_fixed_overhead() {
    assert!(estimated_metadata_bytes(1) > 64 * 1024);
    assert!(estimated_metadata_bytes(1024) > estimated_metadata_bytes(1));
}

#[test]
fn registration_limit_is_derived_and_bounded() {
    let config = EngineConfig {
        memory_limit: 1024,
        ..EngineConfig::default()
    };
    assert_eq!(registration_metadata_limit(&config), 128);
    let config = EngineConfig {
        memory_limit: usize::MAX,
        ..EngineConfig::default()
    };
    assert_eq!(registration_metadata_limit(&config), 64 * 1024 * 1024);
}

#[tokio::test]
async fn rejects_footer_before_decode_when_query_pool_is_too_small() {
    let (_directory, source) = fixture().await;
    let spill = tempdir().unwrap();
    let context = QueryContext::new(MemoryPool::new(1024), spill.path()).unwrap();
    let baseline = context.memory.used();
    let result = load_parquet_metadata(
        &source,
        source.snapshot().clone(),
        Some(&context),
        &MetadataCache::new(usize::MAX),
        usize::MAX,
    )
    .await;
    let message = match result {
        Err(Error::ResourceExhausted(message)) => message,
        Err(error) => panic!("expected a resource exhausted error, got {error}"),
        Ok(_) => panic!("expected rejection"),
    };
    assert!(message.contains(source.uri()));
    assert!(message.contains("requires"));
    assert!(message.contains("available"));
    assert_eq!(context.memory.used(), baseline);
}

#[tokio::test]
async fn rejects_registration_footer_over_per_file_limit() {
    let (_directory, source) = fixture().await;
    let result = load_parquet_metadata(
        &source,
        source.snapshot().clone(),
        None,
        &MetadataCache::new(usize::MAX),
        1,
    )
    .await;
    let message = match result {
        Err(Error::ResourceExhausted(message)) => message,
        Err(error) => panic!("expected a resource exhausted error, got {error}"),
        Ok(_) => panic!("expected rejection"),
    };
    assert!(message.contains(source.uri()));
    assert!(message.contains("requires"));
    assert!(message.contains("available"));
}

#[tokio::test]
async fn reservation_lives_across_clones_and_covers_cache_hits() {
    let (_directory, source) = fixture().await;
    let spill = tempdir().unwrap();
    let context = QueryContext::new(MemoryPool::new(16 * 1024 * 1024), spill.path()).unwrap();
    let cache = MetadataCache::new(usize::MAX);
    let baseline = context.memory.used();

    let metadata = load_parquet_metadata(
        &source,
        source.snapshot().clone(),
        Some(&context),
        &cache,
        usize::MAX,
    )
    .await
    .unwrap();
    let reserved = metadata.reserved_bytes();
    assert!(reserved > 0);
    assert_eq!(context.memory.used(), baseline + reserved);
    let clone = metadata.clone();
    drop(metadata);
    assert_eq!(context.memory.used(), baseline + reserved);
    drop(clone);
    assert_eq!(context.memory.used(), baseline);

    let cached = load_parquet_metadata(
        &source,
        source.snapshot().clone(),
        Some(&context),
        &cache,
        usize::MAX,
    )
    .await
    .unwrap();
    assert_eq!(cached.reserved_bytes(), reserved);
    assert_eq!(context.memory.used(), baseline + reserved);
    drop(cached);
    assert_eq!(context.memory.used(), baseline);
}

async fn fixture() -> (TempDir, ObjectSource) {
    let directory = tempdir().unwrap();
    let path = directory.path().join("metadata.parquet");
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from(vec![1_i64, 2, 3]))],
    )
    .unwrap();
    let mut writer = ArrowWriter::try_new(File::create(&path).unwrap(), schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    let source = LocationResolver::new(S3Config::default())
        .resolve(&[path.display().to_string()])
        .await
        .unwrap()
        .pop()
        .unwrap();
    (directory, source)
}
