use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use futures::StreamExt;
use object_store::{ObjectStoreExt, aws::AmazonS3Builder, path::Path};
use rustdb::{
    Engine, EngineConfig, ParquetPruningMode, QueryMetricsSnapshot, Result, S3Config, Session,
};
use uuid::Uuid;

mod support;

use support::parquet_pruning;

const BUCKET: &str = "rustdb-tests";

#[tokio::test]
async fn minio_deep_pruning_fallback_matrix() -> Result<()> {
    let Some(endpoint) = minio_endpoint() else {
        return Ok(());
    };
    let prefix = format!("deep-pruning/{}/", Uuid::new_v4());
    let regular_path = Path::from(format!("{prefix}regular.parquet"));
    let oversized_path = Path::from(format!("{prefix}oversized-bloom.parquet"));
    let store = store(&endpoint)?;
    let regular = parquet_pruning::deep_pruning_bytes()?;
    let oversized = parquet_pruning::with_bloom_length(
        regular.clone(),
        parquet_pruning::OVERSIZED_BLOOM_LENGTH,
    )?;
    store
        .put(&regular_path, Bytes::from(regular).into())
        .await?;
    store
        .put(&oversized_path, Bytes::from(oversized).into())
        .await?;

    let regular_uri = format!("s3://{BUCKET}/{regular_path}");
    let oversized_uri = format!("s3://{BUCKET}/{oversized_path}");
    exercise_bloom_matrix(&endpoint, &regular_uri).await?;
    exercise_budget_fallbacks(&endpoint, &regular_uri, &oversized_uri).await?;

    store.delete(&regular_path).await?;
    store.delete(&oversized_path).await?;
    Ok(())
}

async fn exercise_bloom_matrix(endpoint: &str, uri: &str) -> Result<()> {
    let mut config = engine_config(endpoint);
    config.parquet_scan.page_index = ParquetPruningMode::Disabled;
    let session = Engine::new(config)?.session();

    let (missing, negative) = count(&session, uri, "id = 51").await?;
    assert_eq!(missing, 0);
    assert!(negative.parquet_bloom_filter_bytes_read > 0);
    assert_eq!(negative.parquet_bloom_row_groups_pruned, 1);
    assert_eq!(negative.rows_scanned, 0);
    assert_s3_read(&negative);

    let (present, positive) = count(&session, uri, "id = 50").await?;
    assert_eq!(present, 1);
    assert!(positive.parquet_bloom_filter_bytes_read > 0);
    assert_eq!(positive.parquet_bloom_row_groups_pruned, 0);
    assert_eq!(positive.rows_scanned, parquet_pruning::ROWS as u64);
    assert_s3_read(&positive);

    let (unsupported, fallback) = count(&session, uri, "score = 50.5").await?;
    assert_eq!(unsupported, 1);
    assert_eq!(fallback.parquet_bloom_filter_bytes_read, 0);
    assert_eq!(fallback.parquet_bloom_row_groups_pruned, 0);
    assert_eq!(fallback.parquet_pruning_budget_skips, 0);
    assert_eq!(fallback.rows_scanned, parquet_pruning::ROWS as u64);
    assert_s3_read(&fallback);

    let mut disabled = engine_config(endpoint);
    disabled.parquet_scan.page_index = ParquetPruningMode::Disabled;
    disabled.parquet_scan.bloom_filter = ParquetPruningMode::Disabled;
    let disabled = Engine::new(disabled)?.session();
    let (missing, disabled_metrics) = count(&disabled, uri, "id = 51").await?;
    assert_eq!(missing, 0);
    assert_eq!(disabled_metrics.parquet_page_index_bytes_read, 0);
    assert_eq!(disabled_metrics.parquet_bloom_filter_bytes_read, 0);
    assert_eq!(disabled_metrics.parquet_bloom_row_groups_pruned, 0);
    assert_eq!(disabled_metrics.parquet_pruning_budget_skips, 0);
    assert_eq!(disabled_metrics.rows_scanned, parquet_pruning::ROWS as u64);
    assert_s3_read(&disabled_metrics);
    Ok(())
}

async fn exercise_budget_fallbacks(
    endpoint: &str,
    regular_uri: &str,
    oversized_uri: &str,
) -> Result<()> {
    let mut no_query_budget = engine_config(endpoint);
    no_query_budget.parquet_scan.page_index = ParquetPruningMode::Disabled;
    no_query_budget.parquet_scan.max_pruning_metadata_bytes = 0;
    let no_query_budget = Engine::new(no_query_budget)?.session();
    let (missing, query_budget) = count(&no_query_budget, regular_uri, "id = 51").await?;
    assert_eq!(missing, 0);
    assert_eq!(query_budget.parquet_page_index_bytes_read, 0);
    assert_eq!(query_budget.parquet_bloom_filter_bytes_read, 0);
    assert_eq!(query_budget.parquet_pruning_budget_skips, 1);
    assert_eq!(query_budget.rows_scanned, parquet_pruning::ROWS as u64);
    assert_s3_read(&query_budget);

    let mut file_budget = engine_config(endpoint);
    file_budget.parquet_scan.page_index = ParquetPruningMode::Disabled;
    let file_budget = Engine::new(file_budget)?.session();
    let (missing, file_budget) = count(&file_budget, oversized_uri, "id = 51").await?;
    assert_eq!(missing, 0);
    assert_eq!(file_budget.parquet_page_index_bytes_read, 0);
    assert_eq!(file_budget.parquet_bloom_filter_bytes_read, 0);
    assert_eq!(file_budget.parquet_pruning_budget_skips, 1);
    assert_eq!(file_budget.rows_scanned, parquet_pruning::ROWS as u64);
    assert_s3_read(&file_budget);
    Ok(())
}

async fn count(
    session: &Session,
    uri: &str,
    predicate: &str,
) -> Result<(i64, QueryMetricsSnapshot)> {
    let mut result = session
        .execute(&format!(
            "SELECT count(*) FROM read_parquet('{uri}') WHERE {predicate}"
        ))
        .await?;
    let metrics = result.metrics();
    let batch = result.stream().next().await.expect("count result")?;
    let value = int64_value(&batch);
    assert!(result.stream().next().await.is_none());
    drop(result);
    Ok((value, metrics.snapshot()))
}

fn int64_value(batch: &RecordBatch) -> i64 {
    batch
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .expect("COUNT returns Int64")
        .value(0)
}

fn assert_s3_read(metrics: &QueryMetricsSnapshot) {
    assert!(metrics.s3_requests > 0, "missing S3 request: {metrics:?}");
    assert!(
        metrics.s3_bytes_transferred > 0,
        "missing S3 byte accounting: {metrics:?}"
    );
    assert!(
        metrics.s3_bytes_transferred >= metrics.parquet_bloom_filter_bytes_read,
        "Bloom bytes exceed total S3 bytes: {metrics:?}"
    );
}

fn engine_config(endpoint: &str) -> EngineConfig {
    EngineConfig::builder()
        .batch_size(128)
        .io_concurrency(1)
        .s3(S3Config {
            endpoint: Some(endpoint.to_owned()),
            region: Some("us-east-1".to_owned()),
            force_path_style: true,
            allow_http: true,
            ..S3Config::default()
        })
        .build()
}

fn store(endpoint: &str) -> Result<impl object_store::ObjectStore> {
    Ok(AmazonS3Builder::from_env()
        .with_bucket_name(BUCKET)
        .with_region("us-east-1")
        .with_endpoint(endpoint)
        .with_allow_http(true)
        .with_virtual_hosted_style_request(false)
        .build()?)
}

fn minio_endpoint() -> Option<String> {
    let required = matches!(std::env::var("RUSTDB_REQUIRE_MINIO").as_deref(), Ok("1"));
    match std::env::var("RUSTDB_MINIO_ENDPOINT") {
        Ok(endpoint) if !endpoint.trim().is_empty() => Some(endpoint),
        Ok(_) if required => panic!("RUSTDB_MINIO_ENDPOINT must not be empty"),
        Err(error) if required => {
            panic!("RUSTDB_REQUIRE_MINIO=1 but RUSTDB_MINIO_ENDPOINT is unavailable: {error}")
        }
        _ => {
            eprintln!("RUSTDB_MINIO_ENDPOINT is unset; skipping MinIO integration");
            None
        }
    }
}
