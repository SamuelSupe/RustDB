use std::{fs::File, sync::Arc};

use arrow::{
    array::{Int64Array, StringArray},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use bytes::Bytes;
use futures::StreamExt;
use object_store::{ObjectStoreExt, aws::AmazonS3Builder, path::Path};
use parquet::{arrow::ArrowWriter, basic::Compression, file::properties::WriterProperties};
use rustdb::{Engine, EngineConfig, Error, Result, S3Config};
use uuid::Uuid;

const BUCKET: &str = "rustdb-tests";
const PUBLIC_BUCKET: &str = "rustdb-public";
const ROW_GROUP_ROWS: usize = 1_024;
const ROW_GROUPS: usize = 8;

#[tokio::test]
async fn queries_csv_and_parquet_from_minio() -> Result<()> {
    let Some(endpoint) = minio_endpoint() else {
        return Ok(());
    };
    let prefix = format!("integration/{}/", Uuid::new_v4());
    let csv_path = Path::from(format!("{prefix}events.csv"));
    let parquet_path = Path::from(format!("{prefix}events.parquet"));
    let pruning_path = Path::from(format!("{prefix}row-group-pruning.parquet"));
    let store = AmazonS3Builder::from_env()
        .with_bucket_name(BUCKET)
        .with_region("us-east-1")
        .with_endpoint(&endpoint)
        .with_allow_http(true)
        .with_virtual_hosted_style_request(false)
        .build()?;

    store
        .put(
            &csv_path,
            Bytes::from_static(b"id,kind\n1,a\n2,b\n3,a\n").into(),
        )
        .await?;
    let parquet_bytes = parquet_fixture()?;
    store
        .put(&parquet_path, Bytes::from(parquet_bytes).into())
        .await?;
    let pruning_bytes = pruning_fixture('x')?;
    let pruning_object_size = u64::try_from(pruning_bytes.len()).unwrap_or(u64::MAX);
    assert!(
        pruning_object_size > 4 * 1_024 * 1_024,
        "the pruning fixture must be large enough to detect whole-object downloads"
    );
    store
        .put(&pruning_path, Bytes::from(pruning_bytes).into())
        .await?;

    let config = EngineConfig {
        batch_size: 128,
        io_concurrency: 1,
        s3: S3Config {
            endpoint: Some(endpoint.clone()),
            region: Some("us-east-1".to_owned()),
            force_path_style: true,
            allow_http: true,
            ..S3Config::default()
        },
        ..EngineConfig::default()
    };
    let mut uncached_config = config.clone();
    uncached_config.metadata_cache_bytes = 0;
    let session = Engine::new(config)?.session();

    let csv_sql = format!(
        "SELECT count(*) FROM read_csv('s3://{BUCKET}/{csv_path}', header = true) WHERE kind = 'a'"
    );
    let mut csv_result = session.execute(&csv_sql).await?;
    let csv_metrics = csv_result.metrics();
    let csv_batch = csv_result.stream().next().await.unwrap()?;
    assert_eq!(int64_value(&csv_batch), 2);
    drop(csv_result);
    let csv_metrics = csv_metrics.snapshot();
    assert!(
        csv_metrics.s3_requests >= 4,
        "location HEAD, schema sample, query snapshot, and data GET must be accounted: {csv_metrics:?}"
    );
    assert!(csv_metrics.s3_bytes_transferred > 0);

    let parquet_sql =
        format!("SELECT count(*) FROM read_parquet('s3://{BUCKET}/{parquet_path}') WHERE id >= 2");
    let mut parquet_result = session.execute(&parquet_sql).await?;
    let parquet_metrics = parquet_result.metrics();
    let parquet_batch = parquet_result.stream().next().await.unwrap()?;
    assert_eq!(int64_value(&parquet_batch), 2);
    drop(parquet_result);
    let parquet_metrics = parquet_metrics.snapshot();
    assert!(parquet_metrics.s3_requests >= 2);
    assert!(parquet_metrics.s3_bytes_transferred > 0);

    let uncached_session = Engine::new(uncached_config)?.session();
    let mut uncached_result = uncached_session.execute(&parquet_sql).await?;
    let uncached_metrics = uncached_result.metrics();
    let uncached_batch = uncached_result.stream().next().await.unwrap()?;
    assert_eq!(int64_value(&uncached_batch), 2);
    drop(uncached_result);
    assert!(
        uncached_metrics.snapshot().s3_requests > parquet_metrics.s3_requests,
        "disabled metadata cache did not add footer range requests"
    );

    let first_retained_id = ROW_GROUP_ROWS * (ROW_GROUPS - 1);
    let pruning_sql = format!(
        "SELECT count(id), min(id), max(id) \
         FROM read_parquet('s3://{BUCKET}/{pruning_path}') \
         WHERE id >= {first_retained_id}"
    );
    let mut pruning_result = session.execute(&pruning_sql).await?;
    let pruning_metrics = pruning_result.metrics();
    let pruning_batch = pruning_result.stream().next().await.unwrap()?;
    assert_eq!(int64_value_at(&pruning_batch, 0), ROW_GROUP_ROWS as i64);
    assert_eq!(
        int64_value_at(&pruning_batch, 1),
        i64::try_from(first_retained_id).unwrap()
    );
    assert_eq!(
        int64_value_at(&pruning_batch, 2),
        i64::try_from(ROW_GROUP_ROWS * ROW_GROUPS - 1).unwrap()
    );
    drop(pruning_result);
    let pruning_metrics = pruning_metrics.snapshot();
    assert!(
        pruning_metrics.row_groups_pruned >= u64::try_from(ROW_GROUPS - 1).unwrap(),
        "expected all non-matching row groups to be pruned: {pruning_metrics:?}"
    );
    assert!(
        pruning_metrics.s3_bytes_transferred < pruning_object_size / 4,
        "projected scan transferred {} bytes from a {} byte object",
        pruning_metrics.s3_bytes_transferred,
        pruning_object_size
    );

    let mut cancelled = session
        .execute(&format!(
            "SELECT payload FROM read_parquet('s3://{BUCKET}/{pruning_path}')"
        ))
        .await?;
    cancelled.cancel();
    let cancellation = cancelled
        .stream()
        .next()
        .await
        .expect("cancelled query must report a terminal result")
        .expect_err("cancelled query must not produce a batch");
    assert!(matches!(cancellation, Error::Cancelled));
    drop(cancelled);

    let mut changing = session
        .execute(&format!(
            "SELECT payload FROM read_parquet('s3://{BUCKET}/{pruning_path}')"
        ))
        .await?;
    let first_batch = changing
        .stream()
        .next()
        .await
        .expect("scan must produce a batch before the object is replaced")?;
    assert!(first_batch.num_rows() > 0);
    store
        .put(&pruning_path, Bytes::from(pruning_fixture('y')?).into())
        .await?;
    let mut changed_error = None;
    while let Some(batch) = changing.stream().next().await {
        match batch {
            Ok(_) => {}
            Err(error) => {
                changed_error = Some(error);
                break;
            }
        }
    }
    let changed_error = changed_error.expect("replacing an object during a query must fail");
    assert!(
        changed_error
            .to_string()
            .contains("object changed during query"),
        "unexpected object replacement error: {changed_error}"
    );

    let public_path = Path::from(format!("{prefix}anonymous.csv"));
    let public_store = AmazonS3Builder::from_env()
        .with_bucket_name(PUBLIC_BUCKET)
        .with_region("us-east-1")
        .with_endpoint(&endpoint)
        .with_allow_http(true)
        .with_virtual_hosted_style_request(false)
        .build()?;
    public_store
        .put(&public_path, Bytes::from_static(b"id\n1\n2\n").into())
        .await?;
    let anonymous = EngineConfig {
        s3: S3Config {
            endpoint: Some(endpoint.clone()),
            region: Some("us-east-1".to_owned()),
            force_path_style: true,
            anonymous: true,
            allow_http: true,
            ..S3Config::default()
        },
        ..EngineConfig::default()
    };
    let anonymous_session = Engine::new(anonymous)?.session();
    let mut anonymous_result = anonymous_session
        .execute(&format!(
            "SELECT count(*) FROM read_csv('s3://{PUBLIC_BUCKET}/{public_path}', header = true)"
        ))
        .await?;
    assert_eq!(
        int64_value(&anonymous_result.stream().next().await.unwrap()?),
        2
    );
    drop(anonymous_result);

    store.delete(&csv_path).await?;
    store.delete(&parquet_path).await?;
    store.delete(&pruning_path).await?;
    public_store.delete(&public_path).await?;
    Ok(())
}

fn minio_endpoint() -> Option<String> {
    let required = matches!(std::env::var("RUSTDB_REQUIRE_MINIO").as_deref(), Ok("1"));
    match std::env::var("RUSTDB_MINIO_ENDPOINT") {
        Ok(endpoint) => {
            assert!(
                !endpoint.trim().is_empty(),
                "RUSTDB_MINIO_ENDPOINT must not be empty"
            );
            Some(endpoint)
        }
        Err(error) if required => {
            panic!("RUSTDB_REQUIRE_MINIO=1 but RUSTDB_MINIO_ENDPOINT is unavailable: {error}")
        }
        Err(_) => {
            eprintln!("RUSTDB_MINIO_ENDPOINT is unset; skipping MinIO integration");
            None
        }
    }
}

fn parquet_fixture() -> Result<Vec<u8>> {
    let temp = tempfile::NamedTempFile::new().map_err(|error| rustdb::Error::io(None, error))?;
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("kind", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["a", "b", "a"])),
        ],
    )?;
    let file = File::create(temp.path())
        .map_err(|error| rustdb::Error::io(Some(temp.path().to_path_buf()), error))?;
    let mut writer = ArrowWriter::try_new(file, schema, None)?;
    writer.write(&batch)?;
    writer.close()?;
    std::fs::read(temp.path())
        .map_err(|error| rustdb::Error::io(Some(temp.path().to_path_buf()), error))
}

fn pruning_fixture(marker: char) -> Result<Vec<u8>> {
    let temp = tempfile::NamedTempFile::new().map_err(|error| rustdb::Error::io(None, error))?;
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("payload", DataType::Utf8, false),
    ]));
    let rows = ROW_GROUP_ROWS * ROW_GROUPS;
    let ids = (0..rows)
        .map(|value| i64::try_from(value).unwrap())
        .collect::<Vec<_>>();
    let payload_suffix = marker.to_string().repeat(1_024);
    let payload = (0..rows)
        .map(|row| format!("{row:08x}:{payload_suffix}"))
        .collect::<Vec<_>>();
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(StringArray::from(payload)),
        ],
    )?;
    let properties = WriterProperties::builder()
        .set_max_row_group_row_count(Some(ROW_GROUP_ROWS))
        .set_compression(Compression::UNCOMPRESSED)
        .set_dictionary_enabled(false)
        .build();
    let file = File::create(temp.path())
        .map_err(|error| rustdb::Error::io(Some(temp.path().to_path_buf()), error))?;
    let mut writer = ArrowWriter::try_new(file, schema, Some(properties))?;
    writer.write(&batch)?;
    writer.close()?;
    std::fs::read(temp.path())
        .map_err(|error| rustdb::Error::io(Some(temp.path().to_path_buf()), error))
}

fn int64_value(batch: &RecordBatch) -> i64 {
    int64_value_at(batch, 0)
}

fn int64_value_at(batch: &RecordBatch, column: usize) -> i64 {
    batch
        .column(column)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0)
}
