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
use rustdb::{
    CsvHeader, CsvOptions, Engine, EngineConfig, Error, ParquetOptions, ParquetSchemaMode, Result,
    S3Config,
};
use uuid::Uuid;

mod support;

use support::{parquet_evolution, parquet_pruning};

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
    let deep_pruning_path = Path::from(format!("{prefix}deep-pruning.parquet"));
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
    let parquet_object_size = parquet_bytes.len();
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
    store
        .put(
            &deep_pruning_path,
            Bytes::from(parquet_pruning::deep_pruning_bytes()?).into(),
        )
        .await?;

    let config = EngineConfig::builder()
        .batch_size(128)
        .io_concurrency(1)
        .s3(S3Config {
            endpoint: Some(endpoint.clone()),
            region: Some("us-east-1".to_owned()),
            force_path_style: true,
            allow_http: true,
            ..S3Config::default()
        })
        .build();
    let mut uncached_config = config.clone();
    uncached_config.metadata_cache_bytes = 0;
    let session = Engine::new(config)?.session();

    let dynamic_a = Path::from(format!("{prefix}dynamic/a.csv"));
    let dynamic_b = Path::from(format!("{prefix}dynamic/b.csv"));
    store
        .put(&dynamic_a, Bytes::from_static(b"id\n1\n").into())
        .await?;
    session
        .register_csv(
            "dynamic_s3",
            [format!("s3://{BUCKET}/{prefix}dynamic/*.csv")],
            CsvOptions {
                header: CsvHeader::Present,
                ..CsvOptions::default()
            },
        )
        .await?;
    assert_eq!(query_count(&session, "dynamic_s3").await?, 1);
    store
        .put(&dynamic_b, Bytes::from_static(b"id\n2\n").into())
        .await?;
    assert_eq!(query_count(&session, "dynamic_s3").await?, 2);
    store.delete(&dynamic_a).await?;
    assert_eq!(query_count(&session, "dynamic_s3").await?, 1);
    store
        .put(
            &dynamic_b,
            Bytes::from_static(b"id,note\n2,refreshed\n").into(),
        )
        .await?;
    let strict_error = match session.execute("SELECT * FROM dynamic_s3").await {
        Err(error) => error,
        Ok(mut result) => result
            .stream()
            .next()
            .await
            .expect("strict CSV schema result")
            .expect_err("strict CSV schema change must fail"),
    };
    let strict_message = strict_error.to_string();
    assert!(
        strict_message.contains("CSV schema mismatch") && strict_message.contains("dynamic/b.csv"),
        "unexpected strict-schema error: {strict_error}"
    );
    let refreshed = session.refresh_table("dynamic_s3").await?;
    assert_eq!(refreshed.fields().len(), 2);
    assert_eq!(query_count(&session, "dynamic_s3").await?, 1);

    let widening_a = Path::from(format!("{prefix}widening/a.parquet"));
    let widening_b = Path::from(format!("{prefix}widening/b.parquet"));
    let widening_c = Path::from(format!("{prefix}widening/c.parquet"));
    let widening_d = Path::from(format!("{prefix}widening/d.parquet"));
    store
        .put(
            &widening_a,
            Bytes::from(parquet_evolution::bytes_i32(
                &[1, 2],
                "label",
                &["one", "two"],
            )?)
            .into(),
        )
        .await?;
    store
        .put(
            &widening_b,
            Bytes::from(parquet_evolution::bytes_i64(
                &[3, 4],
                "label",
                &["three", "four"],
            )?)
            .into(),
        )
        .await?;
    session
        .register_parquet(
            "widening_s3",
            [format!("s3://{BUCKET}/{prefix}widening/*.parquet")],
            ParquetOptions {
                schema_mode: ParquetSchemaMode::SafeWidening,
                ..ParquetOptions::default()
            },
        )
        .await?;
    let mut widened = session
        .execute("SELECT id FROM widening_s3 ORDER BY id")
        .await?;
    let mut widened_ids = Vec::new();
    while let Some(batch) = widened.stream().next().await {
        let batch = batch?;
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("SafeWidening must expose Int64");
        widened_ids.extend(ids.values().iter().copied());
    }
    assert_eq!(widened_ids, [1, 2, 3, 4]);

    store.delete(&widening_a).await?;
    store.delete(&widening_b).await?;
    store
        .put(
            &widening_c,
            Bytes::from(parquet_evolution::bytes_i64(&[10], "new_value", &["ten"])?).into(),
        )
        .await?;
    store
        .put(
            &widening_d,
            Bytes::from(parquet_evolution::bytes_i64(
                &[20],
                "new_value",
                &["twenty"],
            )?)
            .into(),
        )
        .await?;
    let widening_error = match session.execute("SELECT count(*) FROM widening_s3").await {
        Err(error) => error,
        Ok(mut result) => result
            .stream()
            .next()
            .await
            .expect("schema mismatch must produce a terminal result")
            .expect_err("a missing registered column must require REFRESH TABLE"),
    };
    assert!(widening_error.to_string().contains("label"));

    let widening_schema = session.refresh_table("widening_s3").await?;
    assert_eq!(widening_schema.fields().len(), 2);
    assert_eq!(widening_schema.field(0).name(), "id");
    assert_eq!(widening_schema.field(0).data_type(), &DataType::Int64);
    assert_eq!(widening_schema.field(1).name(), "new_value");
    let mut refreshed_result = session
        .execute("SELECT id, new_value FROM widening_s3 ORDER BY id")
        .await?;
    let mut refreshed_rows = Vec::new();
    while let Some(batch) = refreshed_result.stream().next().await {
        let batch = batch?;
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let values = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        refreshed_rows.extend(
            (0..batch.num_rows()).map(|row| (ids.value(row), values.value(row).to_owned())),
        );
    }
    assert_eq!(
        refreshed_rows,
        [(10, "ten".to_owned()), (20, "twenty".to_owned())]
    );

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

    let count_sql = format!("SELECT count(*) FROM read_parquet('s3://{BUCKET}/{parquet_path}')");
    let mut count_result = session.execute(&count_sql).await?;
    let count_metrics = count_result.metrics();
    assert_eq!(
        int64_value(&count_result.stream().next().await.unwrap()?),
        3
    );
    drop(count_result);
    let count_metrics = count_metrics.snapshot();
    assert_eq!(
        count_metrics.bytes_scanned, 0,
        "COUNT(*) decoded data pages"
    );
    assert!(
        count_metrics.s3_bytes_transferred > 0
            && count_metrics.s3_bytes_transferred <= parquet_object_size as u64,
        "metadata-only COUNT transferred invalid byte volume: {count_metrics:?}"
    );

    let large_count_sql =
        format!("SELECT count(*) FROM read_parquet('s3://{BUCKET}/{pruning_path}')");
    let mut large_count_result = session.execute(&large_count_sql).await?;
    let large_count_metrics = large_count_result.metrics();
    assert_eq!(
        int64_value(&large_count_result.stream().next().await.unwrap()?),
        i64::try_from(ROW_GROUP_ROWS * ROW_GROUPS).unwrap()
    );
    drop(large_count_result);
    let large_count_metrics = large_count_metrics.snapshot();
    assert_eq!(large_count_metrics.bytes_scanned, 0);
    assert!(
        large_count_metrics.s3_bytes_transferred > 0
            && large_count_metrics.s3_bytes_transferred < pruning_object_size / 4,
        "metadata-only COUNT transferred {} bytes from a {} byte object",
        large_count_metrics.s3_bytes_transferred,
        pruning_object_size,
    );

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

    let deep_count_sql =
        format!("SELECT count(*) FROM read_parquet('s3://{BUCKET}/{deep_pruning_path}')");
    let mut deep_count = session.execute(&deep_count_sql).await?;
    let deep_count_metrics = deep_count.metrics();
    assert_eq!(
        int64_value(&deep_count.stream().next().await.unwrap()?),
        100
    );
    drop(deep_count);
    let deep_count_metrics = deep_count_metrics.snapshot();
    assert_eq!(deep_count_metrics.parquet_page_index_bytes_read, 0);
    assert_eq!(deep_count_metrics.parquet_bloom_filter_bytes_read, 0);

    let deep_page_sql = format!(
        "SELECT count(*) FROM read_parquet('s3://{BUCKET}/{deep_pruning_path}') WHERE id >= 180"
    );
    let mut deep_page = session.execute(&deep_page_sql).await?;
    let deep_page_metrics = deep_page.metrics();
    assert_eq!(int64_value(&deep_page.stream().next().await.unwrap()?), 10);
    drop(deep_page);
    let deep_page_metrics = deep_page_metrics.snapshot();
    assert!(deep_page_metrics.parquet_page_index_bytes_read > 0);
    assert!(deep_page_metrics.parquet_page_rows_pruned > 0);

    let deep_bloom_sql = format!(
        "SELECT count(*) FROM read_parquet('s3://{BUCKET}/{deep_pruning_path}') WHERE id = 51"
    );
    let mut deep_bloom = session.execute(&deep_bloom_sql).await?;
    let deep_bloom_metrics = deep_bloom.metrics();
    assert_eq!(int64_value(&deep_bloom.stream().next().await.unwrap()?), 0);
    drop(deep_bloom);
    let deep_bloom_metrics = deep_bloom_metrics.snapshot();
    assert!(deep_bloom_metrics.parquet_bloom_filter_bytes_read > 0);
    assert_eq!(deep_bloom_metrics.parquet_bloom_row_groups_pruned, 1);

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
    let anonymous = EngineConfig::builder()
        .s3(S3Config {
            endpoint: Some(endpoint.clone()),
            region: Some("us-east-1".to_owned()),
            force_path_style: true,
            anonymous: true,
            allow_http: true,
            ..S3Config::default()
        })
        .build();
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
    store.delete(&deep_pruning_path).await?;
    store.delete(&widening_c).await?;
    store.delete(&widening_d).await?;
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

async fn query_count(session: &rustdb::Session, table: &str) -> Result<i64> {
    let mut result = session
        .execute(&format!("SELECT count(*) FROM {table}"))
        .await?;
    let batch = result.stream().next().await.expect("count batch")?;
    Ok(int64_value(&batch))
}

fn int64_value_at(batch: &RecordBatch, column: usize) -> i64 {
    batch
        .column(column)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0)
}
