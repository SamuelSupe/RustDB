use std::{fs, sync::Arc};

use arrow::{
    array::{Int64Array, StringArray},
    datatypes::{DataType, Field, Schema},
};
use async_compression::tokio::write::{GzipEncoder, ZstdEncoder};
use futures::TryStreamExt;
use tokio::io::AsyncWriteExt;

use super::{CsvOptions, CsvTable, EngineConfig, MemoryPool, QueryContext, ScanRequest};
use crate::{CsvHeader, datasource::TableProvider};

async fn gzip(bytes: &[u8]) -> Vec<u8> {
    let mut encoder = GzipEncoder::new(Vec::new());
    encoder.write_all(bytes).await.unwrap();
    encoder.shutdown().await.unwrap();
    encoder.into_inner()
}

async fn zstd(bytes: &[u8]) -> Vec<u8> {
    let mut encoder = ZstdEncoder::new(Vec::new());
    encoder.write_all(bytes).await.unwrap();
    encoder.shutdown().await.unwrap();
    encoder.into_inner()
}

fn options() -> CsvOptions {
    CsvOptions::builder()
        .schema(Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("note", DataType::Utf8, false),
        ])))
        .header(CsvHeader::Present)
        .build()
}

#[tokio::test]
async fn auto_detects_plain_gzip_and_zstd_with_concatenated_members() {
    let directory = tempfile::tempdir().unwrap();
    let cases = [
        ("plain.data", b"id,note\n1,first\n2,second\n".to_vec()),
        (
            "gzip.csv",
            [gzip(b"id,note\n1,first\n").await, gzip(b"2,second\n").await].concat(),
        ),
        (
            "zstd.csv",
            [zstd(b"id,note\n1,first\n").await, zstd(b"2,second\n").await].concat(),
        ),
    ];
    for (name, contents) in cases {
        let values = read_values(directory.path(), name, contents, 16 << 20)
            .await
            .unwrap();
        assert_eq!(
            values,
            vec![(1, "first".to_owned()), (2, "second".to_owned())],
            "{name}"
        );
    }
}

#[tokio::test]
async fn rejects_truncated_zstd_and_bad_gzip_crc() {
    let directory = tempfile::tempdir().unwrap();
    let mut bad_gzip = gzip(b"id,note\n1,first\n").await;
    let last = bad_gzip.last_mut().unwrap();
    *last ^= 0xff;
    let mut truncated_zstd = zstd(b"id,note\n1,first\n").await;
    truncated_zstd.truncate(truncated_zstd.len().saturating_sub(3));

    for (name, bytes) in [("bad.csv.gz", bad_gzip), ("bad.csv.zst", truncated_zstd)] {
        let error = read_values(directory.path(), name, bytes, 16 << 20)
            .await
            .unwrap_err()
            .to_string();
        assert!(error.contains(name), "{error}");
    }
}

#[tokio::test]
async fn high_compression_ratio_stays_within_query_memory() {
    let directory = tempfile::tempdir().unwrap();
    let mut csv = String::from("id,note\n");
    for id in 0..50_000 {
        csv.push_str(&format!("{id},same-value-same-value-same-value\n"));
    }
    let compressed = gzip(csv.as_bytes()).await;
    let values = read_values(directory.path(), "ratio.csv", compressed, 2 << 20)
        .await
        .unwrap();
    assert_eq!(values.len(), 50_000);
    assert_eq!(
        values.first(),
        Some(&(0, "same-value-same-value-same-value".to_owned()))
    );
    assert_eq!(
        values.last(),
        Some(&(49_999, "same-value-same-value-same-value".to_owned()))
    );
    assert!(values.iter().enumerate().all(|(expected, (id, note))| {
        *id == i64::try_from(expected).unwrap() && note == "same-value-same-value-same-value"
    }));
}

async fn read_values(
    directory: &std::path::Path,
    name: &str,
    contents: Vec<u8>,
    memory_limit: usize,
) -> crate::Result<Vec<(i64, String)>> {
    let path = directory.join(name);
    fs::write(&path, contents).unwrap();
    let config = EngineConfig::builder()
        .memory_limit(memory_limit)
        .csv_target_morsel_bytes(64 << 10)
        .build();
    let table = CsvTable::try_new(
        vec![path.to_string_lossy().into_owned()],
        options(),
        &config,
    )
    .await?;
    let context = Arc::new(QueryContext::new(MemoryPool::new(memory_limit), directory)?);
    table.prepare(Arc::clone(&context)).await?;
    context.seal_object_snapshots();
    let mut stream = table
        .scan(ScanRequest::new(1_024), Arc::clone(&context))
        .await?;
    let mut values = Vec::new();
    while let Some(batch) = stream.try_next().await? {
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let notes = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        values
            .extend((0..batch.num_rows()).map(|row| (ids.value(row), notes.value(row).to_owned())));
        assert!(context.memory.used() <= context.memory.limit());
    }
    values.sort_unstable_by_key(|(id, _)| *id);
    Ok(values)
}
