use std::{fs::File, sync::Arc};

use arrow::{
    array::Int64Array,
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use bytes::Bytes;
use futures::StreamExt;
use parquet::{
    arrow::ArrowWriter,
    file::{metadata::ParquetMetaDataReader, properties::WriterProperties},
};
use tempfile::tempdir;

use crate::{Engine, EngineConfig, ParquetPruningMode};

#[tokio::test]
async fn malformed_bloom_header_fails_through_the_real_read_path() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("malformed-bloom.parquet");
    write_bloom_fixture(&path);
    corrupt_bloom_header(&path);

    let config = EngineConfig::builder()
        .parquet_page_index(ParquetPruningMode::Disabled)
        .build();
    let session = Engine::new(config).unwrap().session();
    let sql = format!(
        "SELECT count(*) FROM read_parquet('{}') WHERE id = 51",
        path.to_string_lossy()
    );
    let error = match session.execute(&sql).await {
        Err(error) => error,
        Ok(mut result) => result
            .stream()
            .next()
            .await
            .expect("malformed Bloom query must terminate")
            .expect_err("malformed Bloom header must fail"),
    };
    let message = error.to_string();
    assert!(
        message.contains("invalid Parquet Bloom filter"),
        "{message}"
    );
    assert!(
        message.contains(&path.to_string_lossy().to_string()),
        "{message}"
    );
    assert!(message.contains("row group 0"), "{message}");
    assert!(message.contains("column 'id'"), "{message}");
}

fn write_bloom_fixture(path: &std::path::Path) {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from_iter_values(
            (0_i64..100).map(|value| value * 2),
        ))],
    )
    .unwrap();
    let properties = WriterProperties::builder()
        .set_bloom_filter_enabled(true)
        .set_bloom_filter_max_ndv(100)
        .set_max_row_group_row_count(Some(100))
        .build();
    let mut writer =
        ArrowWriter::try_new(File::create(path).unwrap(), schema, Some(properties)).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
}

fn corrupt_bloom_header(path: &std::path::Path) {
    let mut bytes = std::fs::read(path).unwrap();
    let metadata = ParquetMetaDataReader::new()
        .parse_and_finish(&Bytes::copy_from_slice(&bytes))
        .unwrap();
    let column = metadata.row_group(0).column(0);
    let start = usize::try_from(column.bloom_filter_offset().expect("Bloom offset")).unwrap();
    let length = usize::try_from(column.bloom_filter_length().expect("Bloom length")).unwrap();
    bytes[start..start + length].fill(0xff);
    std::fs::write(path, bytes).unwrap();
}
