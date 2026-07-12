use std::{fs::File, path::Path, sync::Arc};

use arrow::{
    array::{Array, Decimal128Array},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use futures::TryStreamExt;
use parquet::{
    arrow::ArrowWriter,
    file::properties::{EnabledStatistics, WriterProperties},
};
use tempfile::tempdir;

use crate::{Engine, EngineConfig, ParquetOptions, ParquetSchemaMode};

#[tokio::test]
async fn sql_decimal_literal_prunes_safely_across_file_scales() {
    let directory = tempdir().unwrap();
    let scale_two = directory.path().join("a-scale-two.parquet");
    let scale_four = directory.path().join("b-scale-four.parquet");
    write_decimal_file(&scale_two, 10, 2);
    write_decimal_file(&scale_four, 12, 4);

    let config = EngineConfig::builder()
        .memory_limit(64 << 20)
        .compute_threads(1)
        .spill_directory(directory.path().join("spill"))
        .build();
    let session = Engine::new(config).unwrap().session();
    session
        .register_parquet(
            "amounts",
            [
                scale_two.to_string_lossy().into_owned(),
                scale_four.to_string_lossy().into_owned(),
            ],
            ParquetOptions {
                schema_mode: ParquetSchemaMode::SafeWidening,
                ..ParquetOptions::default()
            },
        )
        .await
        .unwrap();

    let mut result = session
        .execute("SELECT amount FROM amounts WHERE amount = 1")
        .await
        .unwrap();
    let metrics = result.metrics();
    let batches = result.stream().try_collect::<Vec<_>>().await.unwrap();
    let values = batches
        .iter()
        .flat_map(|batch| {
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .unwrap()
                .iter()
                .flatten()
        })
        .collect::<Vec<_>>();

    assert_eq!(values, vec![10_000, 10_000]);
    assert!(
        batches
            .iter()
            .all(|batch| { batch.column(0).data_type() == &DataType::Decimal128(12, 4) })
    );
    let metrics = metrics.snapshot();
    assert!(metrics.parquet_page_index_bytes_read > 0);
    assert!(metrics.parquet_page_rows_pruned > 0);
}

fn write_decimal_file(path: &Path, precision: u8, scale: i8) {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "amount",
        DataType::Decimal128(precision, scale),
        false,
    )]));
    let factor = 10_i128.pow(u32::try_from(scale).unwrap());
    let values = Decimal128Array::from_iter_values((0_i128..100).map(|value| value * factor))
        .with_precision_and_scale(precision, scale)
        .unwrap();
    let batch = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(values)]).unwrap();
    let properties = WriterProperties::builder()
        .set_statistics_enabled(EnabledStatistics::Page)
        .set_data_page_row_count_limit(10)
        .set_write_batch_size(10)
        .set_max_row_group_row_count(Some(100))
        .build();
    let mut writer =
        ArrowWriter::try_new(File::create(path).unwrap(), schema, Some(properties)).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
}
