use std::{fs::File, sync::Arc};

use arrow::{
    array::{BooleanArray, Date32Array, Int64Array},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use futures::StreamExt;
use parquet::arrow::ArrowWriter;
use rustdb::{Engine, EngineConfig, ParquetOptions, Result};

#[tokio::test]
async fn materializes_and_prunes_local_hive_partitions() -> Result<()> {
    let root = tempfile::tempdir().unwrap();
    write_partition(root.path(), 2025, true, "2025-01-02", &[1, 2])?;
    write_partition(root.path(), 2026, false, "2026-02-03", &[3, 4])?;
    let pattern = format!("{}/*/*/*/*.parquet", root.path().display());

    let session = Engine::new(EngineConfig::default())?.session();
    let sql = format!(
        "SELECT year, active, day FROM read_parquet(\
         '{pattern}', hive_partitioning = true) WHERE year > 2025"
    );
    let mut result = session.execute(&sql).await?;
    let metrics = result.metrics();
    let schema = result.schema();
    assert_eq!(schema.fields().len(), 3);
    assert_eq!(
        schema.field_with_name("year")?.data_type(),
        &DataType::Int64
    );
    assert_eq!(
        schema.field_with_name("active")?.data_type(),
        &DataType::Boolean
    );
    assert_eq!(
        schema.field_with_name("day")?.data_type(),
        &DataType::Date32
    );

    let mut batches = Vec::new();
    while let Some(batch) = result.stream().next().await {
        batches.push(batch?);
    }

    assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 2);
    assert_eq!(batches[0].num_columns(), 3);
    let years = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let active = batches[0]
        .column(1)
        .as_any()
        .downcast_ref::<BooleanArray>()
        .unwrap();
    let days = batches[0]
        .column(2)
        .as_any()
        .downcast_ref::<Date32Array>()
        .unwrap();
    assert!((0..years.len()).all(|index| years.value(index) == 2026));
    assert!((0..active.len()).all(|index| !active.value(index)));
    assert!((0..days.len()).all(|index| days.value(index) > 0));
    assert_eq!(metrics.snapshot().files_pruned, 1);

    session
        .register_parquet("disabled", [pattern], ParquetOptions::default())
        .await?;
    let disabled = session.execute("SELECT * FROM disabled LIMIT 1").await?;
    assert_eq!(disabled.schema().fields().len(), 1);
    Ok(())
}

#[tokio::test]
async fn conflicting_partition_types_fall_back_to_utf8() -> Result<()> {
    let root = tempfile::tempdir().unwrap();
    write_raw_partition(root.path(), "mixed=1", &[1])?;
    write_raw_partition(root.path(), "mixed=text", &[2])?;
    let pattern = format!("{}/*/*.parquet", root.path().display());
    let session = Engine::new(EngineConfig::default())?.session();
    let result = session
        .execute(&format!(
            "SELECT mixed FROM read_parquet('{pattern}', hive_partitioning = true)"
        ))
        .await?;
    assert_eq!(
        result.schema().field_with_name("mixed")?.data_type(),
        &DataType::Utf8
    );
    Ok(())
}

#[tokio::test]
async fn sql_auto_reads_and_prunes_partition_only_projection() -> Result<()> {
    let root = tempfile::tempdir().unwrap();
    write_partition(root.path(), 2025, true, "2025-01-02", &[1])?;
    write_partition(root.path(), 2026, false, "2026-02-03", &[2])?;
    let pattern = format!("{}/*/*/*/*.parquet", root.path().display());
    let sql = format!(
        "SELECT year, active FROM read_parquet('{pattern}', hive_partitioning = 'auto') \
         WHERE year = 2026"
    );
    let session = Engine::new(EngineConfig::default())?.session();
    let mut result = session.execute(&sql).await?;
    let metrics = result.metrics();
    let batch = result
        .stream()
        .next()
        .await
        .ok_or_else(|| rustdb::Error::Execution("Hive query returned no rows".to_owned()))??;

    assert_eq!(batch.num_rows(), 1);
    assert_eq!(
        batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        2026
    );
    assert!(
        !batch
            .column(1)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap()
            .value(0)
    );
    assert_eq!(metrics.snapshot().files_pruned, 1);
    Ok(())
}

fn write_partition(
    root: &std::path::Path,
    year: i64,
    active: bool,
    day: &str,
    values: &[i64],
) -> Result<()> {
    let path = format!("year={year}/active={active}/day={day}");
    write_raw_partition(root, &path, values)
}

fn write_raw_partition(root: &std::path::Path, partition: &str, values: &[i64]) -> Result<()> {
    let directory = root.join(partition);
    std::fs::create_dir_all(&directory)
        .map_err(|error| rustdb::Error::io(Some(directory.clone()), error))?;
    let path = directory.join("part.parquet");
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from(values.to_vec()))],
    )?;
    let file = File::create(&path).map_err(|error| rustdb::Error::io(Some(path), error))?;
    let mut writer = ArrowWriter::try_new(file, schema, None)?;
    writer.write(&batch)?;
    writer.close()?;
    Ok(())
}
