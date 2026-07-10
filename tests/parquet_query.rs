use std::{fs::File, sync::Arc};

use arrow::{
    array::{Float64Array, Int64Array, StringArray},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use futures::StreamExt;
use parquet::{arrow::ArrowWriter, file::properties::WriterProperties};
use rustdb::{Engine, EngineConfig, ParquetOptions, Result};

fn write_fixture(path: &std::path::Path) -> Result<()> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("category", DataType::Utf8, false),
        Field::new("value", DataType::Float64, false),
    ]));
    let properties = WriterProperties::builder()
        .set_max_row_group_row_count(Some(3))
        .build();
    let file = File::create(path).map_err(|error| rustdb::Error::io(Some(path.into()), error))?;
    let mut writer = ArrowWriter::try_new(file, Arc::clone(&schema), Some(properties))?;
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5, 6])),
            Arc::new(StringArray::from(vec!["a", "b", "a", "b", "a", "b"])),
            Arc::new(Float64Array::from(vec![10.0, 20.0, 30.0, 40.0, 50.0, 60.0])),
        ],
    )?;
    writer.write(&batch)?;
    writer.close()?;
    Ok(())
}

#[tokio::test]
async fn queries_parquet_with_projection_pruning_and_sort() -> Result<()> {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("metrics.parquet");
    write_fixture(&path)?;

    let session = Engine::new(EngineConfig::default())?.session();
    session
        .register_parquet(
            "metrics",
            [path.to_string_lossy().into_owned()],
            ParquetOptions::default(),
        )
        .await?;
    let mut result = session
        .execute(
            "SELECT category, count(*) AS n, sum(value) AS total \
             FROM metrics WHERE id > 3 GROUP BY category ORDER BY category",
        )
        .await?;
    let metrics = result.metrics();
    let mut batches = Vec::new();
    while let Some(batch) = result.stream().next().await {
        batches.push(batch?);
    }

    assert_eq!(
        batches.iter().map(|batch| batch.num_rows()).sum::<usize>(),
        2
    );
    let category = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(category.value(0), "a");
    assert_eq!(category.value(1), "b");
    assert!(metrics.snapshot().row_groups_pruned >= 1);
    Ok(())
}

#[tokio::test]
async fn queries_parquet_file_function() -> Result<()> {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("metrics.parquet");
    write_fixture(&path)?;
    let session = Engine::new(EngineConfig::default())?.session();
    let sql = format!("SELECT count(*) FROM read_parquet('{}')", path.display());
    let mut result = session.execute(&sql).await?;
    let batch = result.stream().next().await.unwrap()?;
    let count = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(count.value(0), 6);
    Ok(())
}

#[tokio::test]
async fn registered_table_accepts_file_updates_between_queries() -> Result<()> {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("changing.parquet");
    write_id_fixture(&path, &[1])?;
    let session = Engine::new(EngineConfig::default())?.session();
    session
        .register_parquet(
            "changing",
            [path.to_string_lossy().into_owned()],
            ParquetOptions::default(),
        )
        .await?;

    assert_eq!(
        query_count(&session, "SELECT count(*) FROM changing").await?,
        1
    );
    write_id_fixture(&path, &[1, 2, 3])?;
    assert_eq!(
        query_count(&session, "SELECT count(*) FROM changing").await?,
        3
    );
    Ok(())
}

#[tokio::test]
async fn registered_table_rejects_an_incompatible_schema_change() -> Result<()> {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("schema-change.parquet");
    write_fixture(&path)?;
    let session = Engine::new(EngineConfig::default())?.session();
    session
        .register_parquet(
            "changing_schema",
            [path.to_string_lossy().into_owned()],
            ParquetOptions::default(),
        )
        .await?;
    write_id_fixture(&path, &[1, 2, 3])?;

    let mut result = session.execute("SELECT value FROM changing_schema").await?;
    let error = result
        .stream()
        .next()
        .await
        .expect("scan should report a schema error")
        .unwrap_err();
    assert!(error.to_string().contains("schema changed"));
    assert!(error.to_string().contains("schema-change.parquet"));
    Ok(())
}

#[tokio::test]
async fn reads_all_row_group_morsels_with_bounded_concurrency() -> Result<()> {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("morsels.parquet");
    let values = (0_i64..64).collect::<Vec<_>>();
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let properties = WriterProperties::builder()
        .set_max_row_group_row_count(Some(4))
        .build();
    let file = File::create(&path).map_err(|error| rustdb::Error::io(Some(path.clone()), error))?;
    let mut writer = ArrowWriter::try_new(file, Arc::clone(&schema), Some(properties))?;
    writer.write(&RecordBatch::try_new(
        schema,
        vec![Arc::new(Int64Array::from(values.clone()))],
    )?)?;
    writer.close()?;

    let config = EngineConfig {
        io_concurrency: 3,
        batch_size: 3,
        ..EngineConfig::default()
    };
    let session = Engine::new(config)?.session();
    session
        .register_parquet(
            "morsels",
            [path.to_string_lossy().into_owned()],
            ParquetOptions::default(),
        )
        .await?;
    let mut result = session.execute("SELECT id FROM morsels").await?;
    let mut actual = Vec::new();
    while let Some(batch) = result.stream().next().await {
        let batch = batch?;
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        actual.extend(ids.values().iter().copied());
    }
    actual.sort_unstable();
    assert_eq!(actual, values);
    Ok(())
}

fn write_id_fixture(path: &std::path::Path, values: &[i64]) -> Result<()> {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let file = File::create(path).map_err(|error| rustdb::Error::io(Some(path.into()), error))?;
    let mut writer = ArrowWriter::try_new(file, Arc::clone(&schema), None)?;
    writer.write(&RecordBatch::try_new(
        schema,
        vec![Arc::new(Int64Array::from(values.to_vec()))],
    )?)?;
    writer.close()?;
    Ok(())
}

async fn query_count(session: &rustdb::Session, sql: &str) -> Result<i64> {
    let mut result = session.execute(sql).await?;
    let batch = result
        .stream()
        .next()
        .await
        .ok_or_else(|| rustdb::Error::Execution("count returned no batch".to_owned()))??;
    Ok(batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("count returns Int64")
        .value(0))
}
