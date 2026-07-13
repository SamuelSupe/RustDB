use std::{fs::File, path::Path, sync::Arc};

use arrow::{
    array::Int64Array,
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use futures::StreamExt;
use parquet::{arrow::ArrowWriter, file::properties::WriterProperties};

use crate::{
    Engine, EngineConfig, ParquetOptions, ParquetPruningMode, QueryMetricsSnapshot, Result,
};

#[tokio::test]
async fn sql_in_obeys_the_256_value_pruning_budget() -> Result<()> {
    assert_budget("IN", render_in).await
}

#[tokio::test]
async fn same_column_or_obeys_the_256_value_pruning_budget() -> Result<()> {
    assert_budget("OR", render_or).await
}

async fn assert_budget(style: &str, render: fn(usize) -> String) -> Result<()> {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("predicate-budget.parquet");
    write_even_values(&path)?;

    let (rows, metrics) = query(&path, &render(256)).await?;
    assert_eq!(rows, 0, "{style} residual result");
    assert!(metrics.parquet_bloom_filter_bytes_read > 0, "{style}");
    assert_eq!(metrics.parquet_bloom_row_groups_pruned, 1, "{style}");
    assert_eq!(metrics.rows_scanned, 0, "{style}");

    let (rows, metrics) = query(&path, &render(257)).await?;
    assert_eq!(rows, 0, "{style} residual result");
    assert_eq!(metrics.parquet_bloom_filter_bytes_read, 0, "{style}");
    assert_eq!(metrics.parquet_bloom_row_groups_pruned, 0, "{style}");
    assert_eq!(metrics.rows_scanned, 1_024, "{style}");
    Ok(())
}

async fn query(path: &Path, predicate: &str) -> Result<(usize, QueryMetricsSnapshot)> {
    let mut config = EngineConfig::default();
    config.parquet_scan.page_index = ParquetPruningMode::Disabled;
    let session = Engine::new(config)?.session();
    session
        .register_parquet(
            "budget_values",
            [path.to_string_lossy().into_owned()],
            ParquetOptions::default(),
        )
        .await?;
    let sql = format!("SELECT id FROM budget_values WHERE {predicate}");
    let mut result = session.execute(&sql).await?;
    let metrics = result.metrics();
    let mut rows = 0;
    while let Some(batch) = result.stream().next().await {
        rows += batch?.num_rows();
    }
    Ok((rows, metrics.snapshot()))
}

fn render_in(count: usize) -> String {
    let values = odd_values(count).collect::<Vec<_>>().join(",");
    format!("id IN ({values})")
}

fn render_or(count: usize) -> String {
    odd_values(count)
        .map(|value| format!("id = {value}"))
        .collect::<Vec<_>>()
        .join(" OR ")
}

fn odd_values(count: usize) -> impl Iterator<Item = String> {
    (0..count).map(|value| (value * 2 + 1).to_string())
}

fn write_even_values(path: &Path) -> Result<()> {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from_iter_values(
            (0_i64..1_024).map(|value| value * 2),
        ))],
    )?;
    let properties = WriterProperties::builder()
        .set_bloom_filter_enabled(true)
        .set_bloom_filter_fpp(0.000_000_001)
        .set_bloom_filter_max_ndv(1_024)
        .set_max_row_group_row_count(Some(1_024))
        .build();
    let mut writer = ArrowWriter::try_new(File::create(path).unwrap(), schema, Some(properties))?;
    writer.write(&batch)?;
    writer.close()?;
    Ok(())
}
