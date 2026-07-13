use std::{fs::File, sync::Arc};

use arrow::{
    array::Int64Array,
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use futures::StreamExt;
use parquet::arrow::ArrowWriter;
use rustdb::{
    Engine, EngineConfig, ParquetOptions, ParquetPruningMode, QueryMetricsSnapshot, Result,
};

mod support;

use support::parquet_pruning;

#[tokio::test]
async fn runtime_filter_drives_page_index_before_decode() -> Result<()> {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("page-index.parquet");
    std::fs::write(&path, parquet_pruning::deep_pruning_bytes()?)
        .map_err(|error| rustdb::Error::io(Some(path.clone()), error))?;

    let optimized = EngineConfig::builder()
        .batch_size(128)
        .parquet_bloom_filter(ParquetPruningMode::Disabled)
        .spill_directory(directory.path().join("spill-page-optimized"))
        .build();
    let baseline = EngineConfig::builder()
        .batch_size(128)
        .runtime_filter_bytes(0)
        .parquet_bloom_filter(ParquetPruningMode::Disabled)
        .spill_directory(directory.path().join("spill-page-baseline"))
        .build();
    let sql = "SELECT count(*) FROM fact f \
               JOIN (SELECT 51 AS id) d ON f.id = d.id";
    let (optimized_count, optimized) = run_join(optimized, &path, sql).await?;
    let (baseline_count, baseline) = run_join(baseline, &path, sql).await?;

    assert_eq!(optimized_count, baseline_count);
    assert_eq!(optimized_count, 0);
    assert!(optimized.runtime_filter_hits > 0, "{optimized:?}");
    assert!(optimized.parquet_page_index_bytes_read > 0, "{optimized:?}");
    assert!(optimized.parquet_page_rows_pruned > 0, "{optimized:?}");
    assert!(
        optimized.rows_scanned < baseline.rows_scanned,
        "{optimized:?}\n{baseline:?}"
    );
    assert_eq!(baseline.rows_scanned, parquet_pruning::ROWS as u64);
    assert_eq!(baseline.parquet_page_index_bytes_read, 0);
    assert_eq!(baseline.parquet_page_rows_pruned, 0);
    Ok(())
}

#[tokio::test]
async fn runtime_filter_drives_bloom_pruning_before_data_pages() -> Result<()> {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("bloom.parquet");
    std::fs::write(&path, parquet_pruning::deep_pruning_bytes()?)
        .map_err(|error| rustdb::Error::io(Some(path.clone()), error))?;

    let optimized = EngineConfig::builder()
        .batch_size(128)
        .parquet_page_index(ParquetPruningMode::Disabled)
        .spill_directory(directory.path().join("spill-bloom-optimized"))
        .build();
    let baseline = EngineConfig::builder()
        .batch_size(128)
        .runtime_filter_bytes(0)
        .parquet_page_index(ParquetPruningMode::Disabled)
        .spill_directory(directory.path().join("spill-bloom-baseline"))
        .build();
    let sql = "SELECT count(*) FROM fact f \
               JOIN (SELECT 51 AS id) d ON f.id = d.id";
    let (optimized_count, optimized) = run_join(optimized, &path, sql).await?;
    let (baseline_count, baseline) = run_join(baseline, &path, sql).await?;

    assert_eq!(optimized_count, baseline_count);
    assert_eq!(optimized_count, 0);
    assert!(optimized.runtime_filter_hits > 0, "{optimized:?}");
    assert!(
        optimized.parquet_bloom_filter_bytes_read > 0,
        "{optimized:?}"
    );
    assert_eq!(optimized.parquet_bloom_row_groups_pruned, 1);
    assert_eq!(optimized.rows_scanned, 0);
    assert_eq!(baseline.rows_scanned, parquet_pruning::ROWS as u64);
    assert_eq!(baseline.parquet_bloom_filter_bytes_read, 0);
    assert_eq!(baseline.parquet_bloom_row_groups_pruned, 0);
    Ok(())
}

#[tokio::test]
async fn runtime_filter_prunes_hive_files_before_opening_them() -> Result<()> {
    let directory = tempfile::tempdir().unwrap();
    write_hive_partition(directory.path(), 1)?;
    write_hive_partition(directory.path(), 2)?;
    let pattern = format!("{}/*/*.parquet", directory.path().display());
    let options = ParquetOptions {
        hive_partitioning: true,
        ..ParquetOptions::default()
    };
    let optimized = EngineConfig::builder()
        .batch_size(128)
        .parquet_page_index(ParquetPruningMode::Disabled)
        .parquet_bloom_filter(ParquetPruningMode::Disabled)
        .spill_directory(directory.path().join("spill-hive-optimized"))
        .build();
    let baseline = EngineConfig::builder()
        .batch_size(128)
        .runtime_filter_bytes(0)
        .parquet_page_index(ParquetPruningMode::Disabled)
        .parquet_bloom_filter(ParquetPruningMode::Disabled)
        .spill_directory(directory.path().join("spill-hive-baseline"))
        .build();
    let sql = "SELECT count(*) FROM fact f \
               JOIN (SELECT 1 AS part) d ON f.part = d.part";
    let (optimized_count, optimized) =
        run_registered_join(optimized, pattern.clone(), options.clone(), sql).await?;
    let (baseline_count, baseline) = run_registered_join(baseline, pattern, options, sql).await?;

    assert_eq!(optimized_count, baseline_count);
    assert_eq!(optimized_count, parquet_pruning::ROWS);
    assert_eq!(optimized.discovered_files, 2);
    assert!(optimized.runtime_filter_hits > 0, "{optimized:?}");
    assert_eq!(optimized.files_pruned, 1, "{optimized:?}");
    assert_eq!(optimized.rows_scanned, parquet_pruning::ROWS as u64);
    assert_eq!(baseline.files_pruned, 0, "{baseline:?}");
    assert_eq!(baseline.rows_scanned, (parquet_pruning::ROWS * 2) as u64);
    Ok(())
}

async fn run_join(
    config: EngineConfig,
    path: &std::path::Path,
    sql: &str,
) -> Result<(i64, QueryMetricsSnapshot)> {
    run_registered_join(
        config,
        path.to_string_lossy().into_owned(),
        ParquetOptions::default(),
        sql,
    )
    .await
}

async fn run_registered_join(
    config: EngineConfig,
    location: String,
    options: ParquetOptions,
    sql: &str,
) -> Result<(i64, QueryMetricsSnapshot)> {
    let session = Engine::new(config)?.session();
    session
        .register_parquet("fact", [location], options)
        .await?;
    let mut result = session.execute(sql).await?;
    let metrics = result.metrics();
    let batch = result.stream().next().await.expect("join count batch")?;
    let count = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("COUNT returns Int64")
        .value(0);
    assert!(result.stream().next().await.is_none());
    drop(result);
    Ok((count, metrics.snapshot()))
}

fn write_hive_partition(root: &std::path::Path, partition: i64) -> Result<()> {
    let directory = root.join(format!("part={partition}"));
    std::fs::create_dir_all(&directory)
        .map_err(|error| rustdb::Error::io(Some(directory.clone()), error))?;
    let path = directory.join("data.parquet");
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from_iter_values(
            0_i64..parquet_pruning::ROWS,
        ))],
    )?;
    let file = File::create(&path).map_err(|error| rustdb::Error::io(Some(path), error))?;
    let mut writer = ArrowWriter::try_new(file, schema, None)?;
    writer.write(&batch)?;
    writer.close()?;
    Ok(())
}
