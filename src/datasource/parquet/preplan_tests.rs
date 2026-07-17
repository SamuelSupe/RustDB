use std::{fs::File, path::Path, sync::Arc};

use arrow::{
    array::Int64Array,
    datatypes::{DataType, Field, Schema, SchemaRef},
    record_batch::RecordBatch,
};
use futures::TryStreamExt;
use parquet::{
    arrow::ArrowWriter,
    file::properties::{EnabledStatistics, WriterProperties},
};

use super::ParquetTable;
use crate::{
    EngineConfig, Error, ParquetPruningMode,
    datasource::{
        ComparisonOp, MetadataCache, PredicateValue, ScanPredicate, ScanRequest, TableProvider,
        TableStatistics,
    },
    runtime::{MemoryPool, QueryContext},
    storage::LocationResolver,
};

#[tokio::test]
async fn fixed_preplan_reuses_footer_and_does_not_retry_soft_page_index_skip() {
    let directory = tempfile::tempdir().unwrap();
    let schema = schema();
    let first = directory.path().join("first.rdbseg");
    let second = directory.path().join("second.rdbseg");
    write_page_index_file(&first, Arc::clone(&schema), 0, 100);
    write_page_index_file(&second, Arc::clone(&schema), 100, 100);

    let config = EngineConfig::builder()
        .io_concurrency(2)
        .metadata_cache_bytes(0)
        .parquet_pruning_metadata_bytes(0)
        .build();
    let table = fixed_table(&[&first, &second], Arc::clone(&schema), &config).await;
    let context = query_context(directory.path(), 16 * 1024 * 1024);
    table.prepare(Arc::clone(&context)).await.unwrap();
    context.seal_object_snapshots();
    let baseline = context.memory.used();

    let mut request = ScanRequest::new(100);
    request.predicate = Some(ScanPredicate::Comparison {
        column: 0,
        op: ComparisonOp::GtEq,
        value: PredicateValue::Int64(190),
    });
    let stream = table.scan(request, Arc::clone(&context)).await.unwrap();
    let after_preplan = context.metrics.snapshot();
    assert_eq!(after_preplan.metadata_cache_misses, 4);
    assert_eq!(after_preplan.metadata_cache_hits, 0);
    assert_eq!(after_preplan.parquet_pruning_budget_skips, 2);
    assert!(context.memory.used() > baseline);

    let rows = stream
        .try_fold(0_usize, |rows, batch| async move {
            Ok(rows.saturating_add(batch.num_rows()))
        })
        .await
        .unwrap();
    assert_eq!(rows, 10);
    let after_scan = context.metrics.snapshot();
    assert_eq!(
        after_scan.metadata_cache_misses,
        after_preplan.metadata_cache_misses
    );
    assert_eq!(after_scan.metadata_cache_hits, 0);
    assert_eq!(after_scan.parquet_pruning_budget_skips, 2);
    assert_eq!(after_scan.parquet_page_index_bytes_read, 0);
    assert_eq!(context.memory.used(), baseline);

    let task_context = query_context(directory.path(), 16 * 1024 * 1024);
    table.prepare(Arc::clone(&task_context)).await.unwrap();
    task_context.seal_object_snapshots();
    let task_baseline = task_context.memory.used();
    let tasks = table
        .scan_tasks(ScanRequest::new(100), Arc::clone(&task_context), 2)
        .await
        .unwrap();
    assert_eq!(tasks.len(), 2);
    let task_metrics = task_context.metrics.snapshot();
    assert_eq!(task_metrics.metadata_cache_misses, 2);
    assert_eq!(task_metrics.metadata_cache_hits, 0);
    drop(tasks);
    assert_eq!(task_context.memory.used(), task_baseline);
}

#[tokio::test]
async fn preplan_gate_is_fixed_only() {
    let directory = tempfile::tempdir().unwrap();
    let first = directory.path().join("first.rdbseg");
    let second = directory.path().join("second.rdbseg");
    std::fs::write(&first, b"not parquet").unwrap();
    std::fs::write(&second, b"also not parquet").unwrap();

    let config = EngineConfig::builder()
        .io_concurrency(2)
        .parquet_page_index(ParquetPruningMode::Disabled)
        .build();
    let table = fixed_table(&[&first, &second], schema(), &config).await;

    let lazy_context = query_context(directory.path(), 16 * 1024 * 1024);
    table.prepare(Arc::clone(&lazy_context)).await.unwrap();
    lazy_context.seal_object_snapshots();
    let mut non_fixed = table.clone();
    non_fixed.fixed_files = false;
    let lazy = non_fixed
        .scan(ScanRequest::new(100), Arc::clone(&lazy_context))
        .await;
    assert!(
        lazy.is_ok(),
        "non-fixed scans must keep metadata planning lazy"
    );
    drop(lazy);

    let eager_context = query_context(directory.path(), 16 * 1024 * 1024);
    table.prepare(Arc::clone(&eager_context)).await.unwrap();
    eager_context.seal_object_snapshots();
    let error = match table
        .scan(ScanRequest::new(100), Arc::clone(&eager_context))
        .await
    {
        Ok(_) => panic!("fixed scan should eagerly detect corrupt metadata"),
        Err(error) => error,
    };
    assert!(matches!(error, Error::Parquet(_) | Error::Execution(_)));
}

#[tokio::test]
async fn preplan_uses_the_query_snapshot_and_releases_partial_metadata_on_change() {
    let directory = tempfile::tempdir().unwrap();
    let schema = schema();
    let first = directory.path().join("first.rdbseg");
    let second = directory.path().join("second.rdbseg");
    write_page_index_file(&first, Arc::clone(&schema), 0, 100);
    write_page_index_file(&second, Arc::clone(&schema), 100, 100);

    let config = EngineConfig::builder()
        .io_concurrency(2)
        .parquet_page_index(ParquetPruningMode::Disabled)
        .build();
    let table = fixed_table(&[&first, &second], Arc::clone(&schema), &config).await;
    let context = query_context(directory.path(), 16 * 1024 * 1024);
    table.prepare(Arc::clone(&context)).await.unwrap();
    context.seal_object_snapshots();
    let baseline = context.memory.used();

    write_page_index_file(&second, schema, 100, 101);
    let error = match table
        .scan(ScanRequest::new(100), Arc::clone(&context))
        .await
    {
        Ok(_) => panic!("snapshot change must fail during fixed metadata preplanning"),
        Err(error) => error,
    };
    assert!(error.to_string().contains("object changed during query"));
    assert_eq!(context.memory.used(), baseline);
}

#[tokio::test]
async fn preplan_resource_pressure_falls_back_and_releases_partial_state() {
    let directory = tempfile::tempdir().unwrap();
    let schema = schema();
    let first = directory.path().join("first.rdbseg");
    let second = directory.path().join("second.rdbseg");
    write_page_index_file(&first, Arc::clone(&schema), 0, 100);
    write_page_index_file(&second, Arc::clone(&schema), 100, 100);

    let config = EngineConfig::builder()
        .io_concurrency(2)
        .parquet_page_index(ParquetPruningMode::Disabled)
        .build();
    let table = fixed_table(&[&first, &second], schema, &config).await;

    let descriptor_context = query_context(directory.path(), 2 * 1024 * 1024);
    table
        .prepare(Arc::clone(&descriptor_context))
        .await
        .unwrap();
    descriptor_context.seal_object_snapshots();
    let descriptor_baseline = descriptor_context.memory.used();
    let descriptor_blocker = descriptor_context
        .memory
        .try_reserve(descriptor_context.memory.available().saturating_sub(1))
        .unwrap();
    let lazy = table
        .scan(ScanRequest::new(100), Arc::clone(&descriptor_context))
        .await
        .expect("descriptor admission failure must fall back to lazy planning");
    assert_eq!(
        descriptor_context.metrics.snapshot().metadata_cache_misses,
        0
    );
    drop(lazy);
    assert_eq!(
        descriptor_context.memory.used(),
        descriptor_baseline + descriptor_blocker.size()
    );
    drop(descriptor_blocker);
    assert_eq!(descriptor_context.memory.used(), descriptor_baseline);

    let load_context = query_context(directory.path(), 2 * 1024 * 1024);
    table.prepare(Arc::clone(&load_context)).await.unwrap();
    load_context.seal_object_snapshots();
    let load_baseline = load_context.memory.used();
    let load_blocker = load_context
        .memory
        .try_reserve(load_context.memory.available().saturating_sub(4 * 1024))
        .unwrap();
    let lazy = table
        .scan(ScanRequest::new(100), Arc::clone(&load_context))
        .await
        .expect("footer admission failure must release partial preplan and fall back");
    assert!(load_context.metrics.snapshot().metadata_cache_misses > 0);
    drop(lazy);
    assert_eq!(
        load_context.memory.used(),
        load_baseline + load_blocker.size()
    );
    drop(load_blocker);
    assert_eq!(load_context.memory.used(), load_baseline);

    let cancelled = query_context(directory.path(), 2 * 1024 * 1024);
    table.prepare(Arc::clone(&cancelled)).await.unwrap();
    cancelled.seal_object_snapshots();
    cancelled.control.cancel();
    assert!(matches!(
        table
            .scan(ScanRequest::new(100), Arc::clone(&cancelled))
            .await,
        Err(Error::Cancelled)
    ));
}

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]))
}

fn write_page_index_file(path: &Path, schema: SchemaRef, start: i64, rows: usize) {
    let values = (0..rows).map(|offset| start + offset as i64);
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from_iter_values(values))],
    )
    .unwrap();
    let properties = WriterProperties::builder()
        .set_statistics_enabled(EnabledStatistics::Page)
        .set_data_page_row_count_limit(10)
        .set_write_batch_size(10)
        .set_max_row_group_row_count(Some(rows))
        .build();
    let mut writer =
        ArrowWriter::try_new(File::create(path).unwrap(), schema, Some(properties)).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
}

async fn fixed_table(paths: &[&Path], schema: SchemaRef, config: &EngineConfig) -> ParquetTable {
    let resolver = LocationResolver::with_memory_limit(config.s3.clone(), config.memory_limit);
    let locations = paths
        .iter()
        .map(|path| path.to_string_lossy().into_owned())
        .collect::<Vec<_>>();
    let files = resolver.resolve(&locations).await.unwrap();
    let total_byte_size = files.iter().map(|file| file.snapshot().size).sum();
    let statistics = TableStatistics {
        row_count: None,
        total_byte_size: Some(total_byte_size),
        file_count: files.len(),
    };
    ParquetTable::from_fixed_files(
        files,
        schema,
        statistics,
        config,
        MetadataCache::new(config.metadata_cache_bytes),
    )
    .unwrap()
}

fn query_context(directory: &Path, memory: usize) -> Arc<QueryContext> {
    Arc::new(QueryContext::new(MemoryPool::new(memory), directory).unwrap())
}
