use std::{fs::File, sync::Arc};

use arrow::{
    array::{Int64Array, StringArray},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use futures::{StreamExt, TryStreamExt};
use parquet::{arrow::ArrowWriter, file::properties::WriterProperties};
use tempfile::tempdir;

use super::*;
use crate::{
    datasource::{ComparisonOp, PredicateGuarantee, PredicateValue, ScanPredicate},
    runtime::{MemoryPool, QueryContext},
};

#[tokio::test]
async fn reader_filter_late_materializes_projected_payload() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("row-filter.parquet");
    write_rows(&path);
    let config = EngineConfig::default();
    let table = ParquetTable::try_new_with_cache(
        vec![path.to_string_lossy().into_owned()],
        ParquetOptions::default(),
        &config,
        MetadataCache::new(config.metadata_cache_bytes),
    )
    .await
    .unwrap();
    let context = Arc::new(QueryContext::new(MemoryPool::new(16 << 20), directory.path()).unwrap());
    table.prepare(Arc::clone(&context)).await.unwrap();
    context.seal_object_snapshots();

    let mut request = ScanRequest::new(2);
    request.projection = Some(vec![1]);
    request.predicate = Some(ScanPredicate::Comparison {
        column: 0,
        op: ComparisonOp::Eq,
        value: PredicateValue::Int64(3),
    });
    let batches = table
        .scan(request, Arc::clone(&context))
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();

    let values = batches
        .iter()
        .flat_map(|batch| {
            batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .iter()
        })
        .collect::<Vec<_>>();
    assert_eq!(values, vec![Some("payload-3")]);
    assert_eq!(context.metrics.snapshot().rows_scanned, 1);
    assert_eq!(context.metrics.snapshot().peak_active_lanes, 1);
}

#[tokio::test]
async fn filtered_limit_is_not_consumed_by_pre_filter_rows() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("row-filter-limit.parquet");
    write_rows(&path);
    let config = EngineConfig::default();
    let table = ParquetTable::try_new_with_cache(
        vec![path.to_string_lossy().into_owned()],
        ParquetOptions::default(),
        &config,
        MetadataCache::new(config.metadata_cache_bytes),
    )
    .await
    .unwrap();
    let context = Arc::new(QueryContext::new(MemoryPool::new(16 << 20), directory.path()).unwrap());
    table.prepare(Arc::clone(&context)).await.unwrap();
    context.seal_object_snapshots();

    let mut request = ScanRequest::new(2);
    request.limit = Some(1);
    request.predicate = Some(ScanPredicate::Comparison {
        column: 0,
        op: ComparisonOp::Gt,
        value: PredicateValue::Int64(2),
    });
    let rows = table
        .scan(request, context)
        .await
        .unwrap()
        .fold(0usize, |rows, batch| async move {
            rows + batch.unwrap().num_rows()
        })
        .await;
    assert_eq!(rows, 1);
}

#[tokio::test]
async fn reader_filter_combines_ranges_on_one_physical_column() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("row-filter-range.parquet");
    write_rows(&path);
    let config = EngineConfig::default();
    let table = ParquetTable::try_new_with_cache(
        vec![path.to_string_lossy().into_owned()],
        ParquetOptions::default(),
        &config,
        MetadataCache::new(config.metadata_cache_bytes),
    )
    .await
    .unwrap();
    let context = Arc::new(QueryContext::new(MemoryPool::new(16 << 20), directory.path()).unwrap());
    table.prepare(Arc::clone(&context)).await.unwrap();
    context.seal_object_snapshots();

    let mut request = ScanRequest::new(2);
    request.projection = Some(vec![1]);
    request.predicate = Some(ScanPredicate::And(vec![
        ScanPredicate::Comparison {
            column: 0,
            op: ComparisonOp::Gt,
            value: PredicateValue::Int64(2),
        },
        ScanPredicate::Comparison {
            column: 0,
            op: ComparisonOp::Lt,
            value: PredicateValue::Int64(4),
        },
    ]));
    let batches = table
        .scan(request, Arc::clone(&context))
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();

    let values = batches
        .iter()
        .flat_map(|batch| {
            batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .iter()
        })
        .collect::<Vec<_>>();
    assert_eq!(values, vec![Some("payload-3")]);
    assert_eq!(context.metrics.snapshot().rows_scanned, 1);
}

#[tokio::test]
async fn exact_filter_preserves_rows_for_a_zero_column_scan() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("exact-zero-column.parquet");
    write_rows(&path);
    let config = EngineConfig::default();
    let table = ParquetTable::try_new_with_cache(
        vec![path.to_string_lossy().into_owned()],
        ParquetOptions::default(),
        &config,
        MetadataCache::new(config.metadata_cache_bytes),
    )
    .await
    .unwrap();
    let context = Arc::new(QueryContext::new(MemoryPool::new(16 << 20), directory.path()).unwrap());
    table.prepare(Arc::clone(&context)).await.unwrap();
    context.seal_object_snapshots();

    let mut request = ScanRequest::new(2);
    request.projection = Some(Vec::new());
    request.predicate = Some(ScanPredicate::Comparison {
        column: 0,
        op: ComparisonOp::Gt,
        value: PredicateValue::Int64(2),
    });
    request.predicate_guarantee = PredicateGuarantee::Exact;
    let batches = table
        .scan(request, context)
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();

    assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 2);
    assert!(batches.iter().all(|batch| batch.num_columns() == 0));
}

#[tokio::test]
async fn exact_filtered_limit_is_shared_across_row_group_tasks() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("exact-filtered-limit.parquet");
    write_sparse_rows(&path);
    let config = EngineConfig::default();
    let table = ParquetTable::try_new_with_cache(
        vec![path.to_string_lossy().into_owned()],
        ParquetOptions::default(),
        &config,
        MetadataCache::new(config.metadata_cache_bytes),
    )
    .await
    .unwrap();
    let context = Arc::new(QueryContext::new(MemoryPool::new(16 << 20), directory.path()).unwrap());
    table.prepare(Arc::clone(&context)).await.unwrap();
    context.seal_object_snapshots();
    let baseline = context.memory.used();

    let mut request = ScanRequest::new(2);
    request.projection = Some(vec![0]);
    request.limit = Some(3);
    request.predicate = Some(ScanPredicate::Comparison {
        column: 0,
        op: ComparisonOp::Gt,
        value: PredicateValue::Int64(0),
    });
    request.predicate_guarantee = PredicateGuarantee::Exact;
    let tasks = table
        .scan_tasks(request, Arc::clone(&context), 4)
        .await
        .unwrap();
    assert_eq!(tasks.len(), 4);

    let (rows, terminal_slices, values) =
        futures::stream::iter(tasks.into_iter().map(|task| task.into_stream()))
            .flatten_unordered(4)
            .try_fold(
                (0usize, 0usize, Vec::new()),
                |(rows, terminal_slices, mut values), batch| async move {
                    let array = batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap();
                    values.extend(array.iter().flatten());
                    Ok((
                        rows + batch.num_rows(),
                        terminal_slices + usize::from(batch.num_rows() == 1),
                        values,
                    ))
                },
            )
            .await
            .unwrap();

    assert_eq!(rows, 3);
    assert_eq!(terminal_slices, 1);
    assert!(values.iter().all(|value| *value > 0));
    assert!(context.metrics.snapshot().rows_scanned >= 4);
    assert_eq!(context.memory.used(), baseline);
}

fn write_rows(path: &std::path::Path) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("payload", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![Some(1), None, Some(3), Some(4)])),
            Arc::new(StringArray::from(vec![
                "payload-1",
                "payload-null",
                "payload-3",
                "payload-4",
            ])),
        ],
    )
    .unwrap();
    let properties = WriterProperties::builder()
        .set_max_row_group_row_count(Some(2))
        .build();
    let mut writer =
        ArrowWriter::try_new(File::create(path).unwrap(), schema, Some(properties)).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
}

fn write_sparse_rows(path: &std::path::Path) {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let values = (0..4)
        .flat_map(|group| [-2, -1, group * 2 + 1, group * 2 + 2])
        .map(i64::from)
        .collect::<Vec<_>>();
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from(values))],
    )
    .unwrap();
    let properties = WriterProperties::builder()
        .set_max_row_group_row_count(Some(4))
        .build();
    let mut writer =
        ArrowWriter::try_new(File::create(path).unwrap(), schema, Some(properties)).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
}
