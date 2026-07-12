use std::{fs::File, sync::Arc};

use arrow::{
    array::{Decimal128Array, Int64Array, StringArray, TimestampMicrosecondArray},
    datatypes::{DataType, Field, Schema, TimeUnit},
    record_batch::RecordBatch,
};
use futures::TryStreamExt;
use parquet::{
    arrow::ArrowWriter,
    file::properties::{EnabledStatistics, WriterProperties},
};
use tempfile::tempdir;

use super::ParquetTable;
use crate::{
    EngineConfig, ParquetOptions, ParquetPruningMode,
    datasource::{
        ComparisonOp, MetadataCache, PredicateValue, ScanPredicate, ScanRequest, TableProvider,
    },
    runtime::{MemoryPool, QueryContext},
};

#[tokio::test]
async fn page_index_builds_row_selection_before_decode() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("page-index.parquet");
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from_iter_values(0_i64..100))],
    )
    .unwrap();
    let properties = WriterProperties::builder()
        .set_statistics_enabled(EnabledStatistics::Page)
        .set_data_page_row_count_limit(10)
        .set_write_batch_size(10)
        .set_max_row_group_row_count(Some(100))
        .build();
    let mut writer =
        ArrowWriter::try_new(File::create(&path).unwrap(), schema, Some(properties)).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();

    let config = EngineConfig::default();
    let table = ParquetTable::try_new_with_cache(
        vec![path.to_string_lossy().into_owned()],
        ParquetOptions::default(),
        &config,
        MetadataCache::new(config.metadata_cache_bytes),
    )
    .await
    .unwrap();
    let context =
        Arc::new(QueryContext::new(MemoryPool::new(16 * 1024 * 1024), directory.path()).unwrap());
    table.prepare(Arc::clone(&context)).await.unwrap();
    context.seal_object_snapshots();
    let mut request = ScanRequest::new(100);
    request.predicate = Some(ScanPredicate::Comparison {
        column: 0,
        op: ComparisonOp::GtEq,
        value: PredicateValue::Int64(90),
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
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values()
                .iter()
                .copied()
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    assert!(!values.is_empty());
    assert!((90_i64..100).all(|value| values.contains(&value)));
    assert!(values.len() < 100);
    let metrics = context.metrics.snapshot();
    assert!(metrics.parquet_page_index_bytes_read > 0);
    assert!(metrics.parquet_pages_pruned > 0);
    assert!(metrics.parquet_page_rows_pruned > 0);

    let cached_context =
        Arc::new(QueryContext::new(MemoryPool::new(16 * 1024 * 1024), directory.path()).unwrap());
    table.prepare(Arc::clone(&cached_context)).await.unwrap();
    cached_context.seal_object_snapshots();
    let mut cached_request = ScanRequest::new(100);
    cached_request.predicate = Some(ScanPredicate::Comparison {
        column: 0,
        op: ComparisonOp::GtEq,
        value: PredicateValue::Int64(90),
    });
    let cached_rows = table
        .scan(cached_request, Arc::clone(&cached_context))
        .await
        .unwrap()
        .try_fold(0_usize, |rows, batch| async move {
            Ok(rows.saturating_add(batch.num_rows()))
        })
        .await
        .unwrap();
    assert!(cached_rows < 100);
    let cached_metrics = cached_context.metrics.snapshot();
    assert_eq!(cached_metrics.parquet_page_index_bytes_read, 0);
    assert!(cached_metrics.parquet_page_rows_pruned > 0);

    let mut no_budget = EngineConfig::default();
    no_budget.parquet_scan.max_pruning_metadata_bytes = 0;
    let table = ParquetTable::try_new_with_cache(
        vec![path.to_string_lossy().into_owned()],
        ParquetOptions::default(),
        &no_budget,
        MetadataCache::new(no_budget.metadata_cache_bytes),
    )
    .await
    .unwrap();
    let context =
        Arc::new(QueryContext::new(MemoryPool::new(16 * 1024 * 1024), directory.path()).unwrap());
    table.prepare(Arc::clone(&context)).await.unwrap();
    context.seal_object_snapshots();
    let mut request = ScanRequest::new(100);
    request.predicate = Some(ScanPredicate::Comparison {
        column: 0,
        op: ComparisonOp::GtEq,
        value: PredicateValue::Int64(90),
    });
    let fallback_rows = table
        .scan(request, Arc::clone(&context))
        .await
        .unwrap()
        .try_fold(0_usize, |rows, batch| async move {
            Ok(rows.saturating_add(batch.num_rows()))
        })
        .await
        .unwrap();
    assert_eq!(fallback_rows, 100);
    assert_eq!(context.metrics.snapshot().parquet_page_index_bytes_read, 0);
    assert_eq!(context.metrics.snapshot().parquet_pruning_budget_skips, 1);

    let mut disabled = EngineConfig::default();
    disabled.parquet_scan.page_index = ParquetPruningMode::Disabled;
    let table = ParquetTable::try_new_with_cache(
        vec![path.to_string_lossy().into_owned()],
        ParquetOptions::default(),
        &disabled,
        MetadataCache::new(disabled.metadata_cache_bytes),
    )
    .await
    .unwrap();
    let context =
        Arc::new(QueryContext::new(MemoryPool::new(16 * 1024 * 1024), directory.path()).unwrap());
    table.prepare(Arc::clone(&context)).await.unwrap();
    context.seal_object_snapshots();
    let mut request = ScanRequest::new(100);
    request.predicate = Some(ScanPredicate::Comparison {
        column: 0,
        op: ComparisonOp::GtEq,
        value: PredicateValue::Int64(90),
    });
    let rows = table
        .scan(request, Arc::clone(&context))
        .await
        .unwrap()
        .try_fold(0_usize, |rows, batch| async move {
            Ok(rows.saturating_add(batch.num_rows()))
        })
        .await
        .unwrap();
    assert_eq!(rows, 100);
    assert_eq!(context.metrics.snapshot().parquet_page_index_bytes_read, 0);
    assert_eq!(context.metrics.snapshot().parquet_pages_pruned, 0);
}
#[tokio::test]
async fn bloom_filter_proves_missing_equality_without_data_pages() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("bloom.parquet");
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
        ArrowWriter::try_new(File::create(&path).unwrap(), schema, Some(properties)).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();

    let mut config = EngineConfig::default();
    config.parquet_scan.page_index = ParquetPruningMode::Disabled;
    let table = ParquetTable::try_new_with_cache(
        vec![path.to_string_lossy().into_owned()],
        ParquetOptions::default(),
        &config,
        MetadataCache::new(config.metadata_cache_bytes),
    )
    .await
    .unwrap();
    let context =
        Arc::new(QueryContext::new(MemoryPool::new(16 * 1024 * 1024), directory.path()).unwrap());
    table.prepare(Arc::clone(&context)).await.unwrap();
    context.seal_object_snapshots();
    let mut request = ScanRequest::new(100);
    request.predicate = Some(ScanPredicate::Comparison {
        column: 0,
        op: ComparisonOp::Eq,
        value: PredicateValue::Int64(51),
    });
    let batches = table
        .scan(request, Arc::clone(&context))
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();

    assert!(batches.is_empty());
    let metrics = context.metrics.snapshot();
    assert!(metrics.parquet_bloom_filter_bytes_read > 0);
    assert_eq!(metrics.parquet_bloom_row_groups_pruned, 1);
    assert_eq!(metrics.rows_scanned, 0);

    let positive_context =
        Arc::new(QueryContext::new(MemoryPool::new(16 * 1024 * 1024), directory.path()).unwrap());
    table.prepare(Arc::clone(&positive_context)).await.unwrap();
    positive_context.seal_object_snapshots();
    let mut positive = ScanRequest::new(100);
    positive.predicate = Some(ScanPredicate::Comparison {
        column: 0,
        op: ComparisonOp::Eq,
        value: PredicateValue::Int64(50),
    });
    let positive_rows = table
        .scan(positive, Arc::clone(&positive_context))
        .await
        .unwrap()
        .try_fold(0_usize, |rows, batch| async move {
            Ok(rows.saturating_add(batch.num_rows()))
        })
        .await
        .unwrap();
    assert_eq!(positive_rows, 100);
    let positive_metrics = positive_context.metrics.snapshot();
    assert!(positive_metrics.parquet_bloom_filter_bytes_read > 0);
    assert_eq!(positive_metrics.parquet_bloom_row_groups_pruned, 0);

    let mut disabled_config = config;
    disabled_config.parquet_scan.bloom_filter = ParquetPruningMode::Disabled;
    let disabled_table = ParquetTable::try_new_with_cache(
        vec![path.to_string_lossy().into_owned()],
        ParquetOptions::default(),
        &disabled_config,
        MetadataCache::new(disabled_config.metadata_cache_bytes),
    )
    .await
    .unwrap();
    let disabled_context =
        Arc::new(QueryContext::new(MemoryPool::new(16 * 1024 * 1024), directory.path()).unwrap());
    disabled_table
        .prepare(Arc::clone(&disabled_context))
        .await
        .unwrap();
    disabled_context.seal_object_snapshots();
    let mut disabled = ScanRequest::new(100);
    disabled.predicate = Some(ScanPredicate::Comparison {
        column: 0,
        op: ComparisonOp::Eq,
        value: PredicateValue::Int64(51),
    });
    let disabled_rows = disabled_table
        .scan(disabled, Arc::clone(&disabled_context))
        .await
        .unwrap()
        .try_fold(0_usize, |rows, batch| async move {
            Ok(rows.saturating_add(batch.num_rows()))
        })
        .await
        .unwrap();
    assert_eq!(disabled_rows, 100);
    let disabled_metrics = disabled_context.metrics.snapshot();
    assert_eq!(disabled_metrics.parquet_bloom_filter_bytes_read, 0);
    assert_eq!(disabled_metrics.parquet_bloom_row_groups_pruned, 0);
}

#[tokio::test]
async fn page_index_prunes_text_timestamp_and_decimal_pages() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("typed-page-index.parquet");
    let schema = Arc::new(Schema::new(vec![
        Field::new("text", DataType::Utf8, false),
        Field::new(
            "event_time",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        ),
        Field::new("small_amount", DataType::Decimal128(9, 2), false),
        Field::new("amount", DataType::Decimal128(10, 2), false),
    ]));
    let text = (0..100)
        .map(|value| format!("{value:03}"))
        .collect::<Vec<_>>();
    let amount = Decimal128Array::from_iter_values((0_i128..100).map(|value| value * 100))
        .with_precision_and_scale(10, 2)
        .unwrap();
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(StringArray::from(text)),
            Arc::new(TimestampMicrosecondArray::from_iter_values(
                (0_i64..100).map(|value| value * 1_000_000),
            )),
            Arc::new(
                Decimal128Array::from_iter_values((0_i128..100).map(|value| value * 100))
                    .with_precision_and_scale(9, 2)
                    .unwrap(),
            ),
            Arc::new(amount),
        ],
    )
    .unwrap();
    let properties = WriterProperties::builder()
        .set_statistics_enabled(EnabledStatistics::Page)
        .set_data_page_row_count_limit(10)
        .set_write_batch_size(10)
        .set_max_row_group_row_count(Some(100))
        .build();
    let mut writer =
        ArrowWriter::try_new(File::create(&path).unwrap(), schema, Some(properties)).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();

    let config = EngineConfig::default();
    let table = ParquetTable::try_new_with_cache(
        vec![path.to_string_lossy().into_owned()],
        ParquetOptions::default(),
        &config,
        MetadataCache::new(config.metadata_cache_bytes),
    )
    .await
    .unwrap();
    for (name, predicate, expected_rows, expect_pruning) in [
        (
            "text",
            ScanPredicate::Comparison {
                column: 0,
                op: ComparisonOp::GtEq,
                value: PredicateValue::Utf8("090".into()),
            },
            10,
            true,
        ),
        (
            "timestamp",
            ScanPredicate::Comparison {
                column: 1,
                op: ComparisonOp::GtEq,
                value: PredicateValue::TimestampMicros(90_000_000),
            },
            10,
            true,
        ),
        (
            "int32 decimal",
            ScanPredicate::Comparison {
                column: 2,
                op: ComparisonOp::GtEq,
                value: PredicateValue::Decimal128 {
                    value: 9_000,
                    precision: 4,
                    scale: 2,
                },
            },
            10,
            true,
        ),
        (
            "int64 decimal",
            ScanPredicate::Comparison {
                column: 3,
                op: ComparisonOp::GtEq,
                value: PredicateValue::Decimal128 {
                    value: 9_000,
                    precision: 4,
                    scale: 2,
                },
            },
            10,
            true,
        ),
        (
            "scale-zero decimal literal",
            ScanPredicate::Comparison {
                column: 3,
                op: ComparisonOp::Eq,
                value: PredicateValue::Decimal128 {
                    value: 1,
                    precision: 1,
                    scale: 0,
                },
            },
            10,
            true,
        ),
        (
            "safe-widening decimal scale",
            ScanPredicate::Comparison {
                column: 3,
                op: ComparisonOp::Eq,
                value: PredicateValue::Decimal128 {
                    value: 10_000,
                    precision: 5,
                    scale: 4,
                },
            },
            10,
            true,
        ),
        (
            "inexact decimal rescale",
            ScanPredicate::Comparison {
                column: 3,
                op: ComparisonOp::Eq,
                value: PredicateValue::Decimal128 {
                    value: 1_001,
                    precision: 4,
                    scale: 3,
                },
            },
            100,
            false,
        ),
    ] {
        let context = Arc::new(
            QueryContext::new(MemoryPool::new(16 * 1024 * 1024), directory.path()).unwrap(),
        );
        table.prepare(Arc::clone(&context)).await.unwrap();
        context.seal_object_snapshots();
        let mut request = ScanRequest::new(100);
        request.predicate = Some(predicate);
        let rows = table
            .scan(request, Arc::clone(&context))
            .await
            .unwrap()
            .try_fold(0_usize, |rows, batch| async move {
                Ok(rows.saturating_add(batch.num_rows()))
            })
            .await
            .unwrap();
        assert_eq!(rows, expected_rows, "{name}");
        assert_eq!(
            context.metrics.snapshot().parquet_page_rows_pruned > 0,
            expect_pruning,
            "{name}"
        );
    }
}
