use std::{fs::File, sync::Arc};

use arrow::{
    array::{Int64Array, StringArray},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use futures::{StreamExt, TryStreamExt};
use parquet::{arrow::ArrowWriter, file::properties::WriterProperties};
use tempfile::tempdir;

use super::{ParquetTable, validate_file_schema};
use crate::{
    EngineConfig, Error, ParquetOptions, ParquetSchemaMode,
    datasource::parquet_scan::align_batch,
    datasource::{
        ComparisonOp, MetadataCache, PredicateValue, ScanPredicate, ScanRequest, TableProvider,
    },
    runtime::{MemoryPool, QueryContext},
};

#[test]
fn schema_validation_decodes_only_top_level_dictionaries() {
    let dictionary = DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8));
    let actual = Schema::new(vec![Field::new("name", dictionary, false)]);
    let expected = Schema::new(vec![Field::new("name", DataType::Utf8, false)]);
    for mode in [
        ParquetSchemaMode::Strict,
        ParquetSchemaMode::UnionByName,
        ParquetSchemaMode::SafeWidening,
    ] {
        validate_file_schema("s3://bucket/dictionary.parquet", &actual, &expected, mode).unwrap();
    }

    let dictionary_integer = Schema::new(vec![Field::new(
        "id",
        DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Int32)),
        false,
    )]);
    let widened_integer = Schema::new(vec![Field::new("id", DataType::Int64, false)]);
    validate_file_schema(
        "file:///dictionary-int.parquet",
        &dictionary_integer,
        &widened_integer,
        ParquetSchemaMode::SafeWidening,
    )
    .unwrap();

    let nested_dictionary = DataType::List(Arc::new(Field::new(
        "item",
        DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
        true,
    )));
    let nested_plain = DataType::List(Arc::new(Field::new("item", DataType::Utf8, true)));
    let actual = Schema::new(vec![Field::new("items", nested_dictionary, true)]);
    let expected = Schema::new(vec![Field::new("items", nested_plain, true)]);
    for mode in [
        ParquetSchemaMode::Strict,
        ParquetSchemaMode::UnionByName,
        ParquetSchemaMode::SafeWidening,
    ] {
        let error = validate_file_schema(
            "s3://bucket/nested-dictionary.parquet",
            &actual,
            &expected,
            mode,
        )
        .unwrap_err()
        .to_string();
        assert!(error.contains("nested-dictionary.parquet"), "{error}");
        assert!(error.contains("column 'items'"), "{error}");
    }
}

#[test]
fn alignment_fills_missing_union_columns_with_null() {
    let source_schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]));
    let batch = RecordBatch::try_new(
        source_schema,
        vec![Arc::new(Int64Array::from(vec![1_i64, 2]))],
    )
    .unwrap();
    let target = Arc::new(Schema::new(vec![
        Field::new("missing", DataType::Utf8, true),
        Field::new("a", DataType::Int64, false),
    ]));

    let aligned = align_batch(batch, &target, None, 0, "memory://test").unwrap();
    assert_eq!(aligned.num_rows(), 2);
    assert_eq!(aligned.column(0).null_count(), 2);
    assert_eq!(aligned.schema(), target);
}

#[tokio::test]
async fn applies_projection_limit_and_row_group_pruning() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("events.parquet");
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1_i64, 2, 3, 4])),
            Arc::new(StringArray::from(vec!["a", "b", "c", "d"])),
        ],
    )
    .unwrap();
    let properties = WriterProperties::builder()
        .set_max_row_group_row_count(Some(2))
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
    let mut request = ScanRequest::new(2);
    request.projection = Some(vec![1]);
    request.limit = Some(3);
    table.prepare(Arc::clone(&context)).await.unwrap();
    context.seal_object_snapshots();
    let batches = table
        .scan(request, Arc::clone(&context))
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_eq!(
        batches.iter().map(|batch| batch.num_rows()).sum::<usize>(),
        3
    );
    assert!(
        batches
            .iter()
            .all(|batch| batch.schema().field(0).name() == "name")
    );
    assert_eq!(context.metrics.snapshot().rows_scanned, 3);

    let pruning_context =
        Arc::new(QueryContext::new(MemoryPool::new(16 * 1024 * 1024), directory.path()).unwrap());
    let mut request = ScanRequest::new(2);
    request.predicate = Some(ScanPredicate::Comparison {
        column: 0,
        op: ComparisonOp::Gt,
        value: PredicateValue::Int64(10),
    });
    table.prepare(Arc::clone(&pruning_context)).await.unwrap();
    pruning_context.seal_object_snapshots();
    let batches = table
        .scan(request, Arc::clone(&pruning_context))
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert!(batches.is_empty());
    assert_eq!(pruning_context.metrics.snapshot().row_groups_pruned, 2);
}

#[tokio::test]
async fn empty_projection_preserves_row_counts_without_materializing_columns() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("count-only.parquet");
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]));
    let properties = WriterProperties::builder()
        .set_max_row_group_row_count(Some(2))
        .build();
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1_i64, 2, 3, 4, 5])),
            Arc::new(StringArray::from(vec!["a", "b", "c", "d", "e"])),
        ],
    )
    .unwrap();
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
    let mut request = ScanRequest::new(2);
    request.projection = Some(Vec::new());
    let batches = table
        .scan(request, Arc::clone(&context))
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();

    assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 5);
    assert!(batches.iter().all(|batch| batch.num_columns() == 0));
    let metrics = context.metrics.snapshot();
    assert_eq!(metrics.rows_scanned, 5);
    assert_eq!(metrics.bytes_scanned, 0);
    assert_eq!(metrics.parquet_page_index_bytes_read, 0);
    assert_eq!(metrics.parquet_bloom_filter_bytes_read, 0);
}

#[tokio::test]
async fn many_row_groups_keep_scan_task_metadata_bounded_by_target_lanes() {
    const ROW_GROUPS: usize = 128;
    const TARGET_TASKS: usize = 4;

    let directory = tempdir().unwrap();
    let path = directory.path().join("many-row-groups.parquet");
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let properties = WriterProperties::builder()
        .set_max_row_group_row_count(Some(1))
        .build();
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from_iter_values(
            0..i64::try_from(ROW_GROUPS).unwrap(),
        ))],
    )
    .unwrap();
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

    let tasks = table
        .scan_tasks(ScanRequest::new(8), Arc::clone(&context), TARGET_TASKS)
        .await
        .unwrap();
    assert_eq!(tasks.len(), TARGET_TASKS);

    let rows = futures::stream::iter(tasks.into_iter().map(|task| task.into_stream()))
        .flatten_unordered(TARGET_TASKS)
        .try_fold(0usize, |rows, batch| async move {
            Ok(rows.saturating_add(batch.num_rows()))
        })
        .await
        .unwrap();
    assert_eq!(rows, ROW_GROUPS);
}

#[tokio::test]
async fn refreshes_object_identity_between_queries() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("changing.parquet");
    write_ids(&path, &[1]);
    let config = EngineConfig::default();
    let table = ParquetTable::try_new_with_cache(
        vec![path.to_string_lossy().into_owned()],
        ParquetOptions::default(),
        &config,
        MetadataCache::new(config.metadata_cache_bytes),
    )
    .await
    .unwrap();

    assert_eq!(scan_rows(&table, directory.path()).await, 1);
    write_ids(&path, &[1, 2, 3]);
    assert_eq!(scan_rows(&table, directory.path()).await, 3);
}

#[tokio::test]
async fn query_schema_reservation_lives_with_table() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("schema-lease.parquet");
    write_ids(&path, &[1, 2]);
    let config = EngineConfig::default();
    let context =
        Arc::new(QueryContext::new(MemoryPool::new(16 * 1024 * 1024), directory.path()).unwrap());
    let table = ParquetTable::try_new_with_cache_for_query(
        vec![path.to_string_lossy().into_owned()],
        ParquetOptions::default(),
        &config,
        MetadataCache::new(config.metadata_cache_bytes),
        Some(Arc::clone(&context)),
    )
    .await
    .unwrap();

    let held = table
        ._schema_reservation
        .as_ref()
        .expect("query schema reservation")
        .size();
    let before_drop = context.memory.used();
    assert!(held > 0);
    drop(table);
    assert_eq!(context.memory.used(), before_drop - held);
}

#[tokio::test]
async fn multi_file_schema_reservation_covers_every_retained_schema() {
    let directory = tempdir().unwrap();
    let first_directory = directory.path().join("year=2025");
    let second_directory = directory.path().join("year=2026");
    std::fs::create_dir_all(&first_directory).unwrap();
    std::fs::create_dir_all(&second_directory).unwrap();
    let first = first_directory.join("part-0.parquet");
    let second = second_directory.join("part-1.parquet");
    write_ids(&first, &[1]);
    write_ids(&second, &[2]);

    let config = EngineConfig::default();
    let context =
        Arc::new(QueryContext::new(MemoryPool::new(16 * 1024 * 1024), directory.path()).unwrap());
    let table = ParquetTable::try_new_with_cache_for_query(
        vec![
            first.to_string_lossy().into_owned(),
            second.to_string_lossy().into_owned(),
        ],
        ParquetOptions {
            hive_partitioning: true,
            ..ParquetOptions::default()
        },
        &config,
        MetadataCache::new(config.metadata_cache_bytes),
        Some(Arc::clone(&context)),
    )
    .await
    .unwrap();

    assert!(table.hive.is_some());
    assert!(!Arc::ptr_eq(&table.schema, &table.physical_schema));
    let file_schema_bytes = table.file_schemas.iter().fold(0_usize, |bytes, file| {
        bytes.saturating_add(super::schema_memory_size(&file.schema))
    });
    let retained_schema_floor = file_schema_bytes
        .saturating_add(super::schema_memory_size(&table.physical_schema))
        .saturating_add(super::schema_memory_size(&table.schema))
        .saturating_add(table.hive.as_deref().unwrap().memory_size());
    let held = table
        ._schema_reservation
        .as_ref()
        .expect("query schema reservation")
        .size();

    assert!(held >= retained_schema_floor);
    let before_drop = context.memory.used();
    drop(table);
    assert_eq!(context.memory.used(), before_drop - held);
}

#[tokio::test]
async fn registration_rejects_schema_over_derived_limit() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("bounded-schema.parquet");
    write_ids(&path, &[1]);
    let config = EngineConfig {
        memory_limit: 1024 * 1024,
        ..EngineConfig::default()
    };
    let options = ParquetOptions {
        schema: Some(Arc::new(Schema::new(vec![Field::new(
            "x".repeat(100_000),
            DataType::Int64,
            false,
        )]))),
        ..ParquetOptions::default()
    };
    let result = ParquetTable::try_new_with_cache(
        vec![path.to_string_lossy().into_owned()],
        options,
        &config,
        MetadataCache::new(config.metadata_cache_bytes),
    )
    .await;

    let message = match result {
        Err(Error::ResourceExhausted(message)) => message,
        Err(error) => panic!("expected a resource exhausted error, got {error}"),
        Ok(_) => panic!("schema must be rejected"),
    };
    assert!(message.contains("table schema"));
    assert!(message.contains("requires"));
    assert!(message.contains("available"));
}

fn write_ids(path: &std::path::Path, values: &[i64]) {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from(values.to_vec()))],
    )
    .unwrap();
    let mut writer = ArrowWriter::try_new(File::create(path).unwrap(), schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
}

async fn scan_rows(table: &ParquetTable, spill_root: &std::path::Path) -> usize {
    let context =
        Arc::new(QueryContext::new(MemoryPool::new(16 * 1024 * 1024), spill_root).unwrap());
    table.prepare(Arc::clone(&context)).await.unwrap();
    context.seal_object_snapshots();
    table
        .scan(ScanRequest::new(2), context)
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap()
        .iter()
        .map(RecordBatch::num_rows)
        .sum()
}
