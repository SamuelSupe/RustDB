use std::{fs::File, sync::Arc};

use arrow::{
    array::{Date32Array, Decimal128Array, Int64Array},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use futures::{StreamExt, TryStreamExt};
use parquet::{arrow::ArrowWriter, file::properties::WriterProperties};

use super::ParquetTable;
use crate::{
    EngineConfig,
    datasource::{
        ComparisonOp, MetadataCache, PredicateGuarantee, PredicateValue, ScanPredicate,
        ScanRequest, TableProvider, TableStatistics,
    },
    runtime::{MemoryPool, QueryContext},
    storage::LocationResolver,
};

#[tokio::test]
async fn fixed_exact_scan_reuses_readers_across_bounded_row_group_chunks() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("lineitem.rdbseg");
    let schema = write_rows(&path);
    let config = EngineConfig::builder().io_concurrency(4).build();
    let resolver = LocationResolver::with_memory_limit(config.s3.clone(), config.memory_limit);
    let files = resolver
        .resolve(&[path.to_string_lossy().into_owned()])
        .await
        .unwrap();
    let table = ParquetTable::from_fixed_files(
        files.clone(),
        schema,
        TableStatistics {
            row_count: Some(18),
            total_byte_size: Some(files[0].snapshot().size),
            file_count: 1,
        },
        &config,
        MetadataCache::new(0),
    )
    .unwrap();
    let context = Arc::new(QueryContext::new(MemoryPool::new(64 << 20), directory.path()).unwrap());
    table.prepare(Arc::clone(&context)).await.unwrap();
    context.seal_object_snapshots();
    let baseline = context.memory.used();

    let tasks = table
        .scan_tasks(q6_request(), Arc::clone(&context), 4)
        .await
        .unwrap();
    assert_eq!(tasks.len(), 4);
    let planning_file_opens = context.metrics.snapshot().parquet_local_file_opens;

    let (rows, price_sum) = futures::stream::iter(tasks.into_iter().map(|task| task.into_stream()))
        .flatten_unordered(4)
        .try_fold((0usize, 0i128), |(rows, sum), batch| async move {
            let prices = batch
                .column(3)
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .unwrap();
            Ok((
                rows + batch.num_rows(),
                sum + prices.iter().flatten().sum::<i128>(),
            ))
        })
        .await
        .unwrap();

    assert_eq!(rows, 9);
    assert_eq!(price_sum, 81_000);
    let metrics = context.metrics.snapshot();
    assert_eq!(metrics.parquet_reader_builds, 5);
    assert_eq!(
        metrics.parquet_local_file_opens, 1,
        "planning and all five scan readers must share one opened descriptor",
    );
    assert!(planning_file_opens <= 1);
    assert_eq!(context.memory.used(), baseline);

    let abandoned =
        Arc::new(QueryContext::new(MemoryPool::new(64 << 20), directory.path()).unwrap());
    table.prepare(Arc::clone(&abandoned)).await.unwrap();
    abandoned.seal_object_snapshots();
    let baseline = abandoned.memory.used();
    let tasks = table
        .scan_tasks(q6_request(), Arc::clone(&abandoned), 4)
        .await
        .unwrap();
    assert!(abandoned.memory.used() > baseline);
    drop(tasks);
    assert_eq!(abandoned.memory.used(), baseline);
}

fn q6_request() -> ScanRequest {
    let mut request = ScanRequest::new(2);
    request.projection = Some(vec![0, 1, 2, 3]);
    request.predicate = Some(q6_predicate());
    request.predicate_guarantee = PredicateGuarantee::Exact;
    request
}

fn q6_predicate() -> ScanPredicate {
    let decimal = |column, op, value| ScanPredicate::Comparison {
        column,
        op,
        value: PredicateValue::Decimal128 {
            value,
            precision: 15,
            scale: 2,
        },
    };
    ScanPredicate::And(vec![
        ScanPredicate::Comparison {
            column: 0,
            op: ComparisonOp::GtEq,
            value: PredicateValue::Date32(8_766),
        },
        ScanPredicate::Comparison {
            column: 0,
            op: ComparisonOp::Lt,
            value: PredicateValue::Date32(9_131),
        },
        decimal(1, ComparisonOp::GtEq, 4),
        decimal(1, ComparisonOp::LtEq, 6),
        ScanPredicate::Comparison {
            column: 2,
            op: ComparisonOp::Lt,
            value: PredicateValue::Int64(24),
        },
    ])
}

fn write_rows(path: &std::path::Path) -> Arc<Schema> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("l_shipdate", DataType::Date32, false),
        Field::new("l_discount", DataType::Decimal128(15, 2), false),
        Field::new("l_quantity", DataType::Int64, false),
        Field::new("l_extendedprice", DataType::Decimal128(15, 2), false),
    ]));
    let mut shipdates = Vec::new();
    let mut discounts = Vec::new();
    let mut quantities = Vec::new();
    let mut prices = Vec::new();
    for row in 0..18_i64 {
        let matches = row % 2 == 0;
        shipdates.push(if matches { 8_800 } else { 9_200 });
        discounts.push(if matches { 5 } else { 7 });
        quantities.push(if matches { 10 } else { 30 });
        prices.push((row + 1) as i128 * 1_000);
    }
    let discounts = Decimal128Array::from_iter_values(discounts)
        .with_precision_and_scale(15, 2)
        .unwrap();
    let prices = Decimal128Array::from_iter_values(prices)
        .with_precision_and_scale(15, 2)
        .unwrap();
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Date32Array::from(shipdates)),
            Arc::new(discounts),
            Arc::new(Int64Array::from(quantities)),
            Arc::new(prices),
        ],
    )
    .unwrap();
    let properties = WriterProperties::builder()
        .set_max_row_group_row_count(Some(2))
        .build();
    let mut writer = ArrowWriter::try_new(
        File::create(path).unwrap(),
        Arc::clone(&schema),
        Some(properties),
    )
    .unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    schema
}
