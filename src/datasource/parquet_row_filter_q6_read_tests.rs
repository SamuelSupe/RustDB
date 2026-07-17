use std::{fs::File, sync::Arc};

use arrow::{
    array::{Date32Array, Decimal128Array, Int64Array},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use futures::TryStreamExt;
use parquet::{arrow::ArrowWriter, file::properties::WriterProperties};
use tempfile::tempdir;

use super::*;
use crate::{
    datasource::{ComparisonOp, PredicateGuarantee, PredicateValue, ScanPredicate},
    runtime::{MemoryPool, QueryContext},
};

#[tokio::test]
async fn q6_reader_applies_complete_filter_before_limit() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("q6-two-stage.parquet");
    write_q6_rows(&path);
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
    request.projection = Some(vec![1, 3]);
    request.predicate = Some(q6_predicate());
    request.predicate_guarantee = PredicateGuarantee::Exact;
    request.limit = Some(1);
    let batches = table
        .scan(request, Arc::clone(&context))
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();

    assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
    let batch = &batches[0];
    let discount = batch
        .column(0)
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .unwrap();
    let price = batch
        .column(1)
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .unwrap();
    assert_eq!(discount.value(0), 5);
    assert_eq!(price.value(0), 10_000);
    let metrics = context.metrics.snapshot();
    assert!(metrics.parquet_row_filter_evaluations > 0);
    assert!(metrics.parquet_row_filter_input_rows > 0);
    assert!(metrics.parquet_range_bytes_read > 0);
    assert!(metrics.parquet_narrow_decimal_columns > 0);
    assert!(
        metrics.parquet_row_filter_compute_time <= metrics.parquet_decode_compute_time,
        "row filter is a subset of decoder compute"
    );
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

fn write_q6_rows(path: &std::path::Path) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("l_shipdate", DataType::Date32, true),
        Field::new("l_discount", DataType::Decimal128(15, 2), true),
        Field::new("l_quantity", DataType::Int64, true),
        Field::new("l_extendedprice", DataType::Decimal128(15, 2), false),
    ]));
    let discount = Decimal128Array::from(vec![Some(7), Some(5), Some(5), Some(5), Some(5)])
        .with_precision_and_scale(15, 2)
        .unwrap();
    let price = Decimal128Array::from(vec![20_000, 40_000, 10_000, 30_000, 50_000])
        .with_precision_and_scale(15, 2)
        .unwrap();
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Date32Array::from(vec![
                Some(8_766),
                None,
                Some(8_766),
                Some(8_766),
                Some(9_131),
            ])),
            Arc::new(discount),
            Arc::new(Int64Array::from(vec![
                Some(10),
                Some(10),
                Some(10),
                Some(10),
                Some(10),
            ])),
            Arc::new(price),
        ],
    )
    .unwrap();
    let properties = WriterProperties::builder()
        .set_max_row_group_row_count(Some(5))
        .build();
    let mut writer =
        ArrowWriter::try_new(File::create(path).unwrap(), schema, Some(properties)).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
}
