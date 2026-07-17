use std::{fs::File, sync::Arc};

use arrow::{
    array::{Decimal128Array, Int64Array, StringArray},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use parquet::{arrow::ArrowWriter, basic::Compression, file::properties::WriterProperties};

use super::collect;
use crate::{Engine, EngineConfig};

const ROWS: i64 = 10_000;

#[tokio::test]
async fn native_ctas_from_parquet_survives_reopen_with_exact_aggregates() {
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("native");
    let parquet = directory.path().join("input.parquet");
    write_parquet(&parquet);
    let config = EngineConfig::builder()
        .compute_threads(2)
        .spill_directory(directory.path().join("spill"))
        .build();

    let engine = Engine::open(&database, config.clone()).unwrap();
    collect(
        engine
            .session()
            .execute(&format!(
                "CREATE TABLE facts AS SELECT id, amount FROM read_parquet('{}')",
                parquet.display()
            ))
            .await
            .unwrap(),
    )
    .await;
    drop(engine);

    let reopened = Engine::open(&database, config).unwrap();
    let batches = collect(
        reopened
            .session()
            .execute("SELECT count(*), sum(id), sum(amount) FROM facts")
            .await
            .unwrap(),
    )
    .await;
    let batch = &batches[0];
    assert_eq!(int64_value(batch, 0), ROWS);
    assert_eq!(decimal_value(batch, 1), (0..ROWS).map(i128::from).sum());
    assert_eq!(
        decimal_value(batch, 2),
        (0..ROWS).map(|id| i128::from(id * 3 + 7)).sum()
    );
}

#[tokio::test]
async fn native_snapshot_delegates_exact_filter_to_fixed_parquet() {
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("native");
    let parquet = directory.path().join("input.parquet");
    write_parquet(&parquet);
    let engine = Engine::open(
        &database,
        EngineConfig::builder()
            .compute_threads(2)
            .spill_directory(directory.path().join("spill"))
            .build(),
    )
    .unwrap();
    let session = engine.session();
    collect(
        session
            .execute(&format!(
                "CREATE TABLE facts AS SELECT id, amount FROM read_parquet('{}')",
                parquet.display()
            ))
            .await
            .unwrap(),
    )
    .await;
    assert_no_predicate_sidecars(&database);

    let explain = collect(
        session
            .execute("EXPLAIN SELECT count(*) FROM facts WHERE id >= 9990")
            .await
            .unwrap(),
    )
    .await;
    let text = explain[0]
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap()
        .value(0);
    assert!(
        text.contains("Scan table=facts projection=Some([]) filter=exact"),
        "{text}"
    );
    assert!(
        !text
            .lines()
            .any(|line| line.trim_start().starts_with("Filter ")),
        "{text}"
    );

    let result = session
        .execute("SELECT count(*) FROM facts WHERE id >= 9990")
        .await
        .unwrap();
    let metrics = result.metrics();
    let batches = collect(result).await;
    assert_eq!(int64_value(&batches[0], 0), 10);
    let metrics = metrics.snapshot();
    assert_eq!(metrics.discovered_files, 1);
    assert_eq!(metrics.native_predicate_sidecar_bytes_read, 0);
    assert_eq!(metrics.native_predicate_sidecar_rows_evaluated, 0);
    assert_eq!(metrics.native_predicate_sidecar_rows_selected, 0);
    assert_eq!(metrics.native_predicate_sidecar_exact_bypasses, 0);
    assert_eq!(metrics.native_predicate_sidecar_fallbacks, 0);
    assert!(metrics.parquet_row_filter_evaluations > 0);
    assert!(
        metrics.parquet_page_rows_pruned > 0,
        "Parquet page-index pruning should remain active"
    );

    let projected = session
        .execute("SELECT sum(amount) FROM facts WHERE id >= 9990")
        .await
        .unwrap();
    let projected_metrics = projected.metrics();
    let projected_batches = collect(projected).await;
    assert_eq!(
        decimal_value(&projected_batches[0], 0),
        (9_990..ROWS).map(|id| i128::from(id * 3 + 7)).sum()
    );
    let projected_metrics = projected_metrics.snapshot();
    assert_eq!(
        projected_metrics.native_predicate_sidecar_full_projection_bypasses,
        0
    );
    assert_eq!(
        projected_metrics.native_predicate_sidecar_full_projection_rows,
        0
    );
    assert!(projected_metrics.parquet_reader_builds > 0);
    assert!(projected_metrics.parquet_row_filter_evaluations > 0);
    assert_eq!(projected_metrics.current_memory_bytes, 0);
}

fn decimal_value(batch: &RecordBatch, column: usize) -> i128 {
    batch
        .column(column)
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .unwrap()
        .value(0)
}

fn write_parquet(path: &std::path::Path) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("amount", DataType::Int64, false),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from_iter_values(0..ROWS)),
            Arc::new(Int64Array::from_iter_values((0..ROWS).map(|id| id * 3 + 7))),
        ],
    )
    .unwrap();
    let properties = WriterProperties::builder()
        .set_compression(Compression::UNCOMPRESSED)
        .set_dictionary_enabled(false)
        .build();
    let mut writer =
        ArrowWriter::try_new(File::create(path).unwrap(), schema, Some(properties)).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
}

fn int64_value(batch: &RecordBatch, column: usize) -> i64 {
    batch
        .column(column)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0)
}

fn assert_no_predicate_sidecars(root: &std::path::Path) {
    let mut pending = vec![root.to_path_buf()];
    while let Some(path) = pending.pop() {
        for entry in std::fs::read_dir(path).unwrap() {
            let entry = entry.unwrap();
            let path = entry.path();
            if path.is_dir() {
                pending.push(path);
            } else {
                assert_ne!(
                    path.extension().and_then(|extension| extension.to_str()),
                    Some("rdbpred"),
                    "new Native writes must not create predicate sidecars: {}",
                    path.display()
                );
            }
        }
    }
}
