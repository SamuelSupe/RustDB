use arrow::array::UInt64Array;
use futures::StreamExt;

use super::{collect, query_count};
use crate::{Engine, EngineConfig};

#[tokio::test]
async fn copy_round_trips_local_csv_and_parquet() {
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("native");
    let csv = directory.path().join("events.csv");
    let parquet = directory.path().join("events.parquet");
    let config = EngineConfig::builder()
        .compute_threads(1)
        .spill_directory(directory.path().join("spill"))
        .build();
    let engine = Engine::open(&database, config.clone()).unwrap();
    let session = engine.session();
    collect(
        session
            .execute(
                "CREATE TABLE events AS SELECT * FROM (VALUES (1, 'a'), (2, 'b'), (3, NULL)) AS v(id, label)",
            )
            .await
            .unwrap(),
    )
    .await;

    let csv_status = collect(
        session
            .execute(&format!(
                "COPY events TO '{}' (FORMAT CSV, HEADER TRUE)",
                csv.display()
            ))
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(status_rows(&csv_status), 3);
    assert!(
        std::fs::read_to_string(&csv)
            .unwrap()
            .starts_with("id,label\n")
    );

    let parquet_status = collect(
        session
            .execute(&format!(
                "COPY (SELECT id, label FROM events ORDER BY id) TO '{}' (FORMAT PARQUET)",
                parquet.display()
            ))
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(status_rows(&parquet_status), 3);

    collect(
        session
            .execute("CREATE TABLE csv_import (id BIGINT, label VARCHAR)")
            .await
            .unwrap(),
    )
    .await;
    collect(
        session
            .execute(&format!(
                "COPY csv_import FROM '{}' (FORMAT CSV, HEADER TRUE)",
                csv.display()
            ))
            .await
            .unwrap(),
    )
    .await;
    collect(
        session
            .execute("CREATE TABLE parquet_import (id BIGINT, label VARCHAR)")
            .await
            .unwrap(),
    )
    .await;
    collect(
        session
            .execute(&format!(
                "COPY parquet_import FROM '{}' (FORMAT PARQUET)",
                parquet.display()
            ))
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(query_count(&session, "csv_import").await, 3);
    assert_eq!(query_count(&session, "parquet_import").await, 3);
    drop(session);
    drop(engine);

    let reopened = Engine::open(&database, config).unwrap();
    assert_eq!(query_count(&reopened.session(), "csv_import").await, 3);
    assert_eq!(query_count(&reopened.session(), "parquet_import").await, 3);
    let staging = std::fs::read_dir(directory.path())
        .unwrap()
        .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
        .filter(|name| name.contains("rustdb-copy"))
        .collect::<Vec<_>>();
    assert!(staging.is_empty(), "COPY left staging files: {staging:?}");
}

#[tokio::test]
async fn copy_late_cancellation_reports_that_output_is_already_durable() {
    let directory = tempfile::tempdir().unwrap();
    let output = directory.path().join("durable.csv");
    let engine = Engine::new(
        EngineConfig::builder()
            .compute_threads(1)
            .spill_directory(directory.path().join("spill"))
            .build(),
    )
    .unwrap();
    let session = engine.session();
    let mut result = session
        .execute(&format!(
            "COPY (SELECT * FROM (VALUES (1), (2), (3)) AS v(id)) TO '{}' (FORMAT CSV)",
            output.display()
        ))
        .await
        .unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while !result.context.durable_outcome_is_committed() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("COPY output was not published");
    result.cancel();
    assert!(
        matches!(
            result.stream().next().await.unwrap().unwrap_err(),
            crate::Error::CopyPostCommitFailure { .. }
        ),
        "late cancellation must never report a retryable Cancelled outcome"
    );
    assert!(output.exists());
}

fn status_rows(batches: &[arrow::record_batch::RecordBatch]) -> u64 {
    batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap()
        .value(0)
}
