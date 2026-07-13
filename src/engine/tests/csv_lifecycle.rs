use std::{sync::Arc, time::Duration};

use arrow::datatypes::{DataType, Field, Schema};
use async_compression::tokio::write::GzipEncoder;
use futures::StreamExt;
use tokio::io::AsyncWriteExt;

use crate::{
    CsvCompression, CsvHeader, CsvOptions, Engine, EngineConfig, Error, Result,
    runtime::{QueryMetrics, TaskGroup},
};

const ROWS: usize = 20_000;
const MEMORY_LIMIT: usize = 128 * 1024 * 1024;

#[tokio::test]
async fn slow_csv_consumer_releases_source_framer_and_decoder_state() -> Result<()> {
    let fixture = CsvLifecycleFixture::new(true).await?;
    let mut result = fixture.query().await?;
    let first = result.stream().next().await.expect("first CSV batch")?;
    assert!(first.num_rows() > 0);

    // Let the source/framer fill its bounded morsel queues while the public
    // consumer deliberately stops polling.
    tokio::time::sleep(Duration::from_millis(25)).await;
    let live = result.metrics().snapshot();
    assert!(live.csv_source_bytes > 0, "{live:?}");
    assert!(live.csv_decompressed_bytes > 0, "{live:?}");
    assert!(live.csv_morsels > 1, "{live:?}");
    assert!(live.current_memory_bytes <= MEMORY_LIMIT as u64, "{live:?}");

    let mut rows = first.num_rows();
    while let Some(batch) = result.stream().next().await {
        rows = rows.saturating_add(batch?.num_rows());
    }
    assert_eq!(rows, ROWS);
    assert!(result.metrics().snapshot().peak_csv_parser_lanes > 0);

    let tasks = result.context.tasks.clone();
    let metrics = result.metrics();
    drop(result);
    assert_released(tasks, metrics).await;
    Ok(())
}

#[tokio::test]
async fn cancelling_csv_query_quiesces_every_lane_and_reservation() -> Result<()> {
    let fixture = CsvLifecycleFixture::new(false).await?;
    let mut result = fixture.query().await?;
    let first = result.stream().next().await.expect("first CSV batch")?;
    assert!(first.num_rows() > 0);
    tokio::time::sleep(Duration::from_millis(10)).await;

    result.cancel();
    let terminal = tokio::time::timeout(Duration::from_secs(2), result.stream().next())
        .await
        .expect("cancelled CSV query must terminate")
        .expect("cancelled CSV query must expose an error")
        .expect_err("cancelled CSV query unexpectedly returned another batch");
    assert!(matches!(terminal, Error::Cancelled), "{terminal}");

    let tasks = result.context.tasks.clone();
    let metrics = result.metrics();
    drop(result);
    assert_released(tasks, metrics.clone()).await;
    assert!(!metrics.snapshot().cancel_to_quiesce.is_zero());
    Ok(())
}

#[tokio::test]
async fn abandoning_csv_consumer_reaps_every_lane_and_reservation() -> Result<()> {
    let fixture = CsvLifecycleFixture::new(true).await?;
    let mut result = fixture.query().await?;
    let first = result.stream().next().await.expect("first CSV batch")?;
    assert!(first.num_rows() > 0);
    tokio::time::sleep(Duration::from_millis(10)).await;

    let tasks = result.context.tasks.clone();
    let metrics = result.metrics();
    drop(result);
    assert_released(tasks, metrics).await;
    Ok(())
}

struct CsvLifecycleFixture {
    _directory: tempfile::TempDir,
    session: crate::Session,
}

impl CsvLifecycleFixture {
    async fn new(compressed: bool) -> Result<Self> {
        let directory = tempfile::tempdir().expect("CSV lifecycle tempdir");
        let path = directory.path().join(if compressed {
            "input.data"
        } else {
            "input.csv"
        });
        let mut csv = String::from("id,payload\n");
        for row in 0..ROWS {
            csv.push_str(&format!("{row},payload-{row:08}-{}\n", "x".repeat(96)));
        }
        let bytes = if compressed {
            let mut encoder = GzipEncoder::new(Vec::new());
            encoder.write_all(csv.as_bytes()).await.expect("encode CSV");
            encoder.shutdown().await.expect("finish CSV gzip member");
            encoder.into_inner()
        } else {
            csv.into_bytes()
        };
        std::fs::write(&path, bytes).map_err(|error| Error::io(Some(path.clone()), error))?;

        let config = EngineConfig::builder()
            .memory_limit(MEMORY_LIMIT)
            .batch_size(64)
            .compute_threads(4)
            .io_concurrency(4)
            .csv_target_morsel_bytes(16 * 1024)
            .spill_directory(directory.path().join("spill"))
            .build();
        let session = Engine::new(config)?.session();
        session
            .register_csv(
                "csv_lifecycle",
                [path.to_string_lossy().into_owned()],
                CsvOptions::builder()
                    .schema(Arc::new(Schema::new(vec![
                        Field::new("id", DataType::Int64, false),
                        Field::new("payload", DataType::Utf8, false),
                    ])))
                    .header(CsvHeader::Present)
                    .compression(CsvCompression::Auto)
                    .build(),
            )
            .await?;
        Ok(Self {
            _directory: directory,
            session,
        })
    }

    async fn query(&self) -> Result<super::super::QueryResult> {
        self.session
            .execute("SELECT id, payload FROM csv_lifecycle")
            .await
    }
}

async fn assert_released(tasks: TaskGroup, metrics: QueryMetrics) {
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let snapshot = metrics.snapshot();
            if tasks.active_tasks() == 0
                && snapshot.current_memory_bytes == 0
                && metrics.active_csv_parser_lanes() == 0
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap_or_else(|_| {
        panic!(
            "CSV lifecycle did not quiesce: active_tasks={}, active_parser_lanes={}, metrics={:?}",
            tasks.active_tasks(),
            metrics.active_csv_parser_lanes(),
            metrics.snapshot()
        )
    });
}
