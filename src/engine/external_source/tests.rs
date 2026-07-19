use std::sync::Arc;

use arrow::{
    array::Int64Array,
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use futures::StreamExt;
use parquet::arrow::ArrowWriter;

use crate::{CsvOptions, Engine, EngineConfig, ParquetOptions};

#[tokio::test]
async fn persistent_sources_survive_reopen_refresh_and_remove() {
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("native");
    let csv = directory.path().join("events.csv");
    let parquet = directory.path().join("facts.parquet");
    std::fs::write(&csv, "id\n1\n2\n").unwrap();
    write_parquet(&parquet);

    let config = EngineConfig::builder()
        .spill_directory(directory.path().join("spill-a"))
        .build();
    let engine = Engine::open(&database, config).unwrap();
    engine
        .add_external_csv(
            "events",
            [csv.to_string_lossy().into_owned()],
            CsvOptions::default(),
        )
        .await
        .unwrap();
    engine
        .add_external_parquet(
            "facts",
            [parquet.to_string_lossy().into_owned()],
            ParquetOptions::default(),
        )
        .await
        .unwrap();
    assert_eq!(
        engine
            .list_external_sources()
            .iter()
            .map(|source| (source.name(), source.format()))
            .collect::<Vec<_>>(),
        [("events", "csv"), ("facts", "parquet")]
    );
    assert!(Engine::open(&database, EngineConfig::default()).is_err());
    consume(
        engine
            .session()
            .execute("SELECT * FROM events")
            .await
            .unwrap(),
    )
    .await;
    drop(engine);

    let engine = Engine::open(
        &database,
        EngineConfig::builder()
            .spill_directory(directory.path().join("spill-b"))
            .build(),
    )
    .unwrap();
    assert!(
        engine
            .session()
            .table_names()
            .contains(&"events".to_owned())
    );
    consume(
        engine
            .session()
            .execute("SELECT count(*) FROM facts")
            .await
            .unwrap(),
    )
    .await;

    std::fs::write(&csv, "id,label\n1,one\n").unwrap();
    let refreshed = engine.refresh_external_source("events").await.unwrap();
    assert_eq!(refreshed.fields().len(), 2);
    assert!(engine.remove_external_source("facts").unwrap());
    assert!(!engine.remove_external_source("facts").unwrap());
    drop(engine);

    let engine = Engine::open(
        &database,
        EngineConfig::builder()
            .spill_directory(directory.path().join("spill-c"))
            .build(),
    )
    .unwrap();
    let sources = engine.list_external_sources();
    assert_eq!(sources.len(), 1);
    assert_eq!(sources[0].name(), "events");
    assert_eq!(sources[0].schema().fields().len(), 2);
    assert!(!engine.session().table_names().contains(&"facts".to_owned()));
}

async fn consume(mut result: crate::QueryResult) {
    while let Some(batch) = result.stream().next().await {
        batch.unwrap();
    }
}

fn write_parquet(path: &std::path::Path) {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
    )
    .unwrap();
    let file = std::fs::File::create(path).unwrap();
    let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
}
