use super::query_count;
use crate::{
    CsvHeader, CsvOptions, Engine, EngineConfig, Error, NativeImportFormat, NativeImportOptions,
};

fn csv_import(source: &std::path::Path, location: &str) -> NativeImportOptions {
    NativeImportOptions::new(
        "events-load",
        "events",
        source.join(location).to_string_lossy(),
        NativeImportFormat::Csv,
    )
    .csv_options(CsvOptions::builder().header(CsvHeader::Present).build())
}

#[tokio::test]
async fn native_import_replays_without_source_io_and_rejects_id_conflicts() {
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("database");
    let source = directory.path().join("events.csv");
    std::fs::write(&source, "id,name\n1,one\n2,two\n").unwrap();
    let config = EngineConfig::builder()
        .compute_threads(1)
        .spill_directory(directory.path().join("spill"))
        .build();

    let engine = Engine::open(&database, config.clone()).unwrap();
    let session = engine.session();
    let committed = session
        .import(csv_import(directory.path(), "events.csv"))
        .await
        .unwrap();
    assert!(!committed.replayed());
    assert_eq!(committed.receipt().rows(), 2);
    assert_eq!(committed.receipt().catalog_generation(), 1);

    std::fs::remove_file(&source).unwrap();
    let replayed = session
        .import(csv_import(directory.path(), "events.csv"))
        .await
        .unwrap();
    assert!(replayed.replayed());
    assert_eq!(replayed.receipt(), committed.receipt());

    let error = session
        .import(csv_import(directory.path(), "different.csv"))
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        Error::NativeImportConflict { import_id } if import_id == "events-load"
    ));
    assert_eq!(query_count(&session, "events").await, 2);

    drop(session);
    drop(engine);
    let reopened = Engine::open(&database, config).unwrap();
    let replayed = reopened
        .session()
        .import(csv_import(directory.path(), "events.csv"))
        .await
        .unwrap();
    assert!(replayed.replayed());
    assert_eq!(replayed.receipt().catalog_generation(), 1);
    assert_eq!(query_count(&reopened.session(), "events").await, 2);
}
