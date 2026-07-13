use std::{fs, sync::Arc};

use arrow::{
    array::{Int64Array, StringArray},
    datatypes::{DataType, Field, Schema},
};
use futures::StreamExt;
use rustdb::{CsvHeader, CsvOptions, Engine, EngineConfig, Result};

#[tokio::test]
async fn explicit_schema_header_and_delimiter_execute_end_to_end() -> Result<()> {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("people.psv");
    fs::write(&path, "id|name\n1|Ada\n2|Grace\n").unwrap();
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]));
    let session = Engine::new(EngineConfig::default())?.session();
    session
        .register_csv(
            "people",
            [path.to_string_lossy()],
            CsvOptions::builder()
                .schema(schema)
                .header(CsvHeader::Present)
                .delimiter(b'|')
                .build(),
        )
        .await?;

    let mut result = session
        .execute("SELECT id, name FROM people ORDER BY id")
        .await?;
    let batch = result.stream().next().await.unwrap()?;
    let ids = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let names = batch
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!((ids.value(0), names.value(0)), (1, "Ada"));
    assert_eq!((ids.value(1), names.value(1)), (2, "Grace"));
    Ok(())
}

#[tokio::test]
async fn rejects_bad_utf8_with_the_source_uri() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("bad-utf8.csv");
    fs::write(&path, b"id,name\n1,\xff\n").unwrap();
    let error = registration_error(&path).await;
    assert!(error.contains("bad-utf8.csv"), "unexpected error: {error}");
}

#[tokio::test]
async fn rejects_malformed_records_with_the_source_uri() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("malformed.csv");
    fs::write(&path, b"id,name\n1,Ada,extra\n").unwrap();
    let error = registration_error(&path).await;
    assert!(error.contains("malformed.csv"), "unexpected error: {error}");
}

#[tokio::test]
async fn parallel_decode_error_reports_uri_and_decompressed_offset() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("parallel-malformed.csv");
    let mut contents = String::from("id,name\n");
    for id in 0..10_100 {
        contents.push_str(&format!("{id},valid\n"));
    }
    contents.push_str("10101,Ada,extra\n");
    fs::write(&path, contents).unwrap();
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]));
    let config = EngineConfig::builder()
        .compute_threads(4)
        .io_concurrency(4)
        .csv_target_morsel_bytes(8)
        .build();
    let session = Engine::new(config).unwrap().session();
    session
        .register_csv(
            "invalid_parallel",
            [path.to_string_lossy()],
            CsvOptions::builder()
                .schema(schema)
                .header(CsvHeader::Present)
                .build(),
        )
        .await
        .unwrap();

    let mut result = session
        .execute("SELECT * FROM invalid_parallel")
        .await
        .unwrap();
    let error = loop {
        match result.stream().next().await {
            Some(Ok(_)) => {}
            Some(Err(error)) => break error.to_string(),
            None => panic!("malformed morsel unexpectedly reached EOF"),
        }
    };
    assert!(error.contains("parallel-malformed.csv"), "{error}");
    assert!(error.contains("decompressed offset"), "{error}");
}

#[tokio::test]
async fn rejects_incompatible_schemas_across_files() {
    let directory = tempfile::tempdir().unwrap();
    fs::write(directory.path().join("a.csv"), "id,value\n1,10\n").unwrap();
    fs::write(directory.path().join("b.csv"), "id,value\n2,text\n").unwrap();
    let session = Engine::new(EngineConfig::default()).unwrap().session();
    let error = session
        .register_csv(
            "mixed",
            [format!("{}/*.csv", directory.path().display())],
            CsvOptions::builder().header(CsvHeader::Present).build(),
        )
        .await
        .unwrap_err()
        .to_string();
    assert!(
        error.contains("CSV schema mismatch"),
        "unexpected error: {error}"
    );
    assert!(error.contains("b.csv"), "unexpected error: {error}");
}

async fn registration_error(path: &std::path::Path) -> String {
    let session = Engine::new(EngineConfig::default()).unwrap().session();
    match session
        .register_csv(
            "invalid",
            [path.to_string_lossy()],
            CsvOptions::builder().header(CsvHeader::Present).build(),
        )
        .await
    {
        Err(error) => error.to_string(),
        Ok(()) => {
            let mut result = session.execute("SELECT * FROM invalid").await.unwrap();
            result
                .stream()
                .next()
                .await
                .expect("invalid CSV should produce an error")
                .unwrap_err()
                .to_string()
        }
    }
}
