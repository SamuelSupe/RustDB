use std::sync::atomic::Ordering;

use arrow::array::{Decimal128Array, Int64Array};

use super::collect;
use crate::{Engine, EngineConfig};

#[test]
fn backup_rejects_a_poisoned_native_engine() {
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("native");
    let backup = directory.path().join("backup");
    let engine = Engine::open(
        &database,
        EngineConfig::builder()
            .spill_directory(directory.path().join("spill"))
            .build(),
    )
    .unwrap();
    engine.inner.native_poisoned.store(true, Ordering::Release);

    let error = engine.backup_to(&backup).unwrap_err();

    assert!(matches!(error, crate::Error::NativeStorage { .. }));
    assert!(!backup.exists());
}

#[tokio::test]
async fn local_backup_restores_after_the_source_database_is_removed() {
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("native");
    let backup = directory.path().join("backup");
    let restored = directory.path().join("restored");
    let csv = directory.path().join("input.csv");
    let mut contents = String::from("id,value\n");
    for id in 0..5_000_i64 {
        contents.push_str(&format!("{id},{}\n", id * 7 + 3));
    }
    std::fs::write(&csv, contents).unwrap();
    let config = EngineConfig::builder()
        .compute_threads(2)
        .spill_directory(directory.path().join("spill"))
        .build();
    let engine = Engine::open(&database, config.clone()).unwrap();
    collect(
        engine
            .session()
            .execute(&format!(
                "CREATE TABLE facts AS SELECT id, value FROM read_csv('{}', header = true)",
                csv.display()
            ))
            .await
            .unwrap(),
    )
    .await;
    engine.backup_to(&backup).unwrap();
    drop(engine);
    std::fs::remove_file(csv).unwrap();
    std::fs::remove_dir_all(database).unwrap();

    let restored_engine = Engine::restore_from(&backup, &restored, config).unwrap();
    let batches = collect(
        restored_engine
            .session()
            .execute("SELECT count(*), sum(id), sum(value) FROM facts")
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(value(&batches[0], 0), 5_000);
    assert_eq!(
        decimal_value(&batches[0], 1),
        (0..5_000_i64).map(i128::from).sum()
    );
    assert_eq!(
        decimal_value(&batches[0], 2),
        (0..5_000_i64).map(|id| i128::from(id * 7 + 3)).sum()
    );
}

fn decimal_value(batch: &arrow::record_batch::RecordBatch, column: usize) -> i128 {
    batch
        .column(column)
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .unwrap()
        .value(0)
}

fn value(batch: &arrow::record_batch::RecordBatch, column: usize) -> i64 {
    batch
        .column(column)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0)
}
