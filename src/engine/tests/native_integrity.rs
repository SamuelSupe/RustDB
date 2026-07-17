use std::{fs::File, sync::Arc};

#[cfg(any(target_os = "linux", target_os = "macos"))]
use std::{fs::FileTimes, time::Duration};

use arrow::{
    array::{Int64Array, StringArray},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use parquet::arrow::ArrowWriter;

use super::collect;
use crate::{
    Engine, EngineConfig, Error, QueryMetricsSnapshot, Session, storage::NativeTableSnapshot,
};

#[tokio::test]
async fn load_verification_seeds_queries_and_identity_change_revalidates() {
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("native");
    let csv = directory.path().join("input.csv");
    write_csv(&csv, 0, 5_000);
    let engine = Engine::open(
        &database,
        EngineConfig::builder()
            .spill_directory(directory.path().join("spill"))
            .build(),
    )
    .unwrap();
    let session = engine.session();
    create_events(&session, &csv).await;
    let segment = only_segment(&database);
    let baseline = NativeTableSnapshot::full_verification_count_for_test(&segment);

    let first_metrics = query_events_metrics(&session).await;
    assert_eq!(first_metrics.native_full_verification_segments, 0);
    assert!(first_metrics.native_verification_time <= first_metrics.provider_prepare_time);
    assert_eq!(
        NativeTableSnapshot::full_verification_count_for_test(&segment),
        baseline,
        "first query repeated the full verification completed by commit"
    );
    let second_metrics = query_events_metrics(&session).await;
    assert_eq!(second_metrics.native_full_verification_segments, 0);
    assert_eq!(
        NativeTableSnapshot::full_verification_count_for_test(&segment),
        baseline,
        "unchanged segment was hashed again"
    );

    let replacement = segment.with_extension("replacement");
    std::fs::copy(&segment, &replacement).unwrap();
    std::fs::rename(&replacement, &segment).unwrap();
    let replacement_metrics = query_events_metrics(&session).await;
    assert_eq!(replacement_metrics.native_full_verification_segments, 1);
    assert_eq!(
        NativeTableSnapshot::full_verification_count_for_test(&segment),
        baseline + 1,
        "new file identity did not trigger full verification"
    );
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
#[tokio::test]
async fn restored_mtime_still_revalidates_an_in_place_native_segment_write() {
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("native");
    let csv = directory.path().join("input.csv");
    write_csv(&csv, 0, 5_000);
    let engine = Engine::open(
        &database,
        EngineConfig::builder()
            .spill_directory(directory.path().join("spill"))
            .build(),
    )
    .unwrap();
    let session = engine.session();
    create_events(&session, &csv).await;
    let segment = only_segment(&database);
    let baseline = NativeTableSnapshot::full_verification_count_for_test(&segment);
    let contents = std::fs::read(&segment).unwrap();
    let modified = std::fs::metadata(&segment).unwrap().modified().unwrap();

    std::thread::sleep(Duration::from_millis(2));
    std::fs::write(&segment, contents).unwrap();
    File::options()
        .write(true)
        .open(&segment)
        .unwrap()
        .set_times(FileTimes::new().set_modified(modified))
        .unwrap();

    let metrics = query_events_metrics(&session).await;
    assert_eq!(metrics.native_full_verification_segments, 1);
    assert_eq!(
        NativeTableSnapshot::full_verification_count_for_test(&segment),
        baseline + 1
    );
}

#[tokio::test]
async fn concurrent_native_prepares_share_one_full_verification() {
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("native");
    let csv = directory.path().join("input.csv");
    write_csv(&csv, 0, 5_000);
    let engine = Engine::open(
        &database,
        EngineConfig::builder()
            .max_concurrent_queries(2)
            .spill_directory(directory.path().join("spill"))
            .build(),
    )
    .unwrap();
    let session = engine.session();
    create_events(&session, &csv).await;
    let segment = only_segment(&database);
    let baseline = NativeTableSnapshot::full_verification_count_for_test(&segment);

    let replacement = segment.with_extension("replacement");
    std::fs::copy(&segment, &replacement).unwrap();
    std::fs::rename(&replacement, &segment).unwrap();

    let (left, right) = tokio::join!(
        query_events_metrics(&session),
        query_events_metrics(&session)
    );
    assert_eq!(
        left.native_full_verification_segments + right.native_full_verification_segments,
        1,
        "concurrent prepares did not share the verification leader"
    );
    assert_eq!(
        NativeTableSnapshot::full_verification_count_for_test(&segment),
        baseline + 1,
        "concurrent prepares repeated a full verification"
    );
}

#[tokio::test]
async fn reopen_verification_seeds_the_first_query() {
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("native");
    let csv = directory.path().join("input.csv");
    write_csv(&csv, 0, 5_000);
    let config = EngineConfig::builder()
        .spill_directory(directory.path().join("spill"))
        .build();
    let engine = Engine::open(&database, config.clone()).unwrap();
    let session = engine.session();
    create_events(&session, &csv).await;
    let segment = only_segment(&database);
    let before_reopen = NativeTableSnapshot::full_verification_count_for_test(&segment);
    drop(session);
    drop(engine);

    let engine = Engine::open(&database, config).unwrap();
    let after_reopen = NativeTableSnapshot::full_verification_count_for_test(&segment);
    assert_eq!(after_reopen, before_reopen + 1);
    let metrics = query_events_metrics(&engine.session()).await;

    assert_eq!(metrics.native_full_verification_segments, 0);
    assert_eq!(
        NativeTableSnapshot::full_verification_count_for_test(&segment),
        after_reopen,
        "first query repeated the full verification completed by reopen"
    );
}

#[tokio::test]
async fn append_load_seeds_inherited_and_new_segments() {
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("native");
    let first = directory.path().join("first.csv");
    let appended = directory.path().join("appended.csv");
    write_csv(&first, 0, 4_000);
    write_csv(&appended, 4_000, 2_000);
    let engine = Engine::open(
        &database,
        EngineConfig::builder()
            .spill_directory(directory.path().join("spill"))
            .build(),
    )
    .unwrap();
    let session = engine.session();
    create_events(&session, &first).await;
    collect(
        session
            .execute(&format!(
                "INSERT INTO events SELECT id, label FROM read_csv('{}', header = true)",
                appended.display()
            ))
            .await
            .unwrap(),
    )
    .await;
    let segments = all_segments(&database);
    assert_eq!(segments.len(), 2);
    let baseline = segments
        .iter()
        .map(|segment| NativeTableSnapshot::full_verification_count_for_test(segment))
        .collect::<Vec<_>>();

    let metrics = query_events_metrics(&session).await;

    assert_eq!(metrics.native_full_verification_segments, 0);
    for (segment, expected) in segments.iter().zip(baseline) {
        assert_eq!(
            NativeTableSnapshot::full_verification_count_for_test(segment),
            expected,
            "query repeated append-load verification for {}",
            segment.display()
        );
    }
}

#[tokio::test]
async fn an_open_engine_rejects_a_modified_native_segment_before_query_output() {
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("native");
    let csv = directory.path().join("input.csv");
    let mut contents = String::from("id,label\n");
    for id in 0..5_000_i64 {
        contents.push_str(&format!("{id},value-{id:08}\n"));
    }
    std::fs::write(&csv, contents).unwrap();
    let engine = Engine::open(
        &database,
        EngineConfig::builder()
            .spill_directory(directory.path().join("spill"))
            .build(),
    )
    .unwrap();
    let session = engine.session();
    collect(
        session
            .execute(&format!(
                "CREATE TABLE events AS SELECT id, label FROM read_csv('{}', header = true)",
                csv.display()
            ))
            .await
            .unwrap(),
    )
    .await;

    overwrite_with_valid_parquet(&only_segment(&database));
    let Err(error) = session.execute("SELECT count(*) FROM events").await else {
        panic!("a modified native segment was accepted")
    };
    assert!(matches!(error, Error::NativeStorage { .. }), "{error}");
}

#[tokio::test]
async fn reopen_rejects_a_corrupt_inherited_snapshot_marker() {
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("native");
    let first = directory.path().join("first.csv");
    let appended = directory.path().join("appended.csv");
    write_csv(&first, 0, 4_000);
    write_csv(&appended, 4_000, 2_000);
    let config = EngineConfig::builder()
        .spill_directory(directory.path().join("spill"))
        .build();
    let engine = Engine::open(&database, config.clone()).unwrap();
    let session = engine.session();
    collect(
        session
            .execute(&format!(
                "CREATE TABLE events AS SELECT id, label FROM read_csv('{}', header = true)",
                first.display()
            ))
            .await
            .unwrap(),
    )
    .await;
    collect(
        session
            .execute(&format!(
                "INSERT INTO events SELECT id, label FROM read_csv('{}', header = true)",
                appended.display()
            ))
            .await
            .unwrap(),
    )
    .await;
    drop(session);
    drop(engine);

    let ancestor = snapshot_directories(&database)
        .into_iter()
        .find(|snapshot| !snapshot.join("manifest.json").exists())
        .expect("append must retain its segment owner directory");
    std::fs::write(ancestor.join(".rustdb-snapshot"), b"{broken").unwrap();
    let Err(error) = Engine::open(&database, config) else {
        panic!("a corrupt inherited owner marker was accepted")
    };
    assert!(matches!(error, Error::NativeStorage { .. }));
}

fn overwrite_with_valid_parquet(path: &std::path::Path) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("label", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![99_i64])),
            Arc::new(StringArray::from(vec!["tampered"])),
        ],
    )
    .unwrap();
    let mut writer = ArrowWriter::try_new(File::create(path).unwrap(), schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
}

fn only_segment(database: &std::path::Path) -> std::path::PathBuf {
    let snapshot = snapshot_directories(database).into_iter().next().unwrap();
    std::fs::read_dir(snapshot.join("segments"))
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .path()
}

fn all_segments(database: &std::path::Path) -> Vec<std::path::PathBuf> {
    let mut segments = snapshot_directories(database)
        .into_iter()
        .flat_map(|snapshot| {
            std::fs::read_dir(snapshot.join("segments"))
                .unwrap()
                .map(|entry| entry.unwrap().path())
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    segments.sort();
    segments
}

fn snapshot_directories(database: &std::path::Path) -> Vec<std::path::PathBuf> {
    let table = std::fs::read_dir(database.join("tables"))
        .unwrap()
        .next()
        .unwrap()
        .unwrap();
    std::fs::read_dir(table.path().join("snapshots"))
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .collect()
}

fn write_csv(path: &std::path::Path, start: i64, rows: i64) {
    let mut contents = String::from("id,label\n");
    for id in start..start + rows {
        contents.push_str(&format!("{id},value-{id:08}\n"));
    }
    std::fs::write(path, contents).unwrap();
}

async fn create_events(session: &Session, csv: &std::path::Path) {
    collect(
        session
            .execute(&format!(
                "CREATE TABLE events AS SELECT id, label FROM read_csv('{}', header = true)",
                csv.display()
            ))
            .await
            .unwrap(),
    )
    .await;
}

async fn query_events_metrics(session: &Session) -> QueryMetricsSnapshot {
    let result = session
        .execute("SELECT count(*) FROM events")
        .await
        .unwrap();
    let metrics = result.metrics();
    collect(result).await;
    metrics.snapshot()
}
