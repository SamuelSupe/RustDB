use std::fs;

use arrow::datatypes::{DataType, Field, Schema};

use super::*;
use crate::storage::native::{NativeWriteMode, manifest};

#[test]
fn explicitly_migrates_legacy_database_and_keeps_a_v07_backup() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("database");
    drop(NativeDatabase::open(&path).unwrap());
    make_legacy(&path);

    let legacy = NativeDatabase::open(&path).unwrap();
    let schema = std::sync::Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    assert!(
        legacy
            .plan_write("blocked", NativeWriteMode::Create, 0, schema, 1)
            .is_err()
    );
    drop(legacy);

    let migration = migrate(&path).unwrap();
    assert_eq!(migration.from_version, LEGACY_DATABASE_FORMAT_VERSION);
    assert_eq!(migration.to_version, DATABASE_FORMAT_VERSION);
    let backup = migration.backup_path.unwrap();
    assert!(backup.is_dir());
    assert_eq!(
        marker::read(&backup.join(MARKER_FILE)).unwrap().version(),
        LEGACY_DATABASE_FORMAT_VERSION
    );
    assert_eq!(
        marker::read(&path.join(MARKER_FILE)).unwrap().version(),
        DATABASE_FORMAT_VERSION
    );
    drop(NativeDatabase::open(&path).unwrap());

    let repeated = migrate(&path).unwrap();
    assert_eq!(repeated.from_version, DATABASE_FORMAT_VERSION);
    assert!(repeated.backup_path.is_none());
}

#[test]
fn mismatched_existing_backup_does_not_modify_the_legacy_source() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("database");
    drop(NativeDatabase::open(&path).unwrap());
    make_legacy(&path);
    drop(NativeDatabase::open(directory.path().join("database.v0.7-backup")).unwrap());

    assert!(migrate(&path).is_err());
    assert_eq!(
        marker::read(&path.join(MARKER_FILE)).unwrap().version(),
        LEGACY_DATABASE_FORMAT_VERSION
    );
}

#[test]
fn stale_same_database_backup_does_not_modify_the_legacy_source() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("database");
    drop(NativeDatabase::open(&path).unwrap());
    make_legacy(&path);

    let database = NativeDatabase::open(&path).unwrap();
    let backup = default_backup_path(database.path()).unwrap();
    ensure_backup(&database, &backup).unwrap();
    let database_id = database.database_id().to_owned();
    let generation = database.catalog_generation();
    drop(database);

    manifest::commit(
        &path,
        &database_id,
        generation,
        std::collections::BTreeMap::new(),
    )
    .unwrap();

    let error = migrate(&path).unwrap_err();
    assert!(
        error
            .to_string()
            .contains("not the matching v0.7 catalog snapshot"),
        "unexpected migration error: {error}"
    );
    assert_eq!(
        marker::read(&path.join(MARKER_FILE)).unwrap().version(),
        LEGACY_DATABASE_FORMAT_VERSION
    );
    assert_eq!(NativeDatabase::open(&path).unwrap().catalog_generation(), 1);
}

#[test]
fn migration_resumes_after_each_durable_pre_marker_boundary() {
    for boundary in ["backup", "empty-wal", "upgraded-marker"] {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("database");
        drop(NativeDatabase::open(&path).unwrap());
        make_legacy(&path);

        let database = NativeDatabase::open(&path).unwrap();
        let backup = default_backup_path(database.path()).unwrap();
        ensure_backup(&database, &backup).unwrap();
        if boundary != "backup" {
            ensure_empty_wal(database.path()).unwrap();
        }
        if boundary == "upgraded-marker" {
            marker::upgrade_legacy(
                &database.path().join(MARKER_FILE),
                &uuid::Uuid::new_v4().to_string(),
            )
            .unwrap();
        }
        drop(database);

        let migration = migrate(&path).unwrap();
        assert_eq!(
            marker::read(&path.join(MARKER_FILE)).unwrap().version(),
            DATABASE_FORMAT_VERSION,
            "migration did not recover after {boundary}"
        );
        assert!(path.join("wal").is_dir());
        assert!(backup.is_dir());
        if boundary == "upgraded-marker" {
            assert_eq!(migration.from_version, DATABASE_FORMAT_VERSION);
        } else {
            assert_eq!(migration.from_version, LEGACY_DATABASE_FORMAT_VERSION);
        }
    }
}

#[test]
fn migration_failure_before_marker_keeps_the_source_retryable() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("database");
    drop(NativeDatabase::open(&path).unwrap());
    make_legacy(&path);
    fs::create_dir(path.join("wal")).unwrap();
    fs::write(path.join("wal/unexpected"), b"keep").unwrap();

    assert!(migrate(&path).is_err());
    assert_eq!(
        marker::read(&path.join(MARKER_FILE)).unwrap().version(),
        LEGACY_DATABASE_FORMAT_VERSION
    );
    assert_eq!(fs::read(path.join("wal/unexpected")).unwrap(), b"keep");

    fs::remove_file(path.join("wal/unexpected")).unwrap();
    let migration = migrate(&path).unwrap();
    assert_eq!(migration.from_version, LEGACY_DATABASE_FORMAT_VERSION);
    assert_eq!(
        marker::read(&path.join(MARKER_FILE)).unwrap().version(),
        DATABASE_FORMAT_VERSION
    );
}

fn make_legacy(path: &Path) {
    let marker_path = path.join(MARKER_FILE);
    let mut value: serde_json::Value =
        serde_json::from_slice(&fs::read(&marker_path).unwrap()).unwrap();
    value["version"] = serde_json::Value::from(LEGACY_DATABASE_FORMAT_VERSION);
    fs::write(&marker_path, serde_json::to_vec(&value).unwrap()).unwrap();
    fs::remove_dir(path.join("wal")).unwrap();
}
