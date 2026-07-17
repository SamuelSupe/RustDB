use std::fs;

use uuid::Uuid;

use super::{INIT_FILE, NativeDatabase, lock};
use crate::Error;

#[test]
fn repairs_a_lock_file_interrupted_before_its_marker_was_synced() {
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("database");
    fs::create_dir(&database).unwrap();
    fs::write(database.join(".lock"), b"").unwrap();

    drop(NativeDatabase::open(&database).unwrap());
    drop(NativeDatabase::open(&database).unwrap());
}

#[test]
fn removes_a_strict_orphaned_initialization_temporary() {
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("database");
    fs::create_dir(&database).unwrap();
    drop(lock::DatabaseLock::acquire(&database).unwrap());
    let temporary = initialization_temporary(&database);
    fs::write(&temporary, b"partial marker").unwrap();

    drop(NativeDatabase::open(&database).unwrap());
    assert!(!temporary.exists());
}

#[test]
fn does_not_repair_a_corrupt_lock_in_an_initialized_database() {
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("database");
    drop(NativeDatabase::open(&database).unwrap());
    fs::write(database.join(".lock"), b"").unwrap();

    assert!(matches!(
        NativeDatabase::open(&database).unwrap_err(),
        Error::NativeStorage { .. }
    ));
    assert!(fs::read(database.join(".lock")).unwrap().is_empty());
}

#[test]
fn rejects_an_oversized_lock_marker_without_reading_it_unbounded() {
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("database");
    drop(NativeDatabase::open(&database).unwrap());
    fs::write(database.join(".lock"), vec![b'x'; 64]).unwrap();

    assert!(matches!(
        NativeDatabase::open(&database).unwrap_err(),
        Error::NativeStorage { message, .. }
            if message.contains("database lock marker exceeds")
    ));
}

#[test]
fn preserves_strict_temporary_names_in_an_unknown_directory() {
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("database");
    fs::create_dir(&database).unwrap();
    let unknown = database.join("keep-me");
    let temporary = initialization_temporary(&database);
    fs::write(&unknown, b"unknown").unwrap();
    fs::write(&temporary, b"not owned without an initialization-only root").unwrap();

    assert!(NativeDatabase::open(&database).is_err());
    assert_eq!(fs::read(unknown).unwrap(), b"unknown");
    assert!(temporary.exists());
}

fn initialization_temporary(database: &std::path::Path) -> std::path::PathBuf {
    database.join(format!(".{INIT_FILE}.{}.tmp", Uuid::new_v4()))
}
