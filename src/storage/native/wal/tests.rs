use std::fs;

use uuid::Uuid;

use super::*;

#[test]
fn persists_and_reloads_checked_transaction_records() {
    let directory = tempfile::tempdir().unwrap();
    fs::create_dir(directory.path().join("wal")).unwrap();
    let database_id = Uuid::new_v4().to_string();
    let transaction_id = Uuid::new_v4().to_string();

    let wal = Wal::open(directory.path(), &database_id).unwrap();
    wal.begin(&transaction_id, 7).unwrap();
    wal.commit_catalog(&transaction_id, 7, 8).unwrap();
    drop(wal);

    let reopened = Wal::open(directory.path(), &database_id).unwrap();
    assert_eq!(
        reopened.catalog_commits(),
        vec![CatalogCommit {
            transaction_id,
            expected_generation: 7,
            generation: 8,
        }]
    );
}

#[test]
fn reconciles_an_identical_record_published_before_the_in_memory_state_advanced() {
    let directory = tempfile::tempdir().unwrap();
    fs::create_dir(directory.path().join("wal")).unwrap();
    let database_id = Uuid::new_v4().to_string();
    let transaction_id = Uuid::new_v4().to_string();
    let wal = Wal::open(directory.path(), &database_id).unwrap();
    let record = Record::new(
        &database_id,
        1,
        &transaction_id,
        RecordKind::Begin {
            snapshot_generation: 0,
            mode: TransactionMode::ReadWrite,
        },
    );
    record::write(&record::path(&directory.path().join("wal"), 1), &record).unwrap();

    wal.begin(&transaction_id, 0).unwrap();
    assert_eq!(wal.stats(), (2, 1, 1));
    wal.abort(&transaction_id).unwrap();
    assert_eq!(wal.stats(), (3, 1, 0));
}

#[test]
fn rejects_corruption_and_invalid_state_transitions() {
    let directory = tempfile::tempdir().unwrap();
    fs::create_dir(directory.path().join("wal")).unwrap();
    let database_id = Uuid::new_v4().to_string();
    let transaction_id = Uuid::new_v4().to_string();
    let wal = Wal::open(directory.path(), &database_id).unwrap();

    assert!(wal.commit_catalog(&transaction_id, 0, 1).is_err());
    wal.begin(&transaction_id, 0).unwrap();
    assert!(wal.begin(&transaction_id, 0).is_err());
    drop(wal);

    let path = directory.path().join("wal/00000000000000000001.wal");
    let mut bytes = fs::read(&path).unwrap();
    let index = bytes.iter().position(|byte| *byte == b'0').unwrap();
    bytes[index] = b'9';
    fs::write(path, bytes).unwrap();
    assert!(Wal::open(directory.path(), &database_id).is_err());
}

#[test]
fn replays_a_durable_catalog_commit_intent_on_open() {
    let directory = tempfile::tempdir().unwrap();
    let database_path = directory.path().join("database");
    let database = super::super::NativeDatabase::open(&database_path).unwrap();
    let database_id = database.database_id().to_owned();
    let transaction_id = Uuid::new_v4().to_string();
    let wal = database.wal().unwrap();

    wal.begin(&transaction_id, 0).unwrap();
    super::super::manifest::prepare_commit(
        database.path(),
        &database_id,
        0,
        Default::default(),
        &transaction_id,
    )
    .unwrap();
    wal.commit_catalog(&transaction_id, 0, 1).unwrap();
    drop(wal);
    drop(database);

    let recovered = super::super::NativeDatabase::open(&database_path).unwrap();
    assert_eq!(recovered.catalog_generation(), 1);
    assert_eq!(
        super::super::manifest::load(recovered.path(), &database_id)
            .unwrap()
            .transaction_id(),
        Some(transaction_id.as_str())
    );
}

#[test]
fn removes_a_prepared_generation_without_a_commit_intent() {
    let directory = tempfile::tempdir().unwrap();
    let database_path = directory.path().join("database");
    let database = super::super::NativeDatabase::open(&database_path).unwrap();
    let database_id = database.database_id().to_owned();
    let transaction_id = Uuid::new_v4().to_string();
    let wal = database.wal().unwrap();

    wal.begin(&transaction_id, 0).unwrap();
    super::super::manifest::prepare_commit(
        database.path(),
        &database_id,
        0,
        Default::default(),
        &transaction_id,
    )
    .unwrap();
    drop(wal);
    drop(database);

    let recovered = super::super::NativeDatabase::open(&database_path).unwrap();
    assert_eq!(recovered.catalog_generation(), 0);
    assert!(
        !recovered
            .path()
            .join("catalog/generations/00000000000000000001.json")
            .exists()
    );
}

#[test]
fn database_reopen_persists_abort_for_a_begin_only_transaction() {
    let directory = tempfile::tempdir().unwrap();
    let database_path = directory.path().join("database");
    let database = super::super::NativeDatabase::open(&database_path).unwrap();
    let transaction_id = Uuid::new_v4().to_string();
    database.wal().unwrap().begin(&transaction_id, 0).unwrap();
    drop(database);

    let recovered = super::super::NativeDatabase::open(&database_path).unwrap();
    assert_eq!(recovered.wal().unwrap().stats(), (3, 1, 0));
    drop(recovered);

    let reopened = super::super::NativeDatabase::open(&database_path).unwrap();
    assert_eq!(reopened.wal().unwrap().stats(), (3, 1, 0));
    assert_eq!(reopened.checkpoint().unwrap(), 2);
}
