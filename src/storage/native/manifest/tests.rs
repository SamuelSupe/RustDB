use std::{collections::BTreeMap, fs};

use uuid::Uuid;

use super::{
    CatalogState, TableReference, commit, ensure_generation, load, recover_future_generations,
};
use crate::{
    Error,
    storage::native::{MARKER_FILE, NativeDatabase, marker},
};

#[test]
fn commits_and_reloads_an_immutable_catalog_generation() {
    let directory = tempfile::tempdir().unwrap();
    let database = NativeDatabase::open(directory.path().join("database")).unwrap();
    let database_id = database_id(&database);
    let initial = load(database.path(), &database_id).unwrap();
    assert_eq!(initial.generation(), 0);
    assert!(initial.tables().is_empty());

    let table = table_reference();
    let tables = BTreeMap::from([("orders".to_owned(), table.clone())]);
    let committed = commit(database.path(), &database_id, 0, tables).unwrap();

    assert_eq!(committed.generation(), 1);
    assert_eq!(committed.tables().get("orders"), Some(&table));
    assert_eq!(load(database.path(), &database_id).unwrap(), committed);
}

#[test]
fn stale_commit_does_not_change_current_generation() {
    let directory = tempfile::tempdir().unwrap();
    let database = NativeDatabase::open(directory.path().join("database")).unwrap();
    let database_id = database_id(&database);
    let first = BTreeMap::from([("first".to_owned(), table_reference())]);
    commit(database.path(), &database_id, 0, first.clone()).unwrap();

    let error = commit(
        database.path(),
        &database_id,
        0,
        BTreeMap::from([("second".to_owned(), table_reference())]),
    )
    .unwrap_err();
    assert!(matches!(error, Error::Catalog(_)));
    assert_eq!(
        load(database.path(), &database_id).unwrap().tables(),
        &first
    );
}

#[test]
fn immutable_generation_cannot_be_replaced_and_checksum_is_verified() {
    let directory = tempfile::tempdir().unwrap();
    let database = NativeDatabase::open(directory.path().join("database")).unwrap();
    let database_id = database_id(&database);
    let tables = BTreeMap::from([("orders".to_owned(), table_reference())]);
    commit(database.path(), &database_id, 0, tables).unwrap();

    let conflicting = CatalogState {
        database_id: database_id.clone(),
        format_version: super::FORMAT_VERSION,
        generation: 1,
        tables: BTreeMap::from([("other".to_owned(), table_reference())]),
    };
    assert!(matches!(
        ensure_generation(database.path(), &conflicting).unwrap_err(),
        Error::NativeStorage { .. }
    ));

    let manifest = database
        .path()
        .join("catalog")
        .join("generations")
        .join("00000000000000000001.json");
    let mut envelope: serde_json::Value =
        serde_json::from_slice(&fs::read(&manifest).unwrap()).unwrap();
    envelope["sha256"] = serde_json::Value::String("00".to_owned());
    fs::write(&manifest, serde_json::to_vec(&envelope).unwrap()).unwrap();
    assert!(matches!(
        load(database.path(), &database_id).unwrap_err(),
        Error::NativeStorage { .. }
    ));
}

#[test]
fn recovery_removes_a_valid_unpublished_future_generation() {
    let directory = tempfile::tempdir().unwrap();
    let database = NativeDatabase::open(directory.path().join("database")).unwrap();
    let database_id = database_id(&database);
    let future = CatalogState {
        database_id: database_id.clone(),
        format_version: super::FORMAT_VERSION,
        generation: 1,
        tables: BTreeMap::new(),
    };
    ensure_generation(database.path(), &future).unwrap();
    let path = database
        .path()
        .join("catalog/generations/00000000000000000001.json");
    assert!(path.exists());

    recover_future_generations(database.path(), &database_id).unwrap();
    assert!(!path.exists());
    assert_eq!(load(database.path(), &database_id).unwrap().generation(), 0);
}

fn database_id(database: &NativeDatabase) -> String {
    marker::read(&database.path().join(MARKER_FILE))
        .unwrap()
        .database_id()
        .to_owned()
}

fn table_reference() -> TableReference {
    TableReference::new(
        Uuid::new_v4().to_string(),
        1,
        Uuid::new_v4().to_string(),
        "11".repeat(32),
    )
}
