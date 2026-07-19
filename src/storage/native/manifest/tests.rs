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
fn legacy_v1_manifest_objects_are_loaded_in_main_without_rekeying() {
    #[derive(serde::Serialize)]
    struct LegacyCatalog<'a> {
        database_id: &'a str,
        format_version: u32,
        generation: u64,
        #[serde(skip_serializing_if = "Option::is_none")]
        transaction_id: Option<&'a str>,
        tables: &'a BTreeMap<String, TableReference>,
        #[serde(skip_serializing_if = "BTreeMap::is_empty")]
        views: &'a BTreeMap<String, super::ViewReference>,
    }

    #[derive(serde::Serialize)]
    struct Envelope<'a> {
        manifest: &'a LegacyCatalog<'a>,
        sha256: &'a str,
    }

    let directory = tempfile::tempdir().unwrap();
    let database = NativeDatabase::open(directory.path().join("database")).unwrap();
    let database_id = database_id(&database);
    let path = database
        .path()
        .join("catalog/generations/00000000000000000000.json");
    let tables = BTreeMap::from([("events".to_owned(), table_reference())]);
    let views = BTreeMap::new();
    let legacy = LegacyCatalog {
        database_id: &database_id,
        format_version: super::LEGACY_FORMAT_VERSION,
        generation: 0,
        transaction_id: None,
        tables: &tables,
        views: &views,
    };
    let checksum = super::io::json_sha256(
        &path,
        &legacy,
        super::MAX_CATALOG_MANIFEST_BYTES,
        "legacy catalog manifest",
    )
    .unwrap();
    let bytes = serde_json::to_vec(&Envelope {
        manifest: &legacy,
        sha256: &checksum,
    })
    .unwrap();
    fs::write(&path, bytes).unwrap();

    let loaded = load(database.path(), &database_id).unwrap();
    assert!(loaded.schemas().contains("main"));
    assert!(loaded.tables().contains_key("events"));
    assert!(!loaded.tables().contains_key("main.events"));
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
        transaction_id: None,
        schemas: std::collections::BTreeSet::from(["main".to_owned()]),
        tables: BTreeMap::from([("other".to_owned(), table_reference())]),
        views: BTreeMap::new(),
        imports: BTreeMap::new(),
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
        transaction_id: None,
        schemas: std::collections::BTreeSet::from(["main".to_owned()]),
        tables: BTreeMap::new(),
        views: BTreeMap::new(),
        imports: BTreeMap::new(),
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
