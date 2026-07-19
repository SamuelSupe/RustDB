use std::{
    fs,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    sync::Arc,
};

use arrow::{
    array::Int64Array,
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};

use super::database;
use crate::storage::native::{NativeDatabase, NativeWriteMode};

#[derive(Debug, Eq, PartialEq)]
struct TreeEntry {
    relative: PathBuf,
    mode: u32,
    contents: Option<Vec<u8>>,
}

#[test]
fn check_is_strictly_read_only_even_while_database_is_locked() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("database");
    let database_handle = NativeDatabase::open(&path).unwrap();
    create_table(&database_handle, "events", 1);
    let recovery_candidate = path.join(format!(".rustdb.{}.tmp", uuid::Uuid::new_v4()));
    fs::write(&recovery_candidate, b"leave this file untouched").unwrap();
    fs::set_permissions(&path, fs::Permissions::from_mode(0o755)).unwrap();
    let before = snapshot(&path);

    let report = database(&path).unwrap();

    assert!(report.is_ok(), "unexpected errors: {:?}", report.errors());
    assert_eq!(report.checked_tables(), 1);
    assert_eq!(report.checked_snapshots(), 1);
    assert!(report.checked_files() >= 7);
    assert!(report.checked_bytes() > 0);
    assert!(
        report
            .warnings()
            .iter()
            .any(|issue| issue.code() == "native.insecure_permissions")
    );
    assert_eq!(serde_json::to_value(&report).unwrap()["ok"], true);
    assert_eq!(snapshot(&path), before);
    assert_eq!(
        fs::read(recovery_candidate).unwrap(),
        b"leave this file untouched"
    );
    drop(database_handle);
}

#[test]
fn check_accumulates_corruption_across_tables() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("database");
    let database_handle = NativeDatabase::open(&path).unwrap();
    let first = create_table(&database_handle, "first_table", 1);
    let second = create_table(&database_handle, "second_table", 2);
    drop(database_handle);

    let mut bytes = fs::read(&first).unwrap();
    let index = bytes.len() / 2;
    bytes[index] ^= 0x5a;
    fs::write(&first, bytes).unwrap();
    fs::remove_file(&second).unwrap();

    let report = database(&path).unwrap();

    assert!(!report.is_ok());
    assert_eq!(report.checked_tables(), 2);
    assert_eq!(report.errors().len(), 2, "issues: {:?}", report.errors());
    assert!(
        report
            .errors()
            .iter()
            .any(|issue| issue.message().contains("first_table")
                && issue.message().contains("checksum mismatch"))
    );
    assert!(
        report
            .errors()
            .iter()
            .any(|issue| issue.message().contains("second_table"))
    );
}

#[test]
fn check_rejects_alpha_epoch_without_changing_the_tree() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("database");
    drop(NativeDatabase::open(&path).unwrap());
    let marker = path.join(super::super::MARKER_FILE);
    let mut value: serde_json::Value = serde_json::from_slice(&fs::read(&marker).unwrap()).unwrap();
    value["version"] = serde_json::Value::from(2);
    fs::write(&marker, serde_json::to_vec_pretty(&value).unwrap()).unwrap();
    fs::set_permissions(&path, fs::Permissions::from_mode(0o751)).unwrap();
    let before = snapshot(&path);

    let report = database(&path).unwrap();

    assert!(!report.is_ok());
    assert_eq!(report.errors().len(), 1);
    assert_eq!(report.errors()[0].code(), "native.format_unsupported");
    assert_eq!(snapshot(&path), before);
}

fn create_table(database: &NativeDatabase, name: &str, value: i64) -> PathBuf {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from(vec![value]))],
    )
    .unwrap();
    let plan = database
        .plan_write(
            name,
            NativeWriteMode::Create,
            database.catalog_generation(),
            schema,
            1024 * 1024,
        )
        .unwrap();
    let mut writer = database.start_write(plan).unwrap();
    writer.write_batch(&batch).unwrap();
    let prepared = writer.finish().unwrap();
    database.commit_write(prepared).unwrap();
    database
        .table_snapshot(name)
        .unwrap()
        .segment_paths(database.path())
        .into_iter()
        .next()
        .unwrap()
}

fn snapshot(root: &Path) -> Vec<TreeEntry> {
    let mut entries = Vec::new();
    snapshot_path(root, root, &mut entries);
    entries.sort_by(|left, right| left.relative.cmp(&right.relative));
    entries
}

fn snapshot_path(root: &Path, path: &Path, entries: &mut Vec<TreeEntry>) {
    let metadata = fs::symlink_metadata(path).unwrap();
    entries.push(TreeEntry {
        relative: path.strip_prefix(root).unwrap().to_path_buf(),
        mode: metadata.permissions().mode() & 0o777,
        contents: metadata.is_file().then(|| fs::read(path).unwrap()),
    });
    if metadata.is_dir() {
        let mut children = fs::read_dir(path)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .collect::<Vec<_>>();
        children.sort();
        for child in children {
            snapshot_path(root, &child, entries);
        }
    }
}
