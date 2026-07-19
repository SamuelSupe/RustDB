use std::{
    fs::{self, File, FileTimes},
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, SystemTime},
};

use arrow::{
    array::Int64Array,
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};

use super::{NativeRepairAction, apply, plan};
use crate::{
    Error,
    storage::native::{NativeDatabase, NativeWriteMode, StagedSnapshot},
};

#[derive(Debug, Eq, PartialEq)]
struct TreeEntry {
    relative: PathBuf,
    mode: u32,
    contents: Option<Vec<u8>>,
}

#[test]
fn dry_run_plans_permissions_and_owned_staging_without_changes() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("database");
    let database = NativeDatabase::open(&path).unwrap();
    let staged = StagedSnapshot::begin(database.path(), database.database_id()).unwrap();
    let staged_path = staged.path().to_owned();
    let fresh = StagedSnapshot::begin(database.path(), database.database_id()).unwrap();
    let fresh_path = fresh.path().to_owned();
    std::mem::forget(staged);
    std::mem::forget(fresh);
    drop(database);
    age_tree(&staged_path);
    fs::set_permissions(&path, fs::Permissions::from_mode(0o755)).unwrap();
    fs::set_permissions(
        path.join(super::super::MARKER_FILE),
        fs::Permissions::from_mode(0o644),
    )
    .unwrap();
    let before = snapshot(&path);

    let repair = plan(&path).unwrap();

    assert!(repair.is_applicable(), "blockers: {:?}", repair.blockers());
    assert!(repair.actions().iter().any(|action| matches!(
        action,
        NativeRepairAction::SetPermissions { path: target, mode: 0o700 } if target == &path
    )));
    assert!(repair.actions().iter().any(|action| matches!(
        action,
        NativeRepairAction::RemoveOwnedStaging { path: target, .. } if target == &staged_path
    )));
    assert!(!repair.actions().iter().any(|action| matches!(
        action,
        NativeRepairAction::RemoveOwnedStaging { path: target, .. } if target == &fresh_path
    )));
    assert_eq!(snapshot(&path), before);
    assert!(staged_path.exists());
    assert!(fresh_path.exists());
}

#[test]
fn apply_recovers_current_from_the_highest_proven_generation() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("database");
    let database = NativeDatabase::open(&path).unwrap();
    create_table(&database, "events", 7);
    let generation = database.catalog_generation();
    drop(database);
    let current = path.join("catalog").join("CURRENT");
    fs::write(&current, b"not-a-generation\n").unwrap();

    let report = apply(&path).unwrap();

    assert!(report.succeeded(), "after: {:?}", report.after().errors());
    assert!(report.applied_actions() >= 1);
    assert_eq!(
        fs::read_to_string(&current).unwrap(),
        format!("{generation}\n")
    );
    assert!(report.plan().actions().iter().any(|action| matches!(
        action,
        NativeRepairAction::RestoreCurrent { generation: planned, .. } if *planned == generation
    )));
    let backup = report.backup_path().unwrap();
    assert!(backup.join("manifest.json").is_file());
    assert!(backup.join("manifest.sha256").is_file());
    assert_eq!(
        fs::read(backup.join("catalog").join("CURRENT")).unwrap(),
        b"not-a-generation\n"
    );
    assert!(!contains_extension(backup, "rdbseg"));
}

#[test]
fn apply_refuses_data_corruption_without_backup_or_mutation() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("database");
    let database = NativeDatabase::open(&path).unwrap();
    let segment = create_table(&database, "events", 9);
    drop(database);
    let mut bytes = fs::read(&segment).unwrap();
    let corrupt_at = bytes.len() / 2;
    bytes[corrupt_at] ^= 0x33;
    fs::write(&segment, bytes).unwrap();
    let before = snapshot(&path);

    let repair = plan(&path).unwrap();
    assert!(!repair.is_applicable());
    assert!(repair.actions().is_empty());
    let error = apply(&path).unwrap_err();

    assert!(matches!(error, Error::NativeRepairRefused { .. }));
    assert_eq!(snapshot(&path), before);
    let backup_prefix = format!(
        ".{}.rustdb-repair-",
        path.file_name().unwrap().to_string_lossy()
    );
    assert!(fs::read_dir(directory.path()).unwrap().all(|entry| {
        !entry
            .unwrap()
            .file_name()
            .to_string_lossy()
            .starts_with(&backup_prefix)
    }));
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
    database.commit_write(writer.finish().unwrap()).unwrap();
    database
        .table_snapshot(name)
        .unwrap()
        .segment_paths(database.path())
        .into_iter()
        .next()
        .unwrap()
}

fn contains_extension(root: &Path, extension: &str) -> bool {
    fs::read_dir(root).unwrap().any(|entry| {
        let path = entry.unwrap().path();
        path.extension().and_then(|value| value.to_str()) == Some(extension)
            || (path.is_dir() && contains_extension(&path, extension))
    })
}

fn age_tree(path: &Path) {
    if path.is_dir() {
        let children = fs::read_dir(path)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .collect::<Vec<_>>();
        for child in children {
            age_tree(&child);
        }
    }
    let modified = SystemTime::now() - Duration::from_secs(25 * 60 * 60);
    File::open(path)
        .unwrap()
        .set_times(FileTimes::new().set_modified(modified))
        .unwrap();
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
