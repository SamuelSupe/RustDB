use std::{
    fs,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
};

use super::*;
use crate::Error;

#[derive(Debug, Eq, PartialEq)]
struct TreeEntry {
    relative: PathBuf,
    mode: u32,
    contents: Option<Vec<u8>>,
}

#[test]
fn migrate_is_a_no_op_for_the_current_beta_format() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("database");
    drop(NativeDatabase::open(&path).unwrap());

    let before = snapshot(&path);
    let migration = migrate(&path).unwrap();

    assert_eq!(migration.from_version, format::CURRENT_DATABASE_VERSION);
    assert_eq!(migration.to_version, format::CURRENT_DATABASE_VERSION);
    assert!(migration.backup_path.is_none());
    assert_eq!(snapshot(&path), before);
}

#[test]
fn alpha_formats_are_rejected_without_modifying_contents_or_permissions() {
    for version in [1, 2] {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join(format!("database-v{version}"));
        drop(NativeDatabase::open(&path).unwrap());
        rewrite_marker_version(&path, version, false);
        add_recovery_candidate(&path);
        fs::set_permissions(&path, fs::Permissions::from_mode(0o755)).unwrap();
        let before = snapshot(&path);

        assert_unsupported(NativeDatabase::open(&path).unwrap_err(), version, true);
        assert_eq!(snapshot(&path), before, "open changed alpha v{version}");

        assert_unsupported(crate::Engine::migrate(&path).unwrap_err(), version, true);
        assert_eq!(snapshot(&path), before, "migrate changed alpha v{version}");
    }
}

#[test]
fn future_format_with_unknown_fields_is_rejected_without_modification() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("future-database");
    drop(NativeDatabase::open(&path).unwrap());
    let future = format::CURRENT_DATABASE_VERSION + 1;
    rewrite_marker_version(&path, future, true);
    add_recovery_candidate(&path);
    fs::set_permissions(&path, fs::Permissions::from_mode(0o751)).unwrap();
    let before = snapshot(&path);

    assert_unsupported(NativeDatabase::open(&path).unwrap_err(), future, false);
    assert_eq!(snapshot(&path), before);
}

fn assert_unsupported(error: Error, found_version: u32, alpha: bool) {
    assert!(
        matches!(
            error,
            Error::NativeFormatUnsupported {
                found_version: found,
                current_version: format::CURRENT_DATABASE_VERSION,
                alpha: actual_alpha,
                ..
            } if found == found_version && actual_alpha == alpha
        ),
        "unexpected format error: {error}"
    );
}

fn rewrite_marker_version(path: &Path, version: u32, future_field: bool) {
    let marker_path = path.join(super::super::MARKER_FILE);
    let mut value: serde_json::Value =
        serde_json::from_slice(&fs::read(&marker_path).unwrap()).unwrap();
    value["version"] = serde_json::Value::from(version);
    if future_field {
        value["future_field"] = serde_json::Value::Bool(true);
    }
    fs::write(marker_path, serde_json::to_vec_pretty(&value).unwrap()).unwrap();
}

fn add_recovery_candidate(path: &Path) {
    let temporary = path.join(format!(".rustdb.{}.tmp", uuid::Uuid::new_v4()));
    fs::write(temporary, b"must remain untouched").unwrap();
}

fn snapshot(root: &Path) -> Vec<TreeEntry> {
    let mut entries = Vec::new();
    snapshot_path(root, root, &mut entries);
    entries.sort_by(|left, right| left.relative.cmp(&right.relative));
    entries
}

fn snapshot_path(root: &Path, path: &Path, entries: &mut Vec<TreeEntry>) {
    let metadata = fs::symlink_metadata(path).unwrap();
    let relative = path.strip_prefix(root).unwrap().to_path_buf();
    let contents = metadata.is_file().then(|| fs::read(path).unwrap());
    entries.push(TreeEntry {
        relative,
        mode: metadata.permissions().mode() & 0o777,
        contents,
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
