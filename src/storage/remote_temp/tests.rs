use std::time::{Duration, SystemTime};

use super::{RemoteTempDir, RemoteTempKind, owner_content, owner_path, scavenge_at};

const TTL: Duration = Duration::from_secs(60);

#[test]
fn owned_directory_and_marker_are_private_and_explicit_cleanup_removes_both() {
    let root = tempfile::tempdir().unwrap();
    let temporary = RemoteTempDir::create(root.path(), RemoteTempKind::Backup).unwrap();
    let path = temporary.path().to_owned();
    let marker = owner_path(&path);

    assert!(path.exists());
    assert!(marker.exists());
    let name = path.file_name().unwrap().to_str().unwrap();
    let id = name
        .strip_prefix("rustdb-remote-backup-")
        .and_then(|id| uuid::Uuid::parse_str(id).ok())
        .unwrap();
    assert_eq!(
        std::fs::read_to_string(&marker).unwrap(),
        owner_content(RemoteTempKind::Backup, id)
    );
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(
            std::fs::metadata(&path).unwrap().permissions().mode() & 0o777,
            0o700
        );
        assert_eq!(
            std::fs::metadata(&marker).unwrap().permissions().mode() & 0o777,
            0o600
        );
    }

    temporary.cleanup().unwrap();
    assert!(!path.exists());
    assert!(!marker.exists());
}

#[test]
fn scavenger_removes_only_old_crash_orphans_and_preserves_fresh_or_active() {
    let root = tempfile::tempdir().unwrap();
    let crashed = RemoteTempDir::create(root.path(), RemoteTempKind::Backup).unwrap();
    let crashed_path = crashed.path().to_owned();
    let modified = std::fs::metadata(owner_path(&crashed_path))
        .unwrap()
        .modified()
        .unwrap();
    drop(crashed);

    let fresh = scavenge_at(root.path(), TTL, modified + TTL / 2).unwrap();
    assert_eq!(fresh.removed, 0);
    assert!(crashed_path.exists());

    let active = RemoteTempDir::create(root.path(), RemoteTempKind::Restore).unwrap();
    let active_path = active.path().to_owned();
    let old = scavenge_at(root.path(), TTL, modified + TTL + TTL).unwrap();
    assert_eq!(old.removed, 1);
    assert!(!crashed_path.exists());
    assert!(active_path.exists());

    active.cleanup().unwrap();
}

#[test]
fn scavenger_preserves_forged_missing_marker_unknown_and_symlink_candidates() {
    let root = tempfile::tempdir().unwrap();
    let future = SystemTime::now() + TTL + TTL;
    let forged = candidate(root.path(), "backup");
    private_dir(&forged);
    private_file(&owner_path(&forged), b"forged\n");

    let missing = candidate(root.path(), "restore");
    private_dir(&missing);

    let unknown = root.path().join("do-not-delete");
    private_dir(&unknown);
    std::fs::write(unknown.join("important"), b"keep").unwrap();

    #[cfg(unix)]
    let symlink = {
        use std::os::unix::fs::symlink;
        let target = root.path().join("symlink-target");
        private_dir(&target);
        let link = candidate(root.path(), "backup");
        symlink(&target, &link).unwrap();
        Some((link, target))
    };

    let report = scavenge_at(root.path(), TTL, future).unwrap();
    assert_eq!(report.removed, 0);
    assert!(forged.exists());
    assert!(missing.exists());
    assert_eq!(std::fs::read(unknown.join("important")).unwrap(), b"keep");
    #[cfg(unix)]
    {
        let (link, target) = symlink.unwrap();
        assert!(
            std::fs::symlink_metadata(link)
                .unwrap()
                .file_type()
                .is_symlink()
        );
        assert!(target.exists());
    }
}

#[test]
fn scavenger_removes_only_a_valid_old_owner_when_its_directory_is_absent() {
    let root = tempfile::tempdir().unwrap();
    let temporary = RemoteTempDir::create(root.path(), RemoteTempKind::Restore).unwrap();
    let path = temporary.path().to_owned();
    let marker = owner_path(&path);
    let modified = std::fs::metadata(&marker).unwrap().modified().unwrap();
    drop(temporary);
    std::fs::remove_dir_all(&path).unwrap();

    let report = scavenge_at(root.path(), TTL, modified + TTL + TTL).unwrap();

    assert_eq!(report.removed, 1);
    assert!(!marker.exists());
}

#[test]
fn scavenger_preserves_noncanonical_uuid_directory_names() {
    let root = tempfile::tempdir().unwrap();
    let id = uuid::Uuid::new_v4();
    let path = root
        .path()
        .join(format!("rustdb-remote-backup-{}", id.simple()));
    private_dir(&path);
    private_file(
        &owner_path(&path),
        owner_content(RemoteTempKind::Backup, id).as_bytes(),
    );

    let report = scavenge_at(root.path(), TTL, SystemTime::now() + TTL + TTL).unwrap();

    assert_eq!(report.removed, 0);
    assert!(path.exists());
}

fn candidate(root: &std::path::Path, kind: &str) -> std::path::PathBuf {
    root.join(format!("rustdb-remote-{kind}-{}", uuid::Uuid::new_v4()))
}

fn private_dir(path: &std::path::Path) {
    let mut builder = std::fs::DirBuilder::new();
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        builder.mode(0o700);
    }
    builder.create(path).unwrap();
}

fn private_file(path: &std::path::Path, contents: &[u8]) {
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options.open(path).unwrap();
    std::io::Write::write_all(&mut file, contents).unwrap();
}
