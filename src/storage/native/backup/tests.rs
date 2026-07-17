use std::{fs, io};

use uuid::Uuid;

use super::{NativeDatabase, copy_snapshot, create, publish, temp};
use crate::Error;

#[test]
fn recovery_removes_only_complete_unlocked_backup_temporaries() {
    let directory = tempfile::tempdir().unwrap();
    let source_path = directory.path().join("source");
    let source = NativeDatabase::open(&source_path).unwrap();
    let temporary = temp::TemporaryBackup::create(directory.path(), Uuid::new_v4()).unwrap();
    let temporary_path = temporary.path().to_owned();
    copy_snapshot(&source, &temporary_path).unwrap();

    temp::recover(directory.path()).unwrap();
    assert!(temporary_path.exists(), "an active backup was removed");

    drop(temporary);
    temp::recover(directory.path()).unwrap();
    assert!(!temporary_path.exists(), "a complete orphan was retained");

    let partial = temp::TemporaryBackup::create(directory.path(), Uuid::new_v4()).unwrap();
    let partial_path = partial.path().to_owned();
    fs::write(partial_path.join("partial-copy"), b"incomplete").unwrap();
    drop(partial);
    temp::recover(directory.path()).unwrap();
    assert!(
        !partial_path.exists(),
        "a recognized partial orphan was retained"
    );

    let interrupted_id = Uuid::new_v4();
    let interrupted = temp::TemporaryBackup::create(directory.path(), interrupted_id).unwrap();
    let interrupted_path = interrupted.path().to_owned();
    drop(interrupted);
    fs::OpenOptions::new()
        .write(true)
        .open(
            directory
                .path()
                .join(format!(".rustdb-backup-{interrupted_id}.owner")),
        )
        .unwrap()
        .set_len(0)
        .unwrap();
    temp::recover(directory.path()).unwrap();
    assert!(
        interrupted_path.exists(),
        "an invalid owner marker authorized recursive deletion"
    );

    let unknown = directory
        .path()
        .join(format!(".rustdb-backup-{}.tmp", Uuid::new_v4()));
    fs::create_dir(&unknown).unwrap();
    fs::write(unknown.join("unknown"), b"keep").unwrap();
    temp::recover(directory.path()).unwrap();
    assert_eq!(fs::read(unknown.join("unknown")).unwrap(), b"keep");
}

#[test]
fn failure_after_rename_reports_an_unknown_outcome() {
    let directory = tempfile::tempdir().unwrap();
    let source = NativeDatabase::open(directory.path().join("source")).unwrap();
    let id = Uuid::new_v4();
    let temporary = temp::TemporaryBackup::create(directory.path(), id).unwrap();
    copy_snapshot(&source, temporary.path()).unwrap();
    drop(NativeDatabase::open(temporary.path()).unwrap());
    let destination = directory.path().join("backup");

    let error = publish(&temporary, &destination, directory.path(), id, |path| {
        Err(Error::io(
            Some(path.to_path_buf()),
            io::Error::other("injected parent sync failure"),
        ))
    })
    .unwrap_err();

    assert!(matches!(error, Error::CommitOutcomeUnknown { .. }));
    assert!(destination.exists());
    assert!(!temporary.path().exists());
}

#[test]
fn rejects_a_destination_inside_the_database_without_creating_its_parent() {
    let directory = tempfile::tempdir().unwrap();
    let source = NativeDatabase::open(directory.path().join("source")).unwrap();
    let parent = source.path().join("backups");

    let error = create(&source, &parent.join("backup")).unwrap_err();

    assert!(matches!(error, Error::InvalidArgument(_)));
    assert!(!parent.exists());
}

#[test]
fn publish_does_not_replace_a_racing_destination() {
    let directory = tempfile::tempdir().unwrap();
    let source = NativeDatabase::open(directory.path().join("source")).unwrap();
    let id = Uuid::new_v4();
    let temporary = temp::TemporaryBackup::create(directory.path(), id).unwrap();
    copy_snapshot(&source, temporary.path()).unwrap();
    drop(NativeDatabase::open(temporary.path()).unwrap());
    let destination = directory.path().join("backup");
    fs::create_dir(&destination).unwrap();
    fs::write(destination.join("racing-owner"), b"keep").unwrap();

    assert!(publish(&temporary, &destination, directory.path(), id, |_| Ok(())).is_err());
    assert_eq!(fs::read(destination.join("racing-owner")).unwrap(), b"keep");
    assert!(temporary.path().exists());
}

#[test]
fn temporary_creation_does_not_remove_a_preexisting_strict_path() {
    let directory = tempfile::tempdir().unwrap();
    let id = Uuid::new_v4();
    let path = directory.path().join(format!(".rustdb-backup-{id}.tmp"));
    fs::create_dir(&path).unwrap();
    fs::write(path.join("external"), b"keep").unwrap();

    assert!(temp::TemporaryBackup::create(directory.path(), id).is_err());
    assert_eq!(fs::read(path.join("external")).unwrap(), b"keep");
}

#[test]
fn cleanup_failure_keeps_the_original_error_context() {
    let directory = tempfile::tempdir().unwrap();
    let not_a_directory = directory.path().join("temporary");
    fs::write(&not_a_directory, b"file").unwrap();

    let error = temp::cleanup_failed(
        &not_a_directory,
        directory.path(),
        Error::Execution("copy failed".to_owned()),
    );
    let message = error.to_string();
    assert!(message.contains("copy failed"));
    assert!(message.contains("cleanup failed"));
}
