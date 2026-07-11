use std::{
    fs::{File, OpenOptions},
    io::Write,
    path::Path,
    time::{Duration, SystemTime},
};

use uuid::Uuid;

use crate::{Error, Result};

use super::activity;

const MARKER_FILE_NAME: &str = ".rustdb-spill";
const MARKER_CONTENT: &[u8] = b"rustdb-spill-directory\nversion=1\n";

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct ScavengeReport {
    pub(crate) scanned: u64,
    pub(crate) removed: u64,
    pub(crate) preserved: u64,
}

pub(super) fn write_query_marker(directory: &Path) -> Result<()> {
    let path = directory.join(MARKER_FILE_NAME);
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options
        .open(&path)
        .map_err(|error| Error::io(Some(path.clone()), error))?;
    if let Err(error) = file
        .write_all(MARKER_CONTENT)
        .and_then(|()| file.sync_all())
    {
        drop(file);
        return Err(remove_created_marker(
            &path,
            Error::io(Some(path.clone()), error),
        ));
    }
    drop(file);
    if let Err(error) = super::io::sync_parent_directory(&path) {
        return Err(remove_created_marker(&path, error));
    }
    Ok(())
}

fn remove_created_marker(path: &Path, primary: Error) -> Error {
    match std::fs::remove_file(path) {
        Ok(()) => primary,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => primary,
        Err(error) => Error::Execution(format!(
            "{primary}; additionally failed to remove spill marker '{}': {error}",
            path.display()
        )),
    }
}

pub(crate) fn scavenge_orphans(root: &Path, orphan_ttl: Duration) -> Result<ScavengeReport> {
    scavenge_orphans_at(root, orphan_ttl, SystemTime::now())
}

fn scavenge_orphans_at(
    root: &Path,
    orphan_ttl: Duration,
    now: SystemTime,
) -> Result<ScavengeReport> {
    let entries = match std::fs::read_dir(root) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(ScavengeReport::default());
        }
        Err(error) => return Err(Error::io(Some(root.to_path_buf()), error)),
    };
    let mut report = ScavengeReport::default();
    for entry in entries {
        let entry = entry.map_err(|error| Error::io(Some(root.to_path_buf()), error))?;
        report.scanned += 1;
        let path = entry.path();
        if !is_candidate_query_directory(&entry, &path)?
            || !has_valid_old_marker(&path, orphan_ttl, now)?
        {
            report.preserved += 1;
            continue;
        }
        let Some(_activity_lock) = activity::try_lock_for_cleanup(&path)? else {
            report.preserved += 1;
            continue;
        };
        match std::fs::remove_dir_all(&path) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(Error::io(Some(path.clone()), error)),
        }
        match std::fs::symlink_metadata(&path) {
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(Error::io(Some(path), error)),
            Ok(_) => {
                return Err(Error::Execution(format!(
                    "orphan spill directory '{}' remained after cleanup",
                    path.display()
                )));
            }
        }
        super::io::sync_parent_directory(&path)?;
        report.removed += 1;
    }
    Ok(report)
}

fn is_candidate_query_directory(entry: &std::fs::DirEntry, path: &Path) -> Result<bool> {
    let file_type = match entry.file_type() {
        Ok(file_type) => file_type,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(Error::io(Some(path.to_path_buf()), error)),
    };
    if !file_type.is_dir() || file_type.is_symlink() {
        return Ok(false);
    }
    let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
        return Ok(false);
    };
    Ok(name
        .strip_prefix("query-")
        .is_some_and(|id| Uuid::parse_str(id).is_ok()))
}

fn has_valid_old_marker(directory: &Path, orphan_ttl: Duration, now: SystemTime) -> Result<bool> {
    has_valid_old_marker_with(directory, orphan_ttl, now, read_marker)
}

fn has_valid_old_marker_with<F>(
    directory: &Path,
    orphan_ttl: Duration,
    now: SystemTime,
    read: F,
) -> Result<bool>
where
    F: FnOnce(&Path) -> std::io::Result<Vec<u8>>,
{
    let marker = directory.join(MARKER_FILE_NAME);
    let metadata = match std::fs::symlink_metadata(&marker) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(Error::io(Some(marker), error)),
    };
    if !metadata.file_type().is_file() || metadata.len() != MARKER_CONTENT.len() as u64 {
        return Ok(false);
    }
    let modified = metadata
        .modified()
        .map_err(|error| Error::io(Some(marker.clone()), error))?;
    if !now
        .duration_since(modified)
        .is_ok_and(|age| age >= orphan_ttl)
    {
        return Ok(false);
    }
    match read(&marker) {
        Ok(content) => Ok(content == MARKER_CONTENT),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(Error::io(Some(marker), error)),
    }
}

fn read_marker(path: &Path) -> std::io::Result<Vec<u8>> {
    use std::io::Read;

    let mut file = File::open(path)?;
    let mut content = Vec::with_capacity(MARKER_CONTENT.len());
    file.read_to_end(&mut content)?;
    Ok(content)
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, SystemTime};

    use crate::{
        Error,
        runtime::{MemoryPool, SpillManager},
    };

    use super::super::activity::{ACTIVITY_FILE_NAME, QueryActivityLock};
    use super::{
        MARKER_CONTENT, MARKER_FILE_NAME, has_valid_old_marker_with, scavenge_orphans_at,
        write_query_marker,
    };

    #[test]
    fn marker_is_versioned_and_created_with_private_permissions() {
        let root = tempfile::tempdir().expect("tempdir");
        let directory = root.path().join(format!("query-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&directory).expect("query directory");
        write_query_marker(&directory).expect("marker");
        let marker = directory.join(MARKER_FILE_NAME);
        assert_eq!(std::fs::read(&marker).unwrap(), MARKER_CONTENT);

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(marker).unwrap().permissions().mode() & 0o777;
            assert_eq!(mode, 0o600);
        }
    }

    #[test]
    fn scavenger_removes_only_old_valid_marked_query_directories() {
        let root = tempfile::tempdir().expect("tempdir");
        let ttl = Duration::from_secs(24 * 60 * 60);

        let old = root.path().join(format!("query-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&old).unwrap();
        let old_lock = QueryActivityLock::create(&old).unwrap();
        write_query_marker(&old).unwrap();
        drop(old_lock);
        std::fs::write(old.join("run.arrow"), b"spill").unwrap();
        let marker_modified = std::fs::metadata(old.join(MARKER_FILE_NAME))
            .unwrap()
            .modified()
            .unwrap();

        let unknown = root.path().join("do-not-delete");
        std::fs::create_dir(&unknown).unwrap();
        std::fs::write(unknown.join("important"), b"keep").unwrap();

        let unmarked = root.path().join(format!("query-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&unmarked).unwrap();

        let invalid = root.path().join(format!("query-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&invalid).unwrap();
        std::fs::write(
            invalid.join(MARKER_FILE_NAME),
            b"rustdb-spill-directory\nversion=0\n",
        )
        .unwrap();

        let now = marker_modified
            .checked_add(ttl + Duration::from_secs(1))
            .unwrap_or(SystemTime::now());
        let report = scavenge_orphans_at(root.path(), ttl, now).expect("scavenge");

        assert_eq!(report.scanned, 4);
        assert_eq!(report.removed, 1);
        assert_eq!(report.preserved, 3);
        assert!(!old.exists());
        assert!(unknown.join("important").exists());
        assert!(unmarked.exists());
        assert!(invalid.exists());
    }

    #[test]
    fn old_active_query_directory_is_preserved_until_its_lock_is_released() {
        let root = tempfile::tempdir().expect("tempdir");
        let directory = root.path().join(format!("query-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&directory).unwrap();
        let activity_lock = QueryActivityLock::create(&directory).unwrap();
        write_query_marker(&directory).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(directory.join(ACTIVITY_FILE_NAME))
                .unwrap()
                .permissions()
                .mode()
                & 0o777;
            assert_eq!(mode, 0o600);
        }
        let modified = std::fs::metadata(directory.join(MARKER_FILE_NAME))
            .unwrap()
            .modified()
            .unwrap();
        let now = modified + Duration::from_secs(2);

        let active = scavenge_orphans_at(root.path(), Duration::from_secs(1), now).unwrap();
        assert_eq!(active.removed, 0);
        assert_eq!(active.preserved, 1);
        assert!(directory.exists());

        drop(activity_lock);
        let orphan = scavenge_orphans_at(root.path(), Duration::from_secs(1), now).unwrap();
        assert_eq!(orphan.removed, 1);
        assert_eq!(orphan.preserved, 0);
        assert!(!directory.exists());
    }

    #[test]
    fn spill_manager_holds_the_activity_lock_until_cleanup() {
        let root = tempfile::tempdir().expect("tempdir");
        let manager = SpillManager::new(root.path(), MemoryPool::new(1 << 20)).unwrap();
        let directory = manager.directory().to_path_buf();
        assert!(manager.activity_lock_held());
        let modified = std::fs::metadata(directory.join(MARKER_FILE_NAME))
            .unwrap()
            .modified()
            .unwrap();

        let report = scavenge_orphans_at(
            root.path(),
            Duration::from_secs(1),
            modified + Duration::from_secs(2),
        )
        .unwrap();
        assert_eq!(report.removed, 0);
        assert_eq!(report.preserved, 1);
        assert!(directory.exists());

        manager.cleanup().unwrap();
        assert!(!manager.activity_lock_held());
        assert!(!directory.exists());
    }

    #[test]
    fn malformed_activity_lock_error_is_propagated_without_deleting_the_directory() {
        let root = tempfile::tempdir().expect("tempdir");
        let directory = root.path().join(format!("query-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&directory).unwrap();
        write_query_marker(&directory).unwrap();
        std::fs::create_dir(directory.join(ACTIVITY_FILE_NAME)).unwrap();
        let modified = std::fs::metadata(directory.join(MARKER_FILE_NAME))
            .unwrap()
            .modified()
            .unwrap();

        let error = scavenge_orphans_at(
            root.path(),
            Duration::from_secs(1),
            modified + Duration::from_secs(2),
        )
        .unwrap_err();
        assert!(matches!(error, Error::Execution(_)));
        assert!(error.to_string().contains("not a regular file"));
        assert!(directory.exists());
    }

    #[test]
    fn marker_read_errors_are_propagated_instead_of_treated_as_invalid() {
        let root = tempfile::tempdir().expect("tempdir");
        let directory = root.path().join(format!("query-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&directory).unwrap();
        write_query_marker(&directory).unwrap();
        let modified = std::fs::metadata(directory.join(MARKER_FILE_NAME))
            .unwrap()
            .modified()
            .unwrap();

        let error = has_valid_old_marker_with(
            &directory,
            Duration::from_secs(1),
            modified + Duration::from_secs(2),
            |_| {
                Err(std::io::Error::new(
                    std::io::ErrorKind::PermissionDenied,
                    "injected marker ACL failure",
                ))
            },
        )
        .unwrap_err();
        assert!(matches!(
            error,
            Error::Io { source, .. } if source.kind() == std::io::ErrorKind::PermissionDenied
        ));
        assert!(directory.exists());
    }

    #[test]
    fn fresh_marked_query_directory_is_preserved() {
        let root = tempfile::tempdir().expect("tempdir");
        let fresh = root.path().join(format!("query-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&fresh).unwrap();
        write_query_marker(&fresh).unwrap();
        let now = std::fs::metadata(fresh.join(MARKER_FILE_NAME))
            .unwrap()
            .modified()
            .unwrap();

        let report = scavenge_orphans_at(root.path(), Duration::from_secs(1), now)
            .expect("scavenge fresh directory");
        assert_eq!(report.removed, 0);
        assert_eq!(report.preserved, 1);
        assert!(fresh.exists());
    }

    #[test]
    fn missing_root_is_a_noop() {
        let root = tempfile::tempdir().unwrap();
        let missing = root.path().join("missing");
        let report = scavenge_orphans_at(&missing, Duration::from_secs(1), SystemTime::now())
            .expect("missing root");
        assert_eq!(report.scanned, 0);
        assert_eq!(report.removed, 0);
        assert_eq!(report.preserved, 0);
    }
}
