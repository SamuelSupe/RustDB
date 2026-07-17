use std::{fmt, fs::Metadata};

/// Strong, process-local identity for an opened local file.
///
/// `object_store` ETags remain the wire-compatible conditional token. This
/// identity closes the local same-size/restored-mtime gap for cache keys and
/// opened-file reads on the deployment targets where all fields are exposed.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) struct LocalFileIdentity {
    device: u64,
    inode: u64,
    size: u64,
    mtime_seconds: i64,
    mtime_nanoseconds: i64,
    ctime_seconds: i64,
    ctime_nanoseconds: i64,
}

impl LocalFileIdentity {
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    pub(crate) fn from_metadata(metadata: &Metadata) -> Option<Self> {
        use std::os::unix::fs::MetadataExt;

        Some(Self {
            device: metadata.dev(),
            inode: metadata.ino(),
            size: metadata.len(),
            mtime_seconds: metadata.mtime(),
            mtime_nanoseconds: metadata.mtime_nsec(),
            ctime_seconds: metadata.ctime(),
            ctime_nanoseconds: metadata.ctime_nsec(),
        })
    }

    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    pub(crate) fn from_metadata(_metadata: &Metadata) -> Option<Self> {
        None
    }
}

impl fmt::Display for LocalFileIdentity {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{}:{}:{}:{}:{}:{}:{}",
            self.device,
            self.inode,
            self.size,
            self.mtime_seconds,
            self.mtime_nanoseconds,
            self.ctime_seconds,
            self.ctime_nanoseconds
        )
    }
}

#[cfg(test)]
mod tests {
    use std::{fs, fs::FileTimes, time::Duration};

    use tempfile::tempdir;

    use super::LocalFileIdentity;

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[test]
    fn detects_same_size_mutation_after_mtime_is_restored() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("identity.bin");
        fs::write(&path, b"before").unwrap();
        let before_metadata = fs::metadata(&path).unwrap();
        let before = LocalFileIdentity::from_metadata(&before_metadata).unwrap();
        let modified = before_metadata.modified().unwrap();

        std::thread::sleep(Duration::from_millis(2));
        fs::write(&path, b"after!").unwrap();
        fs::File::options()
            .write(true)
            .open(&path)
            .unwrap()
            .set_times(FileTimes::new().set_modified(modified))
            .unwrap();

        let after = LocalFileIdentity::from_metadata(&fs::metadata(&path).unwrap()).unwrap();
        assert_ne!(before, after);
    }
}
