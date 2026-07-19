use std::{fmt, fs::Metadata, time::SystemTime};

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

/// Reproduces the ETag emitted by `object_store::local::LocalFileSystem`.
///
/// Direct local readers validate an opened descriptor without issuing another
/// object-store request, so their wire identity must use the same quoted form
/// as the query snapshot.
pub(crate) fn local_etag(metadata: &Metadata) -> String {
    let inode = inode(metadata);
    let size = metadata.len();
    let mtime = metadata
        .modified()
        .ok()
        .and_then(|mtime| mtime.duration_since(SystemTime::UNIX_EPOCH).ok())
        .unwrap_or_default()
        .as_micros();
    format!("\"{inode:x}-{mtime:x}-{size:x}\"")
}

#[cfg(unix)]
fn inode(metadata: &Metadata) -> u64 {
    use std::os::unix::fs::MetadataExt;
    metadata.ino()
}

#[cfg(not(unix))]
fn inode(_metadata: &Metadata) -> u64 {
    0
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
