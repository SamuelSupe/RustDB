use std::{fs, io, path::Path};

use serde::Serialize;
use sha2::{Digest, Sha256};
use sysinfo::Disks;

#[derive(Debug, Serialize)]
pub(super) struct StorageSummary {
    database_bytes: Option<u64>,
    filesystem_total_bytes: Option<u64>,
    filesystem_available_bytes: Option<u64>,
    probe_codes: Vec<&'static str>,
}

impl StorageSummary {
    pub(super) fn collect(path: &Path) -> Self {
        let mut probe_codes = Vec::new();
        let database_bytes = match directory_size(path) {
            Ok(bytes) => Some(bytes),
            Err(_) => {
                probe_codes.push("diagnostics.database_size_unavailable");
                None
            }
        };
        let (filesystem_total_bytes, filesystem_available_bytes) = match filesystem_space(path) {
            Some((total, available)) => (Some(total), Some(available)),
            None => {
                probe_codes.push("diagnostics.filesystem_space_unavailable");
                (None, None)
            }
        };
        Self {
            database_bytes,
            filesystem_total_bytes,
            filesystem_available_bytes,
            probe_codes,
        }
    }
}

pub(super) fn path_sha256(path: &Path) -> String {
    let resolved = path.canonicalize().unwrap_or_else(|_| path.to_path_buf());
    let mut digest = Sha256::new();
    digest.update(b"rustdb-diagnostics-database-path-v1\0");
    digest.update(resolved.to_string_lossy().as_bytes());
    digest
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn directory_size(path: &Path) -> io::Result<u64> {
    let metadata = fs::symlink_metadata(path)?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "database root is not a non-symlink directory",
        ));
    }
    let mut total = 0_u64;
    let mut pending = vec![path.to_path_buf()];
    while let Some(directory) = pending.pop() {
        for entry in fs::read_dir(directory)? {
            let entry = entry?;
            let metadata = fs::symlink_metadata(entry.path())?;
            if metadata.file_type().is_symlink() {
                continue;
            }
            if metadata.is_dir() {
                pending.push(entry.path());
            } else if metadata.is_file() {
                total = total.saturating_add(metadata.len());
            }
        }
    }
    Ok(total)
}

fn filesystem_space(path: &Path) -> Option<(u64, u64)> {
    let canonical = path.canonicalize().ok()?;
    Disks::new_with_refreshed_list()
        .list()
        .iter()
        .filter(|disk| canonical.starts_with(disk.mount_point()))
        .max_by_key(|disk| disk.mount_point().components().count())
        .map(|disk| (disk.total_space(), disk.available_space()))
}

#[cfg(test)]
mod tests {
    use std::fs;

    use super::{directory_size, path_sha256};

    #[test]
    fn path_fingerprint_is_stable_and_does_not_expose_the_path() {
        let temporary = tempfile::tempdir().unwrap();
        let first = path_sha256(temporary.path());
        let second = path_sha256(temporary.path());
        let clear_text_path = temporary.path().to_string_lossy();
        assert_eq!(first, second);
        assert_eq!(first.len(), 64);
        assert!(!first.contains(clear_text_path.as_ref()));
    }

    #[test]
    fn directory_size_counts_regular_files() {
        let temporary = tempfile::tempdir().unwrap();
        fs::create_dir(temporary.path().join("nested")).unwrap();
        fs::write(temporary.path().join("a"), [1_u8; 3]).unwrap();
        fs::write(temporary.path().join("nested/b"), [2_u8; 5]).unwrap();
        assert_eq!(directory_size(temporary.path()).unwrap(), 8);
    }
}
