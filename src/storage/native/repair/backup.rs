use std::{
    fs::{self, DirBuilder},
    os::unix::fs::{DirBuilderExt, PermissionsExt},
    path::{Path, PathBuf},
};

use chrono::Utc;
use serde::Serialize;
use sha2::{Digest, Sha256};

use crate::{Error, Result};

const MAX_METADATA_FILE_BYTES: u64 = 64 * 1024 * 1024;
const MAX_BACKUP_BYTES: u64 = 1024 * 1024 * 1024;

#[derive(Serialize)]
struct BackupManifest {
    format: &'static str,
    version: u32,
    source: PathBuf,
    created_at: String,
    files: Vec<BackupEntry>,
}

#[derive(Serialize)]
struct BackupEntry {
    path: PathBuf,
    bytes: u64,
    sha256: String,
    mode: u32,
}

pub(super) fn create(requested_root: &Path) -> Result<PathBuf> {
    let root = fs::canonicalize(requested_root)
        .map_err(|error| Error::io(Some(requested_root.to_path_buf()), error))?;
    let parent = root
        .parent()
        .ok_or_else(|| Error::native_repair_refused(&root, "database has no parent directory"))?;
    let name = root
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| Error::native_repair_refused(&root, "database name is not valid UTF-8"))?;
    let destination = parent.join(format!(".{name}.rustdb-repair-{}", uuid::Uuid::new_v4()));
    let mut builder = DirBuilder::new();
    builder
        .mode(0o700)
        .create(&destination)
        .map_err(|error| Error::io(Some(destination.clone()), error))?;
    super::super::io::sync_dir(parent)?;

    let sources = metadata_sources(&root)?;
    let mut entries = Vec::with_capacity(sources.len());
    let mut total = 0_u64;
    for source in sources {
        let metadata = fs::symlink_metadata(&source)
            .map_err(|error| Error::io(Some(source.clone()), error))?;
        if metadata.file_type().is_symlink() || !metadata.is_file() {
            return Err(Error::native_repair_refused(
                &source,
                "repair backup source is not a regular file",
            ));
        }
        if metadata.len() > MAX_METADATA_FILE_BYTES {
            return Err(Error::native_repair_refused(
                &source,
                "repair metadata backup source exceeds 64 MiB",
            ));
        }
        total = total.checked_add(metadata.len()).ok_or_else(|| {
            Error::native_repair_refused(&source, "repair metadata backup size overflow")
        })?;
        if total > MAX_BACKUP_BYTES {
            return Err(Error::native_repair_refused(
                &destination,
                "repair metadata backup exceeds the 1 GiB safety limit",
            ));
        }
        let bytes = fs::read(&source).map_err(|error| Error::io(Some(source.clone()), error))?;
        if u64::try_from(bytes.len()).ok() != Some(metadata.len()) {
            return Err(Error::native_repair_refused(
                &source,
                "repair metadata changed while being backed up",
            ));
        }
        let relative = source
            .strip_prefix(&root)
            .map_err(|_| Error::native_repair_refused(&source, "backup source escaped database"))?
            .to_path_buf();
        let target = destination.join(&relative);
        if let Some(parent) = target.parent() {
            super::super::io::create_private_dir_all(parent)?;
        }
        super::super::io::atomic_create(&target, &bytes)?;
        entries.push(BackupEntry {
            path: relative,
            bytes: metadata.len(),
            sha256: format!("{:x}", Sha256::digest(&bytes)),
            mode: metadata.permissions().mode() & 0o777,
        });
    }

    let manifest = serde_json::to_vec_pretty(&BackupManifest {
        format: "rustdb-native-repair-metadata",
        version: 1,
        source: root.clone(),
        created_at: Utc::now().to_rfc3339(),
        files: entries,
    })
    .map_err(|error| {
        Error::Internal(format!("failed to encode repair backup manifest: {error}"))
    })?;
    let checksum = format!("{:x}\n", Sha256::digest(&manifest));
    super::super::io::atomic_create(&destination.join("manifest.json"), &manifest)?;
    super::super::io::atomic_create(&destination.join("manifest.sha256"), checksum.as_bytes())?;
    super::super::io::sync_dir(&destination)?;
    super::super::io::sync_dir(parent)?;
    Ok(destination)
}

fn metadata_sources(root: &Path) -> Result<Vec<PathBuf>> {
    let mut files = Vec::new();
    for name in [super::super::MARKER_FILE, super::super::INIT_FILE] {
        push_if_present(&root.join(name), &mut files)?;
    }
    push_if_present(&root.join("catalog").join("CURRENT"), &mut files)?;
    let generations = root.join("catalog").join("generations");
    if generations.is_dir() {
        for entry in fs::read_dir(&generations)
            .map_err(|error| Error::io(Some(generations.clone()), error))?
        {
            let path = entry
                .map_err(|error| Error::io(Some(generations.clone()), error))?
                .path();
            if super::super::manifest::generation_from_name(&path).is_some() {
                files.push(path);
            }
        }
    }
    collect_named(
        &root.join("tables"),
        &["manifest.json", ".rustdb-snapshot"],
        &mut files,
    )?;
    collect_named(
        &root.join("staging"),
        &["manifest.json", ".rustdb-transaction"],
        &mut files,
    )?;
    push_if_present(&root.join("wal").join("CHECKPOINT"), &mut files)?;
    files.sort();
    files.dedup();
    Ok(files)
}

fn collect_named(root: &Path, names: &[&str], files: &mut Vec<PathBuf>) -> Result<()> {
    let metadata = match fs::symlink_metadata(root) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(Error::io(Some(root.to_path_buf()), error)),
    };
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(Error::native_repair_refused(
            root,
            "repair metadata root is not a regular directory",
        ));
    }
    for entry in fs::read_dir(root).map_err(|error| Error::io(Some(root.to_path_buf()), error))? {
        let path = entry
            .map_err(|error| Error::io(Some(root.to_path_buf()), error))?
            .path();
        let metadata =
            fs::symlink_metadata(&path).map_err(|error| Error::io(Some(path.clone()), error))?;
        if metadata.file_type().is_symlink() {
            continue;
        }
        if metadata.is_dir() {
            collect_named(&path, names, files)?;
        } else if metadata.is_file()
            && path
                .file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| names.contains(&name))
        {
            files.push(path);
        }
    }
    Ok(())
}

fn push_if_present(path: &Path, files: &mut Vec<PathBuf>) -> Result<()> {
    match fs::symlink_metadata(path) {
        Ok(_) => files.push(path.to_path_buf()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(Error::io(Some(path.to_path_buf()), error)),
    }
    Ok(())
}
