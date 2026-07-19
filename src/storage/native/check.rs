use std::{
    collections::BTreeSet,
    fs,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
};

use serde::Serialize;

use super::{MARKER_FILE, manifest, marker, table};
use crate::{Error, Result};

#[derive(Clone, Debug, Serialize)]
#[non_exhaustive]
pub struct NativeCheckIssue {
    code: String,
    path: Option<PathBuf>,
    message: String,
}

impl NativeCheckIssue {
    pub(crate) fn new(
        code: impl Into<String>,
        path: Option<PathBuf>,
        message: impl Into<String>,
    ) -> Self {
        Self {
            code: code.into(),
            path,
            message: message.into(),
        }
    }

    pub(crate) fn from_error(error: &Error, fallback: Option<PathBuf>) -> Self {
        Self {
            code: error.code().as_str().to_owned(),
            path: error_path(error).or(fallback),
            message: error.to_string(),
        }
    }

    pub fn code(&self) -> &str {
        &self.code
    }

    pub fn path(&self) -> Option<&Path> {
        self.path.as_deref()
    }

    pub fn message(&self) -> &str {
        &self.message
    }
}

#[derive(Clone, Debug, Serialize)]
#[non_exhaustive]
pub struct NativeCheckReport {
    path: PathBuf,
    ok: bool,
    format_version: Option<u32>,
    catalog_generation: Option<u64>,
    checked_tables: u64,
    checked_snapshots: u64,
    checked_files: u64,
    checked_bytes: u64,
    warnings: Vec<NativeCheckIssue>,
    errors: Vec<NativeCheckIssue>,
    #[serde(skip)]
    verified_files: Vec<PathBuf>,
}

impl NativeCheckReport {
    pub fn is_ok(&self) -> bool {
        self.ok
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn format_version(&self) -> Option<u32> {
        self.format_version
    }

    pub fn catalog_generation(&self) -> Option<u64> {
        self.catalog_generation
    }

    pub fn checked_tables(&self) -> u64 {
        self.checked_tables
    }

    pub fn checked_snapshots(&self) -> u64 {
        self.checked_snapshots
    }

    pub fn checked_files(&self) -> u64 {
        self.checked_files
    }

    pub fn checked_bytes(&self) -> u64 {
        self.checked_bytes
    }

    pub fn warnings(&self) -> &[NativeCheckIssue] {
        &self.warnings
    }

    pub fn errors(&self) -> &[NativeCheckIssue] {
        &self.errors
    }

    pub(crate) fn verified_files(&self) -> &[PathBuf] {
        &self.verified_files
    }
}

pub(super) fn database(path: &Path) -> Result<NativeCheckReport> {
    if path.as_os_str().is_empty() {
        return Err(Error::InvalidArgument(
            "database path must not be empty".to_owned(),
        ));
    }
    let mut checker = Checker::new(path);
    checker.run();
    checker.report.ok = checker.report.errors.is_empty();
    Ok(checker.report)
}

struct Checker {
    report: NativeCheckReport,
    files: BTreeSet<PathBuf>,
    snapshots: BTreeSet<PathBuf>,
}

impl Checker {
    fn new(path: &Path) -> Self {
        Self {
            report: NativeCheckReport {
                path: path.to_path_buf(),
                ok: false,
                format_version: None,
                catalog_generation: None,
                checked_tables: 0,
                checked_snapshots: 0,
                checked_files: 0,
                checked_bytes: 0,
                warnings: Vec::new(),
                errors: Vec::new(),
                verified_files: Vec::new(),
            },
            files: BTreeSet::new(),
            snapshots: BTreeSet::new(),
        }
    }

    fn run(&mut self) {
        if !self.check_root() {
            return;
        }

        let marker_path = self.report.path.join(MARKER_FILE);
        let database_marker = match marker::read(&marker_path) {
            Ok(marker) => marker,
            Err(error) => {
                if let Error::NativeFormatUnsupported { found_version, .. } = &error {
                    self.report.format_version = Some(*found_version);
                }
                self.push_error(error, Some(marker_path));
                return;
            }
        };
        self.report.format_version = Some(database_marker.version());
        self.record_file(&marker_path);
        self.inspect_initialization_marker();

        let catalog = match manifest::load(&self.report.path, database_marker.database_id()) {
            Ok(catalog) => catalog,
            Err(error) => {
                self.push_error(error, Some(self.report.path.join("catalog")));
                return;
            }
        };
        self.report.catalog_generation = Some(catalog.generation());
        self.record_catalog_files(catalog.generation());

        for (name, reference) in catalog.tables() {
            self.report.checked_tables = self.report.checked_tables.saturating_add(1);
            let manifest_path = snapshot_directory(
                &self.report.path,
                reference.table_id(),
                reference.version(),
                reference.snapshot_id(),
            )
            .join("manifest.json");
            match table::load(&self.report.path, database_marker.database_id(), reference) {
                Ok(snapshot) => self.record_snapshot(&snapshot),
                Err(error) => self.push_table_error(name, error, manifest_path),
            }
        }
    }

    fn check_root(&mut self) -> bool {
        let metadata = match fs::symlink_metadata(&self.report.path) {
            Ok(metadata) => metadata,
            Err(error) => {
                self.push_error(
                    Error::io(Some(self.report.path.clone()), error),
                    Some(self.report.path.clone()),
                );
                return false;
            }
        };
        if metadata.file_type().is_symlink() || !metadata.is_dir() {
            self.push_error(
                Error::native_storage(
                    &self.report.path,
                    "native database path must be a non-symlink directory",
                ),
                Some(self.report.path.clone()),
            );
            return false;
        }
        if metadata.permissions().mode() & 0o077 != 0 {
            self.report.warnings.push(NativeCheckIssue {
                code: "native.insecure_permissions".to_owned(),
                path: Some(self.report.path.clone()),
                message:
                    "native database root is accessible by group or other users; expected mode 0700"
                        .to_owned(),
            });
        }
        true
    }

    fn inspect_initialization_marker(&mut self) {
        let path = self.report.path.join(super::INIT_FILE);
        match fs::symlink_metadata(&path) {
            Ok(_) => self.report.warnings.push(NativeCheckIssue {
                code: "native.initialization_marker_present".to_owned(),
                path: Some(path),
                message: "initialization marker is still present; run normal recovery before relying on this database"
                    .to_owned(),
            }),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => self.push_error(Error::io(Some(path.clone()), error), Some(path)),
        }
    }

    fn record_catalog_files(&mut self, generation: u64) {
        let root = self.report.path.clone();
        self.record_file(&root.join("catalog").join("CURRENT"));
        self.record_file(
            &root
                .join("catalog")
                .join("generations")
                .join(format!("{generation:020}.json")),
        );
    }

    fn record_snapshot(&mut self, snapshot: &table::TableSnapshot) {
        self.record_snapshot_files(snapshot);

        // Older manifests are normally pruned after publication. If one is
        // still retained (for example after interrupted cleanup), verify the
        // immutable parent chain as well without requiring it to exist.
        let mut parent = snapshot.parent().cloned();
        while let Some(reference) = parent {
            let directory = snapshot_directory(
                &self.report.path,
                reference.table_id(),
                reference.version(),
                reference.snapshot_id(),
            );
            let manifest_path = directory.join("manifest.json");
            match fs::symlink_metadata(&manifest_path) {
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => break,
                Err(error) => {
                    self.push_error(
                        Error::io(Some(manifest_path.clone()), error),
                        Some(manifest_path),
                    );
                    break;
                }
                Ok(_) => {}
            }
            match table::load(&self.report.path, snapshot.database_id(), &reference) {
                Ok(parent_snapshot) => {
                    parent = parent_snapshot.parent().cloned();
                    self.record_snapshot_files(&parent_snapshot);
                }
                Err(error) => {
                    self.push_error(error, Some(manifest_path));
                    break;
                }
            }
        }
    }

    fn record_snapshot_files(&mut self, snapshot: &table::TableSnapshot) {
        let directory = snapshot.final_directory(&self.report.path);
        if self.snapshots.insert(directory.clone()) {
            self.report.checked_snapshots = self.report.checked_snapshots.saturating_add(1);
        }
        self.record_file(&directory.join("manifest.json"));
        for owner in snapshot.reachable_directories(&self.report.path) {
            self.record_file(&owner.join(".rustdb-snapshot"));
        }
        for path in snapshot.segment_paths(&self.report.path) {
            self.record_file(&path);
        }
        for path in snapshot.predicate_sidecar_paths(&self.report.path) {
            self.record_file(&path);
        }
        for path in snapshot.delete_vector_paths(&self.report.path) {
            self.record_file(&path);
        }
    }

    fn record_file(&mut self, path: &Path) {
        if !self.files.insert(path.to_path_buf()) {
            return;
        }
        match fs::symlink_metadata(path) {
            Ok(metadata) if !metadata.file_type().is_symlink() && metadata.is_file() => {
                self.report.verified_files.push(path.to_path_buf());
                self.report.checked_files = self.report.checked_files.saturating_add(1);
                self.report.checked_bytes =
                    self.report.checked_bytes.saturating_add(metadata.len());
                if metadata.permissions().mode() & 0o077 != 0 {
                    self.report.warnings.push(NativeCheckIssue {
                        code: "native.insecure_permissions".to_owned(),
                        path: Some(path.to_path_buf()),
                        message: "native metadata or data file is accessible by group or other users; expected mode 0600"
                            .to_owned(),
                    });
                }
            }
            Ok(_) => self.push_error(
                Error::native_storage(path, "checked path is not a regular file"),
                Some(path.to_path_buf()),
            ),
            Err(error) => self.push_error(
                Error::io(Some(path.to_path_buf()), error),
                Some(path.to_path_buf()),
            ),
        }
    }

    fn push_table_error(&mut self, name: &str, error: Error, fallback: PathBuf) {
        let path = error_path(&error).or(Some(fallback));
        self.report.errors.push(NativeCheckIssue {
            code: error.code().as_str().to_owned(),
            path,
            message: format!("table '{name}': {error}"),
        });
    }

    fn push_error(&mut self, error: Error, fallback: Option<PathBuf>) {
        self.report
            .errors
            .push(NativeCheckIssue::from_error(&error, fallback));
    }
}

fn snapshot_directory(root: &Path, table_id: &str, version: u64, snapshot_id: &str) -> PathBuf {
    root.join("tables")
        .join(table_id)
        .join("snapshots")
        .join(format!("{version:020}-{snapshot_id}"))
}

fn error_path(error: &Error) -> Option<PathBuf> {
    match error {
        Error::Io { path, .. } => path.clone(),
        Error::NativeDiskQuotaExceeded { path, .. }
        | Error::NativeStorage { path, .. }
        | Error::NativeFormatUnsupported { path, .. }
        | Error::NativeRepairRefused { path, .. }
        | Error::CommitOutcomeUnknown { path, .. }
        | Error::NativeCommitPostCommitFailure { path, .. }
        | Error::CopyPostCommitFailure { path, .. } => Some(path.clone()),
        _ => None,
    }
}

#[cfg(test)]
#[path = "check/tests.rs"]
mod tests;
