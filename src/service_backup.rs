use std::{fs, path::Path};

use serde::{Deserialize, Serialize};

use crate::{Engine, EngineConfig, Error, Result};

mod create;
mod files;
mod location;
mod manifest;
mod restore;
mod state;
#[cfg(test)]
mod tests;
mod validate;

pub(super) const NATIVE_DIRECTORY: &str = "native";
pub(super) const SERVICE_DIRECTORY: &str = "service";
pub(super) const EXCLUDED_STATE: &[&str] = &[
    "query-results-and-journals",
    "spill-and-temporary-data",
    "audit-logs",
    "locks-and-admin-sockets",
];
pub(super) const STATE_FILES: &[&str] = &[
    "principals.json",
    "ca-identity.pem",
    "ca.pem",
    "server-identity.pem",
    "tls.json",
    "bearer.token",
];
pub(super) const STATE_DIRECTORIES: &[(&str, &[&str])] = &[
    ("profile-tokens", &[]),
    (
        "connection.rustdb-profile",
        &["profile.json", "ca.pem", "bearer.token"],
    ),
];

/// Successful validation summary for a versioned, self-contained service backup.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[non_exhaustive]
pub struct ServiceBackupReport {
    location: String,
    database_id: String,
    service_state_included: bool,
    files: u64,
    bytes: u64,
}

impl ServiceBackupReport {
    pub fn location(&self) -> &str {
        &self.location
    }

    pub fn database_id(&self) -> &str {
        &self.database_id
    }

    pub fn service_state_included(&self) -> bool {
        self.service_state_included
    }

    pub fn files(&self) -> u64 {
        self.files
    }

    pub fn bytes(&self) -> u64 {
        self.bytes
    }
}

impl Engine {
    /// Creates one verified bundle containing Native data and safe HTTP control state.
    /// Query results, journals, Spill, audit logs, locks, sockets, and temporary files
    /// are deliberately excluded.
    pub async fn backup_service_to_location(
        &self,
        state_root: impl AsRef<Path>,
        location: &str,
    ) -> Result<ServiceBackupReport> {
        if !location.starts_with("s3://") {
            return create::local(self, state_root.as_ref(), &location::local_path(location)?);
        }
        let temporary = crate::storage::RemoteTempDir::create(
            &self.config().spill.directory,
            crate::storage::RemoteTempKind::Backup,
        )?;
        let snapshot = temporary.path().join("snapshot");
        let mut report = create::local(self, state_root.as_ref(), &snapshot)?;
        report.location = location.to_owned();
        let s3 = self.config().s3.clone();
        let location_owned = location.to_owned();
        let handle = tokio::spawn(async move {
            let result = crate::storage::upload_remote_backup(&snapshot, &location_owned, &s3)
                .await
                .map(|()| report);
            temporary.finish(result)
        });
        handle.await.map_err(|error| {
            Error::Internal(format!(
                "service backup worker stopped unexpectedly: {error}"
            ))
        })?
    }

    /// Validates the manifest, every file checksum, private permissions, and the
    /// embedded Native database without changing the bundle.
    pub async fn check_service_backup_location(
        location: &str,
        config: EngineConfig,
    ) -> Result<ServiceBackupReport> {
        if !location.starts_with("s3://") {
            return validate::local(&location::local_path(location)?, location);
        }
        fs::create_dir_all(&config.spill.directory)
            .map_err(|error| Error::io(Some(config.spill.directory.clone()), error))?;
        let downloaded =
            crate::storage::download_remote_backup(location, &config.spill.directory, &config.s3)
                .await?;
        let result = validate::local(downloaded.snapshot(), location);
        downloaded.finish(result)
    }

    /// Restores a service bundle into a fresh Native path and fresh per-database
    /// state directory. Existing targets are never overwritten.
    pub async fn restore_service_from_location(
        backup: &str,
        database: impl AsRef<Path>,
        state_root: impl AsRef<Path>,
        config: EngineConfig,
    ) -> Result<Self> {
        if !backup.starts_with("s3://") {
            return restore::local(
                &location::local_path(backup)?,
                database.as_ref(),
                state_root.as_ref(),
                config,
            );
        }
        fs::create_dir_all(&config.spill.directory)
            .map_err(|error| Error::io(Some(config.spill.directory.clone()), error))?;
        let downloaded =
            crate::storage::download_remote_backup(backup, &config.spill.directory, &config.s3)
                .await?;
        let result = restore::local(
            downloaded.snapshot(),
            database.as_ref(),
            state_root.as_ref(),
            config,
        );
        downloaded.finish(result)
    }
}
