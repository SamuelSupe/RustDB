use std::{
    collections::HashSet,
    fs,
    path::{Path, PathBuf},
};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::{Error, Result};

use super::{
    Authenticator, PrincipalDirectory, PrincipalId, Role, TokenId, credential::TokenCredential,
};
use crate::http_shell::security::{
    AuditEvent, AuditKind, AuditLog,
    files::{atomic_write_secure, ensure_secure_directory, read_secure_file},
    state::SecurityState,
};

const SCHEMA_VERSION: u32 = 1;
const MAX_DIRECTORY_BYTES: u64 = 8 * 1024 * 1024;
const BOOTSTRAP_PRINCIPAL: &str = "admin";

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PrincipalSummary {
    id: PrincipalId,
    role: Role,
    enabled: bool,
    token_count: usize,
}

impl PrincipalSummary {
    pub fn id(&self) -> &PrincipalId {
        &self.id
    }

    pub fn role(&self) -> Role {
        self.role
    }

    pub fn enabled(&self) -> bool {
        self.enabled
    }

    pub fn token_count(&self) -> usize {
        self.token_count
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TokenProvision {
    token_id: TokenId,
    token_path: PathBuf,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TokenSummary {
    token_id: TokenId,
    principal_id: PrincipalId,
    active: bool,
    revoked: bool,
    valid_until: Option<DateTime<Utc>>,
}

impl TokenSummary {
    pub fn token_id(&self) -> &TokenId {
        &self.token_id
    }

    pub fn principal_id(&self) -> &PrincipalId {
        &self.principal_id
    }

    pub fn active(&self) -> bool {
        self.active
    }

    pub fn revoked(&self) -> bool {
        self.revoked
    }

    pub fn valid_until(&self) -> Option<DateTime<Utc>> {
        self.valid_until
    }
}

impl TokenProvision {
    pub fn token_id(&self) -> &TokenId {
        &self.token_id
    }

    pub fn token_path(&self) -> &Path {
        &self.token_path
    }
}

/// Durable digest-only authentication directory for one Native database.
#[derive(Clone, Debug)]
pub struct PrincipalStore {
    state: SecurityState,
}

impl PrincipalStore {
    pub fn new(state: SecurityState) -> Self {
        Self { state }
    }

    pub fn load_or_bootstrap(&self) -> Result<Authenticator> {
        let path = self.state.principals_path();
        if !path
            .try_exists()
            .map_err(|error| Error::io(path.clone(), error))?
        {
            let admin = PrincipalId::new(BOOTSTRAP_PRINCIPAL)?;
            let mut directory = PrincipalDirectory::new();
            directory.add_principal(admin.clone(), Role::Admin)?;
            let _ = self.add_generated_token(directory, &admin)?;
            self.audit(AuditKind::PrincipalChanged, Some(&admin), "bootstrapped");
        }
        Ok(Authenticator::required(self.load()?))
    }

    /// Reloads the durable digest directory into a running authenticator.
    /// The replacement happens only after the complete file has validated.
    pub fn reload_into(&self, authenticator: &Authenticator) -> Result<()> {
        let directory = self.load()?;
        ensure_enabled_admin(&directory)?;
        authenticator.update_directory(|current| {
            *current = directory;
            Ok(())
        })
    }

    pub(crate) fn validate_existing(&self) -> Result<()> {
        let directory = self.load()?;
        ensure_enabled_admin(&directory)
    }

    pub fn list(&self) -> Result<Vec<PrincipalSummary>> {
        self.ensure_bootstrapped()?;
        let directory = self.load()?;
        let now = Utc::now();
        Ok(directory
            .principals
            .iter()
            .map(|(id, record)| PrincipalSummary {
                id: id.clone(),
                role: record.role,
                enabled: record.enabled,
                token_count: directory
                    .credentials
                    .iter()
                    .filter(|credential| {
                        credential.principal_id() == id && credential.is_active_at(now)
                    })
                    .count(),
            })
            .collect())
    }

    pub fn create_principal(&self, id: PrincipalId, role: Role) -> Result<TokenProvision> {
        self.ensure_bootstrapped()?;
        let mut directory = self.load()?;
        directory.add_principal(id.clone(), role)?;
        let provision = self.add_generated_token(directory, &id)?;
        self.audit(AuditKind::PrincipalChanged, Some(&id), "created");
        Ok(provision)
    }

    pub fn set_enabled(&self, id: &PrincipalId, enabled: bool) -> Result<()> {
        self.ensure_bootstrapped()?;
        let mut directory = self.load()?;
        directory.set_enabled(id, enabled)?;
        ensure_enabled_admin(&directory)?;
        self.save(&directory)?;
        self.audit(
            AuditKind::PrincipalChanged,
            Some(id),
            if enabled { "enabled" } else { "disabled" },
        );
        Ok(())
    }

    pub fn set_role(&self, id: &PrincipalId, role: Role) -> Result<()> {
        self.ensure_bootstrapped()?;
        let mut directory = self.load()?;
        directory.set_role(id, role)?;
        ensure_enabled_admin(&directory)?;
        self.save(&directory)?;
        self.audit(AuditKind::PrincipalChanged, Some(id), "role_changed");
        Ok(())
    }

    /// Adds a new active credential without revoking existing credentials.
    pub fn rotate_token(&self, principal_id: &PrincipalId) -> Result<TokenProvision> {
        self.ensure_bootstrapped()?;
        let directory = self.load()?;
        self.add_generated_token(directory, principal_id)
    }

    pub fn revoke_token(&self, token_id: &TokenId) -> Result<()> {
        self.ensure_bootstrapped()?;
        let mut directory = self.load()?;
        let principal = directory.token_principal(token_id)?.clone();
        directory.revoke_token(token_id)?;
        self.save(&directory)?;
        self.audit(AuditKind::TokenChanged, Some(&principal), "revoked");
        let path = self.token_path(token_id);
        match fs::remove_file(&path) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(Error::io(path, error)),
        }
    }

    /// Revokes durably and always reconciles the live authenticator with the
    /// durable directory, even when removal of the local clear-text profile
    /// token reports a secondary cleanup error.
    pub(crate) fn revoke_token_and_reload(
        &self,
        token_id: &TokenId,
        authenticator: &Authenticator,
    ) -> Result<()> {
        let revoke = self.revoke_token(token_id);
        let reload = self.reload_into(authenticator);
        match (revoke, reload) {
            (Ok(()), Ok(())) => Ok(()),
            (Err(error), Ok(())) | (Ok(()), Err(error)) => Err(error),
            (Err(revoke), Err(reload)) => Err(Error::Execution(format!(
                "{revoke}; additionally failed to reload durable credentials: {reload}"
            ))),
        }
    }

    pub fn list_tokens(&self, principal_id: Option<&PrincipalId>) -> Result<Vec<TokenSummary>> {
        self.ensure_bootstrapped()?;
        let directory = self.load()?;
        if let Some(principal_id) = principal_id
            && !directory.principals.contains_key(principal_id)
        {
            return Err(Error::InvalidArgument(format!(
                "principal {principal_id} does not exist"
            )));
        }
        let now = Utc::now();
        Ok(directory
            .credentials
            .iter()
            .filter(|credential| principal_id.is_none_or(|id| credential.principal_id() == id))
            .map(|credential| {
                let principal_enabled = directory
                    .principals
                    .get(credential.principal_id())
                    .is_some_and(|principal| principal.enabled);
                TokenSummary {
                    token_id: credential.id().clone(),
                    principal_id: credential.principal_id().clone(),
                    active: principal_enabled && credential.is_active_at(now),
                    revoked: credential.revoked(),
                    valid_until: credential.valid_until(),
                }
            })
            .collect())
    }

    /// Returns a local clear-text token file for an enabled Admin profile.
    /// The principal database itself never contains this value.
    pub fn connection_token_path(&self) -> Result<PathBuf> {
        let directory = self.load()?;
        for credential in directory.credentials.iter().rev() {
            let Some(principal) = directory.principals.get(credential.principal_id()) else {
                continue;
            };
            if !principal.enabled
                || principal.role != Role::Admin
                || !credential.is_active_at(Utc::now())
            {
                continue;
            }
            let path = self.token_path(credential.id());
            if path
                .try_exists()
                .map_err(|error| Error::io(path.clone(), error))?
            {
                return Ok(path);
            }
        }
        Err(Error::InvalidArgument(
            "no enabled admin has a local active profile token; rotate an admin token locally"
                .into(),
        ))
    }

    /// Resolves one active local credential for offline profile export without
    /// exposing its clear-text value through the CLI or principal directory.
    pub fn profile_token_path(&self, token_id: &TokenId) -> Result<PathBuf> {
        self.ensure_bootstrapped()?;
        let directory = self.load()?;
        let credential = directory
            .credentials
            .iter()
            .find(|credential| credential.id() == token_id)
            .ok_or_else(|| Error::InvalidArgument("bearer token id does not exist".into()))?;
        let principal = directory
            .principals
            .get(credential.principal_id())
            .ok_or_else(|| Error::InvalidArgument("token principal does not exist".into()))?;
        if !principal.enabled || !credential.is_active_at(Utc::now()) {
            return Err(Error::InvalidArgument(
                "bearer token is not active for an enabled principal".into(),
            ));
        }
        let path = self.token_path(token_id);
        if !path
            .try_exists()
            .map_err(|error| Error::io(path.clone(), error))?
        {
            return Err(Error::InvalidArgument(
                "the selected credential has no local profile token file".into(),
            ));
        }
        Ok(path)
    }

    fn ensure_bootstrapped(&self) -> Result<()> {
        let _ = self.load_or_bootstrap()?;
        Ok(())
    }

    fn add_generated_token(
        &self,
        mut directory: PrincipalDirectory,
        principal_id: &PrincipalId,
    ) -> Result<TokenProvision> {
        let clear_text = generate_token()?;
        let token_id = directory.add_token(principal_id, &clear_text, None)?;
        let token_path = self.token_path(&token_id);
        let mut encoded = clear_text.into_bytes();
        encoded.push(b'\n');
        atomic_write_secure(&token_path, &encoded)?;
        if let Err(error) = self.save(&directory) {
            let _ = fs::remove_file(&token_path);
            return Err(error);
        }
        self.audit(AuditKind::TokenChanged, Some(principal_id), "created");
        Ok(TokenProvision {
            token_id,
            token_path,
        })
    }

    fn token_path(&self, token_id: &TokenId) -> PathBuf {
        self.state
            .directory()
            .join("profile-tokens")
            .join(format!("{}.token", token_id.as_str()))
    }

    fn load(&self) -> Result<PrincipalDirectory> {
        let path = self.state.principals_path();
        let encoded = read_secure_file(&path, MAX_DIRECTORY_BYTES)?;
        let disk: DiskDirectory = serde_json::from_slice(&encoded).map_err(|error| {
            Error::InvalidArgument(format!(
                "invalid principal directory {}: {error}",
                path.display()
            ))
        })?;
        disk.decode(&path)
    }

    fn save(&self, directory: &PrincipalDirectory) -> Result<()> {
        let disk = DiskDirectory::encode(directory);
        let mut encoded = serde_json::to_vec_pretty(&disk).map_err(|error| {
            Error::Internal(format!("failed to encode principal directory: {error}"))
        })?;
        encoded.push(b'\n');
        let token_directory = self.state.directory().join("profile-tokens");
        ensure_secure_directory(&token_directory)?;
        atomic_write_secure(&self.state.principals_path(), &encoded)
    }

    fn audit(&self, kind: AuditKind, principal: Option<&PrincipalId>, outcome: &str) {
        let result = AuditLog::open(&self.state).and_then(|log| {
            log.record(AuditEvent {
                kind,
                principal_id: principal.map(PrincipalId::as_str),
                query_id: None,
                request_id: None,
                sql_fingerprint: None,
                outcome,
            })
        });
        if let Err(error) = result {
            tracing::error!(%error, "failed to persist local security audit event");
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct DiskDirectory {
    schema_version: u32,
    principals: Vec<DiskPrincipal>,
    tokens: Vec<DiskToken>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct DiskPrincipal {
    principal_id: String,
    role: Role,
    enabled: bool,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct DiskToken {
    token_id: String,
    principal_id: String,
    sha256: String,
    revoked: bool,
    valid_until: Option<DateTime<Utc>>,
}

impl DiskDirectory {
    fn encode(directory: &PrincipalDirectory) -> Self {
        Self {
            schema_version: SCHEMA_VERSION,
            principals: directory
                .principals
                .iter()
                .map(|(id, record)| DiskPrincipal {
                    principal_id: id.as_str().to_owned(),
                    role: record.role,
                    enabled: record.enabled,
                })
                .collect(),
            tokens: directory
                .credentials
                .iter()
                .map(|credential| DiskToken {
                    token_id: credential.id().as_str().to_owned(),
                    principal_id: credential.principal_id().as_str().to_owned(),
                    sha256: encode_digest(credential.digest()),
                    revoked: credential.revoked(),
                    valid_until: credential.valid_until(),
                })
                .collect(),
        }
    }

    fn decode(self, path: &Path) -> Result<PrincipalDirectory> {
        if self.schema_version != SCHEMA_VERSION {
            return Err(Error::InvalidArgument(format!(
                "unsupported principal directory schema_version {} at {}; expected {}",
                self.schema_version,
                path.display(),
                SCHEMA_VERSION
            )));
        }
        let mut directory = PrincipalDirectory::new();
        for principal in self.principals {
            let id = PrincipalId::new(principal.principal_id)?;
            directory.add_principal(id.clone(), principal.role)?;
            directory.set_enabled(&id, principal.enabled)?;
        }
        let mut token_ids = HashSet::new();
        let mut digests = HashSet::new();
        for token in self.tokens {
            let token_id = TokenId::new(token.token_id)?;
            if !token_ids.insert(token_id.as_str().to_owned()) {
                return Err(Error::InvalidArgument(
                    "principal directory contains a duplicate token id".into(),
                ));
            }
            let principal_id = PrincipalId::new(token.principal_id)?;
            if !directory.principals.contains_key(&principal_id) {
                return Err(Error::InvalidArgument(format!(
                    "token {} references missing principal {}",
                    token_id.as_str(),
                    principal_id
                )));
            }
            let digest = decode_digest(&token.sha256)?;
            if !digests.insert(digest) {
                return Err(Error::InvalidArgument(
                    "principal directory contains a duplicate token digest".into(),
                ));
            }
            directory.credentials.push(TokenCredential::from_digest(
                token_id,
                principal_id,
                digest,
                token.revoked,
                token.valid_until,
            ));
        }
        Ok(directory)
    }
}

fn generate_token() -> Result<String> {
    let mut random = [0_u8; 32];
    getrandom::fill(&mut random).map_err(|error| {
        Error::Internal(format!("operating system random source failed: {error}"))
    })?;
    Ok(encode_digest(&random))
}

fn ensure_enabled_admin(directory: &PrincipalDirectory) -> Result<()> {
    if directory
        .principals
        .values()
        .any(|principal| principal.enabled && principal.role == Role::Admin)
    {
        Ok(())
    } else {
        Err(Error::InvalidArgument(
            "at least one enabled admin principal is required".into(),
        ))
    }
}

fn encode_digest(bytes: &[u8; 32]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(64);
    for byte in bytes {
        output.push(char::from(HEX[usize::from(byte >> 4)]));
        output.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    output
}

fn decode_digest(value: &str) -> Result<[u8; 32]> {
    if value.len() != 64 || !value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(Error::InvalidArgument(
            "token sha256 digest must contain 64 lowercase hexadecimal characters".into(),
        ));
    }
    if value.bytes().any(|byte| byte.is_ascii_uppercase()) {
        return Err(Error::InvalidArgument(
            "token sha256 digest must use lowercase hexadecimal".into(),
        ));
    }
    let mut output = [0_u8; 32];
    for (index, pair) in value.as_bytes().chunks_exact(2).enumerate() {
        output[index] = (hex_value(pair[0])? << 4) | hex_value(pair[1])?;
    }
    Ok(output)
}

fn hex_value(value: u8) -> Result<u8> {
    match value {
        b'0'..=b'9' => Ok(value - b'0'),
        b'a'..=b'f' => Ok(value - b'a' + 10),
        _ => Err(Error::InvalidArgument(
            "token sha256 digest contains invalid hexadecimal".into(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use chrono::Utc;

    use super::PrincipalStore;
    use crate::http_shell::security::{PrincipalId, Role, SecurityState};

    fn private_tempdir() -> tempfile::TempDir {
        let directory = tempfile::tempdir().unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(directory.path(), fs::Permissions::from_mode(0o700)).unwrap();
        }
        directory
    }

    #[test]
    fn bootstrap_persists_only_digest_and_reloads_authentication() {
        let temporary = private_tempdir();
        let state =
            SecurityState::open(temporary.path(), "00000000-0000-0000-0000-000000000001").unwrap();
        fs::write(state.directory().join("bearer.token"), "a".repeat(64)).unwrap();
        let store = PrincipalStore::new(state.clone());
        let authenticator = store.load_or_bootstrap().unwrap();
        let token_path = store.connection_token_path().unwrap();
        let token = fs::read_to_string(&token_path).unwrap();
        let principals = fs::read_to_string(state.principals_path()).unwrap();

        assert!(!principals.contains(token.trim()));
        assert!(principals.contains("\"schema_version\": 1"));
        assert!(
            authenticator
                .authenticate(Some(token.trim()), Utc::now())
                .is_ok()
        );
        assert!(
            authenticator
                .authenticate(Some(&"a".repeat(64)), Utc::now())
                .is_err()
        );
        assert_private(&state.principals_path());
        assert_private(&token_path);
    }

    #[test]
    fn create_rotate_and_revoke_survive_reload() {
        let temporary = private_tempdir();
        let state =
            SecurityState::open(temporary.path(), "00000000-0000-0000-0000-000000000002").unwrap();
        let store = PrincipalStore::new(state);
        store.load_or_bootstrap().unwrap();
        let alice = PrincipalId::new("alice").unwrap();
        let first = store.create_principal(alice.clone(), Role::Query).unwrap();
        let first_token = fs::read_to_string(first.token_path()).unwrap();
        let second = store.rotate_token(&alice).unwrap();
        let second_token = fs::read_to_string(second.token_path()).unwrap();
        let listed = store.list_tokens(Some(&alice)).unwrap();
        assert_eq!(listed.len(), 2);
        assert!(listed.iter().all(|token| token.principal_id() == &alice));
        assert!(
            listed
                .iter()
                .any(|token| token.token_id() == second.token_id() && token.active())
        );
        assert_eq!(
            store.profile_token_path(second.token_id()).unwrap(),
            second.token_path()
        );
        let authenticator = store.load_or_bootstrap().unwrap();
        assert!(
            authenticator
                .authenticate(Some(first_token.trim()), Utc::now())
                .is_ok()
        );
        assert!(
            authenticator
                .authenticate(Some(second_token.trim()), Utc::now())
                .is_ok()
        );

        store.revoke_token(first.token_id()).unwrap();
        let authenticator = store.load_or_bootstrap().unwrap();
        assert!(
            authenticator
                .authenticate(Some(first_token.trim()), Utc::now())
                .is_err()
        );
        assert!(
            authenticator
                .authenticate(Some(second_token.trim()), Utc::now())
                .is_ok()
        );
        let listed = store.list_tokens(None).unwrap();
        let revoked = listed
            .iter()
            .find(|token| token.token_id() == first.token_id())
            .unwrap();
        assert!(revoked.revoked());
        assert!(!revoked.active());
        assert!(!first.token_path().exists());
    }

    #[test]
    fn live_reload_applies_durable_revocation_when_profile_cleanup_fails() {
        let temporary = private_tempdir();
        let state =
            SecurityState::open(temporary.path(), "00000000-0000-0000-0000-000000000004").unwrap();
        let store = PrincipalStore::new(state);
        let authenticator = store.load_or_bootstrap().unwrap();
        let admin = PrincipalId::new("admin").unwrap();
        let provision = store.rotate_token(&admin).unwrap();
        let token = fs::read_to_string(provision.token_path()).unwrap();
        store.reload_into(&authenticator).unwrap();
        assert!(
            authenticator
                .authenticate(Some(token.trim()), Utc::now())
                .is_ok()
        );

        fs::remove_file(provision.token_path()).unwrap();
        fs::create_dir(provision.token_path()).unwrap();
        assert!(
            store
                .revoke_token_and_reload(provision.token_id(), &authenticator)
                .is_err()
        );
        assert!(
            authenticator
                .authenticate(Some(token.trim()), Utc::now())
                .is_err()
        );
    }

    #[test]
    fn principal_lifecycle_preserves_one_enabled_admin() {
        let temporary = private_tempdir();
        let state =
            SecurityState::open(temporary.path(), "00000000-0000-0000-0000-000000000003").unwrap();
        let store = PrincipalStore::new(state);
        store.load_or_bootstrap().unwrap();
        let admin = PrincipalId::new("admin").unwrap();
        let backup = PrincipalId::new("backup-admin").unwrap();

        assert!(store.set_enabled(&admin, false).is_err());
        let backup_token = store.create_principal(backup.clone(), Role::Admin).unwrap();
        store.set_enabled(&admin, false).unwrap();
        assert_eq!(
            store.connection_token_path().unwrap(),
            backup_token.token_path()
        );
        store.set_role(&backup, Role::Query).unwrap_err();
        store.set_enabled(&admin, true).unwrap();
        store.set_role(&backup, Role::Query).unwrap();

        let summaries = store.list().unwrap();
        let admin = summaries.iter().find(|value| value.id() == &admin).unwrap();
        let backup = summaries
            .iter()
            .find(|value| value.id() == &backup)
            .unwrap();
        assert!(admin.enabled());
        assert_eq!(backup.role(), Role::Query);
    }

    #[cfg(unix)]
    fn assert_private(path: &Path) {
        use std::os::unix::fs::PermissionsExt;
        let mode = fs::metadata(path).unwrap().permissions().mode();
        assert_eq!(mode & 0o077, 0);
    }

    #[cfg(not(unix))]
    fn assert_private(_path: &Path) {}

    use std::path::Path;
}
