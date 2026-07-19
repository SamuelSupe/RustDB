use std::{
    collections::{BTreeMap, btree_map::Entry},
    fmt,
    sync::Arc,
};

use chrono::{DateTime, Utc};
use parking_lot::RwLock;

use crate::{Error, Result};

use super::{
    AuthenticatedActor, PrincipalId, Role, TokenId,
    credential::{TokenCredential, digest},
};

#[derive(Clone, Debug)]
pub(super) struct PrincipalRecord {
    pub(super) role: Role,
    pub(super) enabled: bool,
}

/// Mutable principal and digest-only credential directory.
#[derive(Clone, Default)]
pub struct PrincipalDirectory {
    pub(super) principals: BTreeMap<PrincipalId, PrincipalRecord>,
    pub(super) credentials: Vec<TokenCredential>,
}

impl fmt::Debug for PrincipalDirectory {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PrincipalDirectory")
            .field("principals", &self.principals)
            .field("credentials", &self.credentials)
            .finish()
    }
}

impl PrincipalDirectory {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add_principal(&mut self, id: PrincipalId, role: Role) -> Result<()> {
        match self.principals.entry(id.clone()) {
            Entry::Vacant(entry) => {
                entry.insert(PrincipalRecord {
                    role,
                    enabled: true,
                });
                Ok(())
            }
            Entry::Occupied(_) => Err(Error::InvalidArgument(format!(
                "principal {id} already exists"
            ))),
        }
    }

    pub fn set_enabled(&mut self, id: &PrincipalId, enabled: bool) -> Result<()> {
        self.principal_mut(id)?.enabled = enabled;
        Ok(())
    }

    pub fn set_role(&mut self, id: &PrincipalId, role: Role) -> Result<()> {
        self.principal_mut(id)?.role = role;
        Ok(())
    }

    /// Adds a credential without invalidating existing credentials. This
    /// overlap is what makes token rotation interruption-free.
    pub fn add_token(
        &mut self,
        principal_id: &PrincipalId,
        bearer_token: &str,
        valid_until: Option<DateTime<Utc>>,
    ) -> Result<TokenId> {
        self.principal(principal_id)?;
        let candidate = digest(bearer_token);
        if self
            .credentials
            .iter()
            .any(|credential| credential.has_digest(&candidate))
        {
            return Err(Error::InvalidArgument(
                "bearer token is already registered".into(),
            ));
        }
        let credential = TokenCredential::new(principal_id.clone(), bearer_token, valid_until)?;
        let token_id = credential.id().clone();
        self.credentials.push(credential);
        Ok(token_id)
    }

    pub fn revoke_token(&mut self, token_id: &TokenId) -> Result<()> {
        let credential = self
            .credentials
            .iter_mut()
            .find(|credential| credential.id() == token_id)
            .ok_or_else(|| Error::InvalidArgument("bearer token id does not exist".into()))?;
        credential.revoke();
        Ok(())
    }

    pub(super) fn token_principal(&self, token_id: &TokenId) -> Result<&PrincipalId> {
        self.credentials
            .iter()
            .find(|credential| credential.id() == token_id)
            .map(TokenCredential::principal_id)
            .ok_or_else(|| Error::InvalidArgument("bearer token id does not exist".into()))
    }

    fn authenticate(&self, bearer_token: &str, now: DateTime<Utc>) -> Option<AuthenticatedActor> {
        let presented = digest(bearer_token);
        let mut principal_id = None;
        // Do not exit early: every credential performs the same fixed-size
        // digest comparison regardless of its position in the directory.
        for credential in &self.credentials {
            let matches = credential.matches_digest(&presented);
            let active = credential.is_active_at(now);
            if matches & active {
                principal_id = Some(credential.principal_id());
            }
        }
        let principal_id = principal_id?;
        let principal = self.principals.get(principal_id)?;
        principal.enabled.then(|| AuthenticatedActor::Principal {
            id: principal_id.clone(),
            role: principal.role,
        })
    }

    fn principal(&self, id: &PrincipalId) -> Result<&PrincipalRecord> {
        self.principals
            .get(id)
            .ok_or_else(|| Error::InvalidArgument(format!("principal {id} does not exist")))
    }

    fn principal_mut(&mut self, id: &PrincipalId) -> Result<&mut PrincipalRecord> {
        self.principals
            .get_mut(id)
            .ok_or_else(|| Error::InvalidArgument(format!("principal {id} does not exist")))
    }
}

/// Public authentication failure deliberately hides token lifecycle details.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
#[non_exhaustive]
pub enum AuthenticationError {
    #[error("Bearer authentication is required")]
    Required,
    #[error("Bearer token is invalid")]
    Invalid,
}

#[derive(Clone)]
enum AuthenticatorMode {
    Required(Arc<RwLock<PrincipalDirectory>>),
    Disabled,
}

/// Thread-safe authentication entry point used by request middleware.
#[derive(Clone)]
pub struct Authenticator {
    mode: AuthenticatorMode,
}

impl fmt::Debug for Authenticator {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Authenticator")
            .field(
                "mode",
                &match &self.mode {
                    AuthenticatorMode::Required(_) => "required",
                    AuthenticatorMode::Disabled => "disabled",
                },
            )
            .finish()
    }
}

impl Authenticator {
    pub fn required(directory: PrincipalDirectory) -> Self {
        Self {
            mode: AuthenticatorMode::Required(Arc::new(RwLock::new(directory))),
        }
    }

    /// Constructs the explicit no-auth mode. Server defaults remain unchanged;
    /// callers must select this mode deliberately.
    pub fn explicitly_disabled() -> Self {
        Self {
            mode: AuthenticatorMode::Disabled,
        }
    }

    pub fn authenticate(
        &self,
        bearer_token: Option<&str>,
        now: DateTime<Utc>,
    ) -> std::result::Result<AuthenticatedActor, AuthenticationError> {
        match &self.mode {
            AuthenticatorMode::Disabled => Ok(AuthenticatedActor::AuthenticationDisabled),
            AuthenticatorMode::Required(directory) => {
                let bearer_token = bearer_token.ok_or(AuthenticationError::Required)?;
                directory
                    .read()
                    .authenticate(bearer_token, now)
                    .ok_or(AuthenticationError::Invalid)
            }
        }
    }

    /// Mutates a required-mode directory for local administration. Disabled
    /// mode has no principal state to mutate.
    pub fn update_directory<T>(
        &self,
        update: impl FnOnce(&mut PrincipalDirectory) -> Result<T>,
    ) -> Result<T> {
        match &self.mode {
            AuthenticatorMode::Required(directory) => update(&mut directory.write()),
            AuthenticatorMode::Disabled => Err(Error::InvalidArgument(
                "authentication is explicitly disabled".into(),
            )),
        }
    }
}
