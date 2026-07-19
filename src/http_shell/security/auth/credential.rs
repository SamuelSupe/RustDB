use std::{fmt, str::FromStr};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Deserializer, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::{Error, Result};

use super::PrincipalId;

const MIN_TOKEN_BYTES: usize = 32;
const MAX_TOKEN_BYTES: usize = 1_024;

/// Opaque, non-secret handle used to revoke one credential.
#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize)]
#[serde(transparent)]
pub struct TokenId(String);

impl TokenId {
    pub(crate) fn generate() -> Self {
        Self(Uuid::new_v4().to_string())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn new(value: impl Into<String>) -> Result<Self> {
        let value = value.into();
        Uuid::parse_str(&value)
            .map(|_| Self(value))
            .map_err(|_| Error::InvalidArgument("token id must be a UUID".into()))
    }
}

impl FromStr for TokenId {
    type Err = Error;

    fn from_str(value: &str) -> Result<Self> {
        Self::new(value)
    }
}

impl<'de> Deserialize<'de> for TokenId {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::new(value).map_err(serde::de::Error::custom)
    }
}

#[derive(Clone)]
pub(super) struct TokenCredential {
    id: TokenId,
    principal_id: PrincipalId,
    digest: [u8; 32],
    revoked: bool,
    valid_until: Option<DateTime<Utc>>,
}

impl fmt::Debug for TokenCredential {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TokenCredential")
            .field("id", &self.id)
            .field("principal_id", &self.principal_id)
            .field("digest", &"[REDACTED]")
            .field("revoked", &self.revoked)
            .field("valid_until", &self.valid_until)
            .finish()
    }
}

impl TokenCredential {
    pub(super) fn new(
        principal_id: PrincipalId,
        bearer_token: &str,
        valid_until: Option<DateTime<Utc>>,
    ) -> Result<Self> {
        validate_bearer_token(bearer_token)?;
        Ok(Self {
            id: TokenId::generate(),
            principal_id,
            digest: digest(bearer_token),
            revoked: false,
            valid_until,
        })
    }

    pub(super) fn id(&self) -> &TokenId {
        &self.id
    }

    pub(super) fn from_digest(
        id: TokenId,
        principal_id: PrincipalId,
        digest: [u8; 32],
        revoked: bool,
        valid_until: Option<DateTime<Utc>>,
    ) -> Self {
        Self {
            id,
            principal_id,
            digest,
            revoked,
            valid_until,
        }
    }

    pub(super) fn principal_id(&self) -> &PrincipalId {
        &self.principal_id
    }

    pub(super) fn digest(&self) -> &[u8; 32] {
        &self.digest
    }

    pub(super) fn revoked(&self) -> bool {
        self.revoked
    }

    pub(super) fn valid_until(&self) -> Option<DateTime<Utc>> {
        self.valid_until
    }

    pub(super) fn matches_digest(&self, presented: &[u8; 32]) -> bool {
        constant_time_equal(&self.digest, presented)
    }

    pub(super) fn is_active_at(&self, now: DateTime<Utc>) -> bool {
        !self.revoked && self.valid_until.is_none_or(|deadline| now < deadline)
    }

    pub(super) fn revoke(&mut self) {
        self.revoked = true;
    }

    pub(super) fn has_digest(&self, candidate: &[u8; 32]) -> bool {
        constant_time_equal(&self.digest, candidate)
    }
}

pub(super) fn digest(token: &str) -> [u8; 32] {
    Sha256::digest(token.as_bytes()).into()
}

fn validate_bearer_token(token: &str) -> Result<()> {
    if !(MIN_TOKEN_BYTES..=MAX_TOKEN_BYTES).contains(&token.len()) {
        return Err(Error::InvalidArgument(
            "bearer token must contain between 32 and 1024 bytes".into(),
        ));
    }
    if !token.bytes().all(|byte| byte.is_ascii_graphic()) {
        return Err(Error::InvalidArgument(
            "bearer token must contain only visible ASCII characters".into(),
        ));
    }
    Ok(())
}

fn constant_time_equal(expected: &[u8; 32], actual: &[u8; 32]) -> bool {
    let mut difference = 0_u8;
    for (expected, actual) in expected.iter().zip(actual) {
        difference |= *expected ^ *actual;
    }
    difference == 0
}
