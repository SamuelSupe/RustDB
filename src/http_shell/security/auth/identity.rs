use std::{fmt, str::FromStr};

use serde::{Deserialize, Deserializer, Serialize};

use crate::{Error, Result};

const MAX_PRINCIPAL_ID_BYTES: usize = 128;

/// Stable identity used for query ownership and audit records.
#[derive(Clone, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(transparent)]
pub struct PrincipalId(String);

impl PrincipalId {
    pub fn new(value: impl Into<String>) -> Result<Self> {
        let value = value.into();
        if value.is_empty() || value.len() > MAX_PRINCIPAL_ID_BYTES {
            return Err(Error::InvalidArgument(
                "principal id must contain between 1 and 128 bytes".into(),
            ));
        }
        if !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'@' | b'-'))
        {
            return Err(Error::InvalidArgument(
                "principal id may contain only ASCII letters, digits, '.', '_', '@', and '-'"
                    .into(),
            ));
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for PrincipalId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_tuple("PrincipalId").field(&self.0).finish()
    }
}

impl fmt::Display for PrincipalId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl FromStr for PrincipalId {
    type Err = Error;

    fn from_str(value: &str) -> Result<Self> {
        Self::new(value)
    }
}

impl<'de> Deserialize<'de> for PrincipalId {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::new(value).map_err(serde::de::Error::custom)
    }
}

/// Coarse-grained HTTP shell role. Admin includes query permissions.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[non_exhaustive]
#[serde(rename_all = "snake_case")]
pub enum Role {
    Query,
    Admin,
}

impl Role {
    pub fn allows(self, permission: super::Permission) -> bool {
        match (self, permission) {
            (Self::Admin, _) | (Self::Query, super::Permission::Query) => true,
            (Self::Query, super::Permission::Admin) => false,
        }
    }
}
