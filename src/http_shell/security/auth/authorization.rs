use super::{PrincipalId, Role};
use serde::{Deserialize, Serialize};

/// Permission checked at an HTTP operation boundary.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum Permission {
    Query,
    Admin,
}

/// Identity established by one authentication decision.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum AuthenticatedActor {
    Principal {
        id: PrincipalId,
        role: Role,
    },
    /// Present only when the server was started in the explicit no-auth mode.
    AuthenticationDisabled,
}

impl AuthenticatedActor {
    pub fn principal_id(&self) -> Option<&PrincipalId> {
        match self {
            Self::Principal { id, .. } => Some(id),
            Self::AuthenticationDisabled => None,
        }
    }

    pub fn role(&self) -> Option<Role> {
        match self {
            Self::Principal { role, .. } => Some(*role),
            Self::AuthenticationDisabled => None,
        }
    }

    pub fn is_allowed(&self, permission: Permission) -> bool {
        match self {
            Self::Principal { role, .. } => role.allows(permission),
            Self::AuthenticationDisabled => permission == Permission::Query,
        }
    }

    pub fn query_owner(&self) -> QueryOwner {
        match self {
            Self::Principal { id, .. } => QueryOwner::Principal(id.clone()),
            Self::AuthenticationDisabled => QueryOwner::AuthenticationDisabled,
        }
    }

    /// Admins can access every Query; query-role principals only their own.
    pub fn can_access_query(&self, owner: &QueryOwner) -> bool {
        match self {
            Self::Principal {
                role: Role::Admin, ..
            } => true,
            Self::Principal { id, .. } => owner == &QueryOwner::Principal(id.clone()),
            Self::AuthenticationDisabled => owner == &QueryOwner::AuthenticationDisabled,
        }
    }
}

/// Persistable ownership value attached to every Query.
#[derive(Clone, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[non_exhaustive]
#[serde(rename_all = "snake_case", tag = "kind", content = "principal_id")]
pub enum QueryOwner {
    Principal(PrincipalId),
    AuthenticationDisabled,
}

impl QueryOwner {
    pub(crate) fn audit_id(&self) -> &str {
        match self {
            Self::Principal(id) => id.as_str(),
            Self::AuthenticationDisabled => "anonymous",
        }
    }
}
