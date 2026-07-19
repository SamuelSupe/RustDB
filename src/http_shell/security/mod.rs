//! Local security material for the read-only HTTP shell.
//!
//! This module owns credentials on disk. Secret values deliberately do not
//! implement serialization or an exposing `Debug` representation.

mod audit;
mod auth;
mod endpoint;
mod files;
mod profile;
mod state;
mod tls;
mod token;

pub(crate) use audit::{AuditEvent, AuditKind, AuditLog};
pub use auth::{
    AuthenticatedActor, AuthenticationError, Authenticator, Permission, PrincipalDirectory,
    PrincipalId, PrincipalStore, PrincipalSummary, QueryOwner, Role, TokenId, TokenProvision,
    TokenSummary,
};
pub use endpoint::ServerEndpoint;
pub(crate) use profile::write_managed_profile_bundle;
pub use profile::{
    ClientProfile, copy_profile_bundle, export_profile_bundle, export_profile_bundle_with_token,
    import_profile_bundle, load_profile,
};
pub use state::{SecurityState, default_profile_root, default_state_root};
pub use tls::TlsMaterial;
pub use token::BearerToken;

#[cfg(test)]
mod tests;
