//! Multi-principal authentication and authorization primitives.
//!
//! Clear-text bearer tokens are accepted only while credentials are created or
//! checked. The directory retains SHA-256 digests and never stores the token.

mod authorization;
mod credential;
mod directory;
mod identity;
mod store;

pub use authorization::{AuthenticatedActor, Permission, QueryOwner};
pub use credential::TokenId;
pub use directory::{AuthenticationError, Authenticator, PrincipalDirectory};
pub use identity::{PrincipalId, Role};
pub use store::{PrincipalStore, PrincipalSummary, TokenProvision, TokenSummary};

#[cfg(test)]
mod tests;
