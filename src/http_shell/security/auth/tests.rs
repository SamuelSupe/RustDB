use chrono::{TimeZone, Utc};

use super::{
    AuthenticatedActor, AuthenticationError, Authenticator, Permission, PrincipalDirectory,
    PrincipalId, QueryOwner, Role,
};

const ALICE_TOKEN: &str = "alice-0123456789abcdef0123456789abcdef";
const ALICE_ROTATED: &str = "alice-fedcba9876543210fedcba9876543210";
const BOB_TOKEN: &str = "bob---0123456789abcdef0123456789abcdef";
const ADMIN_TOKEN: &str = "admin-0123456789abcdef0123456789abcdef";

fn id(value: &str) -> PrincipalId {
    PrincipalId::new(value).unwrap()
}

fn now() -> chrono::DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 7, 19, 12, 0, 0).unwrap()
}

#[test]
fn principal_ids_and_role_permissions_are_strict() {
    assert_eq!(id("ops@example.com").as_str(), "ops@example.com");
    assert!(PrincipalId::new("").is_err());
    assert!(PrincipalId::new("has space").is_err());
    assert!(PrincipalId::new("路径").is_err());
    assert!(Role::Query.allows(Permission::Query));
    assert!(!Role::Query.allows(Permission::Admin));
    assert!(Role::Admin.allows(Permission::Query));
    assert!(Role::Admin.allows(Permission::Admin));
}

#[test]
fn required_mode_hides_all_invalid_credential_states() {
    let alice = id("alice");
    let mut directory = PrincipalDirectory::new();
    directory.add_principal(alice.clone(), Role::Query).unwrap();
    directory.add_token(&alice, ALICE_TOKEN, None).unwrap();
    let authenticator = Authenticator::required(directory);

    assert_eq!(
        authenticator.authenticate(None, now()).unwrap_err(),
        AuthenticationError::Required
    );
    assert_eq!(
        authenticator
            .authenticate(Some("not-a-registered-token-with-32-bytes"), now())
            .unwrap_err(),
        AuthenticationError::Invalid
    );
    authenticator
        .update_directory(|directory| directory.set_enabled(&alice, false))
        .unwrap();
    assert_eq!(
        authenticator
            .authenticate(Some(ALICE_TOKEN), now())
            .unwrap_err(),
        AuthenticationError::Invalid
    );
}

#[test]
fn multiple_tokens_allow_overlapping_rotation_and_independent_revocation() {
    let alice = id("alice");
    let mut directory = PrincipalDirectory::new();
    directory.add_principal(alice.clone(), Role::Query).unwrap();
    let first = directory.add_token(&alice, ALICE_TOKEN, None).unwrap();
    directory.add_token(&alice, ALICE_ROTATED, None).unwrap();
    assert!(!format!("{directory:?}").contains(ALICE_TOKEN));
    assert!(!format!("{directory:?}").contains(ALICE_ROTATED));

    let authenticator = Authenticator::required(directory);
    assert!(authenticator.authenticate(Some(ALICE_TOKEN), now()).is_ok());
    assert!(
        authenticator
            .authenticate(Some(ALICE_ROTATED), now())
            .is_ok()
    );
    authenticator
        .update_directory(|directory| directory.revoke_token(&first))
        .unwrap();
    assert_eq!(
        authenticator
            .authenticate(Some(ALICE_TOKEN), now())
            .unwrap_err(),
        AuthenticationError::Invalid
    );
    assert!(
        authenticator
            .authenticate(Some(ALICE_ROTATED), now())
            .is_ok()
    );
}

#[test]
fn token_deadline_is_exclusive_and_duplicate_tokens_are_rejected() {
    let alice = id("alice");
    let bob = id("bob");
    let mut directory = PrincipalDirectory::new();
    directory.add_principal(alice.clone(), Role::Query).unwrap();
    directory.add_principal(bob.clone(), Role::Query).unwrap();
    directory
        .add_token(&alice, ALICE_TOKEN, Some(now()))
        .unwrap();
    assert!(directory.add_token(&bob, ALICE_TOKEN, None).is_err());
    let authenticator = Authenticator::required(directory);
    assert_eq!(
        authenticator
            .authenticate(Some(ALICE_TOKEN), now())
            .unwrap_err(),
        AuthenticationError::Invalid
    );
}

#[test]
fn roles_and_query_ownership_are_enforced() {
    let alice = id("alice");
    let bob = id("bob");
    let admin = id("admin");
    let mut directory = PrincipalDirectory::new();
    directory.add_principal(alice.clone(), Role::Query).unwrap();
    directory.add_principal(bob.clone(), Role::Query).unwrap();
    directory.add_principal(admin.clone(), Role::Admin).unwrap();
    directory.add_token(&alice, ALICE_TOKEN, None).unwrap();
    directory.add_token(&bob, BOB_TOKEN, None).unwrap();
    directory.add_token(&admin, ADMIN_TOKEN, None).unwrap();
    let authenticator = Authenticator::required(directory);
    let alice_actor = authenticator
        .authenticate(Some(ALICE_TOKEN), now())
        .unwrap();
    let bob_actor = authenticator.authenticate(Some(BOB_TOKEN), now()).unwrap();
    let admin_actor = authenticator
        .authenticate(Some(ADMIN_TOKEN), now())
        .unwrap();

    assert!(alice_actor.is_allowed(Permission::Query));
    assert!(!alice_actor.is_allowed(Permission::Admin));
    assert!(admin_actor.is_allowed(Permission::Admin));
    let owner = alice_actor.query_owner();
    assert!(alice_actor.can_access_query(&owner));
    assert!(!bob_actor.can_access_query(&owner));
    assert!(admin_actor.can_access_query(&owner));

    assert_eq!(alice_actor.principal_id(), Some(&alice));
    assert_eq!(alice_actor.role(), Some(Role::Query));
}

#[test]
fn explicit_no_auth_mode_does_not_silently_grant_admin() {
    let authenticator = Authenticator::explicitly_disabled();
    let actor = authenticator.authenticate(None, now()).unwrap();
    assert_eq!(actor, AuthenticatedActor::AuthenticationDisabled);
    assert!(actor.is_allowed(Permission::Query));
    assert!(!actor.is_allowed(Permission::Admin));
    assert!(actor.can_access_query(&QueryOwner::AuthenticationDisabled));
    assert!(!actor.can_access_query(&QueryOwner::Principal(id("alice"))));
    assert!(authenticator.update_directory(|_| Ok(())).is_err());
}

#[test]
fn role_and_enabled_state_changes_apply_without_restarting_authenticator() {
    let alice = id("alice");
    let mut directory = PrincipalDirectory::new();
    directory.add_principal(alice.clone(), Role::Query).unwrap();
    directory.add_token(&alice, ALICE_TOKEN, None).unwrap();
    let authenticator = Authenticator::required(directory);

    authenticator
        .update_directory(|directory| directory.set_role(&alice, Role::Admin))
        .unwrap();
    let actor = authenticator
        .authenticate(Some(ALICE_TOKEN), now())
        .unwrap();
    assert!(actor.is_allowed(Permission::Admin));

    authenticator
        .update_directory(|directory| directory.set_enabled(&alice, false))
        .unwrap();
    assert!(
        authenticator
            .authenticate(Some(ALICE_TOKEN), now())
            .is_err()
    );
    authenticator
        .update_directory(|directory| directory.set_enabled(&alice, true))
        .unwrap();
    assert!(authenticator.authenticate(Some(ALICE_TOKEN), now()).is_ok());
}
