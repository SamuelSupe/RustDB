use std::{
    fs,
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
};

use chrono::{Duration, TimeZone, Utc};
use tempfile::TempDir;
use uuid::Uuid;

use super::{
    BearerToken, SecurityState, ServerEndpoint, TlsMaterial, copy_profile_bundle,
    export_profile_bundle, export_profile_bundle_with_token, import_profile_bundle, load_profile,
};

fn state(temp: &TempDir) -> SecurityState {
    SecurityState::open(temp.path().join("state"), &Uuid::new_v4().to_string()).unwrap()
}

fn loopback_endpoint() -> ServerEndpoint {
    ServerEndpoint::resolve(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 9443), None).unwrap()
}

#[test]
fn state_is_isolated_by_database_id_and_reads_native_marker() {
    let temp = TempDir::new().unwrap();
    let first_id = Uuid::new_v4().to_string();
    let second_id = Uuid::new_v4().to_string();
    let first = SecurityState::open(temp.path().join("state"), &first_id).unwrap();
    let second = SecurityState::open(temp.path().join("state"), &second_id).unwrap();
    assert_ne!(first.directory(), second.directory());
    assert_eq!(first.database_id(), first_id);

    let database = temp.path().join("database");
    fs::create_dir(&database).unwrap();
    fs::write(
        database.join(".rustdb"),
        format!(r#"{{"format":"rustdb-native","version":2,"database_id":"{second_id}"}}"#),
    )
    .unwrap();
    let from_marker =
        SecurityState::for_native_database(temp.path().join("state"), &database).unwrap();
    assert_eq!(from_marker.directory(), second.directory());
    assert_private_directory(from_marker.directory());
}

#[test]
fn bearer_token_is_random_redacted_constant_shape_and_rotatable() {
    let temp = TempDir::new().unwrap();
    let state = state(&temp);
    let token = BearerToken::load_or_create(&state).unwrap();
    let first = fs::read_to_string(state.token_path()).unwrap();
    let first = first.trim_end();
    assert_eq!(first.len(), 64);
    assert!(token.verify(first));
    assert!(!token.verify("wrong"));
    assert_eq!(format!("{token:?}"), "BearerToken([REDACTED])");
    assert_private_file(&state.token_path());

    let rotated = BearerToken::rotate(&state).unwrap();
    let second = fs::read_to_string(state.token_path()).unwrap();
    let second = second.trim_end();
    assert_ne!(first, second);
    assert!(!rotated.verify(first));
    assert!(rotated.verify(second));
}

#[test]
fn endpoint_requires_a_safe_advertise_origin_for_remote_listeners() {
    let loopback = loopback_endpoint();
    assert_eq!(loopback.public_url().as_str(), "https://127.0.0.1:9443/");
    assert_eq!(loopback.certificate_host(), "127.0.0.1");
    let ipv6 =
        ServerEndpoint::resolve(SocketAddr::new(IpAddr::V6(Ipv6Addr::LOCALHOST), 9443), None)
            .unwrap();
    assert_eq!(ipv6.public_url().as_str(), "https://[::1]:9443/");
    assert_eq!(ipv6.certificate_host(), "::1");

    let remote = SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 9443);
    assert!(ServerEndpoint::resolve(remote, None).is_err());
    assert!(ServerEndpoint::resolve(remote, Some("https://0.0.0.0:9443")).is_err());
    assert!(
        ServerEndpoint::resolve(
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 9443),
            Some("https://0.0.0.0:9443")
        )
        .is_err()
    );
    assert!(ServerEndpoint::resolve(remote, Some("http://db.example:9443")).is_err());
    assert!(ServerEndpoint::resolve(remote, Some("https://db.example:9443/query")).is_err());
    let endpoint = ServerEndpoint::resolve(remote, Some("https://db.example:9443")).unwrap();
    assert_eq!(endpoint.certificate_host(), "db.example");
}

#[test]
fn tls_keeps_the_ca_and_renews_only_the_short_lived_leaf() {
    let temp = TempDir::new().unwrap();
    let state = state(&temp);
    let endpoint = loopback_endpoint();
    let now = Utc.with_ymd_and_hms(2026, 7, 19, 12, 0, 0).unwrap();

    let first = TlsMaterial::load_or_create_at(&state, &endpoint, now).unwrap();
    assert!(first.renewed());
    assert_eq!(first.public_url(), endpoint.public_url());
    assert!(
        first
            .server_certificate_pem()
            .unwrap()
            .contains("BEGIN CERTIFICATE")
    );
    assert!(
        first
            .server_private_key_pem()
            .unwrap()
            .contains("BEGIN PRIVATE KEY")
    );
    let ca = fs::read(first.ca_certificate_path()).unwrap();
    let identity = fs::read(first.server_identity_path()).unwrap();

    let reused =
        TlsMaterial::load_or_create_at(&state, &endpoint, now + Duration::days(1)).unwrap();
    assert!(!reused.renewed());
    assert_eq!(fs::read(reused.ca_certificate_path()).unwrap(), ca);
    assert_eq!(fs::read(reused.server_identity_path()).unwrap(), identity);

    fs::remove_file(state.ca_certificate_path()).unwrap();
    let recovered =
        TlsMaterial::load_or_create_at(&state, &endpoint, now + Duration::days(2)).unwrap();
    assert!(!recovered.renewed());
    assert_eq!(fs::read(recovered.ca_certificate_path()).unwrap(), ca);

    let renewed =
        TlsMaterial::load_or_create_at(&state, &endpoint, now + Duration::days(25)).unwrap();
    assert!(renewed.renewed());
    assert_eq!(fs::read(renewed.ca_certificate_path()).unwrap(), ca);
    assert_ne!(fs::read(renewed.server_identity_path()).unwrap(), identity);
    assert_private_file(renewed.ca_certificate_path());
    assert_private_file(renewed.server_identity_path());
}

#[test]
fn profile_bundle_round_trips_without_embedding_the_token_in_metadata() {
    let temp = TempDir::new().unwrap();
    let state = state(&temp);
    let endpoint = loopback_endpoint();
    let tls = TlsMaterial::load_or_create(&state, &endpoint).unwrap();
    let token = BearerToken::load_or_create(&state).unwrap();
    let clear_text = fs::read_to_string(state.token_path()).unwrap();
    let clear_text = clear_text.trim_end();
    assert!(token.verify(clear_text));

    let bundle = temp.path().join("connection-bundle");
    export_profile_bundle(
        &bundle,
        endpoint.public_url(),
        tls.ca_certificate_path(),
        state.token_path(),
    )
    .unwrap();
    assert_private_directory(&bundle);
    assert_private_file(&bundle.join("profile.json"));
    let manifest = fs::read_to_string(bundle.join("profile.json")).unwrap();
    assert!(!manifest.contains(clear_text));

    let copied_bundle = temp.path().join("copied-bundle");
    copy_profile_bundle(&bundle, &copied_bundle).unwrap();
    assert!(copy_profile_bundle(&bundle, &copied_bundle).is_err());
    assert_private_directory(&copied_bundle);

    let profiles = temp.path().join("profiles");
    let imported = import_profile_bundle(&profiles, "analytics", &copied_bundle).unwrap();
    assert_eq!(imported.server_url(), endpoint.public_url());
    assert_eq!(imported, load_profile(&profiles, "analytics").unwrap());
    assert!(
        BearerToken::load(imported.token_path())
            .unwrap()
            .verify(clear_text)
    );
    assert!(import_profile_bundle(&profiles, "../escape", &bundle).is_err());
    assert!(!format!("{imported:?}").contains(clear_text));

    BearerToken::rotate(&state).unwrap();
    let rotated = fs::read_to_string(state.token_path()).unwrap();
    let alternate_bundle = temp.path().join("alternate-bundle");
    export_profile_bundle_with_token(&bundle, &alternate_bundle, state.token_path()).unwrap();
    let alternate = import_profile_bundle(&profiles, "alternate", &alternate_bundle).unwrap();
    assert_eq!(alternate.server_url(), endpoint.public_url());
    assert_eq!(fs::read_to_string(alternate.token_path()).unwrap(), rotated);
}

#[cfg(unix)]
#[test]
fn symlinked_state_and_credentials_are_rejected() {
    use std::os::unix::fs::symlink;

    let temp = TempDir::new().unwrap();
    let real = temp.path().join("real");
    fs::create_dir(&real).unwrap();
    let linked_root = temp.path().join("linked");
    symlink(&real, &linked_root).unwrap();
    assert!(SecurityState::open(&linked_root, &Uuid::new_v4().to_string()).is_err());

    let state = state(&temp);
    let outside = temp.path().join("outside-token");
    fs::write(&outside, "0".repeat(64)).unwrap();
    symlink(&outside, state.token_path()).unwrap();
    assert!(BearerToken::load_or_create(&state).is_err());
    assert_eq!(fs::read_to_string(outside).unwrap(), "0".repeat(64));
}

#[cfg(unix)]
fn assert_private_directory(path: &std::path::Path) {
    use std::os::unix::fs::PermissionsExt;
    assert_eq!(
        fs::metadata(path).unwrap().permissions().mode() & 0o777,
        0o700
    );
}

#[cfg(not(unix))]
fn assert_private_directory(_path: &std::path::Path) {}

#[cfg(unix)]
fn assert_private_file(path: &std::path::Path) {
    use std::os::unix::fs::PermissionsExt;
    assert_eq!(
        fs::metadata(path).unwrap().permissions().mode() & 0o777,
        0o600
    );
}

#[cfg(not(unix))]
fn assert_private_file(_path: &std::path::Path) {}
