use std::time::Duration;

use url::Url;

use super::RemoteClient;

#[test]
fn builder_rejects_insecure_urls_and_zero_attempts() {
    let insecure = RemoteClient::builder(Url::parse("http://127.0.0.1:9000/").unwrap(), "secret")
        .build()
        .unwrap_err();
    assert_eq!(insecure.code.as_ref(), "client.configuration");

    let attempts = RemoteClient::builder(Url::parse("https://127.0.0.1:9000/").unwrap(), "secret")
        .submit_attempts(0)
        .build()
        .unwrap_err();
    assert!(attempts.message.contains("attempts"));
}

#[test]
fn debug_output_never_exposes_the_token() {
    let builder = RemoteClient::builder(
        Url::parse("https://127.0.0.1:9000/").unwrap(),
        "extremely-private-token",
    )
    .connect_timeout(Duration::from_secs(2));
    let debug = format!("{builder:?}");
    assert!(debug.contains("[REDACTED]"));
    assert!(!debug.contains("extremely-private-token"));
}

#[test]
fn query_handle_keeps_only_a_query_identity_in_debug_output() {
    let client = RemoteClient::builder(
        Url::parse("https://127.0.0.1:9000/").unwrap(),
        "extremely-private-token",
    )
    .build()
    .unwrap();
    let handle = client.query("query-123");
    assert_eq!(handle.query_id(), "query-123");
    let debug = format!("{handle:?}");
    assert!(debug.contains("query-123"));
    assert!(!debug.contains("extremely-private-token"));
}

#[test]
fn client_resolves_only_v2_routes() {
    let client = RemoteClient::builder(
        Url::parse("https://127.0.0.1:9000/").unwrap(),
        "extremely-private-token",
    )
    .build()
    .unwrap();
    assert_eq!(client.url("info").unwrap().path(), "/v2/info");
    assert_eq!(
        client.url("queries/query-1").unwrap().path(),
        "/v2/queries/query-1"
    );
}
