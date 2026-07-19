use std::{net::TcpListener, path::PathBuf, time::Duration};

use futures::StreamExt;
use rustdb::{
    Engine, EngineConfig,
    http_shell::{
        HttpServerConfig, QueryRequest, QueryState, RemoteClient, TypedParameter,
        security::import_profile_bundle, serve_with_shutdown,
    },
};
use serde_json::json;

#[tokio::test]
async fn remote_shell_executes_typed_read_only_queries_over_tls() {
    let temporary = tempfile::tempdir().unwrap();
    let database = temporary.path().join("database");
    let state_root = temporary.path().join("state");
    let profile_root = temporary.path().join("profiles");
    let engine = Engine::open(&database, EngineConfig::default()).unwrap();
    let hidden_csv = temporary.path().join("hidden.csv");
    std::fs::write(&hidden_csv, "value\nsecret\n").unwrap();
    let create_view = format!(
        "CREATE VIEW hidden_file AS SELECT * FROM read_csv('{}')",
        hidden_csv.display()
    );
    drain(engine.session().execute(&create_view).await.unwrap()).await;
    let listen = free_loopback_address();
    let config = HttpServerConfig {
        listen,
        state_root: state_root.clone(),
        result_global_limit_bytes: Some(1024 * 1024),
        result_query_limit_bytes: Some(64 * 1024),
        ..HttpServerConfig::default()
    };
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(serve_with_shutdown(engine, config, async move {
        let _ = shutdown_rx.await;
    }));

    let bundle = wait_for_bundle(&state_root, &server).await;
    let profile = import_profile_bundle(&profile_root, "test", &bundle).unwrap();
    let client = RemoteClient::from_profile(&profile).unwrap();
    wait_for_server(&client, &server).await;

    let ca = reqwest::Certificate::from_pem(&std::fs::read(profile.ca_path()).unwrap()).unwrap();
    let raw = reqwest::Client::builder()
        .https_only(true)
        .tls_certs_only([ca])
        .timeout(Duration::from_secs(5))
        .build()
        .unwrap();
    let info_url = profile.server_url().join("v1/info").unwrap();
    let unauthorized = raw
        .get(info_url)
        .bearer_auth("0".repeat(64))
        .send()
        .await
        .unwrap();
    assert_eq!(unauthorized.status(), reqwest::StatusCode::UNAUTHORIZED);

    let token = std::fs::read_to_string(profile.token_path()).unwrap();
    let submit_url = profile.server_url().join("v1/queries").unwrap();
    let replay_request = QueryRequest {
        sql: "SELECT 7 AS replay_value".into(),
        parameters: Vec::new(),
        timeout_ms: None,
    };
    let first = raw
        .post(submit_url.clone())
        .bearer_auth(token.trim())
        .header("idempotency-key", "0123456789abcdef")
        .json(&replay_request)
        .send()
        .await
        .unwrap();
    assert_eq!(first.status(), reqwest::StatusCode::ACCEPTED);
    let first: rustdb::http_shell::SubmitResponse = first.json().await.unwrap();
    let replay = raw
        .post(submit_url.clone())
        .bearer_auth(token.trim())
        .header("idempotency-key", "0123456789abcdef")
        .json(&replay_request)
        .send()
        .await
        .unwrap();
    let replay: rustdb::http_shell::SubmitResponse = replay.json().await.unwrap();
    assert_eq!(replay.query_id, first.query_id);
    assert!(replay.replayed);
    let conflict = raw
        .post(submit_url)
        .bearer_auth(token.trim())
        .header("idempotency-key", "0123456789abcdef")
        .json(&QueryRequest {
            sql: "SELECT 8".into(),
            parameters: Vec::new(),
            timeout_ms: None,
        })
        .send()
        .await
        .unwrap();
    assert_eq!(conflict.status(), reqwest::StatusCode::CONFLICT);
    let replay_status = client
        .wait(&first.query_id, Duration::from_millis(10))
        .await
        .unwrap();
    assert_eq!(replay_status.state, QueryState::Succeeded);
    client.delete(&first.query_id).await.unwrap();

    let paged = client
        .submit(&QueryRequest {
            sql: "VALUES (1), (2), (3)".into(),
            parameters: Vec::new(),
            timeout_ms: None,
        })
        .await
        .unwrap();
    let paged_id = paged.query_id.clone();
    assert_eq!(
        client
            .wait(&paged_id, Duration::from_millis(10))
            .await
            .unwrap()
            .state,
        QueryState::Succeeded
    );
    let mut first_page_url = profile
        .server_url()
        .join(&format!("v1/queries/{paged_id}/results"))
        .unwrap();
    first_page_url.query_pairs_mut().append_pair("limit", "1");
    let first_page: serde_json::Value = raw
        .get(first_page_url.clone())
        .bearer_auth(token.trim())
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let repeated: serde_json::Value = raw
        .get(first_page_url)
        .bearer_auth(token.trim())
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(first_page, repeated);
    let cursor = first_page["page"]["next_cursor"].as_str().unwrap();
    let mut next_url = profile
        .server_url()
        .join(&format!("v1/queries/{paged_id}/results"))
        .unwrap();
    next_url
        .query_pairs_mut()
        .append_pair("cursor", cursor)
        .append_pair("limit", "1");
    let next: serde_json::Value = raw
        .get(next_url)
        .bearer_auth(token.trim())
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(next["page"]["offset"], 1);
    let mut ndjson_url = profile
        .server_url()
        .join(&format!("v1/queries/{paged_id}/results"))
        .unwrap();
    ndjson_url
        .query_pairs_mut()
        .append_pair("offset", "2")
        .append_pair("limit", "1");
    let ndjson = raw
        .get(ndjson_url)
        .bearer_auth(token.trim())
        .header("accept", "application/x-ndjson; charset=utf-8")
        .send()
        .await
        .unwrap();
    assert_eq!(ndjson.headers()["content-type"], "application/x-ndjson");
    let lines = ndjson.text().await.unwrap();
    assert_eq!(lines.lines().count(), 3);
    client.delete(&paged_id).await.unwrap();

    let accepted = client
        .submit(&QueryRequest {
            sql: "SELECT $1 AS exact_value".into(),
            parameters: vec![TypedParameter {
                data_type: "int64".into(),
                value: json!(9_007_199_254_740_993_i64),
                precision: None,
                scale: None,
            }],
            timeout_ms: Some(10_000),
        })
        .await
        .unwrap();
    let status = client
        .wait(&accepted.query_id, Duration::from_millis(10))
        .await
        .unwrap();
    assert_eq!(status.state, QueryState::Succeeded);
    let page = client.page(&accepted.query_id, None, 100).await.unwrap();
    assert_eq!(page.schema[0].name, "exact_value");
    assert_eq!(page.rows, vec![vec![json!(9_007_199_254_740_993_i64)]]);
    assert!(page.page.complete);
    client.delete(&accepted.query_id).await.unwrap();
    client.delete(&accepted.query_id).await.unwrap();

    let forbidden = client
        .submit(&QueryRequest {
            sql: "SELECT * FROM read_csv('/tmp/secret.csv')".into(),
            parameters: Vec::new(),
            timeout_ms: None,
        })
        .await
        .unwrap_err();
    assert!(forbidden.to_string().contains("sql.unsupported"));

    let hidden = client
        .submit(&QueryRequest {
            sql: "SELECT * FROM hidden_file".into(),
            parameters: Vec::new(),
            timeout_ms: None,
        })
        .await
        .unwrap();
    let hidden_id = hidden.query_id.clone();
    let hidden = client
        .wait(&hidden_id, Duration::from_millis(10))
        .await
        .unwrap();
    assert_eq!(hidden.state, QueryState::Failed);
    assert_eq!(hidden.error.unwrap().error, "sql.unsupported");
    client.delete(&hidden_id).await.unwrap();

    let oversized = client
        .submit(&QueryRequest {
            sql: "SELECT $1 AS oversized".into(),
            parameters: vec![TypedParameter {
                data_type: "utf8".into(),
                value: json!("x".repeat(100_000)),
                precision: None,
                scale: None,
            }],
            timeout_ms: None,
        })
        .await
        .unwrap();
    let oversized_id = oversized.query_id.clone();
    let oversized = client
        .wait(&oversized_id, Duration::from_millis(10))
        .await
        .unwrap();
    assert_eq!(oversized.state, QueryState::Failed);
    assert_eq!(oversized.error.unwrap().error, "query.resource_exhausted");
    client.delete(&oversized_id).await.unwrap();

    let _ = shutdown_tx.send(());
    tokio::time::timeout(Duration::from_secs(10), server)
        .await
        .expect("server stopped")
        .expect("server task joined")
        .expect("server shutdown succeeded");
}

async fn drain(mut result: rustdb::QueryResult) {
    while let Some(batch) = result.stream().next().await {
        batch.unwrap();
    }
}

fn free_loopback_address() -> std::net::SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap()
}

async fn wait_for_bundle(
    state_root: &std::path::Path,
    server: &tokio::task::JoinHandle<rustdb::Result<()>>,
) -> PathBuf {
    for _ in 0..200 {
        assert!(
            !server.is_finished(),
            "HTTP server stopped before creating its profile"
        );
        if let Ok(entries) = std::fs::read_dir(state_root) {
            for entry in entries.flatten() {
                let candidate = entry.path().join("connection.rustdb-profile");
                if candidate.is_dir() {
                    return candidate;
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("HTTP server did not create its connection profile");
}

async fn wait_for_server(
    client: &RemoteClient,
    server: &tokio::task::JoinHandle<rustdb::Result<()>>,
) {
    for _ in 0..200 {
        assert!(
            !server.is_finished(),
            "HTTP server stopped before becoming ready"
        );
        if client.check_compatibility().await.is_ok() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("HTTP server did not become ready");
}
