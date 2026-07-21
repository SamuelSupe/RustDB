use std::{fs, process::Command};

fn rustdb() -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_rustdb"));
    for name in [
        "RUSTDB_LISTEN",
        "RUSTDB_ADVERTISE_URL",
        "RUSTDB_RESULT_DIRECTORY",
        "RUSTDB_RESULT_TTL_SECS",
        "RUSTDB_RESULT_GLOBAL_LIMIT",
        "RUSTDB_RESULT_QUERY_LIMIT",
        "RUSTDB_STATE_ROOT",
        "RUSTDB_HTTP_MAX_RUNNING",
        "RUSTDB_HTTP_MAX_QUEUED",
        "RUSTDB_HTTP_MAX_QUERY_TIME_SECS",
        "RUSTDB_HTTP_QUERY_MEMORY_LIMIT",
        "RUSTDB_HTTP_QUERY_SPILL_LIMIT",
        "RUSTDB_HTTP_QUERY_RESULT_LIMIT",
        "RUSTDB_HTTP_PRINCIPAL_MAX_RUNNING",
        "RUSTDB_HTTP_PRINCIPAL_MAX_QUEUED",
        "RUSTDB_HTTP_PRINCIPAL_MEMORY_LIMIT",
        "RUSTDB_HTTP_PRINCIPAL_SPILL_LIMIT",
        "RUSTDB_HTTP_PRINCIPAL_RESULT_LIMIT",
        "RUSTDB_HTTP_PRINCIPAL_WEIGHT",
        "RUSTDB_SERVICE_IO_THREADS",
        "RUSTDB_ADMIN_SOCKET",
        "RUSTDB_TLS_RENEW_INTERVAL_SECS",
        "RUSTDB_MEMORY_LIMIT",
        "RUSTDB_THREADS",
        "RUSTDB_SPILL_ENGINE_LIMIT",
        "RUSTDB_SPILL_QUERY_LIMIT",
        "RUSTDB_S3_REGION",
        "RUSTDB_S3_ENDPOINT",
        "RUSTDB_S3_PATH_STYLE",
        "RUSTDB_S3_ALLOW_HTTP",
        "RUSTDB_S3_ANONYMOUS",
    ] {
        command.env_remove(name);
    }
    command
}

#[test]
fn validates_the_versioned_packaged_configuration() {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("packaging/config/rustdb.example.toml");
    let output = rustdb()
        .args(["config", "validate"])
        .arg(&path)
        .output()
        .unwrap();
    assert!(output.status.success(), "{}", stderr(&output));
    assert!(String::from_utf8_lossy(&output.stdout).contains("configuration is valid"));
}

#[test]
fn validate_defaults_to_rustdb_toml_in_the_working_directory() {
    let directory = tempfile::tempdir().unwrap();
    fs::write(directory.path().join("rustdb.toml"), "schema_version = 2\n").unwrap();
    let output = rustdb()
        .current_dir(directory.path())
        .args(["config", "validate"])
        .output()
        .unwrap();
    assert!(output.status.success(), "{}", stderr(&output));
    assert!(String::from_utf8_lossy(&output.stdout).contains("rustdb.toml"));
}

#[test]
fn rejects_missing_and_unknown_schema_versions_without_starting_a_server() {
    let directory = tempfile::tempdir().unwrap();
    let cases = [
        (
            "missing.toml",
            "[server]\nmax_running = 1\n",
            "schema_version = 2 is required",
        ),
        (
            "legacy.toml",
            "schema_version = 1\n",
            "unsupported schema_version 1",
        ),
        (
            "unknown.toml",
            "schema_version = 9\n",
            "unsupported schema_version 9",
        ),
    ];
    for (name, contents, expected) in cases {
        let path = directory.path().join(name);
        fs::write(&path, contents).unwrap();
        let output = rustdb()
            .args(["config", "validate"])
            .arg(&path)
            .output()
            .unwrap();
        assert!(!output.status.success());
        assert!(stderr(&output).contains(expected), "{}", stderr(&output));
    }
}

#[test]
fn validate_uses_the_same_service_limit_checks_as_serve() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("invalid.toml");
    fs::write(&path, "schema_version = 2\n[server]\nmax_running = 0\n").unwrap();
    let output = rustdb()
        .args(["config", "validate"])
        .arg(path)
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(
        stderr(&output).contains("server.max_running must be greater than zero"),
        "{}",
        stderr(&output)
    );
}

#[test]
fn validate_uses_the_engine_query_admission_check() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("invalid-engine.toml");
    fs::write(&path, "schema_version = 2\n").unwrap();
    let output = rustdb()
        .args(["--max-concurrent-queries", "0", "config", "validate"])
        .arg(path)
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(
        stderr(&output).contains("max_concurrent_queries must be greater than zero"),
        "{}",
        stderr(&output)
    );
}

#[test]
fn validate_rejects_insecure_s3_endpoint_without_explicit_opt_in() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("insecure-s3.toml");
    fs::write(
        &path,
        "schema_version = 2\n[engine]\ns3_endpoint = \"http://minio.example:9000\"\n",
    )
    .unwrap();
    let output = rustdb()
        .args(["config", "validate"])
        .arg(path)
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(
        stderr(&output).contains("allow_http is false"),
        "{}",
        stderr(&output)
    );
}

#[test]
fn validate_accepts_insecure_s3_endpoint_with_explicit_opt_in() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("insecure-s3-opt-in.toml");
    fs::write(
        &path,
        "schema_version = 2\n[engine]\ns3_endpoint = \"http://minio.example:9000\"\ns3_allow_http = true\n",
    )
    .unwrap();
    let output = rustdb()
        .args(["config", "validate"])
        .arg(path)
        .output()
        .unwrap();
    assert!(output.status.success(), "{}", stderr(&output));
}

#[test]
fn rejects_a_query_reservation_larger_than_its_principal_budget() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("invalid-resources.toml");
    fs::write(
        &path,
        "schema_version = 2\n[server]\nquery_memory_limit = \"2GiB\"\nprincipal_memory_limit = \"1GiB\"\n",
    )
    .unwrap();
    let output = rustdb()
        .args(["config", "validate"])
        .arg(path)
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(
        stderr(&output).contains("per-query resource limits must fit within per-principal limits"),
        "{}",
        stderr(&output)
    );
}

#[test]
fn rejects_unknown_fields_in_the_current_schema() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("unknown-field.toml");
    fs::write(&path, "schema_version = 2\nfuture_option = true\n").unwrap();
    let output = rustdb()
        .args(["config", "validate"])
        .arg(path)
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(
        stderr(&output).contains("unknown field"),
        "{}",
        stderr(&output)
    );
}

#[test]
fn validates_rss_guard_watermarks_and_sampling_interval() {
    let directory = tempfile::tempdir().unwrap();
    for (name, server, expected) in [
        (
            "rss-order.toml",
            "rss_warning_ratio = 0.8\nrss_high_ratio = 0.8\n",
            "0 < warning < high < critical <= 1",
        ),
        (
            "rss-interval.toml",
            "rss_sample_interval_ms = 0\n",
            "rss_sample_interval_ms must be greater than zero",
        ),
    ] {
        let path = directory.path().join(name);
        fs::write(&path, format!("schema_version = 2\n[server]\n{server}")).unwrap();
        let output = rustdb()
            .args(["config", "validate"])
            .arg(path)
            .output()
            .unwrap();
        assert!(!output.status.success());
        assert!(stderr(&output).contains(expected), "{}", stderr(&output));
    }
}

#[test]
fn validates_service_io_tls_and_admin_socket_settings() {
    let directory = tempfile::tempdir().unwrap();
    for (name, server, expected) in [
        (
            "service-io.toml",
            "service_io_threads = 0\n",
            "service_io_threads must be greater than zero",
        ),
        (
            "tls-renew.toml",
            "tls_renew_interval_secs = 0\n",
            "tls_renew_interval_secs must be greater than zero",
        ),
        (
            "admin-socket.toml",
            "admin_socket = \"\"\n",
            "admin_socket must not be empty",
        ),
    ] {
        let path = directory.path().join(name);
        fs::write(&path, format!("schema_version = 2\n[server]\n{server}")).unwrap();
        let output = rustdb()
            .args(["config", "validate"])
            .arg(path)
            .output()
            .unwrap();
        assert!(!output.status.success());
        assert!(stderr(&output).contains(expected), "{}", stderr(&output));
    }
}

fn stderr(output: &std::process::Output) -> String {
    String::from_utf8_lossy(&output.stderr).into_owned()
}
