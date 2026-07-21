use std::{fs, process::Command};

use futures::StreamExt;
use rustdb::{
    Engine, EngineConfig,
    http_shell::security::{PrincipalId, PrincipalStore, Role, SecurityState},
};

#[test]
fn beta2_requires_fresh_epoch4_and_config_schema2() {
    assert_eq!(env!("CARGO_PKG_VERSION"), "1.0.0-beta.2");
    let temporary = tempfile::tempdir().unwrap();
    let database = temporary.path().join("database");
    let engine = Engine::open(&database, config(temporary.path())).unwrap();

    let marker: serde_json::Value =
        serde_json::from_slice(&fs::read(database.join(".rustdb")).unwrap()).unwrap();
    assert_eq!(marker["version"], 4);
    drop(engine);

    let current = temporary.path().join("current.toml");
    fs::write(&current, "schema_version = 2\n").unwrap();
    assert!(
        rustdb()
            .args(["config", "validate"])
            .arg(&current)
            .status()
            .unwrap()
            .success()
    );

    let legacy = temporary.path().join("legacy.toml");
    fs::write(&legacy, "schema_version = 1\n").unwrap();
    let rejected = rustdb()
        .args(["config", "validate"])
        .arg(&legacy)
        .output()
        .unwrap();
    assert!(!rejected.status.success());
    assert!(String::from_utf8_lossy(&rejected.stderr).contains("unsupported schema_version 1"));
}

#[tokio::test]
async fn beta2_credentials_and_service_backup_round_trip() {
    let temporary = tempfile::tempdir().unwrap();
    let database = temporary.path().join("database");
    let state_root = temporary.path().join("state");
    let config = config(temporary.path());
    let engine = Engine::open(&database, config.clone()).unwrap();
    let state = SecurityState::for_native_database(&state_root, &database).unwrap();
    let store = PrincipalStore::new(state);
    store.load_or_bootstrap().unwrap();

    let analyst = PrincipalId::new("analyst").unwrap();
    let first = store
        .create_principal(analyst.clone(), Role::Query)
        .unwrap();
    let second = store.rotate_token(&analyst).unwrap();
    assert_eq!(store.list_tokens(Some(&analyst)).unwrap().len(), 2);
    store.revoke_token(first.token_id()).unwrap();
    let tokens = store.list_tokens(Some(&analyst)).unwrap();
    assert_eq!(tokens.iter().filter(|token| token.active()).count(), 1);
    assert_eq!(tokens.iter().filter(|token| token.revoked()).count(), 1);
    assert!(
        tokens
            .iter()
            .any(|token| token.token_id() == second.token_id())
    );

    let backup = temporary.path().join("backup");
    let report = engine
        .backup_service_to_location(&state_root, backup.to_str().unwrap())
        .await
        .unwrap();
    assert!(report.service_state_included());
    Engine::check_service_backup_location(backup.to_str().unwrap(), config.clone())
        .await
        .unwrap();

    let restored_database = temporary.path().join("restored-database");
    let restored_state = temporary.path().join("restored-state");
    let restored = Engine::restore_service_from_location(
        backup.to_str().unwrap(),
        &restored_database,
        &restored_state,
        config,
    )
    .await
    .unwrap();
    let mut result = restored.session().execute("SELECT 1").await.unwrap();
    let mut rows = 0;
    while let Some(batch) = result.stream().next().await {
        rows += batch.unwrap().num_rows();
    }
    drop(result);
    assert_eq!(rows, 1);
    assert_eq!(restored.memory_snapshot().current_bytes, 0);
    assert!(
        restored_state
            .join(report.database_id())
            .join("principals.json")
            .is_file()
    );
    assert!(!backup.join("service/results").exists());
    assert!(!backup.join("service/admin.sock").exists());
    assert!(
        fs::read_dir(temporary.path().join("spill"))
            .unwrap()
            .filter_map(Result::ok)
            .all(|entry| !entry.file_name().to_string_lossy().starts_with("query-"))
    );
}

fn rustdb() -> Command {
    Command::new(env!("CARGO_BIN_EXE_rustdb"))
}

fn config(root: &std::path::Path) -> EngineConfig {
    EngineConfig::builder()
        .spill_directory(root.join("spill"))
        .build()
}
