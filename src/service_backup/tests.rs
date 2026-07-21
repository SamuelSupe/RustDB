use std::fs;

use crate::{
    Engine, EngineConfig,
    http_shell::security::{PrincipalStore, SecurityState},
};

#[tokio::test]
async fn service_bundle_round_trips_control_state_and_detects_corruption() {
    let temporary = tempfile::tempdir().unwrap();
    let database = temporary.path().join("database");
    let state_root = temporary.path().join("state");
    let config = EngineConfig::builder()
        .spill_directory(temporary.path().join("spill"))
        .build();
    let engine = Engine::open(&database, config.clone()).unwrap();
    let state = SecurityState::for_native_database(&state_root, &database).unwrap();
    PrincipalStore::new(state.clone())
        .load_or_bootstrap()
        .unwrap();
    fs::create_dir(state.directory().join("results")).unwrap();
    fs::write(state.directory().join("results/ignored"), b"query result").unwrap();

    let backup = temporary.path().join("backup");
    let report = engine
        .backup_service_to_location(&state_root, backup.to_str().unwrap())
        .await
        .unwrap();
    assert!(report.service_state_included());
    assert!(report.files() > 1);
    assert!(!backup.join("service/results").exists());
    assert!(!backup.join("service/audit.jsonl").exists());
    assert!(!backup.join("service/server.lock").exists());

    let checked = Engine::check_service_backup_location(backup.to_str().unwrap(), config.clone())
        .await
        .unwrap();
    assert_eq!(checked.database_id(), report.database_id());

    let restored_database = temporary.path().join("restored-database");
    let restored_state = temporary.path().join("restored-state");
    let restored = Engine::restore_service_from_location(
        backup.to_str().unwrap(),
        &restored_database,
        &restored_state,
        config.clone(),
    )
    .await
    .unwrap();
    assert!(
        restored_state
            .join(report.database_id())
            .join("principals.json")
            .is_file()
    );
    drop(restored);
    assert!(
        Engine::restore_service_from_location(
            backup.to_str().unwrap(),
            &restored_database,
            &restored_state,
            config.clone(),
        )
        .await
        .is_err()
    );

    fs::write(backup.join("service/principals.json"), b"corrupt\n").unwrap();
    assert!(
        Engine::check_service_backup_location(backup.to_str().unwrap(), config)
            .await
            .is_err()
    );
}
