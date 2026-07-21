use std::fs;

use crate::{Engine, EngineConfig, Error};

use super::{cleanup_service_publication, rollback_publication};

#[test]
fn beta2_hardening_service_cleanup_reports_staging_and_target_failures() {
    let temporary = tempfile::tempdir().unwrap();
    let staging = temporary.path().join("staging");
    let target = temporary.path().join("target");

    fs::write(&staging, b"not a directory").unwrap();
    let error = cleanup_service_publication(&staging, &target).unwrap_err();
    assert!(error.to_string().contains(&staging.display().to_string()));
    fs::remove_file(&staging).unwrap();

    fs::write(&target, b"not a directory").unwrap();
    let error = cleanup_service_publication(&staging, &target).unwrap_err();
    assert!(error.to_string().contains(&target.display().to_string()));
}

#[test]
fn beta2_hardening_rollback_combines_service_state_cleanup_failure() {
    let temporary = tempfile::tempdir().unwrap();
    let database = temporary.path().join("database");
    let engine = Engine::open(
        &database,
        EngineConfig::builder()
            .spill_directory(temporary.path().join("spill"))
            .build(),
    )
    .unwrap();
    let staging = temporary.path().join("staging");
    let target = temporary.path().join("target");
    fs::write(&staging, b"not a directory").unwrap();

    let outcome = rollback_publication(
        engine,
        &database,
        &staging,
        &target,
        Error::Execution("publication failed".to_owned()),
    );
    let error = match outcome {
        Ok(_) => panic!("rollback unexpectedly ignored a cleanup failure"),
        Err(error) => error,
    };
    let message = error.to_string();
    assert!(message.contains("publication failed"));
    assert!(message.contains("service-state cleanup also failed"));
    assert!(message.contains(&staging.display().to_string()));
    assert!(!database.exists());
}
