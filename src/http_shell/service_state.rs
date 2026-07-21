use std::path::Path;

use serde::Serialize;

use crate::{Error, Result};

use super::{
    query::{check_journal_state, lock_journal_state, repair_journal_state},
    result_store::{
        check_state as check_results, lock_state as lock_results, repair_state as repair_results,
    },
    security::{PrincipalStore, SecurityState},
};

#[derive(Clone, Debug, Serialize)]
#[non_exhaustive]
pub struct ServiceStateIssue {
    pub component: &'static str,
    pub message: String,
}

#[derive(Clone, Debug, Serialize)]
#[non_exhaustive]
pub struct ServiceStateReport {
    pub healthy: bool,
    pub repair_applied: bool,
    pub repaired_actions: usize,
    pub checked_queries: usize,
    pub checked_results: usize,
    pub issues: Vec<ServiceStateIssue>,
}

/// Performs a read-only integrity check while holding the existing server lock.
pub fn check_service_state(
    database: impl AsRef<Path>,
    state_root: impl AsRef<Path>,
    result_directory: Option<&Path>,
) -> Result<ServiceStateReport> {
    inspect(
        database.as_ref(),
        state_root.as_ref(),
        result_directory,
        false,
    )
}

/// Applies only marker-gated repairs: incomplete journal tails, owned temporary
/// files, and owned result directories whose manifests or chunks are invalid.
pub fn repair_service_state(
    database: impl AsRef<Path>,
    state_root: impl AsRef<Path>,
    result_directory: Option<&Path>,
    apply: bool,
) -> Result<ServiceStateReport> {
    inspect(
        database.as_ref(),
        state_root.as_ref(),
        result_directory,
        apply,
    )
}

fn inspect(
    database: &Path,
    state_root: &Path,
    result_directory: Option<&Path>,
    apply: bool,
) -> Result<ServiceStateReport> {
    let state = SecurityState::locate_for_native_database(state_root, database)?;
    let _state_lock = state.acquire_existing_server_lock()?;
    let results = result_directory
        .map(Path::to_path_buf)
        .unwrap_or_else(|| state.directory().join("results"));
    let journal = results.join("query-journal");
    // Match online startup's lock order. Both locks are required even for a
    // read-only check so an apply cannot race a live custom result root.
    let _results_lock = lock_results(&results)?;
    let _journal_lock = lock_journal_state(&journal)?;
    let mut report = ServiceStateReport {
        healthy: true,
        repair_applied: apply,
        repaired_actions: 0,
        checked_queries: 0,
        checked_results: 0,
        issues: Vec::new(),
    };

    check_component(&mut report, "principals", || {
        PrincipalStore::new(state.clone()).validate_existing()
    });
    if apply {
        repair_component(&mut report, "query_journal", || {
            repair_journal_state(&journal)
        });
        repair_component(&mut report, "results", || repair_results(&results));
    }
    match check_journal_state(&journal) {
        Ok(count) => report.checked_queries = count,
        Err(error) => report.issue("query_journal", error),
    }
    match check_results(&results) {
        Ok(count) => report.checked_results = count,
        Err(error) => report.issue("results", error),
    }
    report.healthy = report.issues.is_empty();
    Ok(report)
}

fn check_component(
    report: &mut ServiceStateReport,
    component: &'static str,
    check: impl FnOnce() -> Result<()>,
) {
    if let Err(error) = check() {
        report.issue(component, error);
    }
}

fn repair_component(
    report: &mut ServiceStateReport,
    component: &'static str,
    repair: impl FnOnce() -> Result<usize>,
) {
    match repair() {
        Ok(count) => report.repaired_actions = report.repaired_actions.saturating_add(count),
        Err(error) => report.issue(component, error),
    }
}

impl ServiceStateReport {
    fn issue(&mut self, component: &'static str, error: Error) {
        self.issues.push(ServiceStateIssue {
            component,
            message: error.to_string(),
        });
    }
}

#[cfg(test)]
mod tests {
    use super::ServiceStateReport;

    #[test]
    fn report_json_is_stable_and_secret_free() {
        let report = ServiceStateReport {
            healthy: true,
            repair_applied: false,
            repaired_actions: 0,
            checked_queries: 2,
            checked_results: 1,
            issues: Vec::new(),
        };
        let value = serde_json::to_value(report).unwrap();
        assert_eq!(value["checked_queries"], 2);
        assert_eq!(value["healthy"], true);
    }
}
