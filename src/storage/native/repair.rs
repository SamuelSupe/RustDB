use std::path::{Path, PathBuf};

use serde::Serialize;

use super::check::{NativeCheckIssue, NativeCheckReport};
use crate::{Error, Result};

mod apply;
mod backup;
mod cleanup;
mod plan;

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[non_exhaustive]
#[serde(tag = "action", rename_all = "snake_case")]
pub enum NativeRepairAction {
    SetPermissions {
        path: PathBuf,
        mode: u32,
    },
    RemoveOwnedStaging {
        path: PathBuf,
        transaction_id: String,
    },
    RemoveAtomicTemporary {
        path: PathBuf,
        target: PathBuf,
    },
    RestoreCurrent {
        path: PathBuf,
        generation: u64,
    },
}

impl NativeRepairAction {
    pub fn path(&self) -> &Path {
        match self {
            Self::SetPermissions { path, .. }
            | Self::RemoveOwnedStaging { path, .. }
            | Self::RemoveAtomicTemporary { path, .. }
            | Self::RestoreCurrent { path, .. } => path,
        }
    }
}

#[derive(Clone, Debug, Serialize)]
#[non_exhaustive]
pub struct NativeRepairPlan {
    path: PathBuf,
    before: NativeCheckReport,
    actions: Vec<NativeRepairAction>,
    blockers: Vec<NativeCheckIssue>,
}

impl NativeRepairPlan {
    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn before(&self) -> &NativeCheckReport {
        &self.before
    }

    pub fn actions(&self) -> &[NativeRepairAction] {
        &self.actions
    }

    pub fn blockers(&self) -> &[NativeCheckIssue] {
        &self.blockers
    }

    pub fn is_applicable(&self) -> bool {
        self.blockers.is_empty()
    }
}

#[derive(Clone, Debug, Serialize)]
#[non_exhaustive]
pub struct NativeRepairReport {
    plan: NativeRepairPlan,
    applied_actions: u64,
    backup_path: Option<PathBuf>,
    after: NativeCheckReport,
    succeeded: bool,
}

impl NativeRepairReport {
    pub fn plan(&self) -> &NativeRepairPlan {
        &self.plan
    }

    pub fn applied_actions(&self) -> u64 {
        self.applied_actions
    }

    pub fn backup_path(&self) -> Option<&Path> {
        self.backup_path.as_deref()
    }

    pub fn after(&self) -> &NativeCheckReport {
        &self.after
    }

    pub fn succeeded(&self) -> bool {
        self.succeeded
    }
}

pub(super) fn plan(path: &Path) -> Result<NativeRepairPlan> {
    plan::build(path)
}

pub(super) fn apply(path: &Path) -> Result<NativeRepairReport> {
    let initial = plan::build(path)?;
    refuse_blocked(&initial)?;
    if initial.actions.is_empty() {
        let succeeded = initial.before.is_ok();
        return Ok(NativeRepairReport {
            after: initial.before.clone(),
            plan: initial,
            applied_actions: 0,
            backup_path: None,
            succeeded,
        });
    }

    let _lock = super::lock::DatabaseLock::acquire_existing(path)?;
    let fresh = plan::build(path)?;
    refuse_blocked(&fresh)?;
    if fresh.actions.is_empty() {
        let succeeded = fresh.before.is_ok();
        return Ok(NativeRepairReport {
            after: fresh.before.clone(),
            plan: fresh,
            applied_actions: 0,
            backup_path: None,
            succeeded,
        });
    }

    let backup_path = backup::create(path).map_err(|error| {
        Error::native_repair_refused(path, format!("metadata backup failed: {error}"))
    })?;
    let applied_actions = apply::execute(path, &fresh.actions).map_err(|error| {
        Error::native_repair_refused(
            path,
            format!(
                "repair action failed; metadata backup is retained at {}: {error}",
                backup_path.display()
            ),
        )
    })?;
    let after = super::check::database(path)?;
    let succeeded = after.is_ok();
    Ok(NativeRepairReport {
        plan: fresh,
        applied_actions,
        backup_path: Some(backup_path),
        after,
        succeeded,
    })
}

fn refuse_blocked(plan: &NativeRepairPlan) -> Result<()> {
    if plan.blockers.is_empty() {
        return Ok(());
    }
    let first = &plan.blockers[0];
    Err(Error::native_repair_refused(
        plan.path.clone(),
        format!(
            "{} blocker(s); first [{}]: {}",
            plan.blockers.len(),
            first.code(),
            first.message()
        ),
    ))
}

#[cfg(test)]
#[path = "repair/tests.rs"]
mod tests;
