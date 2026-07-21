use sysinfo::{Pid, ProcessesToUpdate, System};

use crate::{Error, Result};

#[cfg(any(target_os = "linux", test))]
use std::{
    fs,
    path::{Component, Path, PathBuf},
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct MemoryObservation {
    pub(super) process_rss_bytes: u64,
    pub(super) physical_memory_bytes: u64,
    pub(super) cgroup_limit_bytes: Option<u64>,
}

#[derive(Debug)]
pub(super) struct SystemMemoryProbe {
    pid: Pid,
    system: System,
}

impl SystemMemoryProbe {
    pub(super) fn new() -> Self {
        Self {
            pid: Pid::from_u32(std::process::id()),
            system: System::new(),
        }
    }

    pub(super) fn sample(&mut self) -> Result<MemoryObservation> {
        self.system.refresh_memory();
        self.system
            .refresh_processes(ProcessesToUpdate::Some(&[self.pid]), false);

        let process_rss_bytes = self
            .system
            .process(self.pid)
            .map(sysinfo::Process::memory)
            .ok_or_else(|| {
                Error::Internal(
                    "current process RSS is unavailable from the system probe".to_owned(),
                )
            })?;
        let physical_memory_bytes = self.system.total_memory();
        if physical_memory_bytes == 0 {
            return Err(Error::Internal(
                "physical memory is unavailable from the system probe".to_owned(),
            ));
        }

        Ok(MemoryObservation {
            process_rss_bytes,
            physical_memory_bytes,
            cgroup_limit_bytes: cgroup_limit_bytes(),
        })
    }
}

#[cfg(target_os = "linux")]
fn cgroup_limit_bytes() -> Option<u64> {
    let membership = fs::read_to_string("/proc/self/cgroup").ok()?;
    cgroup_limit_from(
        &membership,
        Path::new("/sys/fs/cgroup"),
        Path::new("/sys/fs/cgroup/memory"),
    )
}

#[cfg(not(target_os = "linux"))]
const fn cgroup_limit_bytes() -> Option<u64> {
    None
}

#[cfg(any(target_os = "linux", test))]
pub(super) fn cgroup_limit_from(
    membership: &str,
    unified_root: &Path,
    v1_root: &Path,
) -> Option<u64> {
    let mut limits = Vec::new();
    for line in membership.lines() {
        let mut fields = line.splitn(3, ':');
        let (Some(hierarchy), Some(controllers), Some(path)) =
            (fields.next(), fields.next(), fields.next())
        else {
            continue;
        };
        let Some(relative) = safe_relative_path(path) else {
            continue;
        };

        if hierarchy == "0" && controllers.is_empty() {
            if let Some(limit) = read_limit_chain(unified_root, &relative, "memory.max") {
                limits.push(limit);
            }
        } else if controllers.split(',').any(|name| name == "memory")
            && let Some(limit) = read_limit_chain(v1_root, &relative, "memory.limit_in_bytes")
        {
            limits.push(limit);
        }
    }
    limits.into_iter().min()
}

#[cfg(any(target_os = "linux", test))]
fn safe_relative_path(value: &str) -> Option<PathBuf> {
    let mut relative = PathBuf::new();
    for component in Path::new(value).components() {
        match component {
            Component::RootDir | Component::CurDir => {}
            Component::Normal(part) => relative.push(part),
            Component::ParentDir | Component::Prefix(_) => return None,
        }
    }
    Some(relative)
}

#[cfg(any(target_os = "linux", test))]
fn read_limit_chain(root: &Path, relative: &Path, file_name: &str) -> Option<u64> {
    let mut current = root.join(relative);
    let mut limit = None;
    loop {
        if let Some(value) = read_finite_limit(&current.join(file_name)) {
            limit = Some(limit.map_or(value, |existing: u64| existing.min(value)));
        }
        if current == root || !current.starts_with(root) || !current.pop() {
            break;
        }
    }
    limit
}

#[cfg(any(target_os = "linux", test))]
fn read_finite_limit(path: &Path) -> Option<u64> {
    const V1_UNLIMITED_FLOOR: u64 = 1 << 60;

    let value = fs::read_to_string(path).ok()?;
    let value = value.trim();
    if value == "max" {
        return None;
    }
    value
        .parse::<u64>()
        .ok()
        .filter(|limit| *limit < V1_UNLIMITED_FLOOR)
}
