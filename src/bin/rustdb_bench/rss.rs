use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    thread::{self, JoinHandle},
    time::Duration,
};

use rustdb::{Error, Result};
use sysinfo::{Pid, ProcessesToUpdate, System};

#[derive(Clone, Copy, Debug)]
pub(super) struct RssSummary {
    pub(super) before_bytes: Option<u64>,
    pub(super) peak_bytes: Option<u64>,
    pub(super) after_bytes: Option<u64>,
    pub(super) samples: u64,
}

pub(super) struct RssSampler {
    before: Option<u64>,
    peak: Arc<AtomicU64>,
    samples: Arc<AtomicU64>,
    stop: Arc<AtomicBool>,
    handle: Option<JoinHandle<()>>,
}

impl RssSampler {
    pub(super) fn start(interval: Duration) -> Self {
        let before = current_rss_bytes();
        let peak = Arc::new(AtomicU64::new(before.unwrap_or(0)));
        let samples = Arc::new(AtomicU64::new(u64::from(before.is_some())));
        let stop = Arc::new(AtomicBool::new(false));
        let thread_peak = Arc::clone(&peak);
        let thread_samples = Arc::clone(&samples);
        let thread_stop = Arc::clone(&stop);
        let handle = thread::spawn(move || {
            let pid = Pid::from_u32(std::process::id());
            let mut system = System::new();
            while !thread_stop.load(Ordering::Acquire) {
                sample(&mut system, pid, &thread_peak, &thread_samples);
                thread::park_timeout(interval);
            }
        });
        Self {
            before,
            peak,
            samples,
            stop,
            handle: Some(handle),
        }
    }

    pub(super) fn finish(mut self) -> Result<RssSummary> {
        let after = current_rss_bytes();
        if let Some(value) = after {
            self.peak.fetch_max(value, Ordering::Relaxed);
            self.samples.fetch_add(1, Ordering::Relaxed);
        }
        self.stop_and_join()?;
        let samples = self.samples.load(Ordering::Relaxed);
        Ok(RssSummary {
            before_bytes: self.before,
            peak_bytes: (samples != 0).then(|| self.peak.load(Ordering::Relaxed)),
            after_bytes: after,
            samples,
        })
    }

    fn stop_and_join(&mut self) -> Result<()> {
        self.stop.store(true, Ordering::Release);
        if let Some(handle) = self.handle.take() {
            handle.thread().unpark();
            handle
                .join()
                .map_err(|_| Error::Internal("process RSS sampler thread panicked".to_owned()))?;
        }
        Ok(())
    }
}

impl Drop for RssSampler {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(handle) = self.handle.take() {
            handle.thread().unpark();
            let _ = handle.join();
        }
    }
}

pub(super) fn current_rss_bytes() -> Option<u64> {
    let pid = Pid::from_u32(std::process::id());
    let mut system = System::new();
    system.refresh_processes(ProcessesToUpdate::Some(&[pid]), false);
    system.process(pid).map(sysinfo::Process::memory)
}

fn sample(system: &mut System, pid: Pid, peak: &AtomicU64, samples: &AtomicU64) {
    system.refresh_processes(ProcessesToUpdate::Some(&[pid]), false);
    if let Some(value) = system.process(pid).map(sysinfo::Process::memory) {
        peak.fetch_max(value, Ordering::Relaxed);
        samples.fetch_add(1, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::RssSampler;

    #[test]
    fn sampler_reports_process_rss_without_leaking_its_thread() {
        let summary = RssSampler::start(Duration::from_millis(1))
            .finish()
            .unwrap();
        assert!(summary.samples >= 1);
        assert!(summary.peak_bytes.is_some());
    }
}
