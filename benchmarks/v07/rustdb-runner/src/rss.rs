use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    thread,
    time::Duration,
};

use sysinfo::{Pid, ProcessesToUpdate, System};

pub(crate) struct RssSampler {
    baseline: u64,
    peak: Arc<AtomicU64>,
    stop: Arc<AtomicBool>,
    worker: Option<thread::JoinHandle<()>>,
}

impl RssSampler {
    pub(crate) fn start() -> Self {
        let baseline = current_rss_bytes().unwrap_or(0);
        let peak = Arc::new(AtomicU64::new(baseline));
        let stop = Arc::new(AtomicBool::new(false));
        let worker_peak = Arc::clone(&peak);
        let worker_stop = Arc::clone(&stop);
        let worker = thread::spawn(move || {
            while !worker_stop.load(Ordering::Acquire) {
                if let Some(value) = current_rss_bytes() {
                    worker_peak.fetch_max(value, Ordering::AcqRel);
                }
                thread::sleep(Duration::from_millis(2));
            }
            if let Some(value) = current_rss_bytes() {
                worker_peak.fetch_max(value, Ordering::AcqRel);
            }
        });
        Self {
            baseline,
            peak,
            stop,
            worker: Some(worker),
        }
    }

    pub(crate) fn stop(mut self) -> (u64, u64) {
        self.finish()
    }

    fn finish(&mut self) -> (u64, u64) {
        self.stop.store(true, Ordering::Release);
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
        (self.baseline, self.peak.load(Ordering::Acquire))
    }
}

impl Drop for RssSampler {
    fn drop(&mut self) {
        let _ = self.finish();
    }
}

fn current_rss_bytes() -> Option<u64> {
    let pid = Pid::from_u32(std::process::id());
    let mut system = System::new();
    system.refresh_processes(ProcessesToUpdate::Some(&[pid]), false);
    system.process(pid).map(sysinfo::Process::memory)
}
