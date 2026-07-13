use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};

use parking_lot::Mutex;

#[derive(Clone, Debug, Default, Eq, PartialEq)]
#[non_exhaustive]
pub struct OperatorMetricsSnapshot {
    pub id: u64,
    pub parent_id: Option<u64>,
    pub name: String,
    pub input_rows: u64,
    pub input_batches: u64,
    pub output_rows: u64,
    pub output_batches: u64,
    pub output_bytes: u64,
    pub elapsed: Duration,
    pub wait: Duration,
}

#[derive(Debug, Default)]
pub(super) struct OperatorRegistry {
    next_id: AtomicU64,
    entries: Mutex<Vec<Arc<OperatorCounters>>>,
}

#[derive(Clone, Debug)]
pub(crate) struct OperatorHandle {
    counters: Arc<OperatorCounters>,
}

#[derive(Debug)]
struct OperatorCounters {
    id: u64,
    parent_id: Option<u64>,
    name: &'static str,
    input_rows: AtomicU64,
    input_batches: AtomicU64,
    output_rows: AtomicU64,
    output_batches: AtomicU64,
    output_bytes: AtomicU64,
    elapsed_ns: AtomicU64,
    wait_ns: AtomicU64,
    finished: AtomicBool,
}

impl OperatorRegistry {
    pub(super) fn register(&self, name: &'static str, parent_id: Option<u64>) -> OperatorHandle {
        let counters = Arc::new(OperatorCounters {
            id: self.next_id.fetch_add(1, Ordering::Relaxed),
            parent_id,
            name,
            input_rows: AtomicU64::new(0),
            input_batches: AtomicU64::new(0),
            output_rows: AtomicU64::new(0),
            output_batches: AtomicU64::new(0),
            output_bytes: AtomicU64::new(0),
            elapsed_ns: AtomicU64::new(0),
            wait_ns: AtomicU64::new(0),
            finished: AtomicBool::new(false),
        });
        self.entries.lock().push(Arc::clone(&counters));
        OperatorHandle { counters }
    }

    pub(super) fn snapshot(&self) -> Vec<OperatorMetricsSnapshot> {
        let entries = self.entries.lock().clone();
        entries
            .iter()
            .map(|entry| {
                let (child_rows, child_batches) = entries
                    .iter()
                    .filter(|child| child.parent_id == Some(entry.id))
                    .fold((0u64, 0u64), |(rows, batches), child| {
                        (
                            rows.saturating_add(child.output_rows.load(Ordering::Relaxed)),
                            batches.saturating_add(child.output_batches.load(Ordering::Relaxed)),
                        )
                    });
                let recorded_rows = entry.input_rows.load(Ordering::Relaxed);
                let recorded_batches = entry.input_batches.load(Ordering::Relaxed);
                OperatorMetricsSnapshot {
                    id: entry.id,
                    parent_id: entry.parent_id,
                    name: entry.name.to_owned(),
                    input_rows: if recorded_batches == 0 {
                        child_rows
                    } else {
                        recorded_rows
                    },
                    input_batches: if recorded_batches == 0 {
                        child_batches
                    } else {
                        recorded_batches
                    },
                    output_rows: entry.output_rows.load(Ordering::Relaxed),
                    output_batches: entry.output_batches.load(Ordering::Relaxed),
                    output_bytes: entry.output_bytes.load(Ordering::Relaxed),
                    elapsed: Duration::from_nanos(entry.elapsed_ns.load(Ordering::Relaxed)),
                    wait: Duration::from_nanos(entry.wait_ns.load(Ordering::Relaxed)),
                }
            })
            .collect()
    }
}

impl OperatorHandle {
    pub(crate) fn id(&self) -> u64 {
        self.counters.id
    }

    pub(crate) fn record_input(&self, rows: u64) {
        add(&self.counters.input_rows, rows);
        add(&self.counters.input_batches, 1);
    }

    pub(crate) fn record_output(&self, rows: u64, bytes: u64) {
        add(&self.counters.output_rows, rows);
        add(&self.counters.output_batches, 1);
        add(&self.counters.output_bytes, bytes);
    }

    pub(crate) fn record_wait(&self, duration: Duration) {
        add(&self.counters.wait_ns, duration_ns(duration));
    }

    pub(crate) fn record_elapsed(&self, duration: Duration) {
        add(&self.counters.elapsed_ns, duration_ns(duration));
    }

    pub(crate) fn finish(&self, duration: Duration) {
        if !self.counters.finished.swap(true, Ordering::AcqRel) {
            self.counters
                .elapsed_ns
                .store(duration_ns(duration), Ordering::Relaxed);
        }
    }
}

fn add(counter: &AtomicU64, value: u64) {
    let _ = counter.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
        Some(current.saturating_add(value))
    });
}

fn duration_ns(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}
