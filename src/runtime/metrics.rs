use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use parking_lot::RwLock;

use super::MemoryPool;

#[derive(Clone, Debug)]
pub struct QueryMetrics {
    inner: Arc<Inner>,
}

#[derive(Debug)]
struct Inner {
    started_at: Instant,
    memory_pool: RwLock<Option<MemoryPool>>,
    elapsed_ns: AtomicU64,
    rows_scanned: AtomicU64,
    rows_returned: AtomicU64,
    batches_scanned: AtomicU64,
    batches_returned: AtomicU64,
    bytes_scanned: AtomicU64,
    bytes_returned: AtomicU64,
    files_pruned: AtomicU64,
    row_groups_pruned: AtomicU64,
    s3_requests: AtomicU64,
    s3_bytes_transferred: AtomicU64,
    peak_memory_bytes: AtomicU64,
    spill_bytes: AtomicU64,
    spill_partitions: AtomicU64,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct QueryMetricsSnapshot {
    pub elapsed: Duration,
    pub rows_scanned: u64,
    pub rows_returned: u64,
    pub batches_scanned: u64,
    pub batches_returned: u64,
    pub bytes_scanned: u64,
    pub bytes_returned: u64,
    pub files_pruned: u64,
    pub row_groups_pruned: u64,
    pub s3_requests: u64,
    pub s3_bytes_transferred: u64,
    pub peak_memory_bytes: u64,
    pub spill_bytes: u64,
    pub spill_partitions: u64,
}

impl QueryMetrics {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Inner {
                started_at: Instant::now(),
                memory_pool: RwLock::new(None),
                elapsed_ns: AtomicU64::new(0),
                rows_scanned: AtomicU64::new(0),
                rows_returned: AtomicU64::new(0),
                batches_scanned: AtomicU64::new(0),
                batches_returned: AtomicU64::new(0),
                bytes_scanned: AtomicU64::new(0),
                bytes_returned: AtomicU64::new(0),
                files_pruned: AtomicU64::new(0),
                row_groups_pruned: AtomicU64::new(0),
                s3_requests: AtomicU64::new(0),
                s3_bytes_transferred: AtomicU64::new(0),
                peak_memory_bytes: AtomicU64::new(0),
                spill_bytes: AtomicU64::new(0),
                spill_partitions: AtomicU64::new(0),
            }),
        }
    }

    pub fn with_memory_pool(memory_pool: MemoryPool) -> Self {
        let metrics = Self::new();
        metrics.bind_memory_pool(memory_pool);
        metrics
    }

    pub fn bind_memory_pool(&self, memory_pool: MemoryPool) {
        *self.inner.memory_pool.write() = Some(memory_pool);
    }

    pub fn finish(&self) {
        let elapsed = duration_ns(self.inner.started_at.elapsed());
        let _ = self.inner.elapsed_ns.compare_exchange(
            0,
            elapsed.max(1),
            Ordering::Relaxed,
            Ordering::Relaxed,
        );
    }

    pub fn elapsed(&self) -> Duration {
        let finished = self.inner.elapsed_ns.load(Ordering::Relaxed);
        if finished == 0 {
            self.inner.started_at.elapsed()
        } else {
            Duration::from_nanos(finished)
        }
    }

    pub fn record_scan(&self, rows: u64, batches: u64, bytes: u64) {
        add(&self.inner.rows_scanned, rows);
        add(&self.inner.batches_scanned, batches);
        add(&self.inner.bytes_scanned, bytes);
    }

    pub fn record_output(&self, rows: u64, batches: u64, bytes: u64) {
        add(&self.inner.rows_returned, rows);
        add(&self.inner.batches_returned, batches);
        add(&self.inner.bytes_returned, bytes);
    }

    pub fn add_files_pruned(&self, count: u64) {
        add(&self.inner.files_pruned, count);
    }

    pub fn add_row_groups_pruned(&self, count: u64) {
        add(&self.inner.row_groups_pruned, count);
    }

    pub fn add_s3_requests(&self, count: u64) {
        add(&self.inner.s3_requests, count);
    }

    pub fn add_s3_bytes_transferred(&self, bytes: u64) {
        add(&self.inner.s3_bytes_transferred, bytes);
    }

    /// Records a completed S3 GET and the response body bytes it transferred.
    pub fn record_s3_get(&self, bytes: u64) {
        add(&self.inner.s3_requests, 1);
        add(&self.inner.s3_bytes_transferred, bytes);
    }

    pub fn observe_memory(&self, bytes: usize) {
        set_max(&self.inner.peak_memory_bytes, usize_to_u64(bytes));
    }

    pub fn record_spill(&self, bytes: u64, partitions: u64) {
        add(&self.inner.spill_bytes, bytes);
        add(&self.inner.spill_partitions, partitions);
    }

    pub fn snapshot(&self) -> QueryMetricsSnapshot {
        let pool_peak = self
            .inner
            .memory_pool
            .read()
            .as_ref()
            .map(MemoryPool::peak)
            .map(usize_to_u64)
            .unwrap_or(0);
        QueryMetricsSnapshot {
            elapsed: self.elapsed(),
            rows_scanned: load(&self.inner.rows_scanned),
            rows_returned: load(&self.inner.rows_returned),
            batches_scanned: load(&self.inner.batches_scanned),
            batches_returned: load(&self.inner.batches_returned),
            bytes_scanned: load(&self.inner.bytes_scanned),
            bytes_returned: load(&self.inner.bytes_returned),
            files_pruned: load(&self.inner.files_pruned),
            row_groups_pruned: load(&self.inner.row_groups_pruned),
            s3_requests: load(&self.inner.s3_requests),
            s3_bytes_transferred: load(&self.inner.s3_bytes_transferred),
            peak_memory_bytes: load(&self.inner.peak_memory_bytes).max(pool_peak),
            spill_bytes: load(&self.inner.spill_bytes),
            spill_partitions: load(&self.inner.spill_partitions),
        }
    }
}

impl Default for QueryMetrics {
    fn default() -> Self {
        Self::new()
    }
}

fn add(counter: &AtomicU64, value: u64) {
    let _ = counter.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
        Some(current.saturating_add(value))
    });
}

fn set_max(counter: &AtomicU64, value: u64) {
    let _ = counter.fetch_max(value, Ordering::Relaxed);
}

fn load(counter: &AtomicU64) -> u64 {
    counter.load(Ordering::Relaxed)
}

fn duration_ns(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}

fn usize_to_u64(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::QueryMetrics;

    #[test]
    fn records_metrics_and_memory_high_watermark() {
        let metrics = QueryMetrics::new();
        metrics.record_scan(10, 1, 100);
        metrics.record_output(4, 1, 20);
        metrics.observe_memory(80);
        metrics.observe_memory(20);
        metrics.record_spill(512, 2);
        metrics.add_s3_requests(1);
        metrics.record_s3_get(123);
        metrics.finish();

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.rows_scanned, 10);
        assert_eq!(snapshot.rows_returned, 4);
        assert_eq!(snapshot.peak_memory_bytes, 80);
        assert_eq!(snapshot.spill_bytes, 512);
        assert_eq!(snapshot.spill_partitions, 2);
        assert_eq!(snapshot.s3_requests, 2);
        assert_eq!(snapshot.s3_bytes_transferred, 123);
        assert!(!snapshot.elapsed.is_zero());
    }
}
