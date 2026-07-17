use std::{
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};

use super::QueryMetrics;

#[derive(Debug, Default)]
pub(super) struct TimingMetrics {
    compute_permit_wait_ns: AtomicU64,
    queue_backpressure_wait_ns: AtomicU64,
    csv_morsel_queue_wait_ns: AtomicU64,
    scan_pipeline_output_queue_wait_ns: AtomicU64,
    aggregate_lane_dispatch_queue_wait_ns: AtomicU64,
    aggregate_partial_output_queue_wait_ns: AtomicU64,
    barrier_wait_ns: AtomicU64,
    parquet_range_read_ns: AtomicU64,
    parquet_range_bytes_read: AtomicU64,
    parquet_decode_compute_ns: AtomicU64,
    parquet_decode_compute_permit_wait_ns: AtomicU64,
    parquet_decode_polls: AtomicU64,
    parquet_decode_pending_polls: AtomicU64,
    parquet_row_filter_compute_ns: AtomicU64,
    parquet_row_filter_evaluations: AtomicU64,
    parquet_row_filter_input_rows: AtomicU64,
    parquet_alignment_ns: AtomicU64,
    csv_source_io_time_ns: AtomicU64,
    csv_framing_time_ns: AtomicU64,
    csv_decode_compute_time_ns: AtomicU64,
}

pub(super) struct TimingSnapshot {
    pub(super) compute_permit_wait: Duration,
    pub(super) queue_backpressure_wait: Duration,
    pub(super) csv_morsel_queue_wait: Duration,
    pub(super) scan_pipeline_output_queue_wait: Duration,
    pub(super) aggregate_lane_dispatch_queue_wait: Duration,
    pub(super) aggregate_partial_output_queue_wait: Duration,
    pub(super) barrier_wait: Duration,
    pub(super) parquet_range_read_time: Duration,
    pub(super) parquet_range_bytes_read: u64,
    pub(super) parquet_decode_compute_time: Duration,
    pub(super) parquet_decode_compute_permit_wait: Duration,
    pub(super) parquet_decode_polls: u64,
    pub(super) parquet_decode_pending_polls: u64,
    pub(super) parquet_row_filter_compute_time: Duration,
    pub(super) parquet_row_filter_evaluations: u64,
    pub(super) parquet_row_filter_input_rows: u64,
    pub(super) parquet_alignment_time: Duration,
    pub(super) csv_source_io_time: Duration,
    pub(super) csv_framing_time: Duration,
    pub(super) csv_decode_compute_time: Duration,
}

impl TimingMetrics {
    pub(super) fn snapshot(&self) -> TimingSnapshot {
        TimingSnapshot {
            compute_permit_wait: load_duration(&self.compute_permit_wait_ns),
            queue_backpressure_wait: load_duration(&self.queue_backpressure_wait_ns),
            csv_morsel_queue_wait: load_duration(&self.csv_morsel_queue_wait_ns),
            scan_pipeline_output_queue_wait: load_duration(
                &self.scan_pipeline_output_queue_wait_ns,
            ),
            aggregate_lane_dispatch_queue_wait: load_duration(
                &self.aggregate_lane_dispatch_queue_wait_ns,
            ),
            aggregate_partial_output_queue_wait: load_duration(
                &self.aggregate_partial_output_queue_wait_ns,
            ),
            barrier_wait: load_duration(&self.barrier_wait_ns),
            parquet_range_read_time: load_duration(&self.parquet_range_read_ns),
            parquet_range_bytes_read: load(&self.parquet_range_bytes_read),
            parquet_decode_compute_time: load_duration(&self.parquet_decode_compute_ns),
            parquet_decode_compute_permit_wait: load_duration(
                &self.parquet_decode_compute_permit_wait_ns,
            ),
            parquet_decode_polls: load(&self.parquet_decode_polls),
            parquet_decode_pending_polls: load(&self.parquet_decode_pending_polls),
            parquet_row_filter_compute_time: load_duration(&self.parquet_row_filter_compute_ns),
            parquet_row_filter_evaluations: load(&self.parquet_row_filter_evaluations),
            parquet_row_filter_input_rows: load(&self.parquet_row_filter_input_rows),
            parquet_alignment_time: load_duration(&self.parquet_alignment_ns),
            csv_source_io_time: load_duration(&self.csv_source_io_time_ns),
            csv_framing_time: load_duration(&self.csv_framing_time_ns),
            csv_decode_compute_time: load_duration(&self.csv_decode_compute_time_ns),
        }
    }
}

impl QueryMetrics {
    pub(crate) fn record_compute_permit_wait(&self, elapsed: Duration) {
        add_duration(&self.inner.timing.compute_permit_wait_ns, elapsed);
    }

    pub(crate) fn record_queue_backpressure_wait(&self, elapsed: Duration) {
        add_duration(&self.inner.timing.queue_backpressure_wait_ns, elapsed);
    }

    pub(crate) fn record_csv_morsel_queue_wait(&self, elapsed: Duration) {
        self.record_queue_backpressure_wait(elapsed);
        add_duration(&self.inner.timing.csv_morsel_queue_wait_ns, elapsed);
    }

    pub(crate) fn record_scan_pipeline_output_queue_wait(&self, elapsed: Duration) {
        self.record_queue_backpressure_wait(elapsed);
        add_duration(
            &self.inner.timing.scan_pipeline_output_queue_wait_ns,
            elapsed,
        );
    }

    pub(crate) fn record_aggregate_lane_dispatch_queue_wait(&self, elapsed: Duration) {
        self.record_queue_backpressure_wait(elapsed);
        add_duration(
            &self.inner.timing.aggregate_lane_dispatch_queue_wait_ns,
            elapsed,
        );
    }

    pub(crate) fn record_aggregate_partial_output_queue_wait(&self, elapsed: Duration) {
        self.record_queue_backpressure_wait(elapsed);
        add_duration(
            &self.inner.timing.aggregate_partial_output_queue_wait_ns,
            elapsed,
        );
    }

    pub(crate) fn record_barrier_wait(&self, elapsed: Duration) {
        add_duration(&self.inner.timing.barrier_wait_ns, elapsed);
    }

    pub(crate) fn record_parquet_range_read(&self, bytes: u64, elapsed: Duration) {
        add(&self.inner.timing.parquet_range_bytes_read, bytes);
        add_duration(&self.inner.timing.parquet_range_read_ns, elapsed);
    }

    pub(crate) fn record_parquet_decode_activity(
        &self,
        elapsed: Duration,
        polls: u64,
        pending_polls: u64,
    ) {
        add(&self.inner.timing.parquet_decode_polls, polls);
        add(
            &self.inner.timing.parquet_decode_pending_polls,
            pending_polls,
        );
        add_duration(&self.inner.timing.parquet_decode_compute_ns, elapsed);
    }

    pub(crate) fn record_parquet_decode_compute_permit_wait(&self, elapsed: Duration) {
        add_duration(
            &self.inner.timing.parquet_decode_compute_permit_wait_ns,
            elapsed,
        );
    }

    pub(crate) fn record_parquet_row_filter_compute(&self, input_rows: usize, elapsed: Duration) {
        add(&self.inner.timing.parquet_row_filter_evaluations, 1);
        add(
            &self.inner.timing.parquet_row_filter_input_rows,
            u64::try_from(input_rows).unwrap_or(u64::MAX),
        );
        add_duration(&self.inner.timing.parquet_row_filter_compute_ns, elapsed);
    }

    pub(crate) fn record_parquet_alignment_time(&self, elapsed: Duration) {
        add_duration(&self.inner.timing.parquet_alignment_ns, elapsed);
    }

    pub(crate) fn record_csv_source_io_time(&self, elapsed: Duration) {
        add_duration(&self.inner.timing.csv_source_io_time_ns, elapsed);
    }

    pub(crate) fn record_csv_framing_time(&self, elapsed: Duration) {
        add_duration(&self.inner.timing.csv_framing_time_ns, elapsed);
    }

    pub(crate) fn record_csv_decode_compute_time(&self, elapsed: Duration) {
        add_duration(&self.inner.timing.csv_decode_compute_time_ns, elapsed);
    }
}

fn add_duration(counter: &AtomicU64, elapsed: Duration) {
    let nanos = u64::try_from(elapsed.as_nanos()).unwrap_or(u64::MAX);
    if nanos == 0 {
        return;
    }
    let _ = counter.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
        Some(current.saturating_add(nanos))
    });
}

fn load_duration(counter: &AtomicU64) -> Duration {
    Duration::from_nanos(counter.load(Ordering::Relaxed))
}

fn add(counter: &AtomicU64, value: u64) {
    if value == 0 {
        return;
    }
    let _ = counter.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
        Some(current.saturating_add(value))
    });
}

fn load(counter: &AtomicU64) -> u64 {
    counter.load(Ordering::Relaxed)
}
