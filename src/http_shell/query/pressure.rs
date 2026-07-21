use std::{cmp::Reverse, collections::HashMap, sync::Arc};

use super::QueryRecord;

pub(super) fn cancel_largest(records: &HashMap<String, Arc<QueryRecord>>) -> Option<String> {
    let mut live = Vec::new();
    let mut without_metrics = Vec::new();
    for record in records.values() {
        if !record.running_and_cancellable() {
            continue;
        }
        let started = record
            .state
            .read()
            .started_at_ms
            .unwrap_or(record.created_at_ms);
        match record.current_memory_bytes() {
            Some(bytes) => live.push((
                Reverse(bytes),
                started,
                record.id.clone(),
                Arc::clone(record),
            )),
            None => without_metrics.push((started, record.id.clone(), Arc::clone(record))),
        }
    }

    live.sort_by(|left, right| left.0.cmp(&right.0).then_with(|| left.1.cmp(&right.1)));
    for (_, _, id, record) in live {
        if record.request_pressure_cancel() {
            return Some(id);
        }
    }
    without_metrics.sort_by(|left, right| left.0.cmp(&right.0).then_with(|| left.1.cmp(&right.1)));
    for (_, id, record) in without_metrics {
        if record.request_pressure_cancel() {
            return Some(id);
        }
    }
    None
}
