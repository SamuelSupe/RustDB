use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use crate::datasource::{PredicateGuarantee, ScanRequest};

/// Query-wide row budget for an exact Parquet reader predicate.
///
/// Claims happen only after Arrow's complete row filter has produced a batch.
/// Exhaustion is deliberately not coupled to query cancellation: sibling
/// readers stop naturally when they next observe the empty budget.
#[derive(Debug)]
pub(super) struct FilteredLimit {
    remaining: AtomicUsize,
}

impl FilteredLimit {
    pub(super) fn for_request(request: &ScanRequest) -> Option<Arc<Self>> {
        if request.predicate_guarantee != PredicateGuarantee::Exact || request.predicate.is_none() {
            return None;
        }
        request.limit.map(|remaining| {
            Arc::new(Self {
                remaining: AtomicUsize::new(remaining),
            })
        })
    }

    pub(super) fn exhausted(&self) -> bool {
        self.remaining.load(Ordering::Relaxed) == 0
    }

    /// Atomically claims up to `rows` output rows without underflowing.
    pub(super) fn claim(&self, rows: usize) -> usize {
        if rows == 0 {
            return 0;
        }
        let mut remaining = self.remaining.load(Ordering::Relaxed);
        loop {
            if remaining == 0 {
                return 0;
            }
            let claimed = remaining.min(rows);
            match self.remaining.compare_exchange_weak(
                remaining,
                remaining - claimed,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => return claimed,
                Err(actual) => remaining = actual,
            }
        }
    }
}

/// Chooses the per-reader limit while preserving the semantic owner of rows.
pub(super) fn reader_limit(
    request: &ScanRequest,
    effective_rows: usize,
    raw_remaining: &mut usize,
) -> Option<usize> {
    let limit = request.limit?;
    if request.predicate.is_none() {
        let admitted = effective_rows.min(*raw_remaining);
        *raw_remaining = raw_remaining.saturating_sub(admitted);
        return Some(admitted);
    }
    if request.predicate_guarantee == PredicateGuarantee::Exact {
        return Some(effective_rows.min(limit));
    }
    // A best-effort predicate is only a pruning hint. Its residual is owned by
    // an upper Filter, so a reader-level limit could discard qualifying rows.
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::datasource::{ComparisonOp, PredicateValue, ScanPredicate};

    #[test]
    fn exact_filtered_limit_claims_and_reader_policy_are_safe() {
        let limit = FilteredLimit {
            remaining: AtomicUsize::new(3),
        };
        assert_eq!(limit.claim(2), 2);
        assert_eq!(limit.claim(2), 1);
        assert_eq!(limit.claim(1), 0);
        assert!(limit.exhausted());

        let mut request = ScanRequest::new(8);
        request.limit = Some(3);
        let mut raw_remaining = 3;
        assert_eq!(reader_limit(&request, 2, &mut raw_remaining), Some(2));
        assert_eq!(raw_remaining, 1);

        request.predicate = Some(ScanPredicate::Comparison {
            column: 0,
            op: ComparisonOp::Gt,
            value: PredicateValue::Int64(0),
        });
        assert_eq!(reader_limit(&request, 8, &mut raw_remaining), None);
        assert_eq!(raw_remaining, 1);

        request.predicate_guarantee = PredicateGuarantee::Exact;
        assert_eq!(reader_limit(&request, 8, &mut raw_remaining), Some(3));
        assert_eq!(raw_remaining, 1);
    }
}
