use std::sync::Arc;

use parking_lot::Mutex;

use crate::ParquetScanConfig;

const MAX_QUERY_PRUNING_BYTES: usize = 64 * 1024 * 1024;
pub(super) const MAX_FILE_PAGE_INDEX_BYTES: usize = 16 * 1024 * 1024;

#[derive(Clone, Debug)]
pub(super) struct PruningBudget {
    inner: Arc<Mutex<BudgetState>>,
}

#[derive(Debug)]
struct BudgetState {
    limit: usize,
    used: usize,
}

impl PruningBudget {
    pub(super) fn for_query(config: &ParquetScanConfig, query_limit: usize) -> Self {
        let limit = config
            .max_pruning_metadata_bytes
            .min(MAX_QUERY_PRUNING_BYTES)
            .min(query_limit / 16);
        Self {
            inner: Arc::new(Mutex::new(BudgetState { limit, used: 0 })),
        }
    }

    pub(super) fn try_reserve(&self, bytes: usize) -> Option<PruningLease> {
        let mut state = self.inner.lock();
        let next = state.used.checked_add(bytes)?;
        if next > state.limit {
            return None;
        }
        state.used = next;
        Some(PruningLease {
            budget: self.clone(),
            bytes,
        })
    }

    #[cfg(test)]
    fn used(&self) -> usize {
        self.inner.lock().used
    }
}

#[derive(Debug)]
pub(super) struct PruningLease {
    budget: PruningBudget,
    bytes: usize,
}

impl PruningLease {
    pub(super) fn try_resize(&mut self, bytes: usize) -> bool {
        let mut state = self.budget.inner.lock();
        if bytes > self.bytes {
            let Some(next) = state.used.checked_add(bytes - self.bytes) else {
                return false;
            };
            if next > state.limit {
                return false;
            }
            state.used = next;
        } else {
            state.used -= self.bytes - bytes;
        }
        self.bytes = bytes;
        true
    }
}

impl Drop for PruningLease {
    fn drop(&mut self) {
        let mut state = self.budget.inner.lock();
        state.used = state.used.saturating_sub(self.bytes);
    }
}

#[cfg(test)]
mod tests {
    use super::PruningBudget;
    use crate::ParquetScanConfig;

    #[test]
    fn effective_budget_is_bounded_and_released() {
        let config = ParquetScanConfig {
            max_pruning_metadata_bytes: usize::MAX,
            ..ParquetScanConfig::default()
        };
        let budget = PruningBudget::for_query(&config, 64 * 1024 * 1024);
        let mut lease = budget.try_reserve(3 * 1024 * 1024).unwrap();
        assert_eq!(budget.used(), 3 * 1024 * 1024);
        assert!(!lease.try_resize(5 * 1024 * 1024));
        assert!(budget.try_reserve(1024 * 1024).is_some());
        drop(lease);
        assert_eq!(budget.used(), 0);
    }
}
