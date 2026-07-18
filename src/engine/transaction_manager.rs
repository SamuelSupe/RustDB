use std::{
    collections::BTreeMap,
    sync::{Arc, Weak},
};

use parking_lot::Mutex;
use uuid::Uuid;

#[derive(Clone, Default)]
pub(super) struct TransactionManager {
    inner: Arc<Inner>,
}

#[derive(Default)]
struct Inner {
    active: Mutex<BTreeMap<Uuid, ActiveTransaction>>,
}

#[allow(dead_code)]
#[derive(Clone, Copy)]
struct ActiveTransaction {
    snapshot_generation: u64,
    read_write: bool,
}

pub(super) struct TransactionLease {
    manager: Weak<Inner>,
    transaction_id: Uuid,
}

impl TransactionManager {
    pub(super) fn register(
        &self,
        transaction_id: Uuid,
        snapshot_generation: u64,
        read_write: bool,
    ) -> TransactionLease {
        let previous = self.inner.active.lock().insert(
            transaction_id,
            ActiveTransaction {
                snapshot_generation,
                read_write,
            },
        );
        debug_assert!(previous.is_none(), "transaction UUID collision");
        TransactionLease {
            manager: Arc::downgrade(&self.inner),
            transaction_id,
        }
    }

    #[allow(dead_code)]
    pub(super) fn oldest_snapshot_generation(&self) -> Option<u64> {
        self.inner
            .active
            .lock()
            .values()
            .map(|transaction| transaction.snapshot_generation)
            .min()
    }

    #[cfg(test)]
    pub(super) fn counts(&self) -> (usize, usize) {
        let active = self.inner.active.lock();
        (
            active.len(),
            active
                .values()
                .filter(|transaction| transaction.read_write)
                .count(),
        )
    }
}

impl Drop for TransactionLease {
    fn drop(&mut self) {
        let Some(manager) = self.manager.upgrade() else {
            return;
        };
        manager.active.lock().remove(&self.transaction_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tracks_the_oldest_live_snapshot_until_its_lease_is_dropped() {
        let manager = TransactionManager::default();
        let first = manager.register(Uuid::new_v4(), 4, false);
        let second = manager.register(Uuid::new_v4(), 8, true);

        assert_eq!(manager.counts(), (2, 1));
        assert_eq!(manager.oldest_snapshot_generation(), Some(4));
        drop(first);
        assert_eq!(manager.oldest_snapshot_generation(), Some(8));
        drop(second);
        assert_eq!(manager.oldest_snapshot_generation(), None);
    }
}
