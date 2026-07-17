const CAPACITY: usize = 16;

#[derive(Clone, Copy)]
struct Entry<'a> {
    key: &'a [u8],
    state_id: usize,
}

/// A tiny batch-local cache for repeated Arrow row keys.
///
/// Entries borrow the encoded batch, so no key ownership or reservation is
/// transferred here. Once the capacity is exceeded, the cache disables itself
/// and leaves every remaining lookup to the persistent aggregate index.
pub(in crate::execution::aggregate) struct EncodedGroupIdCache<'a> {
    entries: [Option<Entry<'a>>; CAPACITY],
    len: usize,
    saturated: bool,
}

impl<'a> EncodedGroupIdCache<'a> {
    pub(in crate::execution::aggregate) fn new() -> Self {
        Self {
            entries: [None; CAPACITY],
            len: 0,
            saturated: false,
        }
    }

    pub(in crate::execution::aggregate) fn get(&self, key: &[u8]) -> Option<usize> {
        if self.saturated {
            return None;
        }
        self.entries[..self.len]
            .iter()
            .flatten()
            .find(|entry| entry.key == key)
            .map(|entry| entry.state_id)
    }

    pub(in crate::execution::aggregate) fn insert(&mut self, key: &'a [u8], state_id: usize) {
        if self.saturated {
            return;
        }
        if self.len == CAPACITY {
            // The batch is not low-cardinality. Disable the linear cache so
            // uncached rows retain the original persistent-index cost.
            self.entries = [None; CAPACITY];
            self.saturated = true;
            return;
        }
        self.entries[self.len] = Some(Entry { key, state_id });
        self.len += 1;
    }

    pub(in crate::execution::aggregate) fn clear(&mut self) {
        self.entries = [None; CAPACITY];
        self.len = 0;
        self.saturated = false;
    }
}

#[cfg(test)]
mod tests {
    use super::{CAPACITY, EncodedGroupIdCache};

    #[test]
    fn repeated_keys_reuse_the_resolved_state_id() {
        let key = b"same";
        let mut cache = EncodedGroupIdCache::new();

        assert_eq!(cache.get(key), None);
        cache.insert(key, 7);
        assert_eq!(cache.get(key), Some(7));
        assert_eq!(cache.get(b"other"), None);
    }

    #[test]
    fn full_cache_disables_itself_and_falls_back_for_the_batch() {
        let keys: [[u8; 1]; CAPACITY] = std::array::from_fn(|index| [index as u8]);
        let extra = [u8::MAX];
        let mut cache = EncodedGroupIdCache::new();

        for (state_id, key) in keys.iter().enumerate() {
            cache.insert(key, state_id);
        }
        cache.insert(&extra, CAPACITY);

        assert_eq!(cache.get(&keys[CAPACITY - 1]), None);
        assert_eq!(cache.get(&extra), None);
    }

    #[test]
    fn clear_invalidates_ids_after_state_remapping() {
        let key = b"victim";
        let mut cache = EncodedGroupIdCache::new();
        cache.insert(key, 9);

        cache.clear();

        assert_eq!(cache.get(key), None);
    }
}
