use std::{
    collections::HashMap,
    fmt,
    mem::size_of,
    sync::Arc,
    time::{Duration, Instant},
};

use lru::LruCache;
use parking_lot::Mutex;
use parquet::{arrow::arrow_reader::ArrowReaderMetadata, bloom_filter::Sbbf};
use tokio::sync::watch;

use crate::storage::{ObjectSnapshot, ObjectSource};
use crate::{Result, runtime::QueryControl};

mod singleflight;

use singleflight::{BloomFlight, BloomLoadGuard, MetadataFlight, MetadataLoadGuard};

#[derive(Clone)]
pub(crate) struct MetadataCache {
    inner: Arc<Mutex<CacheState>>,
}

struct CacheState {
    max_bytes: usize,
    used_bytes: usize,
    entries: LruCache<MetadataKey, CacheEntry>,
    in_flight: HashMap<MetadataKey, watch::Sender<MetadataFlight>>,
    bloom_entries: LruCache<BloomKey, BloomEntry>,
    bloom_in_flight: HashMap<BloomKey, watch::Sender<BloomFlight>>,
    #[cfg(test)]
    hits: u64,
    #[cfg(test)]
    misses: u64,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct MetadataKey {
    uri: String,
    size: u64,
    e_tag: Option<String>,
    version: Option<String>,
    level: MetadataLevel,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
enum MetadataLevel {
    Footer,
    PageIndex,
}

struct CacheEntry {
    metadata: ArrowReaderMetadata,
    weight: usize,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct BloomKey {
    object: MetadataKey,
    row_group: usize,
    leaf: usize,
    offset: u64,
    length: usize,
}

struct BloomEntry {
    filter: Arc<Sbbf>,
    weight: usize,
}

impl MetadataCache {
    pub(crate) fn new(max_bytes: usize) -> Self {
        Self {
            inner: Arc::new(Mutex::new(CacheState {
                max_bytes,
                used_bytes: 0,
                entries: LruCache::unbounded(),
                in_flight: HashMap::new(),
                bloom_entries: LruCache::unbounded(),
                bloom_in_flight: HashMap::new(),
                #[cfg(test)]
                hits: 0,
                #[cfg(test)]
                misses: 0,
            })),
        }
    }

    #[cfg(test)]
    pub(crate) fn get_footer(
        &self,
        source: &ObjectSource,
        snapshot: &ObjectSnapshot,
    ) -> Option<ArrowReaderMetadata> {
        self.get(source, snapshot, MetadataLevel::Footer)
    }

    #[cfg(test)]
    pub(crate) fn get_page_index(
        &self,
        source: &ObjectSource,
        snapshot: &ObjectSnapshot,
    ) -> Option<ArrowReaderMetadata> {
        self.get(source, snapshot, MetadataLevel::PageIndex)
    }

    pub(crate) async fn acquire_footer(
        &self,
        source: &ObjectSource,
        snapshot: &ObjectSnapshot,
        control: Option<&QueryControl>,
    ) -> Result<MetadataLoad> {
        self.acquire(source, snapshot, MetadataLevel::Footer, control)
            .await
    }

    pub(crate) async fn acquire_page_index(
        &self,
        source: &ObjectSource,
        snapshot: &ObjectSnapshot,
        control: Option<&QueryControl>,
    ) -> Result<MetadataLoad> {
        self.acquire(source, snapshot, MetadataLevel::PageIndex, control)
            .await
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn acquire_bloom(
        &self,
        source: &ObjectSource,
        snapshot: &ObjectSnapshot,
        row_group: usize,
        leaf: usize,
        offset: u64,
        length: usize,
        control: Option<&QueryControl>,
    ) -> Result<BloomLoad> {
        let key = BloomKey::new(source, snapshot, row_group, leaf, offset, length);
        let mut wait_started = None;
        loop {
            if let Some(control) = control {
                control.check_cancelled()?;
            }
            let wait = {
                let mut state = self.inner.lock();
                if let Some(filter) = state
                    .bloom_entries
                    .get(&key)
                    .map(|entry| Arc::clone(&entry.filter))
                {
                    #[cfg(test)]
                    {
                        state.hits = state.hits.saturating_add(1);
                    }
                    return Ok(BloomLoad::Cached {
                        filter: Some(filter),
                        wait: elapsed(wait_started),
                    });
                }
                #[cfg(test)]
                {
                    state.misses = state.misses.saturating_add(1);
                }
                if let Some(sender) = state.bloom_in_flight.get(&key) {
                    Some(sender.subscribe())
                } else {
                    let (sender, _) = watch::channel(BloomFlight::Loading);
                    state.bloom_in_flight.insert(key.clone(), sender);
                    return Ok(BloomLoad::Leader {
                        guard: BloomLoadGuard::new(self.clone(), key),
                        wait: elapsed(wait_started),
                    });
                }
            };
            let mut wait = wait.expect("non-leader Bloom load has a waiter");
            wait_started.get_or_insert_with(Instant::now);
            let changed = if let Some(control) = control {
                tokio::select! {
                    _ = control.cancelled() => {
                        control.check_cancelled()?;
                        continue;
                    },
                    changed = wait.changed() => changed
                }
            } else {
                wait.changed().await
            };
            if changed.is_err() {
                continue;
            }
            match wait.borrow().clone() {
                BloomFlight::Ready(Ok(filter)) => {
                    return Ok(BloomLoad::Shared {
                        filter,
                        wait: elapsed(wait_started),
                    });
                }
                BloomFlight::Ready(Err(error)) => return Err(error.into_error()),
                BloomFlight::Loading | BloomFlight::Retry => {}
            }
        }
    }

    async fn acquire(
        &self,
        source: &ObjectSource,
        snapshot: &ObjectSnapshot,
        level: MetadataLevel,
        control: Option<&QueryControl>,
    ) -> Result<MetadataLoad> {
        let key = MetadataKey::new(source, snapshot, level);
        let mut wait_started = None;
        loop {
            if let Some(control) = control {
                control.check_cancelled()?;
            }
            let wait = {
                let mut state = self.inner.lock();
                if let Some(metadata) = state.entries.get(&key).map(|entry| entry.metadata.clone())
                {
                    #[cfg(test)]
                    {
                        state.hits = state.hits.saturating_add(1);
                    }
                    return Ok(MetadataLoad::Cached {
                        metadata,
                        wait: elapsed(wait_started),
                    });
                }
                #[cfg(test)]
                {
                    state.misses = state.misses.saturating_add(1);
                }
                if let Some(sender) = state.in_flight.get(&key) {
                    Some(sender.subscribe())
                } else {
                    let (sender, _) = watch::channel(MetadataFlight::Loading);
                    state.in_flight.insert(key.clone(), sender);
                    return Ok(MetadataLoad::Leader {
                        guard: MetadataLoadGuard::new(self.clone(), key),
                        wait: elapsed(wait_started),
                    });
                }
            };
            let mut wait = wait.expect("non-leader metadata load has a waiter");
            wait_started.get_or_insert_with(Instant::now);
            let changed = if let Some(control) = control {
                tokio::select! {
                    _ = control.cancelled() => {
                        control.check_cancelled()?;
                        continue;
                    },
                    changed = wait.changed() => changed
                }
            } else {
                wait.changed().await
            };
            if changed.is_err() {
                continue;
            }
            match wait.borrow().clone() {
                MetadataFlight::Ready(Ok(metadata)) => {
                    return Ok(MetadataLoad::Shared {
                        metadata,
                        wait: elapsed(wait_started),
                    });
                }
                MetadataFlight::Ready(Err(error)) => return Err(error.into_error()),
                MetadataFlight::Loading | MetadataFlight::Retry => {}
            }
        }
    }

    #[cfg(test)]
    fn get(
        &self,
        source: &ObjectSource,
        snapshot: &ObjectSnapshot,
        level: MetadataLevel,
    ) -> Option<ArrowReaderMetadata> {
        let key = MetadataKey::new(source, snapshot, level);
        let mut state = self.inner.lock();
        let metadata = state.entries.get(&key).map(|entry| entry.metadata.clone());
        #[cfg(test)]
        if metadata.is_some() {
            state.hits = state.hits.saturating_add(1);
        } else {
            state.misses = state.misses.saturating_add(1);
        }
        metadata
    }

    pub(crate) fn insert_footer(
        &self,
        source: &ObjectSource,
        snapshot: &ObjectSnapshot,
        metadata: ArrowReaderMetadata,
    ) {
        self.insert(source, snapshot, MetadataLevel::Footer, metadata);
    }

    pub(crate) fn insert_page_index(
        &self,
        source: &ObjectSource,
        snapshot: &ObjectSnapshot,
        metadata: ArrowReaderMetadata,
    ) {
        self.insert(source, snapshot, MetadataLevel::PageIndex, metadata);
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn insert_bloom(
        &self,
        source: &ObjectSource,
        snapshot: &ObjectSnapshot,
        row_group: usize,
        leaf: usize,
        offset: u64,
        length: usize,
        filter: Arc<Sbbf>,
    ) {
        let key = BloomKey::new(source, snapshot, row_group, leaf, offset, length);
        let weight = bloom_weight(&key);
        let mut state = self.inner.lock();
        if state.max_bytes == 0 || weight > state.max_bytes {
            return;
        }
        if let Some((_, previous)) = state.bloom_entries.push(key, BloomEntry { filter, weight }) {
            state.used_bytes = state.used_bytes.saturating_sub(previous.weight);
        }
        state.used_bytes = state.used_bytes.saturating_add(weight);
        evict_to_budget(&mut state);
    }

    fn insert(
        &self,
        source: &ObjectSource,
        snapshot: &ObjectSnapshot,
        level: MetadataLevel,
        metadata: ArrowReaderMetadata,
    ) {
        let weight = metadata_weight(source, snapshot, &metadata);
        let key = MetadataKey::new(source, snapshot, level);
        let mut state = self.inner.lock();
        if state.max_bytes == 0 || weight > state.max_bytes {
            return;
        }
        if let Some((_, previous)) = state.entries.push(key, CacheEntry { metadata, weight }) {
            state.used_bytes = state.used_bytes.saturating_sub(previous.weight);
        }
        state.used_bytes = state.used_bytes.saturating_add(weight);
        evict_to_budget(&mut state);
        debug_assert!(state.used_bytes <= state.max_bytes);
    }

    fn finish_metadata(&self, key: &MetadataKey, outcome: MetadataFlight) {
        let mut state = self.inner.lock();
        if let Some(sender) = state.in_flight.remove(key) {
            sender.send_replace(outcome);
        }
    }

    fn finish_bloom(&self, key: &BloomKey, outcome: BloomFlight) {
        let mut state = self.inner.lock();
        if let Some(sender) = state.bloom_in_flight.remove(key) {
            sender.send_replace(outcome);
        }
    }

    #[cfg(test)]
    fn stats(&self) -> CacheStats {
        let state = self.inner.lock();
        CacheStats {
            entries: state.entries.len(),
            used_bytes: state.used_bytes,
            hits: state.hits,
            misses: state.misses,
        }
    }
}

fn evict_to_budget(state: &mut CacheState) {
    while state.used_bytes > state.max_bytes {
        let weight = state
            .bloom_entries
            .pop_lru()
            .map(|(_, entry)| entry.weight)
            .or_else(|| state.entries.pop_lru().map(|(_, entry)| entry.weight));
        let Some(weight) = weight else {
            state.used_bytes = 0;
            break;
        };
        state.used_bytes = state.used_bytes.saturating_sub(weight);
    }
    debug_assert!(state.used_bytes <= state.max_bytes);
}

pub(crate) enum MetadataLoad {
    Cached {
        metadata: ArrowReaderMetadata,
        wait: Duration,
    },
    Shared {
        metadata: ArrowReaderMetadata,
        wait: Duration,
    },
    Leader {
        guard: MetadataLoadGuard,
        wait: Duration,
    },
}

pub(crate) enum BloomLoad {
    Cached {
        filter: Option<Arc<Sbbf>>,
        wait: Duration,
    },
    Shared {
        filter: Option<Arc<Sbbf>>,
        wait: Duration,
    },
    Leader {
        guard: BloomLoadGuard,
        wait: Duration,
    },
}

fn elapsed(started: Option<Instant>) -> Duration {
    started.map_or(Duration::ZERO, |started| started.elapsed())
}

/// Conservative live weight used both by the cache and by query-scoped
/// metadata reservations. Keeping the calculation in one place prevents a
/// cached footer from bypassing the query memory budget.
pub(super) fn metadata_weight(
    source: &ObjectSource,
    snapshot: &ObjectSnapshot,
    metadata: &ArrowReaderMetadata,
) -> usize {
    entry_weight(
        &MetadataKey::new(source, snapshot, MetadataLevel::Footer),
        metadata,
    )
}

impl MetadataKey {
    fn new(source: &ObjectSource, snapshot: &ObjectSnapshot, level: MetadataLevel) -> Self {
        Self {
            uri: source.uri().to_owned(),
            size: snapshot.size,
            e_tag: snapshot.e_tag.clone(),
            version: snapshot.version.clone(),
            level,
        }
    }
}

impl BloomKey {
    fn new(
        source: &ObjectSource,
        snapshot: &ObjectSnapshot,
        row_group: usize,
        leaf: usize,
        offset: u64,
        length: usize,
    ) -> Self {
        Self {
            object: MetadataKey::new(source, snapshot, MetadataLevel::Footer),
            row_group,
            leaf,
            offset,
            length,
        }
    }
}

fn bloom_weight(key: &BloomKey) -> usize {
    size_of::<BloomEntry>()
        .saturating_add(size_of::<BloomKey>())
        .saturating_add(key.object.uri.len())
        .saturating_add(key.object.e_tag.as_ref().map_or(0, String::len))
        .saturating_add(key.object.version.as_ref().map_or(0, String::len))
        .saturating_add(key.length)
        .saturating_add(256)
}

fn entry_weight(key: &MetadataKey, metadata: &ArrowReaderMetadata) -> usize {
    let key_bytes = size_of::<MetadataKey>()
        .saturating_add(key.uri.len())
        .saturating_add(key.e_tag.as_ref().map_or(0, String::len))
        .saturating_add(key.version.as_ref().map_or(0, String::len));
    let schema_bytes = metadata
        .schema()
        .fields()
        .size()
        .saturating_mul(2)
        .saturating_add(size_of::<arrow::datatypes::Schema>());
    key_bytes
        .saturating_add(metadata.metadata().memory_size())
        .saturating_add(schema_bytes)
        .saturating_add(size_of::<ArrowReaderMetadata>())
        .saturating_add(size_of::<CacheEntry>())
        .saturating_add(4 * 1024)
}

impl fmt::Debug for MetadataCache {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let state = self.inner.lock();
        formatter
            .debug_struct("MetadataCache")
            .field("max_bytes", &state.max_bytes)
            .field("used_bytes", &state.used_bytes)
            .field("entries", &state.entries.len())
            .finish()
    }
}

#[cfg(test)]
#[derive(Debug)]
struct CacheStats {
    entries: usize,
    used_bytes: usize,
    hits: u64,
    misses: u64,
}

#[cfg(test)]
#[path = "metadata_cache/tests.rs"]
mod tests;

#[cfg(test)]
#[path = "metadata_cache/singleflight_tests.rs"]
mod singleflight_tests;
