use std::{fmt, mem::size_of, sync::Arc};

use lru::LruCache;
use parking_lot::Mutex;
use parquet::arrow::arrow_reader::ArrowReaderMetadata;

use crate::storage::{ObjectSnapshot, ObjectSource};

#[derive(Clone)]
pub(crate) struct MetadataCache {
    inner: Arc<Mutex<CacheState>>,
}

struct CacheState {
    max_bytes: usize,
    used_bytes: usize,
    entries: LruCache<MetadataKey, CacheEntry>,
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
}

struct CacheEntry {
    metadata: ArrowReaderMetadata,
    weight: usize,
}

impl MetadataCache {
    pub(crate) fn new(max_bytes: usize) -> Self {
        Self {
            inner: Arc::new(Mutex::new(CacheState {
                max_bytes,
                used_bytes: 0,
                entries: LruCache::unbounded(),
                #[cfg(test)]
                hits: 0,
                #[cfg(test)]
                misses: 0,
            })),
        }
    }

    pub(crate) fn get(
        &self,
        source: &ObjectSource,
        snapshot: &ObjectSnapshot,
    ) -> Option<ArrowReaderMetadata> {
        let key = MetadataKey::new(source, snapshot);
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

    pub(crate) fn insert(
        &self,
        source: &ObjectSource,
        snapshot: &ObjectSnapshot,
        metadata: ArrowReaderMetadata,
    ) {
        let key = MetadataKey::new(source, snapshot);
        let weight = entry_weight(&key, &metadata);
        let mut state = self.inner.lock();
        if state.max_bytes == 0 || weight > state.max_bytes {
            return;
        }
        if let Some((_, previous)) = state.entries.push(key, CacheEntry { metadata, weight }) {
            state.used_bytes = state.used_bytes.saturating_sub(previous.weight);
        }
        state.used_bytes = state.used_bytes.saturating_add(weight);
        while state.used_bytes > state.max_bytes {
            let Some((_, evicted)) = state.entries.pop_lru() else {
                state.used_bytes = 0;
                break;
            };
            state.used_bytes = state.used_bytes.saturating_sub(evicted.weight);
        }
        debug_assert!(state.used_bytes <= state.max_bytes);
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

impl MetadataKey {
    fn new(source: &ObjectSource, snapshot: &ObjectSnapshot) -> Self {
        Self {
            uri: source.uri().to_owned(),
            size: snapshot.size,
            e_tag: snapshot.e_tag.clone(),
            version: snapshot.version.clone(),
        }
    }
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
        .saturating_add(size_of::<arrow::datatypes::Schema>());
    key_bytes
        .saturating_add(metadata.metadata().memory_size())
        .saturating_add(schema_bytes)
        .saturating_add(size_of::<CacheEntry>())
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
mod tests {
    use std::{fs::File, sync::Arc};

    use arrow::{
        array::Int64Array,
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    };
    use parquet::{
        arrow::{ArrowWriter, arrow_reader::ArrowReaderMetadata},
        file::reader::{FileReader, SerializedFileReader},
    };
    use tempfile::tempdir;

    use super::{MetadataCache, MetadataKey, entry_weight};
    use crate::{S3Config, storage::LocationResolver};

    #[tokio::test]
    async fn hits_and_invalidates_on_object_identity() {
        let (source, metadata) = fixture().await;
        let snapshot = source.snapshot().clone();
        let cache = MetadataCache::new(usize::MAX);
        assert!(cache.get(&source, &snapshot).is_none());
        cache.insert(&source, &snapshot, metadata.clone());
        assert!(cache.get(&source, &snapshot).is_some());

        let mut changed = snapshot.clone();
        changed.e_tag = Some("new-etag".to_owned());
        assert!(cache.get(&source, &changed).is_none());
        let stats = cache.stats();
        assert_eq!(stats.hits, 1);
        assert_eq!(stats.misses, 2);
    }

    #[tokio::test]
    async fn evicts_lru_without_exceeding_byte_limit() {
        let (source, metadata) = fixture().await;
        let first = source.snapshot().clone();
        let mut second = first.clone();
        second.version = Some("v2".to_owned());
        let mut third = first.clone();
        third.version = Some("v3".to_owned());
        let first_weight = entry_weight(&MetadataKey::new(&source, &first), &metadata);
        let second_weight = entry_weight(&MetadataKey::new(&source, &second), &metadata);
        let limit = first_weight.saturating_add(second_weight);
        let cache = MetadataCache::new(limit);

        cache.insert(&source, &first, metadata.clone());
        cache.insert(&source, &second, metadata.clone());
        assert!(cache.get(&source, &first).is_some());
        cache.insert(&source, &third, metadata);

        let stats = cache.stats();
        assert_eq!(stats.entries, 2);
        assert!(stats.used_bytes <= limit);
        assert!(cache.get(&source, &first).is_some());
        assert!(cache.get(&source, &second).is_none());
        assert!(cache.get(&source, &third).is_some());
    }

    async fn fixture() -> (crate::storage::ObjectSource, ArrowReaderMetadata) {
        let directory = tempdir().unwrap();
        let path = directory.path().join("cache.parquet");
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(vec![1_i64, 2]))],
        )
        .unwrap();
        let mut writer = ArrowWriter::try_new(File::create(&path).unwrap(), schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        let reader = SerializedFileReader::new(File::open(&path).unwrap()).unwrap();
        let metadata =
            ArrowReaderMetadata::try_new(Arc::new(reader.metadata().clone()), Default::default())
                .unwrap();
        let source = LocationResolver::new(S3Config::default())
            .resolve(&[path.display().to_string()])
            .await
            .unwrap()
            .pop()
            .unwrap();
        (source, metadata)
    }
}
