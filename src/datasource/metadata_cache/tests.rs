use std::{fs::File, sync::Arc, time::Duration};

use arrow::{
    array::Int64Array,
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use parquet::{
    arrow::{ArrowWriter, arrow_reader::ArrowReaderMetadata},
    bloom_filter::Sbbf,
    file::reader::{FileReader, SerializedFileReader},
};
use tempfile::tempdir;

use super::{BloomLoad, MetadataCache, MetadataKey, MetadataLevel, MetadataLoad, entry_weight};
use crate::{S3Config, storage::LocationResolver};

#[tokio::test]
async fn hits_and_invalidates_on_object_identity() {
    let (source, metadata) = fixture().await;
    let snapshot = source.snapshot().clone();
    let cache = MetadataCache::new(usize::MAX);
    assert!(cache.get_footer(&source, &snapshot).is_none());
    cache.insert_footer(&source, &snapshot, metadata.clone());
    assert!(cache.get_footer(&source, &snapshot).is_some());
    assert!(cache.get_page_index(&source, &snapshot).is_none());

    let mut changed = snapshot.clone();
    changed.e_tag = Some("new-etag".to_owned());
    assert!(cache.get_footer(&source, &changed).is_none());
    let stats = cache.stats();
    assert_eq!(stats.hits, 1);
    assert_eq!(stats.misses, 3);
}

#[tokio::test]
async fn footer_and_page_index_entries_are_distinct() {
    let (source, metadata) = fixture().await;
    let snapshot = source.snapshot().clone();
    let cache = MetadataCache::new(usize::MAX);

    cache.insert_footer(&source, &snapshot, metadata.clone());
    assert!(cache.get_footer(&source, &snapshot).is_some());
    assert!(cache.get_page_index(&source, &snapshot).is_none());

    cache.insert_page_index(&source, &snapshot, metadata);
    assert!(cache.get_footer(&source, &snapshot).is_some());
    assert!(cache.get_page_index(&source, &snapshot).is_some());
    assert_eq!(cache.stats().entries, 2);
}

#[tokio::test]
async fn evicts_lru_without_exceeding_byte_limit() {
    let (source, metadata) = fixture().await;
    let first = source.snapshot().clone();
    let mut second = first.clone();
    second.version = Some("v2".to_owned());
    let mut third = first.clone();
    third.version = Some("v3".to_owned());
    let first_weight = entry_weight(
        &MetadataKey::new(&source, &first, MetadataLevel::Footer),
        &metadata,
    );
    let second_weight = entry_weight(
        &MetadataKey::new(&source, &second, MetadataLevel::Footer),
        &metadata,
    );
    let limit = first_weight.saturating_add(second_weight);
    let cache = MetadataCache::new(limit);

    cache.insert_footer(&source, &first, metadata.clone());
    cache.insert_footer(&source, &second, metadata.clone());
    assert!(cache.get_footer(&source, &first).is_some());
    cache.insert_footer(&source, &third, metadata);

    let stats = cache.stats();
    assert_eq!(stats.entries, 2);
    assert!(stats.used_bytes <= limit);
    assert!(cache.get_footer(&source, &first).is_some());
    assert!(cache.get_footer(&source, &second).is_none());
    assert!(cache.get_footer(&source, &third).is_some());
}

#[tokio::test]
async fn concurrent_footer_misses_have_one_loader() {
    let (source, metadata) = fixture().await;
    let snapshot = source.snapshot().clone();
    let cache = MetadataCache::new(usize::MAX);
    let MetadataLoad::Leader { guard: leader, .. } = cache
        .acquire_footer(&source, &snapshot, None)
        .await
        .unwrap()
    else {
        panic!("first miss must become the loader");
    };

    let waiter_cache = cache.clone();
    let waiter_source = source.clone();
    let waiter_snapshot = snapshot.clone();
    let waiter = tokio::spawn(async move {
        waiter_cache
            .acquire_footer(&waiter_source, &waiter_snapshot, None)
            .await
    });
    tokio::task::yield_now().await;
    assert!(!waiter.is_finished());

    cache.insert_footer(&source, &snapshot, metadata);
    drop(leader);
    let result = tokio::time::timeout(Duration::from_secs(1), waiter)
        .await
        .expect("waiter should be notified")
        .unwrap();
    assert!(matches!(
        result,
        Ok(MetadataLoad::Cached { wait, .. }) if !wait.is_zero()
    ));
}

#[tokio::test]
async fn concurrent_page_index_misses_have_one_loader() {
    let (source, metadata) = fixture().await;
    let snapshot = source.snapshot().clone();
    let cache = MetadataCache::new(usize::MAX);
    let MetadataLoad::Leader { guard: leader, .. } = cache
        .acquire_page_index(&source, &snapshot, None)
        .await
        .unwrap()
    else {
        panic!("first page-index miss must become the loader");
    };

    let waiter_cache = cache.clone();
    let waiter_source = source.clone();
    let waiter_snapshot = snapshot.clone();
    let waiter = tokio::spawn(async move {
        waiter_cache
            .acquire_page_index(&waiter_source, &waiter_snapshot, None)
            .await
    });
    tokio::task::yield_now().await;
    assert!(!waiter.is_finished());

    cache.insert_page_index(&source, &snapshot, metadata);
    drop(leader);
    let result = tokio::time::timeout(Duration::from_secs(1), waiter)
        .await
        .expect("page-index waiter should be notified")
        .unwrap();
    assert!(matches!(
        result,
        Ok(MetadataLoad::Cached { wait, .. }) if !wait.is_zero()
    ));
}

#[tokio::test]
async fn concurrent_bloom_misses_have_one_loader_and_identity_changes_miss() {
    let (source, _) = fixture().await;
    let snapshot = source.snapshot().clone();
    let cache = MetadataCache::new(usize::MAX);
    let BloomLoad::Leader { guard: leader, .. } = cache
        .acquire_bloom(&source, &snapshot, 2, 3, 4096, 1024, None)
        .await
        .unwrap()
    else {
        panic!("first Bloom miss must become the loader");
    };

    let waiter_cache = cache.clone();
    let waiter_source = source.clone();
    let waiter_snapshot = snapshot.clone();
    let waiter = tokio::spawn(async move {
        waiter_cache
            .acquire_bloom(&waiter_source, &waiter_snapshot, 2, 3, 4096, 1024, None)
            .await
    });
    tokio::task::yield_now().await;
    assert!(!waiter.is_finished());

    cache.insert_bloom(
        &source,
        &snapshot,
        2,
        3,
        4096,
        1024,
        Arc::new(Sbbf::new_with_num_of_bytes(1024)),
    );
    drop(leader);
    let result = tokio::time::timeout(Duration::from_secs(1), waiter)
        .await
        .expect("Bloom waiter should be notified")
        .unwrap();
    assert!(matches!(
        result,
        Ok(BloomLoad::Cached { wait, .. }) if !wait.is_zero()
    ));

    let mut changed = snapshot;
    changed.e_tag = Some("changed-bloom-etag".into());
    assert!(matches!(
        cache
            .acquire_bloom(&source, &changed, 2, 3, 4096, 1024, None)
            .await
            .unwrap(),
        BloomLoad::Leader { .. }
    ));
}

#[tokio::test]
async fn cancelled_waiter_does_not_wait_for_footer_loader() {
    let (source, _) = fixture().await;
    let snapshot = source.snapshot().clone();
    let cache = MetadataCache::new(usize::MAX);
    let MetadataLoad::Leader { guard: leader, .. } = cache
        .acquire_footer(&source, &snapshot, None)
        .await
        .unwrap()
    else {
        panic!("first miss must become the loader");
    };

    let control = crate::runtime::QueryControl::new();
    let waiter_control = control.clone();
    let waiter_cache = cache.clone();
    let waiter_source = source.clone();
    let waiter_snapshot = snapshot.clone();
    let waiter = tokio::spawn(async move {
        waiter_cache
            .acquire_footer(&waiter_source, &waiter_snapshot, Some(&waiter_control))
            .await
    });
    tokio::task::yield_now().await;
    control.cancel();
    let result = tokio::time::timeout(Duration::from_secs(1), waiter)
        .await
        .expect("cancelled waiter should terminate")
        .unwrap();
    assert!(matches!(result, Err(crate::Error::Cancelled)));
    drop(leader);
}

#[tokio::test]
async fn cancelled_waiter_does_not_wait_for_page_index_loader() {
    let (source, _) = fixture().await;
    let snapshot = source.snapshot().clone();
    let cache = MetadataCache::new(usize::MAX);
    let MetadataLoad::Leader { guard: leader, .. } = cache
        .acquire_page_index(&source, &snapshot, None)
        .await
        .unwrap()
    else {
        panic!("first page-index miss must become the loader");
    };

    let control = crate::runtime::QueryControl::new();
    let waiter_control = control.clone();
    let waiter_cache = cache.clone();
    let waiter_source = source.clone();
    let waiter_snapshot = snapshot.clone();
    let waiter = tokio::spawn(async move {
        waiter_cache
            .acquire_page_index(&waiter_source, &waiter_snapshot, Some(&waiter_control))
            .await
    });
    tokio::task::yield_now().await;
    control.cancel();

    let result = tokio::time::timeout(Duration::from_secs(1), waiter)
        .await
        .expect("cancelled page-index waiter should terminate")
        .unwrap();
    assert!(matches!(result, Err(crate::Error::Cancelled)));
    drop(leader);
}

#[tokio::test]
async fn cancelled_waiter_does_not_wait_for_bloom_loader() {
    let (source, _) = fixture().await;
    let snapshot = source.snapshot().clone();
    let cache = MetadataCache::new(usize::MAX);
    let BloomLoad::Leader { guard: leader, .. } = cache
        .acquire_bloom(&source, &snapshot, 2, 3, 4096, 1024, None)
        .await
        .unwrap()
    else {
        panic!("first Bloom miss must become the loader");
    };

    let control = crate::runtime::QueryControl::new();
    let waiter_control = control.clone();
    let waiter_cache = cache.clone();
    let waiter_source = source.clone();
    let waiter_snapshot = snapshot.clone();
    let waiter = tokio::spawn(async move {
        waiter_cache
            .acquire_bloom(
                &waiter_source,
                &waiter_snapshot,
                2,
                3,
                4096,
                1024,
                Some(&waiter_control),
            )
            .await
    });
    tokio::task::yield_now().await;
    control.cancel();

    let result = tokio::time::timeout(Duration::from_secs(1), waiter)
        .await
        .expect("cancelled Bloom waiter should terminate")
        .unwrap();
    assert!(matches!(result, Err(crate::Error::Cancelled)));
    drop(leader);
}

pub(super) async fn fixture() -> (crate::storage::ObjectSource, ArrowReaderMetadata) {
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
