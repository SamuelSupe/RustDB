use std::{sync::Arc, time::Duration};

use parquet::bloom_filter::Sbbf;

use super::{
    BloomLoad, MetadataCache, MetadataKey, MetadataLevel, MetadataLoad, entry_weight,
    tests::fixture,
};
use crate::Error;

const WAITERS: usize = 6;

#[tokio::test]
async fn zero_capacity_footer_reuses_the_in_flight_result() {
    let (source, metadata) = fixture().await;
    let snapshot = source.snapshot().clone();
    let cache = MetadataCache::new(0);
    let MetadataLoad::Leader { guard, .. } = cache
        .acquire_footer(&source, &snapshot, None)
        .await
        .unwrap()
    else {
        panic!("first footer miss must lead");
    };
    let waiters = spawn_footer_waiters(&cache, &source, &snapshot);
    wait_for_metadata_waiters(&cache, WAITERS).await;

    cache.insert_footer(&source, &snapshot, metadata.clone());
    guard.succeed(metadata);

    for waiter in waiters {
        assert!(matches!(
            waiter.await.unwrap().unwrap(),
            MetadataLoad::Shared { .. }
        ));
    }
    assert_eq!(cache.stats().entries, 0);
}

#[tokio::test]
async fn oversized_page_index_reuses_the_in_flight_result() {
    let (source, metadata) = fixture().await;
    let snapshot = source.snapshot().clone();
    let weight = entry_weight(
        &MetadataKey::new(&source, &snapshot, MetadataLevel::PageIndex),
        &metadata,
    );
    let cache = MetadataCache::new(weight.saturating_sub(1));
    let MetadataLoad::Leader { guard, .. } = cache
        .acquire_page_index(&source, &snapshot, None)
        .await
        .unwrap()
    else {
        panic!("first page-index miss must lead");
    };
    let waiters = spawn_page_index_waiters(&cache, &source, &snapshot);
    wait_for_metadata_waiters(&cache, WAITERS).await;

    cache.insert_page_index(&source, &snapshot, metadata.clone());
    guard.succeed(metadata);

    for waiter in waiters {
        assert!(matches!(
            waiter.await.unwrap().unwrap(),
            MetadataLoad::Shared { .. }
        ));
    }
    assert_eq!(cache.stats().entries, 0);
}

#[tokio::test]
async fn zero_capacity_bloom_reuses_the_in_flight_result() {
    let (source, _) = fixture().await;
    let snapshot = source.snapshot().clone();
    let cache = MetadataCache::new(0);
    let BloomLoad::Leader { guard, .. } = cache
        .acquire_bloom(&source, &snapshot, 2, 3, 4096, 1024, None)
        .await
        .unwrap()
    else {
        panic!("first Bloom miss must lead");
    };
    let waiters = spawn_bloom_waiters(&cache, &source, &snapshot);
    wait_for_bloom_waiters(&cache, WAITERS).await;

    let filter = Arc::new(Sbbf::new_with_num_of_bytes(1024));
    cache.insert_bloom(&source, &snapshot, 2, 3, 4096, 1024, Arc::clone(&filter));
    guard.succeed(Some(filter));

    for waiter in waiters {
        assert!(matches!(
            waiter.await.unwrap().unwrap(),
            BloomLoad::Shared {
                filter: Some(_),
                ..
            }
        ));
    }
    assert_eq!(cache.stats().entries, 0);
}

#[tokio::test]
async fn cancelled_footer_leader_yields_to_a_healthy_waiter() {
    let (source, metadata) = fixture().await;
    let snapshot = source.snapshot().clone();
    let cache = MetadataCache::new(0);
    let MetadataLoad::Leader { guard, .. } = cache
        .acquire_footer(&source, &snapshot, None)
        .await
        .unwrap()
    else {
        panic!("first footer miss must lead");
    };
    let waiter_cache = cache.clone();
    let waiter_source = source.clone();
    let waiter_snapshot = snapshot.clone();
    let waiter = tokio::spawn(async move {
        waiter_cache
            .acquire_footer(&waiter_source, &waiter_snapshot, None)
            .await
    });
    wait_for_metadata_waiters(&cache, 1).await;

    assert!(matches!(guard.fail(Error::Cancelled), Error::Cancelled));
    let MetadataLoad::Leader {
        guard: retry_guard, ..
    } = tokio::time::timeout(Duration::from_secs(1), waiter)
        .await
        .expect("healthy waiter should take over")
        .unwrap()
        .unwrap()
    else {
        panic!("query-local cancellation must not be shared");
    };
    retry_guard.succeed(metadata);
}

#[tokio::test]
async fn resource_exhausted_bloom_leader_yields_to_a_healthy_waiter() {
    let (source, _) = fixture().await;
    let snapshot = source.snapshot().clone();
    let cache = MetadataCache::new(0);
    let BloomLoad::Leader { guard, .. } = cache
        .acquire_bloom(&source, &snapshot, 2, 3, 4096, 1024, None)
        .await
        .unwrap()
    else {
        panic!("first Bloom miss must lead");
    };
    let waiter_cache = cache.clone();
    let waiter_source = source.clone();
    let waiter_snapshot = snapshot.clone();
    let waiter = tokio::spawn(async move {
        waiter_cache
            .acquire_bloom(&waiter_source, &waiter_snapshot, 2, 3, 4096, 1024, None)
            .await
    });
    wait_for_bloom_waiters(&cache, 1).await;

    let error = guard.fail(Error::ResourceExhausted("leader query budget".into()));
    assert!(matches!(error, Error::ResourceExhausted(_)));
    let BloomLoad::Leader {
        guard: retry_guard, ..
    } = tokio::time::timeout(Duration::from_secs(1), waiter)
        .await
        .expect("healthy waiter should take over")
        .unwrap()
        .unwrap()
    else {
        panic!("query-local resource pressure must not be shared");
    };
    retry_guard.succeed(None);
}

#[tokio::test]
async fn concurrent_page_index_error_is_shared_without_a_second_loader() {
    let (source, _) = fixture().await;
    let snapshot = source.snapshot().clone();
    let cache = MetadataCache::new(0);
    let MetadataLoad::Leader { guard, .. } = cache
        .acquire_page_index(&source, &snapshot, None)
        .await
        .unwrap()
    else {
        panic!("first page-index miss must lead");
    };
    let waiters = spawn_page_index_waiters(&cache, &source, &snapshot);
    wait_for_metadata_waiters(&cache, WAITERS).await;

    let error = guard.fail(Error::Execution("injected page-index failure".to_owned()));
    assert!(matches!(error, Error::Execution(_)));
    for waiter in waiters {
        assert!(matches!(
            waiter.await.unwrap(),
            Err(Error::Execution(message)) if message == "injected page-index failure"
        ));
    }
}

#[tokio::test]
async fn concurrent_footer_error_preserves_the_structured_error_kind() {
    let (source, _) = fixture().await;
    let snapshot = source.snapshot().clone();
    let cache = MetadataCache::new(0);
    let MetadataLoad::Leader { guard, .. } = cache
        .acquire_footer(&source, &snapshot, None)
        .await
        .unwrap()
    else {
        panic!("first footer miss must lead");
    };
    let waiters = spawn_footer_waiters(&cache, &source, &snapshot);
    wait_for_metadata_waiters(&cache, WAITERS).await;

    let error = guard.fail(Error::Parquet(parquet::errors::ParquetError::General(
        "injected footer failure".to_owned(),
    )));
    assert!(matches!(error, Error::Parquet(_)));
    for waiter in waiters {
        assert!(matches!(waiter.await.unwrap(), Err(Error::Parquet(_))));
    }
}

#[tokio::test]
async fn concurrent_bloom_error_is_shared_without_a_second_loader() {
    let (source, _) = fixture().await;
    let snapshot = source.snapshot().clone();
    let cache = MetadataCache::new(0);
    let BloomLoad::Leader { guard, .. } = cache
        .acquire_bloom(&source, &snapshot, 2, 3, 4096, 1024, None)
        .await
        .unwrap()
    else {
        panic!("first Bloom miss must lead");
    };
    let waiters = spawn_bloom_waiters(&cache, &source, &snapshot);
    wait_for_bloom_waiters(&cache, WAITERS).await;

    let error = guard.fail(Error::Execution("injected Bloom failure".to_owned()));
    assert!(matches!(error, Error::Execution(_)));
    for waiter in waiters {
        assert!(matches!(
            waiter.await.unwrap(),
            Err(Error::Execution(message)) if message == "injected Bloom failure"
        ));
    }
}

fn spawn_footer_waiters(
    cache: &MetadataCache,
    source: &crate::storage::ObjectSource,
    snapshot: &crate::storage::ObjectSnapshot,
) -> Vec<tokio::task::JoinHandle<crate::Result<MetadataLoad>>> {
    (0..WAITERS)
        .map(|_| {
            let cache = cache.clone();
            let source = source.clone();
            let snapshot = snapshot.clone();
            tokio::spawn(async move { cache.acquire_footer(&source, &snapshot, None).await })
        })
        .collect()
}

fn spawn_page_index_waiters(
    cache: &MetadataCache,
    source: &crate::storage::ObjectSource,
    snapshot: &crate::storage::ObjectSnapshot,
) -> Vec<tokio::task::JoinHandle<crate::Result<MetadataLoad>>> {
    (0..WAITERS)
        .map(|_| {
            let cache = cache.clone();
            let source = source.clone();
            let snapshot = snapshot.clone();
            tokio::spawn(async move { cache.acquire_page_index(&source, &snapshot, None).await })
        })
        .collect()
}

fn spawn_bloom_waiters(
    cache: &MetadataCache,
    source: &crate::storage::ObjectSource,
    snapshot: &crate::storage::ObjectSnapshot,
) -> Vec<tokio::task::JoinHandle<crate::Result<BloomLoad>>> {
    (0..WAITERS)
        .map(|_| {
            let cache = cache.clone();
            let source = source.clone();
            let snapshot = snapshot.clone();
            tokio::spawn(async move {
                cache
                    .acquire_bloom(&source, &snapshot, 2, 3, 4096, 1024, None)
                    .await
            })
        })
        .collect()
}

async fn wait_for_metadata_waiters(cache: &MetadataCache, expected: usize) {
    wait_for_receivers(expected, || {
        cache
            .inner
            .lock()
            .in_flight
            .values()
            .map(tokio::sync::watch::Sender::receiver_count)
            .sum()
    })
    .await;
}

async fn wait_for_bloom_waiters(cache: &MetadataCache, expected: usize) {
    wait_for_receivers(expected, || {
        cache
            .inner
            .lock()
            .bloom_in_flight
            .values()
            .map(tokio::sync::watch::Sender::receiver_count)
            .sum()
    })
    .await;
}

async fn wait_for_receivers(expected: usize, count: impl Fn() -> usize) {
    tokio::time::timeout(Duration::from_secs(1), async {
        while count() < expected {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("all singleflight waiters should subscribe");
}
