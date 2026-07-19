use std::{
    fmt,
    io::Write,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use async_trait::async_trait;
use object_store::{MultipartUpload, PutPayload, PutResult, UploadPart};
use tokio::sync::Notify;

use super::{PART_BYTES, SharedMultipart};
use crate::{
    Error,
    runtime::{MemoryPool, QueryControl, TaskGroup},
};

#[tokio::test]
async fn cancelled_finish_waiter_does_not_drop_the_owned_upload() {
    let state = Arc::new(BlockingState::default());
    let group = TaskGroup::new(QueryControl::new());
    let client = SharedMultipart::create(
        Box::new(BlockingUpload {
            state: Arc::clone(&state),
        }),
        &group,
        MemoryPool::new(32 << 20),
    )
    .await
    .unwrap();
    let finish_client = client.clone();
    let finish = tokio::spawn(async move { finish_client.finish().await });

    state.complete_started.notified().await;
    finish.abort();
    assert!(finish.await.unwrap_err().is_cancelled());
    assert_ne!(group.active_tasks(), 0);

    let abort_client = client.clone();
    let abort = tokio::spawn(async move { abort_client.abort().await });
    state.complete_release.notify_one();
    abort.await.unwrap().unwrap();
    group.quiesce().await;

    assert_eq!(state.completes.load(Ordering::Acquire), 1);
    assert_eq!(state.aborts.load(Ordering::Acquire), 0);
    assert_eq!(group.active_tasks(), 0);
}

#[tokio::test]
async fn abandoning_every_client_makes_the_actor_abort_the_upload() {
    let state = Arc::new(BlockingState::default());
    let group = TaskGroup::new(QueryControl::new());
    let client = SharedMultipart::create(
        Box::new(BlockingUpload {
            state: Arc::clone(&state),
        }),
        &group,
        MemoryPool::new(32 << 20),
    )
    .await
    .unwrap();

    drop(client);
    group.quiesce().await;

    assert_eq!(state.completes.load(Ordering::Acquire), 0);
    assert_eq!(state.aborts.load(Ordering::Acquire), 1);
    assert_eq!(group.active_tasks(), 0);
}

#[tokio::test]
async fn queued_and_in_flight_parts_hold_memory_until_abort() {
    let state = Arc::new(BlockingPartsState::default());
    let group = TaskGroup::new(QueryControl::new());
    let memory = MemoryPool::new(PART_BYTES * 2);
    let mut client = SharedMultipart::create(
        Box::new(BlockingPartsUpload {
            state: Arc::clone(&state),
        }),
        &group,
        memory.clone(),
    )
    .await
    .unwrap();
    let part = vec![7_u8; PART_BYTES];

    client.write_all(&part).unwrap();
    client.write_all(&part).unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        while state.parts_started.load(Ordering::Acquire) != 2 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("multipart actor must own both parts");
    assert_eq!(memory.used(), PART_BYTES * 2);

    let io_error = client.write_all(&[1]).unwrap_err();
    assert!(io_error.to_string().contains("resource exhausted"));
    assert!(matches!(
        client.take_resource_error(),
        Some(Error::ResourceExhausted(_))
    ));

    client.abort().await.unwrap();
    group.quiesce().await;
    assert_eq!(memory.used(), 0);
    assert_eq!(state.aborts.load(Ordering::Acquire), 1);
}

#[derive(Default)]
struct BlockingState {
    complete_started: Notify,
    complete_release: Notify,
    completes: AtomicUsize,
    aborts: AtomicUsize,
}

struct BlockingUpload {
    state: Arc<BlockingState>,
}

#[derive(Default)]
struct BlockingPartsState {
    parts_started: AtomicUsize,
    part_release: Notify,
    aborts: AtomicUsize,
}

struct BlockingPartsUpload {
    state: Arc<BlockingPartsState>,
}

impl fmt::Debug for BlockingPartsUpload {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_struct("BlockingPartsUpload").finish()
    }
}

#[async_trait]
impl MultipartUpload for BlockingPartsUpload {
    fn put_part(&mut self, _data: PutPayload) -> UploadPart {
        self.state.parts_started.fetch_add(1, Ordering::AcqRel);
        let state = Arc::clone(&self.state);
        Box::pin(async move {
            state.part_release.notified().await;
            Ok(())
        })
    }

    async fn complete(&mut self) -> object_store::Result<PutResult> {
        Ok(PutResult {
            e_tag: Some("test-etag".to_owned()),
            version: None,
            extensions: Default::default(),
        })
    }

    async fn abort(&mut self) -> object_store::Result<()> {
        self.state.aborts.fetch_add(1, Ordering::AcqRel);
        Ok(())
    }
}

impl fmt::Debug for BlockingUpload {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_struct("BlockingUpload").finish()
    }
}

#[async_trait]
impl MultipartUpload for BlockingUpload {
    fn put_part(&mut self, _data: PutPayload) -> UploadPart {
        Box::pin(async { Ok(()) })
    }

    async fn complete(&mut self) -> object_store::Result<PutResult> {
        self.state.complete_started.notify_one();
        self.state.complete_release.notified().await;
        self.state.completes.fetch_add(1, Ordering::AcqRel);
        Ok(PutResult {
            e_tag: Some("test-etag".to_owned()),
            version: None,
            extensions: Default::default(),
        })
    }

    async fn abort(&mut self) -> object_store::Result<()> {
        self.state.aborts.fetch_add(1, Ordering::AcqRel);
        Ok(())
    }
}
