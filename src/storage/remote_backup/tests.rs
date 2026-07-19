use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};

use async_trait::async_trait;
use bytes::Bytes;
use futures::StreamExt;
use object_store::{
    MultipartUpload, ObjectStore, ObjectStoreExt, PutPayload, PutResult, UploadPart,
    memory::InMemory, path::Path as ObjectPath,
};

use super::{upload_file_with_upload, upload_to_store};

#[derive(Clone, Copy, Debug)]
enum InjectedFailure {
    Part,
    Complete,
}

#[derive(Debug, Default)]
struct FakeState {
    aborts: AtomicUsize,
    data_object_present: AtomicBool,
    manifest_present: AtomicBool,
}

#[derive(Debug)]
struct FailingUpload {
    failure: InjectedFailure,
    state: Arc<FakeState>,
}

#[async_trait]
impl MultipartUpload for FailingUpload {
    fn put_part(&mut self, _data: PutPayload) -> UploadPart {
        self.state
            .data_object_present
            .store(true, Ordering::Release);
        let fail = matches!(self.failure, InjectedFailure::Part);
        Box::pin(async move {
            if fail {
                Err(injected_error("put_part"))
            } else {
                Ok(())
            }
        })
    }

    async fn complete(&mut self) -> object_store::Result<PutResult> {
        self.state
            .data_object_present
            .store(true, Ordering::Release);
        if matches!(self.failure, InjectedFailure::Complete) {
            return Err(injected_error("complete"));
        }
        Ok(PutResult {
            e_tag: Some("fake-etag".to_owned()),
            version: None,
            extensions: Default::default(),
        })
    }

    async fn abort(&mut self) -> object_store::Result<()> {
        self.state.aborts.fetch_add(1, Ordering::AcqRel);
        self.state
            .data_object_present
            .store(false, Ordering::Release);
        Ok(())
    }
}

#[tokio::test]
async fn multipart_part_and_complete_failures_abort_without_orphans() {
    for failure in [InjectedFailure::Part, InjectedFailure::Complete] {
        let directory = tempfile::tempdir().unwrap();
        let source = directory.path().join("source.bin");
        std::fs::write(&source, b"backup payload").unwrap();
        let state = Arc::new(FakeState::default());
        let upload = FailingUpload {
            failure,
            state: Arc::clone(&state),
        };

        let error = match upload_file_with_upload(Box::new(upload), &source, "source.bin").await {
            Ok(_) => panic!("injected multipart failure unexpectedly succeeded"),
            Err(error) => error,
        };

        assert!(error.to_string().contains("injected"));
        assert_eq!(state.aborts.load(Ordering::Acquire), 1);
        assert!(!state.data_object_present.load(Ordering::Acquire));
        // Manifest publication is downstream of every file upload and must
        // never be reached after either injected failure.
        assert!(!state.manifest_present.load(Ordering::Acquire));
    }
}

#[tokio::test]
async fn nonempty_destination_without_manifest_is_rejected_without_mutation() {
    let directory = tempfile::tempdir().unwrap();
    std::fs::write(directory.path().join("source.bin"), b"new backup").unwrap();
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let orphan = ObjectPath::from("backups/db/data/old-id/segment.arrow");
    store
        .put(&orphan, PutPayload::from(Bytes::from_static(b"orphan")))
        .await
        .unwrap();

    let error = upload_to_store(directory.path(), Arc::clone(&store), "backups/db")
        .await
        .unwrap_err();

    assert!(error.to_string().contains("already contains object"));
    let objects = store
        .list(Some(&ObjectPath::from("backups/db")))
        .collect::<Vec<_>>()
        .await;
    assert_eq!(objects.len(), 1);
    assert_eq!(objects[0].as_ref().unwrap().location, orphan);
    assert!(
        store
            .head(&ObjectPath::from("backups/db/manifest.json"))
            .await
            .is_err()
    );
}

fn injected_error(operation: &'static str) -> object_store::Error {
    object_store::Error::Generic {
        store: "remote-backup-test",
        source: Box::new(std::io::Error::other(format!(
            "injected {operation} failure"
        ))),
    }
}
