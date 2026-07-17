use std::{fs, sync::Arc};

#[cfg(any(target_os = "linux", target_os = "macos"))]
use std::{fs::FileTimes, time::Duration};

use object_store::ObjectStoreExt;
use tempfile::tempdir;

use super::{LocationResolver, ObjectSnapshot, literal_prefix};
use crate::{
    S3Config,
    runtime::{MemoryPool, QueryContext},
};

#[tokio::test]
async fn expands_local_globs_in_stable_order_and_deduplicates() {
    let directory = tempdir().unwrap();
    fs::write(directory.path().join("b.csv"), b"b\n2\n").unwrap();
    fs::write(directory.path().join("a.csv"), b"a\n1\n").unwrap();
    let pattern = format!("{}/*.csv", directory.path().display());
    let locations = vec![pattern.clone(), pattern];

    let objects = LocationResolver::new(S3Config::default())
        .resolve(&locations)
        .await
        .unwrap();

    assert_eq!(objects.len(), 2);
    assert!(objects[0].uri().ends_with("a.csv"));
    assert!(objects[1].uri().ends_with("b.csv"));
    assert_eq!(
        objects[0].head_snapshot().await.unwrap(),
        objects[0].snapshot().clone()
    );
}

#[tokio::test]
async fn query_resolution_registers_the_initial_object_identity() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("changing.csv");
    fs::write(&path, b"value\nold\n").unwrap();
    let context = Arc::new(QueryContext::new(MemoryPool::new(1 << 20), directory.path()).unwrap());
    let objects = LocationResolver::new(S3Config::default())
        .resolve_for_query(&[path.display().to_string()], &context)
        .await
        .unwrap();
    assert_eq!(context.metrics.snapshot().discovered_files, 1);

    fs::write(&path, b"value\nnew-and-different\n").unwrap();
    let current = objects[0].head_snapshot().await.unwrap();
    let error = context
        .register_object_snapshot(objects[0].uri(), current)
        .unwrap_err();
    assert!(error.to_string().contains("identity changed"));
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
#[tokio::test]
async fn local_snapshot_detects_same_size_mutation_with_restored_mtime() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("identity.bin");
    fs::write(&path, b"before").unwrap();
    let object = LocationResolver::new(S3Config::default())
        .resolve(&[path.display().to_string()])
        .await
        .unwrap()
        .remove(0);
    let before = object.snapshot().clone();
    let modified = fs::metadata(&path).unwrap().modified().unwrap();

    std::thread::sleep(Duration::from_millis(2));
    fs::write(&path, b"after!").unwrap();
    fs::File::options()
        .write(true)
        .open(&path)
        .unwrap()
        .set_times(FileTimes::new().set_modified(modified))
        .unwrap();

    let after = object.head_snapshot().await.unwrap();
    assert_eq!(before.size, after.size);
    assert_eq!(
        before.e_tag, after.e_tag,
        "weak local ETag unexpectedly changed"
    );
    assert_ne!(before.local_identity, after.local_identity);
}

#[tokio::test]
async fn get_response_must_preserve_every_snapshotted_identity_token() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("identity.csv");
    fs::write(&path, b"value\n1\n").unwrap();
    let object = LocationResolver::new(S3Config::default())
        .resolve(&[path.display().to_string()])
        .await
        .unwrap()
        .remove(0);
    let mut response = object.store().head(object.location()).await.unwrap();
    response.e_tag = None;
    response.version = None;
    let expected = ObjectSnapshot {
        size: response.size,
        e_tag: Some("expected-etag".to_owned()),
        version: Some("expected-version".to_owned()),
        local_identity: None,
    };

    let error = expected
        .validate_get_response(object.uri(), &response)
        .unwrap_err()
        .to_string();

    assert!(error.contains("object changed during query"), "{error}");
    assert!(error.contains("identity.csv"), "{error}");
}

#[tokio::test]
async fn local_get_response_accepts_its_wire_identity_without_an_fd_identity() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("identity.csv");
    fs::write(&path, b"value\n1\n").unwrap();
    let object = LocationResolver::new(S3Config::default())
        .resolve(&[path.display().to_string()])
        .await
        .unwrap()
        .remove(0);
    let expected = object.head_snapshot().await.unwrap();
    assert!(expected.local_identity.is_some());

    let response = object.store().get(object.location()).await.unwrap();
    expected
        .validate_get_response(object.uri(), &response.meta)
        .unwrap();
}

#[tokio::test]
async fn rejects_file_lists_that_exceed_the_metadata_cap() {
    let directory = tempdir().unwrap();
    for index in 0..64 {
        fs::write(
            directory.path().join(format!("part-{index:03}.csv")),
            b"v\n1\n",
        )
        .unwrap();
    }
    let locations = vec![format!("{}/*.csv", directory.path().display())];
    let error = LocationResolver::with_metadata_limit(S3Config::default(), 1_024)
        .resolve(&locations)
        .await
        .unwrap_err();

    assert!(matches!(error, crate::Error::ResourceExhausted(_)));
    assert!(error.to_string().contains("file metadata limit"));
}

#[test]
fn extracts_listing_prefix_before_glob_segment() {
    assert_eq!(
        literal_prefix("events/year=2026/*.parquet"),
        "events/year=2026"
    );
    assert_eq!(literal_prefix("*.parquet"), "");
}
