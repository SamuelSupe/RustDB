use std::{fs, sync::Arc};

#[cfg(any(target_os = "linux", target_os = "macos"))]
use std::{fs::FileTimes, time::Duration};

use bytes::Bytes;
use object_store::{
    ObjectStore, ObjectStoreExt, PutPayload, memory::InMemory, path::Path as ObjectPath,
};
use tempfile::tempdir;

use super::{LocationResolver, ObjectSnapshot, literal_prefix, resolve_concrete_s3_source};
use crate::{
    S3Config,
    runtime::{MemoryPool, QueryContext},
    storage::{CopyManifestEntry, local_etag},
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
async fn local_etag_matches_the_object_store_wire_identity() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("identity.csv");
    fs::write(&path, b"value\n1\n").unwrap();
    let object = LocationResolver::new(S3Config::default())
        .resolve(&[path.display().to_string()])
        .await
        .unwrap()
        .remove(0);
    let expected = local_etag(&fs::metadata(path).unwrap());

    assert_eq!(object.snapshot().e_tag.as_deref(), Some(expected.as_str()));
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

#[tokio::test]
async fn file_source_uri_rejects_secrets_without_echoing_them() {
    for (uri, secret) in [
        (
            "file://alice:password-secret@localhost/tmp/input.csv",
            "password-secret",
        ),
        ("file:///tmp/input.csv?token=query-secret", "query-secret"),
        ("file:///tmp/input.csv#fragment-secret", "fragment-secret"),
    ] {
        let error = LocationResolver::new(S3Config::default())
            .resolve(&[uri.to_owned()])
            .await
            .unwrap_err()
            .to_string();
        assert!(!error.contains(secret), "{error}");
        assert!(!error.contains(uri), "{error}");
    }
}

#[test]
fn extracts_listing_prefix_before_glob_segment() {
    assert_eq!(
        literal_prefix("events/year=2026/*.parquet"),
        "events/year=2026"
    );
    assert_eq!(literal_prefix("*.parquet"), "");
}

#[tokio::test]
async fn rejects_ambiguous_exact_object_and_copy_manifest() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let path = ObjectPath::from("exports/result");
    let manifest = ObjectPath::from("exports/result/_rustdb_manifest.json");
    store
        .put(&path, PutPayload::from(Bytes::from_static(b"exact")))
        .await
        .unwrap();
    store
        .put(&manifest, PutPayload::from(Bytes::from_static(b"manifest")))
        .await
        .unwrap();

    let error =
        resolve_concrete_s3_source("bucket", "s3://bucket/exports/result", &path, store, None)
            .await
            .unwrap_err();
    assert!(error.to_string().contains("ambiguous S3 source"));
}

#[tokio::test]
async fn copy_manifest_rejects_a_same_size_replacement() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let prefix = ObjectPath::from("exports/result");
    let data = ObjectPath::from("exports/result/part.csv");
    let manifest = ObjectPath::from("exports/result/_rustdb_manifest.json");
    let identity = store
        .put(&data, PutPayload::from(Bytes::from_static(b"old\n")))
        .await
        .unwrap();
    let payload = crate::storage::encode_copy_manifest(&CopyManifestEntry {
        format: "csv".to_owned(),
        object: data.to_string(),
        bytes: 4,
        sha256: "a".repeat(64),
        e_tag: identity.e_tag,
        version: identity.version,
    })
    .unwrap();
    store
        .put(&manifest, PutPayload::from(Bytes::from(payload)))
        .await
        .unwrap();
    store
        .put(&data, PutPayload::from(Bytes::from_static(b"new\n")))
        .await
        .unwrap();

    let error =
        resolve_concrete_s3_source("bucket", "s3://bucket/exports/result", &prefix, store, None)
            .await
            .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("identity recorded by its manifest")
    );
}

#[tokio::test]
async fn legacy_copy_manifest_verifies_the_data_checksum() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let prefix = ObjectPath::from("exports/legacy");
    let data = ObjectPath::from("exports/legacy/part.csv");
    let manifest = ObjectPath::from("exports/legacy/_rustdb_manifest.json");
    store
        .put(&data, PutPayload::from(Bytes::from_static(b"new\n")))
        .await
        .unwrap();
    let payload = crate::storage::copy_manifest::encode_legacy(&CopyManifestEntry {
        format: "csv".to_owned(),
        object: data.to_string(),
        bytes: 4,
        sha256: "0".repeat(64),
        e_tag: None,
        version: None,
    })
    .unwrap();
    store
        .put(&manifest, PutPayload::from(Bytes::from(payload)))
        .await
        .unwrap();

    let error =
        resolve_concrete_s3_source("bucket", "s3://bucket/exports/legacy", &prefix, store, None)
            .await
            .unwrap_err();
    assert!(error.to_string().contains("failed checksum validation"));
}
