use std::{fs, sync::Arc};

use tempfile::tempdir;

use super::{LocationResolver, literal_prefix};
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

    fs::write(&path, b"value\nnew-and-different\n").unwrap();
    let current = objects[0].head_snapshot().await.unwrap();
    let error = context
        .register_object_snapshot(objects[0].uri(), current)
        .unwrap_err();
    assert!(error.to_string().contains("identity changed"));
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
