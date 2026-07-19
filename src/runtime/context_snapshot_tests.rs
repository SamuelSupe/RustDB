use super::{QueryContext, snapshot_entry_bytes};
use crate::{Error, runtime::MemoryPool, storage::ObjectSnapshot};

const URI: &str = "s3://bucket/data.parquet";

fn snapshot(version: Option<&str>) -> ObjectSnapshot {
    ObjectSnapshot {
        size: 10,
        e_tag: Some("opaque-etag".to_owned()),
        version: version.map(str::to_owned),
        local_identity: None,
    }
}

#[test]
fn query_snapshot_allows_identity_enrichment_before_sealing() {
    let root = tempfile::tempdir().expect("tempdir");
    let memory = MemoryPool::new(4_096);
    let context = QueryContext::new(memory.clone(), root.path()).expect("context");
    let listed = snapshot(None);
    context
        .register_object_snapshot(URI, listed.clone())
        .unwrap();
    let listed_bytes = memory.used();
    assert_eq!(listed_bytes, snapshot_entry_bytes(URI, &listed));

    let headed = snapshot(Some("version-1"));
    context
        .register_object_snapshot(URI, headed.clone())
        .unwrap();
    let refined_bytes = memory.used();
    assert_eq!(refined_bytes, snapshot_entry_bytes(URI, &headed));
    context
        .register_object_snapshot(URI, headed.clone())
        .unwrap();
    assert_eq!(memory.used(), refined_bytes);
    assert_identity_changed(context.register_object_snapshot(URI, listed));

    context.seal_object_snapshots();
    assert_eq!(context.object_snapshot(URI).unwrap(), headed);
    context
        .register_object_snapshot(URI, context.object_snapshot(URI).unwrap())
        .unwrap();
    assert_eq!(memory.used(), refined_bytes);
    assert_identity_changed(context.register_object_snapshot(URI, snapshot(Some("version-2"))));
    drop(context);
    assert_eq!(memory.used(), 0);
}

#[test]
fn snapshot_refinement_is_atomic_when_memory_is_exhausted() {
    let root = tempfile::tempdir().expect("tempdir");
    let listed = snapshot(None);
    let memory = MemoryPool::new(snapshot_entry_bytes(URI, &listed) + 32);
    let context = QueryContext::new(memory.clone(), root.path()).expect("context");
    context
        .register_object_snapshot(URI, listed.clone())
        .unwrap();
    let charged = memory.used();

    let error = context
        .register_object_snapshot(
            URI,
            ObjectSnapshot {
                version: Some("v".repeat(128)),
                ..listed.clone()
            },
        )
        .unwrap_err();
    assert!(matches!(error, Error::ResourceExhausted(_)));
    assert_eq!(memory.used(), charged);

    context.seal_object_snapshots();
    assert_identity_changed(context.register_object_snapshot(URI, snapshot(Some("v1"))));
    assert_eq!(context.object_snapshot(URI).unwrap(), listed);
    drop(context);
    assert_eq!(memory.used(), 0);
}

fn assert_identity_changed(result: crate::Result<()>) {
    assert!(result.unwrap_err().to_string().contains("identity changed"));
}
