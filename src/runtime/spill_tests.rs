use std::sync::Arc;

use arrow::{
    array::{ArrayRef, Int32Array, StringArray},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use futures::StreamExt;

use super::{QueryControl, QueryMetrics, SpillManager};
use crate::Error;

fn batch() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef,
            Arc::new(StringArray::from(vec!["a", "b", "c"])) as ArrayRef,
        ],
    )
    .expect("valid batch")
}

#[test]
fn round_trips_lz4_ipc_and_cleans_on_drop() {
    let root = tempfile::tempdir().expect("tempdir");
    let batch = batch();
    let directory;
    {
        let manager = SpillManager::new(root.path()).expect("spill manager");
        directory = manager.directory().to_owned();
        let spill = manager
            .write_record_batches("sort/run", batch.schema(), vec![batch.clone()])
            .expect("write spill");
        let read = manager.read_batches(&spill).expect("read spill");
        assert_eq!(read, vec![batch]);
        assert!(spill.path().starts_with(&directory));

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(spill.path())
                .expect("metadata")
                .permissions()
                .mode()
                & 0o777;
            assert_eq!(mode, 0o600);
        }
    }
    assert!(!directory.exists());
}

#[tokio::test]
async fn cancellation_stops_reading_and_removes_query_directory() {
    let root = tempfile::tempdir().expect("tempdir");
    let control = QueryControl::new();
    let metrics = QueryMetrics::new();
    let manager = SpillManager::for_query(
        root.path(),
        uuid::Uuid::new_v4(),
        &control,
        Some(metrics.clone()),
    )
    .expect("spill manager");
    let batch = batch();
    let spill = manager
        .write_record_batches("aggregate", batch.schema(), vec![batch])
        .expect("write spill");
    let mut stream = manager.read_stream(&spill).expect("read stream");
    assert!(manager.directory().exists());
    assert_eq!(metrics.snapshot().spill_partitions, 1);

    control.cancel();
    assert!(!manager.directory().exists());
    assert!(matches!(stream.next().await, Some(Err(Error::Cancelled))));
}
