use std::{io::Seek, sync::Arc};

use arrow::{
    array::Int64Array,
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use bytes::Bytes;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload, memory::InMemory, path::Path};
use parquet::file::reader::{FileReader, SerializedFileReader};

use super::{
    Encoder, LocalWriter, RemoteWriter, ensure_remote_destination_available, local_durability_error,
};
use crate::{
    command::{CopyCsvOptions, CopyFormat},
    runtime::{MemoryPool, QueryContext},
};

#[tokio::test]
async fn remote_copy_rejects_an_exact_destination_object() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let destination = Path::from("exports/result");
    let manifest = Path::from("exports/result/_rustdb_manifest.json");
    store
        .put(&destination, PutPayload::from(Bytes::from_static(b"old")))
        .await
        .unwrap();

    let error = ensure_remote_destination_available(&store, &destination, &manifest)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("already exists"));
}

#[tokio::test]
async fn remote_copy_rejects_an_existing_child_manifest() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let destination = Path::from("exports/result");
    let manifest = Path::from("exports/result/_rustdb_manifest.json");
    store
        .put(&manifest, PutPayload::from(Bytes::from_static(b"old")))
        .await
        .unwrap();

    let error = ensure_remote_destination_available(&store, &destination, &manifest)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("already exists"));
}

#[tokio::test]
async fn remote_copy_accepts_an_unused_destination() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let destination = Path::from("exports/result");
    let manifest = Path::from("exports/result/_rustdb_manifest.json");

    ensure_remote_destination_available(&store, &destination, &manifest)
        .await
        .unwrap();
}

#[tokio::test]
async fn remote_copy_rejects_an_incomplete_child_object() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let destination = Path::from("exports/result");
    let manifest = Path::from("exports/result/_rustdb_manifest.json");
    let orphan = Path::from("exports/result/part-abandoned.csv");
    store
        .put(&orphan, PutPayload::from(Bytes::from_static(b"partial")))
        .await
        .unwrap();

    let error = ensure_remote_destination_available(&store, &destination, &manifest)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("incomplete output"));
    assert!(store.head(&orphan).await.is_ok());
}

#[test]
fn local_copy_rejects_a_matching_abandoned_staging_file() {
    let temp = tempfile::tempdir().unwrap();
    let destination = temp.path().join("result.csv");
    let abandoned = temp.path().join(format!(
        ".result.csv.rustdb-copy-{}.tmp",
        uuid::Uuid::new_v4()
    ));
    std::fs::write(&abandoned, b"partial").unwrap();
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));

    let error = match LocalWriter::create(
        destination,
        CopyFormat::Csv,
        CopyCsvOptions::default(),
        schema,
        MemoryPool::new(1 << 20),
        8_192,
    ) {
        Ok(_) => panic!("abandoned staging must block COPY retry"),
        Err(error) => error,
    };
    assert!(error.to_string().contains("staging file already exists"));
    assert!(abandoned.exists());
}

#[test]
fn parquet_encoder_uses_query_bounded_row_groups_and_releases_memory() {
    let memory = MemoryPool::new(1 << 20);
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let output = tempfile::tempfile().unwrap();
    let mut encoder = Encoder::create(
        output,
        CopyFormat::Parquet,
        &CopyCsvOptions::default(),
        Arc::clone(&schema),
        memory.clone(),
        2,
    )
    .unwrap();
    let batch = RecordBatch::try_new(
        schema,
        vec![Arc::new(Int64Array::from(vec![1_i64, 2, 3, 4, 5]))],
    )
    .unwrap();

    encoder.write(&batch).unwrap();
    let mut output = encoder.finish().unwrap();
    assert_eq!(memory.used(), 0);

    output.rewind().unwrap();
    let reader = SerializedFileReader::new(output).unwrap();
    assert_eq!(reader.num_row_groups(), 3);
    for index in 0..reader.num_row_groups() {
        assert!(reader.metadata().row_group(index).num_rows() <= 2);
    }
}

#[test]
fn wide_csv_batch_is_rejected_before_unreserved_encoding() {
    let memory = MemoryPool::new(1 << 10);
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Utf8,
        false,
    )]));
    let mut encoder = Encoder::create(
        Vec::new(),
        CopyFormat::Csv,
        &CopyCsvOptions::default(),
        Arc::clone(&schema),
        memory.clone(),
        8_192,
    )
    .unwrap();
    let batch = RecordBatch::try_new(
        schema,
        vec![Arc::new(arrow::array::StringArray::from(vec![
            "\"\\,\n".repeat(1 << 10),
        ]))],
    )
    .unwrap();

    let error = encoder.write(&batch).unwrap_err();
    assert!(matches!(error, crate::Error::ResourceExhausted(_)));
    drop(encoder);
    assert_eq!(memory.used(), 0);
}

#[tokio::test]
async fn remote_writer_completes_actor_upload_and_publishes_manifest() {
    let temp = tempfile::tempdir().unwrap();
    let context = Arc::new(QueryContext::new(MemoryPool::new(32 << 20), temp.path()).unwrap());
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let mut writer = RemoteWriter::create(
        "memory://exports/complete".to_owned(),
        Arc::clone(&store),
        Path::from("exports/complete"),
        CopyFormat::Csv,
        CopyCsvOptions::default(),
        Arc::clone(&schema),
        &context,
    )
    .await
    .unwrap();
    let data = writer.data.clone();
    let manifest = writer.manifest.clone();
    writer
        .write(
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1_i64, 2]))]).unwrap(),
            &context,
        )
        .await
        .unwrap();

    let bytes = writer.finish(&context).await.unwrap();
    assert_ne!(bytes, 0);
    drop(writer);
    context.tasks.quiesce().await;

    assert!(store.head(&data).await.is_ok());
    assert!(store.head(&manifest).await.is_ok());
    assert_eq!(context.tasks.active_tasks(), 0);
    assert_eq!(context.memory.used(), 0);
    assert!(!context.has_protected_async_cleanup());
}

#[tokio::test]
async fn remote_copy_reports_buffer_reservation_failure_and_releases_memory() {
    let temp = tempfile::tempdir().unwrap();
    let memory = MemoryPool::new(4 << 20);
    let context = Arc::new(QueryContext::new(memory.clone(), temp.path()).unwrap());
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let mut writer = RemoteWriter::create(
        "memory://exports/limited".to_owned(),
        Arc::clone(&store),
        Path::from("exports/limited"),
        CopyFormat::Csv,
        CopyCsvOptions::default(),
        Arc::clone(&schema),
        &context,
    )
    .await
    .unwrap();
    let error = writer
        .write(
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1_i64]))]).unwrap(),
            &context,
        )
        .await
        .unwrap_err();
    assert!(matches!(error, crate::Error::ResourceExhausted(_)));
    drop(writer);
    context.tasks.quiesce().await;
    assert_eq!(memory.used(), 0);
    assert!(!context.has_protected_async_cleanup());
}

#[test]
fn local_publication_sync_failure_reports_an_unknown_outcome() {
    let error = local_durability_error(
        std::path::Path::new("/tmp/out.parquet"),
        crate::Error::io(None, std::io::Error::other("injected sync failure")),
    );
    assert!(matches!(error, crate::Error::CommitOutcomeUnknown { .. }));
}

#[tokio::test]
async fn abandoned_remote_writer_cleanup_is_query_task_owned() {
    let temp = tempfile::tempdir().unwrap();
    let context = Arc::new(QueryContext::new(MemoryPool::new(32 << 20), temp.path()).unwrap());
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let writer = remote_writer(Arc::clone(&store), "exports/abandoned", &context).await;
    let data = writer.data.clone();

    drop(writer);
    tokio::time::timeout(std::time::Duration::from_secs(2), context.tasks.quiesce())
        .await
        .expect("remote COPY abandonment cleanup must quiesce");

    assert_eq!(context.tasks.active_tasks(), 0);
    assert_eq!(context.memory.used(), 0);
    assert!(!context.has_protected_async_cleanup());
    assert!(matches!(
        store.head(&data).await,
        Err(object_store::Error::NotFound { .. })
    ));
}

#[tokio::test]
async fn remote_writer_panic_aborts_before_task_group_quiescence() {
    let temp = tempfile::tempdir().unwrap();
    let context = Arc::new(QueryContext::new(MemoryPool::new(32 << 20), temp.path()).unwrap());
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let writer = remote_writer(Arc::clone(&store), "exports/panic", &context).await;
    let data = writer.data.clone();
    context
        .tasks
        .spawn("remote-copy-panic-test", async move {
            let _writer = writer;
            panic!("injected remote COPY panic");
            #[allow(unreachable_code)]
            Ok(())
        })
        .unwrap();

    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while context.tasks.first_failure().is_none() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("panic must be recorded");
    tokio::time::timeout(std::time::Duration::from_secs(2), context.tasks.quiesce())
        .await
        .expect("panic cleanup must quiesce");

    assert_eq!(context.tasks.active_tasks(), 0);
    assert_eq!(context.memory.used(), 0);
    assert!(!context.has_protected_async_cleanup());
    assert!(matches!(
        store.head(&data).await,
        Err(object_store::Error::NotFound { .. })
    ));
}

#[tokio::test]
async fn cancellation_keeps_remote_cleanup_registered_during_unwind() {
    let temp = tempfile::tempdir().unwrap();
    let context = Arc::new(QueryContext::new(MemoryPool::new(32 << 20), temp.path()).unwrap());
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let writer = remote_writer(Arc::clone(&store), "exports/cancel", &context).await;
    let data = writer.data.clone();
    let control = context.control.clone();
    let worker_control = control.clone();
    context
        .tasks
        .spawn("remote-copy-cancel-test", async move {
            worker_control.cancelled().await;
            drop(writer);
            Ok(())
        })
        .unwrap();

    control.cancel();
    tokio::time::timeout(std::time::Duration::from_secs(2), context.tasks.quiesce())
        .await
        .expect("cancellation cleanup must quiesce");

    assert_eq!(context.tasks.active_tasks(), 0);
    assert_eq!(context.memory.used(), 0);
    assert!(!context.has_protected_async_cleanup());
    assert!(matches!(
        store.head(&data).await,
        Err(object_store::Error::NotFound { .. })
    ));
}

async fn remote_writer(
    store: Arc<dyn ObjectStore>,
    destination: &str,
    context: &QueryContext,
) -> RemoteWriter {
    RemoteWriter::create(
        format!("memory://{destination}"),
        store,
        Path::from(destination),
        CopyFormat::Csv,
        CopyCsvOptions::default(),
        Arc::new(Schema::empty()),
        context,
    )
    .await
    .unwrap()
}
