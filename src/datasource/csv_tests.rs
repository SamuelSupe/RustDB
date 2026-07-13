use std::{fs, sync::Arc};

use arrow::array::{Array, StringArray};
use futures::{StreamExt, TryStreamExt};
use tempfile::tempdir;

use super::CsvTable;
use crate::{
    CsvOptions, EngineConfig,
    datasource::{ScanRequest, ScanTask, TableProvider},
    runtime::{MemoryPool, QueryContext},
};

#[path = "csv_tests/compression.rs"]
mod compression;

#[tokio::test]
async fn one_file_is_split_into_parallel_record_aligned_tasks() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("parallel.csv");
    let mut contents = String::from("id,note\n1,\"first\ncontinued\"\n");
    for id in 2..=3_000 {
        contents.push_str(&format!("{id},value-{id}\n"));
    }
    fs::write(&path, contents).unwrap();
    let config = EngineConfig::builder()
        .io_concurrency(4)
        .csv_target_morsel_bytes(16 * 1024)
        .build();
    let table = CsvTable::try_new(
        vec![path.to_string_lossy().into_owned()],
        CsvOptions::default(),
        &config,
    )
    .await
    .unwrap();
    let context =
        Arc::new(QueryContext::new(MemoryPool::new(512 * 1024), directory.path()).unwrap());
    table.prepare(Arc::clone(&context)).await.unwrap();
    context.seal_object_snapshots();
    let baseline = context.memory.used();
    let tasks = table
        .scan_tasks(ScanRequest::new(3), Arc::clone(&context), 4)
        .await
        .unwrap();
    assert_eq!(tasks.len(), 4);
    let mut batches =
        futures::stream::iter(tasks.into_iter().map(ScanTask::into_stream)).flatten_unordered(4);
    let mut rows = 0;
    while let Some(batch) = batches.try_next().await.unwrap() {
        rows += batch.num_rows();
        assert!(context.memory.used() <= context.memory.limit());
        tokio::task::yield_now().await;
    }
    drop(batches);
    assert_eq!(rows, 3_000);
    assert_eq!(context.memory.used(), baseline);
}

#[tokio::test]
async fn parallel_limit_stops_after_the_first_morsels() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("limited.csv");
    let mut contents = String::from("id,payload\n");
    for id in 0..50_000 {
        contents.push_str(&format!("{id},{}\n", "x".repeat(32)));
    }
    fs::write(&path, &contents).unwrap();
    let config = EngineConfig::builder()
        .io_concurrency(4)
        .csv_target_morsel_bytes(256)
        .build();
    let table = CsvTable::try_new(
        vec![path.to_string_lossy().into_owned()],
        CsvOptions::default(),
        &config,
    )
    .await
    .unwrap();
    let context =
        Arc::new(QueryContext::new(MemoryPool::new(2 * 1024 * 1024), directory.path()).unwrap());
    table.prepare(Arc::clone(&context)).await.unwrap();
    context.seal_object_snapshots();
    let mut request = ScanRequest::new(8);
    request.limit = Some(1);
    let tasks = table
        .scan_tasks(request, Arc::clone(&context), 4)
        .await
        .unwrap();
    let rows = futures::stream::iter(tasks.into_iter().map(ScanTask::into_stream))
        .flatten_unordered(4)
        .try_collect::<Vec<_>>()
        .await
        .unwrap()
        .iter()
        .map(|batch| batch.num_rows())
        .sum::<usize>();
    assert_eq!(rows, 1);
    assert!(context.metrics.snapshot().bytes_scanned < contents.len() as u64);
}

#[tokio::test]
async fn streams_quoted_records_across_files_with_projection() {
    let directory = tempdir().unwrap();
    fs::write(
        directory.path().join("a.csv"),
        b"id,note\n1,\"first\nsecond\"\n",
    )
    .unwrap();
    fs::write(directory.path().join("b.csv"), b"id,note\n2,last\n").unwrap();
    let config = EngineConfig {
        io_concurrency: 2,
        ..EngineConfig::default()
    };
    let table = CsvTable::try_new(
        vec![format!("{}/*.csv", directory.path().display())],
        CsvOptions::default(),
        &config,
    )
    .await
    .unwrap();
    let context =
        Arc::new(QueryContext::new(MemoryPool::new(16 * 1024 * 1024), directory.path()).unwrap());
    let mut request = ScanRequest::new(1);
    request.projection = Some(vec![1]);
    table.prepare(Arc::clone(&context)).await.unwrap();
    context.seal_object_snapshots();

    let batches = table
        .scan(request, context)
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();

    assert_eq!(
        batches.iter().map(|batch| batch.num_rows()).sum::<usize>(),
        2
    );
    assert!(batches.iter().all(|batch| batch.num_columns() == 1));
    assert_eq!(batches[0].schema().field(0).name(), "note");
}

#[tokio::test]
async fn full_get_rejects_size_change_without_an_identity_token() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("changing.csv");
    fs::write(&path, b"id\n1\n").unwrap();
    let table = CsvTable::try_new(
        vec![path.to_string_lossy().into_owned()],
        CsvOptions::default(),
        &EngineConfig::default(),
    )
    .await
    .unwrap();
    let file = &table.files[0];
    let mut snapshot = file.snapshot().clone();
    snapshot.e_tag = None;
    snapshot.version = None;
    let context =
        Arc::new(QueryContext::new(MemoryPool::new(16 * 1024 * 1024), directory.path()).unwrap());
    context
        .register_object_snapshot(file.uri(), snapshot)
        .unwrap();
    context.seal_object_snapshots();

    fs::write(&path, b"id\n123456789\n").unwrap();
    let mut input = table.scan(ScanRequest::new(8), context).await.unwrap();
    let error = input
        .next()
        .await
        .expect("changed CSV must produce a terminal result")
        .expect_err("changed CSV must fail before decoding its body")
        .to_string();

    assert!(error.contains("object changed during query"), "{error}");
    assert!(error.contains("changing.csv"), "{error}");
    assert!(error.contains("expected size"), "{error}");
}

#[tokio::test]
async fn scan_tasks_share_one_global_limit_across_files() {
    let directory = tempdir().unwrap();
    for (name, start) in [("a.csv", 0), ("b.csv", 10), ("c.csv", 20)] {
        fs::write(
            directory.path().join(name),
            format!("id\n{start}\n{}\n{}\n", start + 1, start + 2),
        )
        .unwrap();
    }
    let table = CsvTable::try_new(
        vec![format!("{}/*.csv", directory.path().display())],
        CsvOptions::default(),
        &EngineConfig::default(),
    )
    .await
    .unwrap();
    let context =
        Arc::new(QueryContext::new(MemoryPool::new(16 * 1024 * 1024), directory.path()).unwrap());
    table.prepare(Arc::clone(&context)).await.unwrap();
    context.seal_object_snapshots();
    let mut request = ScanRequest::new(2);
    request.limit = Some(4);
    let tasks = table
        .scan_tasks(request, Arc::clone(&context), 2)
        .await
        .unwrap();
    assert_eq!(tasks.len(), 2);
    let batches = futures::stream::iter(tasks.into_iter().map(|task| task.into_stream()))
        .flatten_unordered(2)
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_eq!(
        batches.iter().map(|batch| batch.num_rows()).sum::<usize>(),
        4
    );
}

#[tokio::test]
async fn scan_task_count_never_exceeds_io_concurrency() {
    let directory = tempdir().unwrap();
    for name in ["a.csv", "b.csv", "c.csv"] {
        fs::write(directory.path().join(name), b"id\n1\n").unwrap();
    }
    let config = EngineConfig {
        io_concurrency: 1,
        ..EngineConfig::default()
    };
    let table = CsvTable::try_new(
        vec![format!("{}/*.csv", directory.path().display())],
        CsvOptions::default(),
        &config,
    )
    .await
    .unwrap();
    let context =
        Arc::new(QueryContext::new(MemoryPool::new(16 * 1024 * 1024), directory.path()).unwrap());

    let tasks = table
        .scan_tasks(ScanRequest::new(8), context, 8)
        .await
        .unwrap();

    assert_eq!(tasks.len(), 1);
}

#[tokio::test]
async fn query_schema_reservation_lives_with_the_csv_table() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("schema.csv");
    fs::write(&path, b"id,name\n1,one\n").unwrap();
    let context =
        Arc::new(QueryContext::new(MemoryPool::new(16 * 1024 * 1024), directory.path()).unwrap());
    let table = CsvTable::try_new_for_query(
        vec![path.to_string_lossy().into_owned()],
        CsvOptions::default(),
        &EngineConfig::default(),
        Some(Arc::clone(&context)),
    )
    .await
    .unwrap();
    let retained = context.memory.used();
    let clone = table.clone();
    drop(table);
    assert_eq!(context.memory.used(), retained);
    drop(clone);
    assert!(context.memory.used() < retained);
}

#[tokio::test]
async fn keeps_quoted_record_open_across_object_stream_chunks() {
    const OBJECT_STREAM_CHUNK: usize = 8 * 1024;

    let directory = tempdir().unwrap();
    let path = directory.path().join("large.csv");
    let mut contents = String::from("id,note\n1,\"");
    contents.push_str(&"x".repeat(OBJECT_STREAM_CHUNK - "id,note\n1,\"".len() - 1));
    contents.push('\n');
    contents.push_str("continued\"\n");
    for id in 2..=3_500 {
        contents.push_str(&format!("{id},plain-{id}\n"));
    }
    assert_eq!(contents.as_bytes()[OBJECT_STREAM_CHUNK - 1], b'\n');
    assert!(contents.len() > OBJECT_STREAM_CHUNK * 4);
    fs::write(&path, contents.as_bytes()).unwrap();

    let table = CsvTable::try_new(
        vec![path.to_string_lossy().into_owned()],
        CsvOptions::default(),
        &EngineConfig::default(),
    )
    .await
    .unwrap();
    let context =
        Arc::new(QueryContext::new(MemoryPool::new(16 * 1024 * 1024), directory.path()).unwrap());
    let mut request = ScanRequest::new(257);
    request.projection = Some(vec![1]);
    table.prepare(Arc::clone(&context)).await.unwrap();
    context.seal_object_snapshots();

    let batches = table
        .scan(request, Arc::clone(&context))
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();

    assert_eq!(
        batches.iter().map(|batch| batch.num_rows()).sum::<usize>(),
        3_500
    );
    let first = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert!(!first.is_null(0));
    assert!(first.value(0).contains("\ncontinued"));
    assert_eq!(
        context.metrics.snapshot().bytes_scanned,
        u64::try_from(contents.len()).unwrap()
    );
}

#[tokio::test]
async fn refreshes_the_object_snapshot_for_each_scan() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("changing.csv");
    fs::write(&path, b"value\nold\n").unwrap();
    let table = CsvTable::try_new(
        vec![path.to_string_lossy().into_owned()],
        CsvOptions::default(),
        &EngineConfig::default(),
    )
    .await
    .unwrap();

    fs::write(&path, b"value\nnew\n").unwrap();
    let context =
        Arc::new(QueryContext::new(MemoryPool::new(16 * 1024 * 1024), directory.path()).unwrap());
    table.prepare(Arc::clone(&context)).await.unwrap();
    context.seal_object_snapshots();
    let batches = table
        .scan(ScanRequest::new(8), context)
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    let values = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(values.value(0), "new");
}

#[tokio::test]
async fn resolves_file_snapshots_lazily_without_a_scan_wide_collect() {
    let directory = tempdir().unwrap();
    fs::write(directory.path().join("a.csv"), b"value\nfirst\n").unwrap();
    fs::write(directory.path().join("b.csv"), b"value\nsecond\n").unwrap();
    let config = EngineConfig {
        io_concurrency: 1,
        ..EngineConfig::default()
    };
    let table = CsvTable::try_new(
        vec![format!("{}/*.csv", directory.path().display())],
        CsvOptions::default(),
        &config,
    )
    .await
    .unwrap();
    let context =
        Arc::new(QueryContext::new(MemoryPool::new(16 * 1024 * 1024), directory.path()).unwrap());
    let first = &table.files[0];
    context
        .register_object_snapshot(first.uri(), first.head_snapshot().await.unwrap())
        .unwrap();
    context.seal_object_snapshots();

    let mut stream = table
        .scan(ScanRequest::new(8), Arc::clone(&context))
        .await
        .expect("scan construction must not resolve every snapshot");
    let first_batch = stream.try_next().await.unwrap().unwrap();
    let values = first_batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(values.value(0), "first");

    let error = stream.try_collect::<Vec<_>>().await.unwrap_err();
    assert!(error.to_string().contains("not present"));
}
