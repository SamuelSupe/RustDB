use std::{sync::Arc, time::Duration};

use arrow::{
    array::{Array, Int64Array, StringArray},
    datatypes::{DataType, Field, Schema, SchemaRef},
};
use futures::{StreamExt, TryStreamExt, stream};
use tokio::{
    sync::{Mutex as AsyncMutex, Notify},
    time::timeout,
};

use super::{DecodeTask, decode_stream};
use crate::{
    CsvHeader, CsvOptions, Engine, EngineConfig, Error, S3Config,
    datasource::{ScanRequest, ScanTask},
    runtime::{MemoryPool, QueryContext, estimate_schema_batch_bytes},
    storage::{LocationResolver, ObjectSource},
};

use super::super::{ParallelCsvScan, scan_tasks};

async fn resolve_file(path: &std::path::Path, context: &QueryContext) -> ObjectSource {
    let files = LocationResolver::new(S3Config::default())
        .resolve(&[path.to_string_lossy().into_owned()])
        .await
        .unwrap();
    let file = files[0].clone();
    context
        .register_object_snapshot(file.uri(), file.snapshot().clone())
        .unwrap();
    context.seal_object_snapshots();
    file
}

fn engine_context(
    directory: &std::path::Path,
    memory_limit: usize,
    threads: usize,
) -> (Engine, Arc<QueryContext>) {
    let config = EngineConfig::builder()
        .memory_limit(memory_limit)
        .compute_threads(threads)
        .io_concurrency(threads)
        .spill_directory(directory.join("spill"))
        .build();
    let engine = Engine::new(config).unwrap();
    let context = engine.query_context_for_test().unwrap();
    context.configure_compute_lanes(threads);
    (engine, context)
}

#[tokio::test]
async fn partial_batches_carry_across_sixteen_morsels_and_four_lanes() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("carry.csv");
    let mut contents = String::from("id,note,unused\n");
    for id in 0..16 {
        contents.push_str(&format!("{id},\"part-{id}\ncontinued\",ignored\n"));
    }
    std::fs::write(&path, contents).unwrap();

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("note", DataType::Utf8, false),
        Field::new("unused", DataType::Utf8, false),
    ]));
    let options = CsvOptions::builder()
        .schema(Arc::clone(&schema))
        .header(CsvHeader::Present)
        .build();
    let (_engine, context) = engine_context(directory.path(), 4 << 20, 4);
    let file = resolve_file(&path, &context).await;
    let baseline = context.memory.used();
    let mut request = ScanRequest::new(5);
    request.projection = Some(vec![1]);
    let tasks = scan_tasks(
        ParallelCsvScan {
            file,
            schema,
            options,
            has_header: true,
            request,
            task_count: 4,
            target_morsel_bytes: 1,
        },
        Arc::clone(&context),
    )
    .unwrap();
    let batches = stream::iter(tasks.into_iter().map(ScanTask::into_stream))
        .flatten_unordered(4)
        .try_collect::<Vec<_>>()
        .await
        .unwrap();

    assert_eq!(context.metrics.snapshot().csv_morsels, 16);
    assert!(
        batches.len() <= 7,
        "four lanes should add at most three tails to the four-batch ideal, got {}",
        batches.len()
    );
    let mut notes = batches
        .iter()
        .flat_map(|batch| {
            let notes = batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            (0..notes.len())
                .map(|row| notes.value(row).to_owned())
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    notes.sort();
    let mut expected = (0..16)
        .map(|id| format!("part-{id}\ncontinued"))
        .collect::<Vec<_>>();
    expected.sort();
    assert_eq!(notes, expected);

    drop(batches);
    context.tasks.quiesce().await;
    assert_eq!(context.memory.used(), baseline);
}

#[tokio::test]
async fn full_schema_decoder_workspace_is_retained_for_a_narrow_projection() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("workspace.csv");
    let names = (0..16)
        .map(|column| format!("c{column}"))
        .collect::<Vec<_>>();
    let mut contents = format!("{}\n", names.join(","));
    for row in 0..5 {
        let values = (0..16)
            .map(|column| format!("r{row}c{column}"))
            .collect::<Vec<_>>();
        contents.push_str(&format!("{}\n", values.join(",")));
    }
    std::fs::write(&path, contents).unwrap();

    let schema = Arc::new(Schema::new(
        names
            .iter()
            .map(|name| Field::new(name, DataType::Utf8, false))
            .collect::<Vec<_>>(),
    ));
    let projected = Arc::new(schema.project(&[0]).unwrap());
    let full_workspace = estimate_schema_batch_bytes(schema.as_ref(), 5);
    let projected_credit = estimate_schema_batch_bytes(projected.as_ref(), 5);
    assert!(full_workspace > projected_credit);
    let options = CsvOptions::builder()
        .schema(Arc::clone(&schema))
        .header(CsvHeader::Present)
        .build();
    let (_engine, context) = engine_context(directory.path(), 4 << 20, 1);
    let file = resolve_file(&path, &context).await;
    let baseline = context.memory.used();
    let mut request = ScanRequest::new(5);
    request.projection = Some(vec![0]);
    let mut tasks = scan_tasks(
        ParallelCsvScan {
            file,
            schema,
            options,
            has_header: true,
            request,
            task_count: 1,
            target_morsel_bytes: 1,
        },
        Arc::clone(&context),
    )
    .unwrap();
    let mut input = tasks.pop().unwrap().into_stream();
    let batch = input.next().await.unwrap().unwrap();

    assert_eq!(batch.num_columns(), 1);
    assert_eq!(batch.num_rows(), 5);
    let held = context.memory.used().saturating_sub(baseline);
    assert!(
        held >= full_workspace.saturating_add(batch.memory_size()),
        "decoder lifetime held {held} bytes, expected at least full-schema workspace {full_workspace} plus output {}",
        batch.memory_size()
    );
    assert!(input.next().await.is_none());
    drop(batch);
    drop(input);
    context.tasks.quiesce().await;
    assert_eq!(context.memory.used(), baseline);
}

#[tokio::test]
async fn small_limit_flushes_at_a_morsel_boundary_and_stops_the_source() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("limit.csv");
    let mut contents = String::from("id,value\n");
    for id in 0..20_000 {
        contents.push_str(&format!("{id},value-{id}\n"));
    }
    std::fs::write(&path, &contents).unwrap();

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Utf8, false),
    ]));
    let options = CsvOptions::builder()
        .schema(Arc::clone(&schema))
        .header(CsvHeader::Present)
        .build();
    let (_engine, context) = engine_context(directory.path(), 8 << 20, 1);
    let file = resolve_file(&path, &context).await;
    let baseline = context.memory.used();
    let mut request = ScanRequest::new(8_192);
    request.projection = Some(vec![0]);
    request.limit = Some(1);
    let mut tasks = scan_tasks(
        ParallelCsvScan {
            file,
            schema,
            options,
            has_header: true,
            request,
            task_count: 1,
            target_morsel_bytes: 64,
        },
        Arc::clone(&context),
    )
    .unwrap();
    let batches = tasks
        .pop()
        .unwrap()
        .into_stream()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();

    assert_eq!(
        batches.iter().map(|batch| batch.num_rows()).sum::<usize>(),
        1
    );
    let ids = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(ids.value(0), 0);
    drop(batches);
    context.tasks.quiesce().await;
    let metrics = context.metrics.snapshot();
    assert!(
        metrics.csv_source_bytes < u64::try_from(contents.len()).unwrap(),
        "LIMIT read the complete CSV source"
    );
    assert_eq!(context.memory.used(), baseline);
}

#[tokio::test]
async fn decoder_and_producer_startup_admit_atomically_without_residual_state() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("startup.csv");
    std::fs::write(&path, "7\n").unwrap();
    let files = LocationResolver::new(S3Config::default())
        .resolve(&[path.to_string_lossy().into_owned()])
        .await
        .unwrap();
    let file = files[0].clone();
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int64,
        false,
    )]));
    let output_schema = Arc::new(schema.project(&[0]).unwrap());
    let batch_size = 1;
    let decoder_bytes = estimate_schema_batch_bytes(schema.as_ref(), batch_size)
        .max(1)
        .saturating_add(estimate_schema_batch_bytes(output_schema.as_ref(), batch_size).max(1));
    let read_bytes = 64_usize;
    let producer_startup_bytes = read_bytes * 2;

    let probe = context_with_operation_limit(
        directory.path(),
        "startup-probe",
        decoder_bytes + producer_startup_bytes + 4_096,
    );
    register_snapshot(&probe, &file);
    let snapshot_bytes = probe.memory.used();
    drop(probe);

    let enough_target = snapshot_bytes
        .saturating_add(decoder_bytes)
        .saturating_add(producer_startup_bytes);
    let enough = context_with_operation_limit(directory.path(), "startup-enough", enough_target);
    enough.configure_compute_lanes(1);
    register_snapshot(&enough, &file);
    assert_eq!(enough.memory.used(), snapshot_bytes);
    let enough_batches = timeout(
        Duration::from_secs(1),
        startup_scan(
            Arc::clone(&enough),
            file.clone(),
            Arc::clone(&schema),
            read_bytes,
        )
        .try_collect::<Vec<_>>(),
    )
    .await
    .expect("exact decoder+producer startup admission hung")
    .unwrap();
    assert_eq!(enough_batches.len(), 1);
    let values = enough_batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(values.value(0), 7);
    drop(enough_batches);
    timeout(Duration::from_secs(1), enough.tasks.quiesce())
        .await
        .expect("exact startup tasks did not quiesce");
    assert_eq!(enough.tasks.active_tasks(), 0);
    assert_eq!(enough.memory.used(), snapshot_bytes);

    let insufficient =
        context_with_operation_limit(directory.path(), "startup-insufficient", enough_target - 1);
    insufficient.configure_compute_lanes(1);
    register_snapshot(&insufficient, &file);
    assert_eq!(insufficient.memory.used(), snapshot_bytes);
    let mut input = startup_scan(Arc::clone(&insufficient), file, schema, read_bytes);
    let error = timeout(Duration::from_secs(1), input.next())
        .await
        .expect("insufficient decoder+producer startup admission hung")
        .expect("insufficient startup must return an error")
        .unwrap_err();
    assert!(
        matches!(&error, Error::ResourceExhausted(message) if message.contains("CSV startup")),
        "{error:?}"
    );
    drop(input);
    timeout(Duration::from_secs(1), insufficient.tasks.quiesce())
        .await
        .expect("failed startup tasks did not quiesce");
    assert_eq!(insufficient.tasks.active_tasks(), 0);
    assert_eq!(insufficient.memory.used(), snapshot_bytes);
}

fn startup_scan(
    context: Arc<QueryContext>,
    file: ObjectSource,
    schema: SchemaRef,
    target_morsel_bytes: usize,
) -> crate::runtime::MemoryBatchStream {
    let options = CsvOptions::builder()
        .schema(Arc::clone(&schema))
        .header(CsvHeader::Absent)
        .build();
    let mut request = ScanRequest::new(1);
    request.projection = Some(vec![0]);
    scan_tasks(
        ParallelCsvScan {
            file,
            schema,
            options,
            has_header: false,
            request,
            task_count: 1,
            target_morsel_bytes,
        },
        context,
    )
    .unwrap()
    .pop()
    .unwrap()
    .into_stream()
}

fn register_snapshot(context: &QueryContext, file: &ObjectSource) {
    context
        .register_object_snapshot(file.uri(), file.snapshot().clone())
        .unwrap();
    context.seal_object_snapshots();
}

#[tokio::test]
async fn combined_decoder_admission_succeeds_or_fails_without_hanging() {
    let directory = tempfile::tempdir().unwrap();
    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Utf8, false),
        Field::new("b", DataType::Utf8, false),
        Field::new("c", DataType::Utf8, false),
        Field::new("d", DataType::Utf8, false),
    ]));
    let output_schema = Arc::new(schema.project(&[0]).unwrap());
    let batch_size = 128;
    let output_credit = estimate_schema_batch_bytes(output_schema.as_ref(), batch_size).max(1);
    let workspace = estimate_schema_batch_bytes(schema.as_ref(), batch_size).max(1);
    let combined = workspace.checked_add(output_credit).unwrap();

    let enough = context_with_operation_limit(directory.path(), "enough", combined);
    assert_eq!(enough.memory.operation_limit(), combined);
    let mut enough_stream = closed_decoder_stream(
        Arc::clone(&enough),
        Arc::clone(&schema),
        Arc::clone(&output_schema),
        batch_size,
        output_credit,
    );
    let enough_result = timeout(Duration::from_secs(1), enough_stream.next())
        .await
        .expect("exact combined admission hung");
    assert!(enough_result.is_none());
    assert_eq!(enough.memory.used(), 0);

    let insufficient = context_with_operation_limit(directory.path(), "insufficient", combined - 1);
    assert_eq!(insufficient.memory.operation_limit(), combined - 1);
    let mut insufficient_stream = closed_decoder_stream(
        Arc::clone(&insufficient),
        schema,
        output_schema,
        batch_size,
        output_credit,
    );
    let error = timeout(Duration::from_secs(1), insufficient_stream.next())
        .await
        .expect("insufficient combined admission hung")
        .expect("insufficient admission must return an error")
        .unwrap_err();
    assert!(matches!(error, Error::ResourceExhausted(_)), "{error:?}");
    drop(insufficient_stream);
    assert_eq!(insufficient.memory.used(), 0);
}

fn context_with_operation_limit(
    directory: &std::path::Path,
    label: &str,
    target: usize,
) -> Arc<QueryContext> {
    let mut raw_limit = target;
    for attempt in 0..8 {
        let context = Arc::new(
            QueryContext::new(
                MemoryPool::new(raw_limit),
                directory.join(format!("{label}-{attempt}")),
            )
            .unwrap(),
        );
        let operation_limit = context.memory.operation_limit();
        match operation_limit.cmp(&target) {
            std::cmp::Ordering::Equal => return context,
            std::cmp::Ordering::Less => {
                raw_limit = raw_limit.saturating_add(target - operation_limit);
            }
            std::cmp::Ordering::Greater => {
                raw_limit = raw_limit.saturating_sub(operation_limit - target);
            }
        }
    }
    panic!("could not construct a query pool with operation limit {target}");
}

fn closed_decoder_stream(
    context: Arc<QueryContext>,
    schema: SchemaRef,
    output_schema: SchemaRef,
    batch_size: usize,
    output_preclaim_bytes: usize,
) -> crate::runtime::MemoryBatchStream {
    let (sender, receiver) = tokio::sync::mpsc::channel(1);
    drop(sender);
    decode_stream(DecodeTask {
        schema: Arc::clone(&schema),
        output_schema,
        options: CsvOptions::builder()
            .schema(schema)
            .header(CsvHeader::Absent)
            .build(),
        projection: Some(vec![0]),
        remaining: None,
        uri: Arc::from("memory://closed.csv"),
        batch_size,
        output_preclaim_bytes,
        receiver: Arc::new(AsyncMutex::new(receiver)),
        producer_ready: Arc::new(Notify::new()),
        context,
    })
}
