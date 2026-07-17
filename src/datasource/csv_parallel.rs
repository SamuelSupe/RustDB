use std::{sync::Arc, sync::atomic::AtomicUsize};

use arrow::datatypes::SchemaRef;
use tokio::sync::{Mutex as AsyncMutex, Notify, mpsc};

use super::{ScanRequest, ScanTask};
use crate::{
    CsvOptions, Error, Result,
    runtime::{QueryContext, estimate_schema_batch_bytes},
    storage::ObjectSource,
};

mod decoder;
mod morsel_target;
mod producer;
mod read_size;

#[cfg(test)]
use decoder::receive_morsel;
use decoder::{DecodeTask, decode_stream};
#[cfg(test)]
use producer::MorselMemory;
use producer::{CsvMorsel, produce_morsels};

pub(super) struct ParallelCsvScan {
    pub(super) file: ObjectSource,
    pub(super) schema: SchemaRef,
    pub(super) options: CsvOptions,
    pub(super) has_header: bool,
    pub(super) request: ScanRequest,
    pub(super) task_count: usize,
    pub(super) target_morsel_bytes: usize,
}

pub(super) fn scan_tasks(
    scan: ParallelCsvScan,
    context: Arc<QueryContext>,
) -> Result<Vec<ScanTask>> {
    let ParallelCsvScan {
        file,
        schema,
        options,
        has_header,
        request,
        task_count,
        target_morsel_bytes,
    } = scan;
    if task_count == 0 {
        return Err(Error::InvalidArgument(
            "parallel CSV scan requires at least one task".to_owned(),
        ));
    }
    let output_schema = request.projected_schema(&schema)?;
    let preclaim = estimate_schema_batch_bytes(output_schema.as_ref(), request.batch_size);
    let remaining = request.limit.map(|limit| Arc::new(AtomicUsize::new(limit)));
    let uri: Arc<str> = Arc::from(file.uri());
    let (sender, receiver) = mpsc::channel(task_count);
    let receiver = Arc::new(AsyncMutex::new(receiver));
    let decoder_ready = Arc::new(Notify::new());

    let producer_context = Arc::clone(&context);
    let producer_options = options.clone();
    let producer_ready = Arc::clone(&decoder_ready);
    context.tasks.spawn("CSV morsel producer", async move {
        tokio::select! {
            _ = producer_context.control.cancelled() => producer_context.check_cancelled()?,
            () = producer_ready.notified() => {}
        }
        produce_morsels(
            file,
            producer_options,
            has_header,
            target_morsel_bytes,
            task_count,
            sender,
            producer_context,
        )
        .await
    })?;

    Ok((0..task_count)
        .map(|task| {
            let stream_context = Arc::clone(&context);
            let receiver = Arc::clone(&receiver);
            let producer_ready = Arc::clone(&decoder_ready);
            let schema = Arc::clone(&schema);
            let output_schema = Arc::clone(&output_schema);
            let options = options.clone();
            let projection = request.projection.clone();
            let remaining = remaining.clone();
            let uri = Arc::clone(&uri);
            let batch_size = request.batch_size;
            ScanTask::new(
                task,
                decode_stream(DecodeTask {
                    schema,
                    output_schema,
                    options,
                    projection,
                    remaining,
                    uri,
                    batch_size,
                    output_preclaim_bytes: preclaim.max(1),
                    receiver,
                    producer_ready,
                    context: stream_context,
                }),
            )
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::{
        array::{Array, StringArray},
        datatypes::{DataType, Field, Schema},
    };
    use bytes::Bytes;
    use futures::{StreamExt, TryStreamExt, stream};
    use tokio::{sync::Mutex as AsyncMutex, time::timeout};

    use super::{CsvMorsel, MorselMemory, produce_morsels, receive_morsel};
    use crate::{
        CsvHeader, CsvOptions, Engine, EngineConfig, Error, S3Config,
        datasource::{CsvTable, ScanRequest, ScanTask, TableProvider},
        runtime::{MemoryPool, QueryContext},
        storage::LocationResolver,
    };

    #[tokio::test]
    async fn shared_queue_releases_the_receiver_before_decode_work() {
        let directory = tempfile::tempdir().unwrap();
        let context =
            Arc::new(QueryContext::new(MemoryPool::new(1 << 20), directory.path()).unwrap());
        let memory = Arc::new(MorselMemory::new(context.memory.try_reserve(2).unwrap()));
        let (sender, receiver) = tokio::sync::mpsc::channel(2);
        let receiver = Arc::new(AsyncMutex::new(receiver));
        for offset in 0..2_u64 {
            sender
                .send(CsvMorsel {
                    bytes: Bytes::from_static(b"x"),
                    decompressed_offset: offset,
                    memory: Arc::clone(&memory),
                })
                .await
                .unwrap();
        }

        let first = receive_morsel(&receiver, &context).await.unwrap().unwrap();
        let second = timeout(
            std::time::Duration::from_secs(1),
            receive_morsel(&receiver, &context),
        )
        .await
        .expect("holding one morsel must not retain the receiver lock")
        .unwrap()
        .unwrap();
        assert_eq!(
            (first.decompressed_offset, second.decompressed_offset),
            (0, 1)
        );
    }

    #[tokio::test]
    async fn decoder_reuse_preserves_record_aligned_projection_and_scheduler_bound() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("reuse.csv");
        let mut contents = String::from("id,note,unused\n1,\"first\ncontinued\",x\n");
        for id in 2..=16 {
            contents.push_str(&format!("{id},value-{id},x\n"));
        }
        std::fs::write(&path, contents).unwrap();
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("note", DataType::Utf8, false),
            Field::new("unused", DataType::Utf8, false),
        ]));
        let options = CsvOptions::builder()
            .schema(schema)
            .header(CsvHeader::Present)
            .build();
        let config = EngineConfig::builder()
            .compute_threads(2)
            .io_concurrency(4)
            .csv_target_morsel_bytes(8)
            .spill_directory(directory.path().join("spill"))
            .build();
        let table = CsvTable::try_new(vec![path.to_string_lossy().into_owned()], options, &config)
            .await
            .unwrap();
        let engine = Engine::new(config).unwrap();
        let context = engine.query_context_for_test().unwrap();
        context.configure_compute_lanes(4);
        table.prepare(Arc::clone(&context)).await.unwrap();
        context.seal_object_snapshots();
        let baseline = context.memory.used();
        let mut request = ScanRequest::new(1);
        request.projection = Some(vec![1]);
        let tasks = table
            .scan_tasks(request, Arc::clone(&context), 4)
            .await
            .unwrap();
        let batches = stream::iter(tasks.into_iter().map(ScanTask::into_stream))
            .flatten_unordered(4)
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        let mut values = batches
            .iter()
            .flat_map(|batch| {
                let strings = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap();
                (0..strings.len())
                    .map(|row| strings.value(row).to_owned())
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        values.sort();
        assert_eq!(values.len(), 16);
        assert!(values.iter().any(|value| value == "first\ncontinued"));
        let metrics = context.metrics.snapshot();
        assert!(metrics.csv_morsels > 4);
        assert!(metrics.csv_source_io_time > std::time::Duration::ZERO);
        assert!(metrics.csv_framing_time > std::time::Duration::ZERO);
        assert!(metrics.csv_decode_compute_time > std::time::Duration::ZERO);
        let (active, peak, queued, _) = engine.compute_scheduler_counts_for_test();
        assert_eq!((active, queued), (0, 0));
        assert!((1..=2).contains(&peak));
        drop(batches);
        assert_eq!(context.memory.used(), baseline);
    }

    #[tokio::test]
    async fn oversized_record_error_reports_uri_and_decompressed_offset() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("oversized.csv");
        let header = b"id,payload\n";
        let mut contents = header.to_vec();
        contents.extend_from_slice(b"1,");
        contents.extend(std::iter::repeat_n(b'x', 512 * 1024));
        std::fs::write(&path, contents).unwrap();

        let files = LocationResolver::new(S3Config::default())
            .resolve(&[path.to_string_lossy().into_owned()])
            .await
            .unwrap();
        let file = files[0].clone();
        let context =
            Arc::new(QueryContext::new(MemoryPool::new(230 * 1024), directory.path()).unwrap());
        context
            .register_object_snapshot(file.uri(), file.snapshot().clone())
            .unwrap();
        context.seal_object_snapshots();
        let baseline = context.memory.used();
        let (sender, _receiver) = tokio::sync::mpsc::channel(1);

        let error = produce_morsels(
            file.clone(),
            CsvOptions::default(),
            true,
            1024 * 1024,
            1,
            sender,
            Arc::clone(&context),
        )
        .await
        .unwrap_err();
        let Error::ResourceExhausted(message) = error else {
            panic!("expected resource exhaustion, got {error:?}");
        };
        assert!(message.contains(file.uri()), "{message}");
        assert!(
            message.contains(&format!("decompressed offset {}", header.len())),
            "{message}"
        );
        assert!(message.contains("query limit"), "{message}");
        assert_eq!(context.memory.used(), baseline);
    }

    #[tokio::test]
    async fn closed_consumer_stops_the_source_before_eof() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("closed.csv");
        let contents = b"1\n".repeat(1 << 20);
        std::fs::write(&path, &contents).unwrap();
        let files = LocationResolver::new(S3Config::default())
            .resolve(&[path.to_string_lossy().into_owned()])
            .await
            .unwrap();
        let file = files[0].clone();
        let context =
            Arc::new(QueryContext::new(MemoryPool::new(4 << 20), directory.path()).unwrap());
        context
            .register_object_snapshot(file.uri(), file.snapshot().clone())
            .unwrap();
        context.seal_object_snapshots();
        let baseline = context.memory.used();
        let (sender, receiver) = tokio::sync::mpsc::channel(1);
        drop(receiver);

        timeout(
            std::time::Duration::from_secs(1),
            produce_morsels(
                file,
                CsvOptions::default(),
                false,
                1 << 20,
                1,
                sender,
                Arc::clone(&context),
            ),
        )
        .await
        .expect("closed consumer did not stop the CSV source")
        .unwrap();
        assert!(
            context.metrics.snapshot().csv_source_bytes < u64::try_from(contents.len()).unwrap(),
            "closed consumer read the complete CSV source"
        );
        assert_eq!(context.memory.used(), baseline);
    }
}
