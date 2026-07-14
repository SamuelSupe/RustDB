use std::{
    sync::Arc,
    sync::atomic::{AtomicUsize, Ordering},
};

use arrow::{csv::ReaderBuilder, datatypes::SchemaRef};
use async_stream::try_stream;
use bytes::Bytes;
use parking_lot::Mutex;
use tokio::{io::AsyncReadExt, sync::mpsc};

use super::{
    ScanRequest, ScanTask,
    csv_infer::format,
    csv_input::{input_error, open_csv_input},
    csv_morsel::RecordMorselizer,
};
use crate::{
    CsvOptions, Error, Result,
    runtime::{
        MemoryReservation, QueryContext, boxed_record_batch_stream, estimate_schema_batch_bytes,
    },
    storage::ObjectSource,
};

struct CsvMorsel {
    bytes: Bytes,
    decompressed_offset: u64,
    memory: Arc<MorselMemory>,
}

impl Drop for CsvMorsel {
    fn drop(&mut self) {
        self.memory.shrink(self.bytes.len());
    }
}

struct MorselMemory {
    reservation: Mutex<MemoryReservation>,
}

impl MorselMemory {
    fn new(reservation: MemoryReservation) -> Self {
        Self {
            reservation: Mutex::new(reservation),
        }
    }

    fn absorb(&self, reservation: MemoryReservation) -> Result<()> {
        self.reservation.lock().absorb(reservation)
    }

    fn shrink(&self, bytes: usize) {
        self.reservation.lock().shrink(bytes);
    }
}

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
    let output_schema = request.projected_schema(&schema)?;
    let preclaim = estimate_schema_batch_bytes(output_schema.as_ref(), request.batch_size);
    let remaining = request.limit.map(|limit| Arc::new(AtomicUsize::new(limit)));
    let uri: Arc<str> = Arc::from(file.uri());
    let mut senders = Vec::with_capacity(task_count);
    let mut receivers = Vec::with_capacity(task_count);
    for _ in 0..task_count {
        let (sender, receiver) = mpsc::channel(1);
        senders.push(sender);
        receivers.push(receiver);
    }

    let producer_context = Arc::clone(&context);
    let producer_options = options.clone();
    context.tasks.spawn("CSV morsel producer", async move {
        produce_morsels(
            file,
            producer_options,
            has_header,
            target_morsel_bytes,
            senders,
            producer_context,
        )
        .await
    })?;

    Ok(receivers
        .into_iter()
        .enumerate()
        .map(|(task, mut receiver)| {
            let task_context = Arc::clone(&context);
            let stream_context = Arc::clone(&context);
            let schema = Arc::clone(&schema);
            let output_schema = Arc::clone(&output_schema);
            let options = options.clone();
            let projection = request.projection.clone();
            let remaining = remaining.clone();
            let uri = Arc::clone(&uri);
            let batch_size = request.batch_size;
            ScanTask::from_public(
                task,
                boxed_record_batch_stream(try_stream! {
                    'morsels: loop {
                        if remaining.as_ref().is_some_and(|remaining| {
                            remaining.load(Ordering::Acquire) == 0
                        }) {
                            break;
                        }
                        let morsel = tokio::select! {
                            _ = stream_context.control.cancelled() => {
                                stream_context.check_cancelled().map(|()| None)
                            },
                            morsel = receiver.recv() => Ok(morsel),
                        }?;
                        let Some(morsel) = morsel else {
                            stream_context.check_cancelled()?;
                            break;
                        };
                        let morsel_bytes = morsel.bytes.len();
                        let mut builder = ReaderBuilder::new(Arc::clone(&schema))
                            .with_format(format(&options, false))
                            .with_batch_size(batch_size)
                            .with_truncated_rows(false);
                        if let Some(projection) = &projection {
                            builder = builder.with_projection(projection.clone());
                        }
                        let mut decoder = builder.build_decoder();
                        let mut offset = 0;
                        let mut bytes_scanned = u64::try_from(morsel_bytes).unwrap_or(u64::MAX);
                        loop {
                            if remaining.as_ref().is_some_and(|remaining| {
                                remaining.load(Ordering::Acquire) == 0
                            }) {
                                break 'morsels;
                            }
                            let decoded = {
                                let _parser_lane = stream_context.metrics.enter_csv_parser_lane();
                                decoder.decode(&morsel.bytes[offset..]).map_err(|error| {
                                    csv_decode_error(
                                        &uri,
                                        morsel.decompressed_offset.saturating_add(
                                            u64::try_from(offset).unwrap_or(u64::MAX),
                                        ),
                                        error,
                                    )
                                })?
                            };
                            offset += decoded;
                            let batch = {
                                let _parser_lane = stream_context.metrics.enter_csv_parser_lane();
                                decoder.flush().map_err(|error| {
                                    csv_decode_error(
                                        &uri,
                                        morsel.decompressed_offset.saturating_add(
                                            u64::try_from(offset).unwrap_or(u64::MAX),
                                        ),
                                        error,
                                    )
                                })?
                            };
                            let Some(batch) = batch else {
                                if offset == morsel.bytes.len() {
                                    break;
                                }
                                if decoded == 0 {
                                    Err(Error::Internal(
                                        "CSV morsel decoder made no progress".to_owned(),
                                    ))?;
                                }
                                continue;
                            };
                            let claimed = remaining.as_ref().map_or(batch.num_rows(), |remaining| {
                                claim_rows(remaining, batch.num_rows())
                            });
                            if claimed == 0 {
                                break 'morsels;
                            }
                            let batch = if claimed < batch.num_rows() {
                                batch.slice(0, claimed)
                            } else {
                                batch
                            };
                            stream_context.metrics.record_scan(
                                u64::try_from(batch.num_rows()).unwrap_or(u64::MAX),
                                1,
                                bytes_scanned,
                            );
                            bytes_scanned = 0;
                            debug_assert_eq!(batch.schema(), output_schema);
                            yield batch;
                        }
                    }
                }),
                task_context,
                preclaim,
                "parallel CSV scan task",
            )
        })
        .collect())
}

async fn produce_morsels(
    file: ObjectSource,
    options: CsvOptions,
    has_header: bool,
    target_bytes: usize,
    senders: Vec<mpsc::Sender<CsvMorsel>>,
    context: Arc<QueryContext>,
) -> Result<()> {
    let snapshot = context.object_snapshot(file.uri())?;
    let mut input = open_csv_input(
        &file,
        &snapshot,
        options.compression,
        Some((&context.control, &context.metrics)),
    )
    .await?;
    let max_read_bytes = context.memory.limit().saturating_div(8).clamp(1, 1 << 20);
    let read_bytes = target_bytes.clamp(1, max_read_bytes);
    let _read_memory = context
        .reserve_memory(read_bytes, "CSV input buffer")
        .await
        .map_err(|error| csv_memory_error(file.uri(), 0, error))?;
    let mut read_buffer = vec![0_u8; read_bytes];
    let mut splitter =
        RecordMorselizer::new(target_bytes, options.quote, options.escape, has_header);
    let morsel_memory = Arc::new(MorselMemory::new(context.memory.reservation()));
    let mut next_sender = 0;
    let mut decompressed_offset = 0_u64;

    loop {
        let read = tokio::select! {
            _ = context.control.cancelled() => return context.check_cancelled(),
            result = input.read(&mut read_buffer) => {
                result.map_err(|error| input_error(file.uri(), error))?
            },
        };
        if read == 0 {
            break;
        }
        context
            .metrics
            .add_csv_decompressed_bytes(u64::try_from(read).unwrap_or(u64::MAX));
        let buffered_before = splitter.buffered_len();
        let buffered_offset =
            decompressed_offset.saturating_sub(u64::try_from(buffered_before).unwrap_or(u64::MAX));
        decompressed_offset =
            decompressed_offset.saturating_add(u64::try_from(read).unwrap_or(u64::MAX));
        let growth = context
            .reserve_memory_while_holding(
                read,
                buffered_before.saturating_add(read_bytes),
                "CSV morsel buffer",
            )
            .await
            .map_err(|error| csv_memory_error(file.uri(), buffered_offset, error))?;
        morsel_memory.absorb(growth)?;
        let morsels = splitter.push(&read_buffer[..read]);
        let discarded = release_discarded_bytes(
            &morsel_memory,
            buffered_before.saturating_add(read),
            splitter.buffered_len(),
            &morsels,
        );
        send_morsels(
            morsels,
            buffered_offset.saturating_add(u64::try_from(discarded).unwrap_or(u64::MAX)),
            &senders,
            &mut next_sender,
            Arc::clone(&morsel_memory),
            &context,
        )
        .await?;
    }
    let buffered_before = splitter.buffered_len();
    let buffered_offset =
        decompressed_offset.saturating_sub(u64::try_from(buffered_before).unwrap_or(u64::MAX));
    let morsels = splitter.finish();
    let discarded = release_discarded_bytes(&morsel_memory, buffered_before, 0, &morsels);
    send_morsels(
        morsels,
        buffered_offset.saturating_add(u64::try_from(discarded).unwrap_or(u64::MAX)),
        &senders,
        &mut next_sender,
        morsel_memory,
        &context,
    )
    .await
}

fn csv_memory_error(uri: &str, decompressed_offset: u64, error: Error) -> Error {
    match error {
        Error::ResourceExhausted(message) => Error::ResourceExhausted(format!(
            "CSV record buffer for {uri} at decompressed offset {decompressed_offset}: {message}"
        )),
        error => error,
    }
}

fn release_discarded_bytes(
    memory: &MorselMemory,
    input_bytes: usize,
    buffered_bytes: usize,
    morsels: &[Bytes],
) -> usize {
    let retained = morsels.iter().fold(buffered_bytes, |total, morsel| {
        total.saturating_add(morsel.len())
    });
    let discarded = input_bytes.saturating_sub(retained);
    memory.shrink(discarded);
    discarded
}

async fn send_morsels(
    morsels: Vec<Bytes>,
    mut decompressed_offset: u64,
    senders: &[mpsc::Sender<CsvMorsel>],
    next_sender: &mut usize,
    memory: Arc<MorselMemory>,
    context: &QueryContext,
) -> Result<()> {
    for bytes in morsels {
        let next_offset =
            decompressed_offset.saturating_add(u64::try_from(bytes.len()).unwrap_or(u64::MAX));
        let mut morsel = CsvMorsel {
            bytes,
            decompressed_offset,
            memory: Arc::clone(&memory),
        };
        context.metrics.add_csv_morsels(1);
        let mut delivered = false;
        for _ in 0..senders.len() {
            let index = *next_sender % senders.len();
            *next_sender = (*next_sender).wrapping_add(1);
            match tokio::select! {
                _ = context.control.cancelled() => return context.check_cancelled(),
                result = senders[index].send(morsel) => result,
            } {
                Ok(()) => {
                    delivered = true;
                    break;
                }
                Err(error) => morsel = error.0,
            }
        }
        if !delivered {
            return Ok(());
        }
        decompressed_offset = next_offset;
    }
    Ok(())
}

fn csv_decode_error(uri: &str, decompressed_offset: u64, error: arrow::error::ArrowError) -> Error {
    Error::Execution(format!(
        "CSV decode failed for {uri} at decompressed offset {decompressed_offset}: {error}"
    ))
}

fn claim_rows(remaining: &AtomicUsize, available: usize) -> usize {
    let mut current = remaining.load(Ordering::Acquire);
    loop {
        let claimed = current.min(available);
        if claimed == 0 {
            return 0;
        }
        match remaining.compare_exchange_weak(
            current,
            current - claimed,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => return claimed,
            Err(updated) => current = updated,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::produce_morsels;
    use crate::{
        CsvOptions, Error, S3Config,
        runtime::{MemoryPool, QueryContext},
        storage::LocationResolver,
    };

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
        let (sender, _receiver) = tokio::sync::mpsc::channel(1);

        let error = produce_morsels(
            file.clone(),
            CsvOptions::default(),
            true,
            1024 * 1024,
            vec![sender],
            context,
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
    }
}
