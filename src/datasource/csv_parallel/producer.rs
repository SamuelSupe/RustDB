use std::sync::Arc;

use bytes::Bytes;
use parking_lot::Mutex;
use tokio::{io::AsyncReadExt, sync::mpsc};

use super::super::{
    csv_input::{CsvInput, input_error, open_csv_input},
    csv_morsel::RecordMorselizer,
};
use crate::{
    CsvOptions, Error, Result,
    runtime::{MemoryReservation, QueryContext},
    storage::ObjectSource,
};

pub(super) struct CsvMorsel {
    pub(super) bytes: Bytes,
    pub(super) decompressed_offset: u64,
    pub(super) memory: Arc<MorselMemory>,
}

impl Drop for CsvMorsel {
    fn drop(&mut self) {
        self.memory.shrink(self.bytes.len());
    }
}

pub(super) struct MorselMemory {
    reservation: Mutex<MemoryReservation>,
}

impl MorselMemory {
    pub(super) fn new(reservation: MemoryReservation) -> Self {
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

pub(super) async fn produce_morsels(
    file: ObjectSource,
    options: CsvOptions,
    has_header: bool,
    configured_target_bytes: usize,
    task_count: usize,
    sender: mpsc::Sender<CsvMorsel>,
    context: Arc<QueryContext>,
) -> Result<()> {
    let snapshot = context.object_snapshot(file.uri())?;
    let target_bytes = super::morsel_target::effective_target_bytes(
        configured_target_bytes,
        snapshot.size,
        task_count,
    );
    let mut input = open_csv_input(
        &file,
        &snapshot,
        options.compression,
        Some((&context.control, &context.metrics)),
    )
    .await?;
    let read_bytes =
        super::read_size::effective_read_bytes(target_bytes, context.memory.operation_limit());
    let startup_bytes = read_bytes.checked_mul(2).ok_or_else(|| {
        Error::ResourceExhausted(format!(
            "CSV startup buffers for {} exceed this platform",
            file.uri()
        ))
    })?;
    let mut read_memory = context
        .memory
        .try_reserve(startup_bytes)
        .map_err(|error| csv_startup_memory_error(file.uri(), read_bytes, &context, error))?;
    let first_growth = read_memory.split_off(read_bytes)?;
    let mut splitter =
        RecordMorselizer::new(target_bytes, options.quote, options.escape, has_header);
    let morsel_memory = Arc::new(MorselMemory::new(context.memory.reservation()));
    let mut decompressed_offset = 0_u64;
    let mut first = vec![0_u8; read_bytes].into_boxed_slice();

    produce_serially(
        &mut input,
        &mut first,
        file.uri(),
        &mut splitter,
        &sender,
        &morsel_memory,
        &mut decompressed_offset,
        first_growth,
        &context,
    )
    .await?;
    drop(first);
    drop(read_memory);

    finish_morsels(
        splitter,
        decompressed_offset,
        &sender,
        morsel_memory,
        &context,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn produce_serially(
    input: &mut CsvInput,
    buffer: &mut [u8],
    uri: &str,
    splitter: &mut RecordMorselizer,
    sender: &mpsc::Sender<CsvMorsel>,
    memory: &Arc<MorselMemory>,
    decompressed_offset: &mut u64,
    first_growth: MemoryReservation,
    context: &QueryContext,
) -> Result<()> {
    let mut first_growth = Some(first_growth);
    loop {
        let read = read_input(input, buffer, uri, context).await?;
        if read == 0 {
            return Ok(());
        }
        if !process_read(
            &buffer[..read],
            uri,
            splitter,
            sender,
            memory,
            decompressed_offset,
            buffer.len(),
            first_growth.take(),
            context,
        )
        .await?
        {
            return Ok(());
        }
    }
}

async fn read_input(
    input: &mut CsvInput,
    buffer: &mut [u8],
    uri: &str,
    context: &QueryContext,
) -> Result<usize> {
    tokio::select! {
        _ = context.control.cancelled() => context.check_cancelled().map(|()| 0),
        result = input.read(buffer) => result.map_err(|error| input_error(uri, error)),
    }
}

#[allow(clippy::too_many_arguments)]
async fn process_read(
    bytes: &[u8],
    uri: &str,
    splitter: &mut RecordMorselizer,
    sender: &mpsc::Sender<CsvMorsel>,
    memory: &Arc<MorselMemory>,
    decompressed_offset: &mut u64,
    held_read_bytes: usize,
    startup_growth: Option<MemoryReservation>,
    context: &QueryContext,
) -> Result<bool> {
    context
        .metrics
        .add_csv_decompressed_bytes(u64::try_from(bytes.len()).unwrap_or(u64::MAX));
    let buffered_before = splitter.buffered_len();
    let buffered_offset =
        decompressed_offset.saturating_sub(u64::try_from(buffered_before).unwrap_or(u64::MAX));
    *decompressed_offset =
        decompressed_offset.saturating_add(u64::try_from(bytes.len()).unwrap_or(u64::MAX));
    let growth = if let Some(mut growth) = startup_growth {
        let unused = growth.size().checked_sub(bytes.len()).ok_or_else(|| {
            Error::Internal("CSV first read exceeded its admitted input buffer".to_owned())
        })?;
        growth.shrink(unused);
        growth
    } else {
        context
            .reserve_memory_while_holding(
                bytes.len(),
                buffered_before.saturating_add(held_read_bytes),
                "CSV morsel buffer",
            )
            .await
            .map_err(|error| csv_memory_error(uri, buffered_offset, error))?
    };
    memory.absorb(growth)?;
    let framing_started = std::time::Instant::now();
    let morsels = splitter.push(bytes);
    context
        .metrics
        .record_csv_framing_time(framing_started.elapsed());
    let discarded = release_discarded_bytes(
        memory,
        buffered_before.saturating_add(bytes.len()),
        splitter.buffered_len(),
        &morsels,
    );
    send_morsels(
        morsels,
        buffered_offset.saturating_add(u64::try_from(discarded).unwrap_or(u64::MAX)),
        sender,
        Arc::clone(memory),
        context,
    )
    .await
}

async fn finish_morsels(
    splitter: RecordMorselizer,
    decompressed_offset: u64,
    sender: &mpsc::Sender<CsvMorsel>,
    memory: Arc<MorselMemory>,
    context: &QueryContext,
) -> Result<()> {
    let buffered_before = splitter.buffered_len();
    let buffered_offset =
        decompressed_offset.saturating_sub(u64::try_from(buffered_before).unwrap_or(u64::MAX));
    let framing_started = std::time::Instant::now();
    let morsels = splitter.finish();
    context
        .metrics
        .record_csv_framing_time(framing_started.elapsed());
    let discarded = release_discarded_bytes(&memory, buffered_before, 0, &morsels);
    let _ = send_morsels(
        morsels,
        buffered_offset.saturating_add(u64::try_from(discarded).unwrap_or(u64::MAX)),
        sender,
        memory,
        context,
    )
    .await?;
    Ok(())
}

fn csv_memory_error(uri: &str, decompressed_offset: u64, error: Error) -> Error {
    match error {
        Error::ResourceExhausted(message) => Error::ResourceExhausted(format!(
            "CSV record buffer for {uri} at decompressed offset {decompressed_offset}: {message}"
        )),
        error => error,
    }
}

fn csv_startup_memory_error(
    uri: &str,
    read_bytes: usize,
    context: &QueryContext,
    error: Error,
) -> Error {
    Error::ResourceExhausted(format!(
        "CSV startup for {uri} requires {} bytes for the input buffer and first morsel growth (query limit {}, available {}): {error}",
        read_bytes.saturating_mul(2),
        context.memory.operation_limit(),
        context.memory.available(),
    ))
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
    sender: &mpsc::Sender<CsvMorsel>,
    memory: Arc<MorselMemory>,
    context: &QueryContext,
) -> Result<bool> {
    for bytes in morsels {
        let next_offset =
            decompressed_offset.saturating_add(u64::try_from(bytes.len()).unwrap_or(u64::MAX));
        let morsel = CsvMorsel {
            bytes,
            decompressed_offset,
            memory: Arc::clone(&memory),
        };
        context.metrics.add_csv_morsels(1);
        context.check_cancelled()?;
        let sent = match sender.try_send(morsel) {
            Ok(()) => Some(Ok(())),
            Err(mpsc::error::TrySendError::Full(morsel)) => {
                let started = std::time::Instant::now();
                let result = tokio::select! {
                    _ = context.control.cancelled() => None,
                    result = sender.send(morsel) => Some(result),
                };
                context
                    .metrics
                    .record_csv_morsel_queue_wait(started.elapsed());
                result
            }
            Err(mpsc::error::TrySendError::Closed(_)) => return Ok(false),
        };
        let Some(sent) = sent else {
            return context.check_cancelled().map(|()| false);
        };
        if sent.is_err() {
            return Ok(false);
        }
        decompressed_offset = next_offset;
    }
    Ok(true)
}
