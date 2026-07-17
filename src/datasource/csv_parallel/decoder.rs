use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Instant,
};

use arrow::{csv::ReaderBuilder, datatypes::SchemaRef, record_batch::RecordBatch};
use async_stream::try_stream;
use tokio::sync::{Mutex as AsyncMutex, Notify, mpsc};

use super::CsvMorsel;
use crate::{
    CsvOptions, Error, Result,
    datasource::csv_infer::format,
    runtime::{
        BatchEnvelope, MemoryBatchStream, MemoryReservation, QueryContext,
        boxed_memory_batch_stream, estimate_schema_batch_bytes,
    },
};

pub(super) struct DecodeTask {
    pub(super) schema: SchemaRef,
    pub(super) output_schema: SchemaRef,
    pub(super) options: CsvOptions,
    pub(super) projection: Option<Vec<usize>>,
    pub(super) remaining: Option<Arc<AtomicUsize>>,
    pub(super) uri: Arc<str>,
    pub(super) batch_size: usize,
    pub(super) output_preclaim_bytes: usize,
    pub(super) receiver: Arc<AsyncMutex<mpsc::Receiver<CsvMorsel>>>,
    pub(super) producer_ready: Arc<Notify>,
    pub(super) context: Arc<QueryContext>,
}

pub(super) fn decode_stream(task: DecodeTask) -> MemoryBatchStream {
    boxed_memory_batch_stream(try_stream! {
        let DecodeTask {
            schema,
            output_schema,
            options,
            projection,
            remaining,
            uri,
            batch_size,
            output_preclaim_bytes,
            receiver,
            producer_ready,
            context,
        } = task;
        let decoder_workspace_bytes =
            estimate_schema_batch_bytes(schema.as_ref(), batch_size).max(1);
        let combined_admission = decoder_workspace_bytes
            .checked_add(output_preclaim_bytes)
            .ok_or_else(|| {
                Error::ResourceExhausted(
                    "parallel CSV decoder admission exceeds this platform".to_owned(),
                )
            })?;
        // Admit the retained decoder workspace and its first output credit in
        // one operation. This avoids every lane holding one lease while
        // waiting for the other under a tight query budget.
        let mut decoder_workspace = context
            .reserve_memory(combined_admission, "parallel CSV decoder admission")
            .await?;
        let mut output_credit = Some(decoder_workspace.split_off(output_preclaim_bytes)?);
        producer_ready.notify_one();
        let mut builder = ReaderBuilder::new(schema)
            .with_format(format(&options, false))
            .with_batch_size(batch_size)
            .with_truncated_rows(false);
        if let Some(projection) = projection {
            builder = builder.with_projection(projection);
        }
        let mut decoder = builder.build_decoder();
        let mut decoder_dirty = false;
        let mut pending_scan_bytes = 0_u64;
        let mut last_decompressed_offset = 0_u64;

        'morsels: loop {
            if limit_reached(remaining.as_deref()) {
                break;
            }
            let morsel = receive_morsel(&receiver, &context).await?;
            let Some(morsel) = morsel else {
                context.check_cancelled()?;
                let batch = if decoder_dirty {
                    flush_decoder(
                        &mut decoder,
                        &uri,
                        last_decompressed_offset,
                        &context,
                        true,
                    )
                    .await?
                } else {
                    None
                };
                if let Some(batch) = batch {
                    if let Some(batch) = claim_batch(batch, remaining.as_deref()) {
                        context.metrics.record_scan(
                            u64::try_from(batch.num_rows()).unwrap_or(u64::MAX),
                            1,
                            pending_scan_bytes,
                        );
                        debug_assert_eq!(batch.schema(), output_schema);
                        yield envelope_batch(batch, &mut output_credit)?;
                    }
                } else if pending_scan_bytes != 0 {
                    context.metrics.record_scan(0, 0, pending_scan_bytes);
                }
                break;
            };

            pending_scan_bytes = pending_scan_bytes
                .saturating_add(u64::try_from(morsel.bytes.len()).unwrap_or(u64::MAX));
            last_decompressed_offset = morsel.decompressed_offset.saturating_add(
                u64::try_from(morsel.bytes.len()).unwrap_or(u64::MAX),
            );
            let mut offset = 0;
            while offset < morsel.bytes.len() {
                context.check_cancelled()?;
                if limit_reached(remaining.as_deref()) {
                    break 'morsels;
                }

                ensure_output_credit(
                    &mut output_credit,
                    output_preclaim_bytes,
                    decoder_workspace
                        .size()
                        .saturating_add(morsel.bytes.len()),
                    &context,
                )
                .await?;
                let compute = context.acquire_compute().await?;
                context.check_cancelled()?;
                if limit_reached(remaining.as_deref()) {
                    drop(compute);
                    break 'morsels;
                }
                let (decoded, batch) = {
                    let _compute = compute;
                    let _active = context.scheduler.enter_lane();
                    let _parser_lane = context.metrics.enter_csv_parser_lane();
                    let started = Instant::now();
                    let result = (|| {
                        let decoded = decoder.decode(&morsel.bytes[offset..]).map_err(|error| {
                            csv_decode_error(
                                &uri,
                                morsel.decompressed_offset.saturating_add(
                                    u64::try_from(offset).unwrap_or(u64::MAX),
                                ),
                                error,
                            )
                        })?;
                        let batch = if decoder.capacity() == 0 {
                            decoder.flush().map_err(|error| {
                                csv_decode_error(
                                    &uri,
                                    morsel.decompressed_offset.saturating_add(
                                        u64::try_from(offset.saturating_add(decoded))
                                            .unwrap_or(u64::MAX),
                                    ),
                                    error,
                                )
                            })?
                        } else {
                            None
                        };
                        Ok::<_, Error>((decoded, batch))
                    })();
                    context
                        .metrics
                        .record_csv_decode_compute_time(started.elapsed());
                    result?
                };
                offset = offset.saturating_add(decoded);
                decoder_dirty |= decoded != 0;
                if decoded == 0 && batch.is_none() {
                    Err(Error::Internal(
                        "CSV morsel decoder made no progress".to_owned(),
                    ))?;
                }
                let Some(batch) = batch else {
                    continue;
                };
                decoder_dirty = false;
                let Some(batch) = claim_batch(batch, remaining.as_deref()) else {
                    break 'morsels;
                };
                context.metrics.record_scan(
                    u64::try_from(batch.num_rows()).unwrap_or(u64::MAX),
                    1,
                    pending_scan_bytes,
                );
                pending_scan_bytes = 0;
                debug_assert_eq!(batch.schema(), output_schema);
                yield envelope_batch(batch, &mut output_credit)?;
            }
            if limit_reached(remaining.as_deref()) {
                drop(morsel);
                break;
            }
            // Every non-terminal morsel is newline-terminated. A final record
            // without a newline must wait for recv(None), which supplies
            // Arrow's explicit end-of-input signal before flushing.
            if remaining.is_some()
                && decoder_dirty
                && morsel.bytes.last() == Some(&b'\n')
            {
                let batch = flush_decoder(
                    &mut decoder,
                    &uri,
                    last_decompressed_offset,
                    &context,
                    false,
                )
                .await?;
                decoder_dirty = false;
                if let Some(batch) = batch {
                    let Some(batch) = claim_batch(batch, remaining.as_deref()) else {
                        drop(morsel);
                        break;
                    };
                    context.metrics.record_scan(
                        u64::try_from(batch.num_rows()).unwrap_or(u64::MAX),
                        1,
                        pending_scan_bytes,
                    );
                    pending_scan_bytes = 0;
                    debug_assert_eq!(batch.schema(), output_schema);
                    yield envelope_batch(batch, &mut output_credit)?;
                }
            }
            // The decoder owns copied record data. Release the source morsel
            // and its reservation before waiting for more input to complete a
            // partial batch.
            drop(morsel);
        }
    })
}

async fn ensure_output_credit(
    output_credit: &mut Option<MemoryReservation>,
    bytes: usize,
    held_bytes: usize,
    context: &QueryContext,
) -> Result<()> {
    if output_credit.is_none() {
        *output_credit = Some(
            context
                .reserve_memory_while_holding(bytes, held_bytes, "parallel CSV output credit")
                .await?,
        );
    }
    Ok(())
}

fn envelope_batch(
    batch: RecordBatch,
    output_credit: &mut Option<MemoryReservation>,
) -> Result<BatchEnvelope> {
    let credit = output_credit.take().ok_or_else(|| {
        Error::Internal("parallel CSV decoder produced a batch without output credit".to_owned())
    })?;
    BatchEnvelope::from_reservation(batch, credit, "parallel CSV scan task")
}

async fn flush_decoder(
    decoder: &mut arrow::csv::reader::Decoder,
    uri: &str,
    decompressed_offset: u64,
    context: &QueryContext,
    end_of_input: bool,
) -> Result<Option<RecordBatch>> {
    let compute = context.acquire_compute().await?;
    context.check_cancelled()?;
    let _compute = compute;
    let _active = context.scheduler.enter_lane();
    let _parser_lane = context.metrics.enter_csv_parser_lane();
    let started = Instant::now();
    let batch = (|| {
        if end_of_input {
            // Arrow's decoder uses an empty slice as the explicit end-of-input
            // signal, which also completes a final record without a newline.
            let decoded = decoder
                .decode(&[])
                .map_err(|error| csv_decode_error(uri, decompressed_offset, error))?;
            debug_assert_eq!(decoded, 0);
        }
        decoder
            .flush()
            .map_err(|error| csv_decode_error(uri, decompressed_offset, error))
    })();
    context
        .metrics
        .record_csv_decode_compute_time(started.elapsed());
    batch
}

fn limit_reached(remaining: Option<&AtomicUsize>) -> bool {
    remaining.is_some_and(|remaining| remaining.load(Ordering::Acquire) == 0)
}

fn claim_batch(batch: RecordBatch, remaining: Option<&AtomicUsize>) -> Option<RecordBatch> {
    let claimed = remaining.map_or(batch.num_rows(), |remaining| {
        claim_rows(remaining, batch.num_rows())
    });
    match claimed {
        0 => None,
        claimed if claimed < batch.num_rows() => Some(batch.slice(0, claimed)),
        _ => Some(batch),
    }
}

pub(super) async fn receive_morsel(
    receiver: &AsyncMutex<mpsc::Receiver<CsvMorsel>>,
    context: &QueryContext,
) -> Result<Option<CsvMorsel>> {
    let mut receiver = tokio::select! {
        _ = context.control.cancelled() => return context.check_cancelled().map(|()| None),
        receiver = receiver.lock() => receiver,
    };
    tokio::select! {
        _ = context.control.cancelled() => context.check_cancelled().map(|()| None),
        morsel = receiver.recv() => Ok(morsel),
    }
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
mod tests;
