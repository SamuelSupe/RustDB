mod merge;
mod parallel;
mod run;

#[cfg(test)]
mod tests;

use std::{mem::size_of, sync::Arc};

use arrow::{
    array::{ArrayRef, UInt32Array},
    compute::{SortOptions, take_record_batch},
    datatypes::SchemaRef,
    record_batch::{RecordBatch, RecordBatchOptions},
    row::{RowConverter, SortField},
};
use futures::StreamExt;

use crate::runtime::{
    BatchEnvelope, IntoMemoryBatchStream, MemoryBatchStream, QueryContext,
    boxed_memory_batch_stream,
};
use crate::sql::SortExpr;
use crate::{Error, Result};

use super::expr::evaluate;
use super::value::{canonical_sort_key_type, canonicalize_sort_key};
use merge::{MemoryRun, MergeIterator, MergeRun};
use run::{RunCleanup, compact_pending_runs, compact_runs, sort_batches, spill_run};

pub(in crate::execution) const MERGE_FAN_IN: usize = 8;

pub(crate) fn sort<I>(
    input: I,
    expressions: Vec<SortExpr>,
    fetch: Option<usize>,
    schema: SchemaRef,
    context: Arc<QueryContext>,
    batch_size: usize,
) -> MemoryBatchStream
where
    I: IntoMemoryBatchStream,
{
    let input = input.into_memory_batch_stream(Arc::clone(&context), "sort input");
    if parallel::is_supported(&context) {
        return parallel::sort(input, expressions, fetch, schema, context, batch_size);
    }
    serial_sort(input, expressions, fetch, schema, context, batch_size)
}

fn serial_sort<I>(
    input: I,
    expressions: Vec<SortExpr>,
    fetch: Option<usize>,
    schema: SchemaRef,
    context: Arc<QueryContext>,
    batch_size: usize,
) -> MemoryBatchStream
where
    I: IntoMemoryBatchStream,
{
    let mut input = input.into_memory_batch_stream(Arc::clone(&context), "sort input");
    boxed_memory_batch_stream(async_stream::try_stream! {
        if expressions.is_empty() {
            Err(Error::InvalidArgument("ORDER BY requires at least one expression".into()))?;
        }
        if fetch == Some(0) {
            return;
        }

        let batch_size = batch_size.max(1);
        let converter = make_converter(&expressions)?;
        let spill_headroom = context
            .spill
            .writer_headroom_bytes("sort-merge", schema.as_ref());
        let sort_pool = context.memory.child(
            format!("sort-{}", context.query_id),
            sort_state_limit(context.memory.limit()),
        );
        let mut reservation = sort_pool.reservation();
        let mut input_memory = context.memory.reservation();
        let mut buffered_input_bytes = 0usize;
        let mut buffered = Vec::new();
        let mut cleanup = RunCleanup::new(context.spill.clone());
        let mut spill_batch_rows = batch_size;

        while let Some(batch) = input.next().await {
            context.check_cancelled()?;
            let batch = batch?;
            if batch.batch().num_rows() == 0 {
                continue;
            }
            let input_bytes = batch.memory_size().max(1);
            if context.memory.available() < spill_headroom {
                if buffered.is_empty() {
                    Err(input_batch_error(input_bytes, &context))?;
                }
                let (run, rows) = spill_run(
                    &buffered,
                    &expressions,
                    &converter,
                    fetch,
                    &schema,
                    &context,
                    batch_size,
                    sort_pool.limit(),
                )?;
                cleanup.add(run);
                spill_batch_rows = spill_batch_rows.min(rows);
                buffered = Vec::new();
                input_memory.shrink(buffered_input_bytes);
                buffered_input_bytes = 0;
                reservation.try_resize(0)?;
                compact_pending_runs(
                    &mut cleanup,
                    &expressions,
                    fetch,
                    &schema,
                    &context,
                    &sort_pool,
                    spill_batch_rows,
                )?;
                if context.memory.available() < spill_headroom {
                    Err(input_batch_error(input_bytes, &context))?;
                }
            }
            let (batch, batch_memory) = batch.into_parts();
            input_memory.absorb(batch_memory)?;
            let estimate = estimate_sort_bytes(&batch, expressions.len());
            if estimate > sort_pool.limit() {
                if !buffered.is_empty() {
                    let (run, rows) = spill_run(
                        &buffered,
                        &expressions,
                        &converter,
                        fetch,
                        &schema,
                        &context,
                        batch_size,
                        sort_pool.limit(),
                    )?;
                    cleanup.add(run);
                    spill_batch_rows = spill_batch_rows.min(rows);
                    buffered = Vec::new();
                    input_memory.shrink(buffered_input_bytes);
                    buffered_input_bytes = 0;
                    reservation.try_resize(0)?;
                    compact_pending_runs(
                        &mut cleanup,
                        &expressions,
                        fetch,
                        &schema,
                        &context,
                        &sort_pool,
                        spill_batch_rows,
                    )?;
                }

                let mut offset = 0usize;
                while offset < batch.num_rows() {
                    context.check_cancelled()?;
                    // Every completed run adds active-file metadata. Recompute
                    // the next slice against the remaining budget instead of
                    // reusing the first run's larger allowance.
                    let rows_per_run = rows_within_limit(
                        &batch,
                        expressions.len(),
                        available_workspace(&sort_pool, spill_headroom, &context),
                    )?;
                    let length = rows_per_run.min(batch.num_rows() - offset);
                    let slice = batch.slice(offset, length);
                    let slice_estimate = estimate_sort_rows(&batch, expressions.len(), length);
                    reserve_workspace(
                        &mut reservation,
                        slice_estimate,
                        spill_headroom,
                        &context,
                    )?;
                    let (run, rows) = spill_run(
                        &[slice],
                        &expressions,
                        &converter,
                        fetch,
                        &schema,
                        &context,
                        batch_size,
                        sort_pool.limit(),
                    )?;
                    cleanup.add(run);
                    spill_batch_rows = spill_batch_rows.min(rows);
                    reservation.try_resize(0)?;
                    compact_pending_runs(
                        &mut cleanup,
                        &expressions,
                        fetch,
                        &schema,
                        &context,
                        &sort_pool,
                        spill_batch_rows,
                    )?;
                    offset += length;
                }
                drop(batch);
                input_memory.shrink(input_bytes);
                continue;
            }
            if !try_grow_workspace(&mut reservation, estimate, spill_headroom, &context) {
                if buffered.is_empty() {
                    Err(sort_workspace_error(estimate, &context))?;
                }
                let (run, rows) = spill_run(
                    &buffered,
                    &expressions,
                    &converter,
                    fetch,
                    &schema,
                    &context,
                    batch_size,
                    sort_pool.limit(),
                )?;
                cleanup.add(run);
                spill_batch_rows = spill_batch_rows.min(rows);
                buffered = Vec::new();
                input_memory.shrink(buffered_input_bytes);
                buffered_input_bytes = 0;
                reservation.try_resize(0)?;
                compact_pending_runs(
                    &mut cleanup,
                    &expressions,
                    fetch,
                    &schema,
                    &context,
                    &sort_pool,
                    spill_batch_rows,
                )?;
                if !try_grow_workspace(&mut reservation, estimate, spill_headroom, &context) {
                    Err(sort_workspace_error(estimate, &context))?;
                }
            }
            context.metrics.observe_memory(context.memory.used());
            buffered_input_bytes = buffered_input_bytes.saturating_add(input_bytes);
            buffered.push(batch);
        }

        if cleanup.is_empty() {
            if buffered.is_empty() {
                return;
            }
            let sorted = sort_batches(&buffered, &expressions, &converter, fetch, &schema)?;
            drop(buffered);
            input_memory.shrink(buffered_input_bytes);
            let sorted_bytes = sorted.get_array_memory_size().max(1);
            reservation
                .try_resize(sorted_bytes)
                .map_err(|_| sort_workspace_error(sorted_bytes, &context))?;
            let mut offset = 0usize;
            while offset < sorted.num_rows() {
                context.check_cancelled()?;
                let mut length = batch_size.min(sorted.num_rows() - offset);
                let available = context.memory.limit().saturating_sub(reservation.size());
                while length > 1
                    && output_slice_workspace_bytes(&sorted, offset, length)? > available
                {
                    length = length.div_ceil(2);
                }
                let workspace = context
                    .reserve_memory_while_holding(
                        output_slice_workspace_bytes(&sorted, offset, length)?,
                        reservation.size(),
                        "sort output workspace",
                    )
                    .await?;
                let indices = UInt32Array::from_iter_values(
                    (offset..offset + length).map(|row| u32::try_from(row).unwrap_or(u32::MAX)),
                );
                let output = take_record_batch(&sorted, &indices)?;
                yield BatchEnvelope::from_reservation(output, workspace, "sort output")?;
                offset += length;
            }
            return;
        }

        if !buffered.is_empty() {
            let (run, rows) = spill_run(
                &buffered,
                &expressions,
                &converter,
                fetch,
                &schema,
                &context,
                batch_size,
                sort_pool.limit(),
            )?;
            cleanup.add(run);
            spill_batch_rows = spill_batch_rows.min(rows);
            drop(buffered);
            input_memory.shrink(buffered_input_bytes);
        }
        reservation.try_resize(0)?;
        drop(input_memory);
        compact_pending_runs(
            &mut cleanup,
            &expressions,
            fetch,
            &schema,
            &context,
            &sort_pool,
            spill_batch_rows,
        )?;
        drop(reservation);

        let runs = compact_runs(
            cleanup.files(),
            &mut cleanup,
            &expressions,
            fetch,
            &schema,
            &context,
            &sort_pool,
            spill_batch_rows,
        )?;
        let mut merge = MergeIterator::new(
            &runs,
            expressions,
            fetch,
            Arc::clone(&schema),
            Arc::clone(&context),
            sort_pool.reservation(),
            batch_size,
        )?;
        while let Some(batch) = merge.next_envelope().await? {
            context.check_cancelled()?;
            yield batch;
        }
    })
}

fn sort_state_limit(query_limit: usize) -> usize {
    // The other half remains available for active spill-file metadata and for
    // merge cursors/output while a buffered run is being written.
    query_limit.checked_div(2).unwrap_or(0).max(1)
}

fn make_converter(expressions: &[SortExpr]) -> Result<RowConverter> {
    let fields = expressions
        .iter()
        .map(|expression| {
            SortField::new_with_options(
                canonical_sort_key_type(&expression.expr.data_type),
                SortOptions {
                    descending: expression.descending,
                    nulls_first: expression.nulls_first,
                },
            )
        })
        .collect();
    Ok(RowConverter::new(fields)?)
}

fn evaluate_keys(expressions: &[SortExpr], batch: &RecordBatch) -> Result<Vec<ArrayRef>> {
    expressions
        .iter()
        .map(|expression| {
            let array = evaluate(&expression.expr, batch)?;
            if array.data_type() != &expression.expr.data_type {
                return Err(Error::Internal(format!(
                    "sort expression {} produced {}, expected {}",
                    expression.expr.display_name,
                    array.data_type(),
                    expression.expr.data_type
                )));
            }
            canonicalize_sort_key(array)
        })
        .collect()
}

fn estimate_sort_bytes(batch: &RecordBatch, key_count: usize) -> usize {
    batch
        .get_array_memory_size()
        .saturating_mul(3)
        .saturating_add(
            batch
                .num_rows()
                .saturating_mul(key_count.saturating_mul(16).saturating_add(4)),
        )
        .max(1)
}

fn estimate_sort_rows(batch: &RecordBatch, key_count: usize, rows: usize) -> usize {
    let data_per_row = batch
        .get_array_memory_size()
        .div_ceil(batch.num_rows().max(1));
    rows.saturating_mul(data_per_row.saturating_mul(3))
        .saturating_add(rows.saturating_mul(key_count.saturating_mul(16).saturating_add(4)))
        .max(1)
}

fn rows_within_limit(batch: &RecordBatch, key_count: usize, memory_limit: usize) -> Result<usize> {
    let one_row = estimate_sort_rows(batch, key_count, 1);
    if one_row > memory_limit {
        return Err(Error::ResourceExhausted(format!(
            "sort requires at least {one_row} bytes for one row, but the query limit is {memory_limit} bytes"
        )));
    }
    Ok((memory_limit / one_row).max(1).min(batch.num_rows()))
}

fn available_workspace(
    pool: &crate::runtime::MemoryPool,
    spill_headroom: usize,
    context: &QueryContext,
) -> usize {
    pool.available()
        .min(context.memory.available().saturating_sub(spill_headroom))
}

fn try_grow_workspace(
    reservation: &mut crate::runtime::MemoryReservation,
    bytes: usize,
    spill_headroom: usize,
    context: &QueryContext,
) -> bool {
    bytes <= context.memory.available().saturating_sub(spill_headroom)
        && reservation.try_grow(bytes).is_ok()
}

fn reserve_workspace(
    reservation: &mut crate::runtime::MemoryReservation,
    bytes: usize,
    spill_headroom: usize,
    context: &QueryContext,
) -> Result<()> {
    let growth = bytes.saturating_sub(reservation.size());
    if growth > context.memory.available().saturating_sub(spill_headroom) {
        return Err(sort_workspace_error(bytes, context));
    }
    reservation
        .try_resize(bytes)
        .map_err(|_| sort_workspace_error(bytes, context))
}

fn input_batch_error(bytes: usize, context: &QueryContext) -> Error {
    Error::ResourceExhausted(format!(
        "sort cannot reserve the complete input batch: {bytes} bytes required, query limit {} \
         bytes, currently available {} bytes",
        context.memory.limit(),
        context.memory.available()
    ))
}

fn sort_workspace_error(bytes: usize, context: &QueryContext) -> Error {
    Error::ResourceExhausted(format!(
        "sort cannot reserve {bytes} bytes of run workspace while retaining its input batch \
         and spill I/O state (query limit {} bytes, currently available {} bytes)",
        context.memory.limit(),
        context.memory.available()
    ))
}

fn empty_columns_batch(schema: SchemaRef, rows: usize) -> Result<RecordBatch> {
    let options = RecordBatchOptions::new().with_row_count(Some(rows));
    Ok(RecordBatch::try_new_with_options(
        schema,
        Vec::new(),
        &options,
    )?)
}

fn output_slice_workspace_bytes(batch: &RecordBatch, offset: usize, rows: usize) -> Result<usize> {
    let logical = batch.columns().iter().try_fold(0usize, |bytes, column| {
        let data = column.to_data().slice(offset, rows);
        Ok::<_, arrow::error::ArrowError>(bytes.saturating_add(data.get_slice_memory_size()?))
    })?;
    Ok(logical
        .saturating_mul(2)
        .saturating_add(rows.saturating_mul(size_of::<u32>()).saturating_mul(2))
        .saturating_add(batch.num_columns().saturating_mul(512))
        .saturating_add(1_024)
        .max(1))
}
