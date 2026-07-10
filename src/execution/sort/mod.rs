mod merge;
mod run;

#[cfg(test)]
mod tests;

use std::sync::Arc;

use arrow::{
    array::ArrayRef,
    compute::SortOptions,
    datatypes::SchemaRef,
    record_batch::{RecordBatch, RecordBatchOptions},
    row::{RowConverter, SortField},
};
use futures::StreamExt;

use crate::runtime::{QueryContext, RecordBatchStream, boxed_record_batch_stream};
use crate::sql::SortExpr;
use crate::{Error, Result};

use super::expr::evaluate;
use merge::MergeIterator;
use run::{RunCleanup, compact_runs, sort_batches, spill_run};

const MERGE_FAN_IN: usize = 8;

pub(crate) fn sort(
    mut input: RecordBatchStream,
    expressions: Vec<SortExpr>,
    fetch: Option<usize>,
    schema: SchemaRef,
    context: Arc<QueryContext>,
    batch_size: usize,
) -> RecordBatchStream {
    boxed_record_batch_stream(async_stream::try_stream! {
        if expressions.is_empty() {
            Err(Error::InvalidArgument("ORDER BY requires at least one expression".into()))?;
        }
        if fetch == Some(0) {
            return;
        }

        let batch_size = batch_size.max(1);
        let converter = make_converter(&expressions)?;
        let sort_pool = context.memory.child(
            format!("sort-{}", context.query_id),
            context.memory.limit(),
        );
        let mut reservation = sort_pool.reservation();
        let mut buffered = Vec::new();
        let mut cleanup = RunCleanup::new(context.spill.clone());
        let mut spill_batch_rows = batch_size;

        while let Some(batch) = input.next().await {
            context.check_cancelled()?;
            let batch = batch?;
            if batch.num_rows() == 0 {
                continue;
            }
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
                    )?;
                    cleanup.add(run);
                    spill_batch_rows = spill_batch_rows.min(rows);
                    buffered.clear();
                    reservation.try_resize(0)?;
                }

                let rows_per_run = rows_within_limit(
                    &batch,
                    expressions.len(),
                    sort_pool.limit(),
                )?;
                for offset in (0..batch.num_rows()).step_by(rows_per_run) {
                    context.check_cancelled()?;
                    let length = rows_per_run.min(batch.num_rows() - offset);
                    let slice = batch.slice(offset, length);
                    let slice_estimate = estimate_sort_rows(&batch, expressions.len(), length);
                    reservation.try_resize(slice_estimate)?;
                    let (run, rows) = spill_run(
                        &[slice],
                        &expressions,
                        &converter,
                        fetch,
                        &schema,
                        &context,
                        batch_size,
                    )?;
                    cleanup.add(run);
                    spill_batch_rows = spill_batch_rows.min(rows);
                    reservation.try_resize(0)?;
                }
                continue;
            }
            if reservation.try_grow(estimate).is_err() {
                if buffered.is_empty() {
                    reservation.try_grow(estimate)?;
                    buffered.push(batch);
                    continue;
                }
                let (run, rows) = spill_run(
                    &buffered,
                    &expressions,
                    &converter,
                    fetch,
                    &schema,
                    &context,
                    batch_size,
                )?;
                cleanup.add(run);
                spill_batch_rows = spill_batch_rows.min(rows);
                buffered.clear();
                reservation.try_resize(0)?;
                reservation.try_grow(estimate)?;
            }
            context.metrics.observe_memory(context.memory.used());
            buffered.push(batch);
        }

        if cleanup.is_empty() {
            if buffered.is_empty() {
                return;
            }
            let sorted = sort_batches(&buffered, &expressions, &converter, fetch, &schema)?;
            for offset in (0..sorted.num_rows()).step_by(batch_size) {
                context.check_cancelled()?;
                let length = batch_size.min(sorted.num_rows() - offset);
                yield sorted.slice(offset, length);
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
            )?;
            cleanup.add(run);
            spill_batch_rows = spill_batch_rows.min(rows);
            buffered.clear();
        }
        reservation.try_resize(0)?;
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
        for batch in &mut merge {
            context.check_cancelled()?;
            yield batch?;
        }
    })
}

fn make_converter(expressions: &[SortExpr]) -> Result<RowConverter> {
    let fields = expressions
        .iter()
        .map(|expression| {
            SortField::new_with_options(
                expression.expr.data_type.clone(),
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
            Ok(array)
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

fn empty_columns_batch(schema: SchemaRef, rows: usize) -> Result<RecordBatch> {
    let options = RecordBatchOptions::new().with_row_count(Some(rows));
    Ok(RecordBatch::try_new_with_options(
        schema,
        Vec::new(),
        &options,
    )?)
}
