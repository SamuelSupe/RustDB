use std::sync::Arc;

use arrow::{
    array::UInt32Array,
    compute::{concat_batches, take_record_batch},
    datatypes::SchemaRef,
    record_batch::RecordBatch,
    row::RowConverter,
};

use crate::runtime::{MemoryPool, QueryContext, SpillFile, SpillManager};
use crate::sql::SortExpr;
use crate::{Error, Result};

use super::merge::MergeIterator;
use super::{MERGE_FAN_IN, empty_columns_batch, evaluate_keys};

pub(super) fn sort_batches(
    batches: &[RecordBatch],
    expressions: &[SortExpr],
    converter: &RowConverter,
    fetch: Option<usize>,
    schema: &SchemaRef,
) -> Result<RecordBatch> {
    let combined = concat_batches(schema, batches)?;
    let keys = evaluate_keys(expressions, &combined)?;
    let rows = converter.convert_columns(&keys)?;
    let row_count = rows.num_rows();
    if row_count > u32::MAX as usize {
        return Err(Error::ResourceExhausted(
            "a sort run cannot contain more than u32::MAX rows".into(),
        ));
    }

    let mut indices: Vec<u32> = (0..u32::try_from(row_count).unwrap_or(u32::MAX)).collect();
    let limit = fetch.unwrap_or(row_count).min(row_count);
    let compare =
        |left: &u32, right: &u32| rows.row(*left as usize).cmp(&rows.row(*right as usize));
    if limit < indices.len() {
        indices.select_nth_unstable_by(limit, compare);
        indices.truncate(limit);
    }
    indices.sort_unstable_by(compare);

    if schema.fields().is_empty() {
        return empty_columns_batch(Arc::clone(schema), indices.len());
    }
    Ok(take_record_batch(&combined, &UInt32Array::from(indices))?)
}

#[allow(clippy::too_many_arguments)]
pub(super) fn spill_run(
    batches: &[RecordBatch],
    expressions: &[SortExpr],
    converter: &RowConverter,
    fetch: Option<usize>,
    schema: &SchemaRef,
    context: &QueryContext,
    batch_size: usize,
) -> Result<(SpillFile, usize)> {
    context.check_cancelled()?;
    let sorted = sort_batches(batches, expressions, converter, fetch, schema)?;
    let chunk_rows = spill_chunk_rows(&sorted, batch_size, context.memory.limit());
    let chunks = (0..sorted.num_rows())
        .step_by(chunk_rows)
        .map(|offset| Ok(sorted.slice(offset, chunk_rows.min(sorted.num_rows() - offset))));
    let file = context
        .spill
        .write_batches("sort-run", Arc::clone(schema), chunks)?;
    Ok((file, chunk_rows))
}

fn spill_chunk_rows(batch: &RecordBatch, batch_size: usize, memory_limit: usize) -> usize {
    if batch.num_rows() == 0 {
        return 1;
    }
    let bytes_per_row = batch
        .get_array_memory_size()
        .saturating_add(batch.num_rows() - 1)
        / batch.num_rows();
    let target = memory_limit
        .checked_div(MERGE_FAN_IN.saturating_mul(4))
        .unwrap_or(0)
        .max(bytes_per_row);
    batch_size.min(target / bytes_per_row.max(1)).max(1)
}

#[allow(clippy::too_many_arguments)]
pub(super) fn compact_runs(
    mut runs: Vec<SpillFile>,
    cleanup: &mut RunCleanup,
    expressions: &[SortExpr],
    fetch: Option<usize>,
    schema: &SchemaRef,
    context: &Arc<QueryContext>,
    pool: &MemoryPool,
    batch_size: usize,
) -> Result<Vec<SpillFile>> {
    while runs.len() > MERGE_FAN_IN {
        let mut next = Vec::with_capacity(runs.len().div_ceil(MERGE_FAN_IN));
        for group in runs.chunks(MERGE_FAN_IN) {
            context.check_cancelled()?;
            if group.len() == 1 {
                next.push(group[0].clone());
                continue;
            }
            let merge = MergeIterator::new(
                group,
                expressions.to_vec(),
                fetch,
                Arc::clone(schema),
                Arc::clone(context),
                pool.reservation(),
                batch_size,
            )?;
            let merged = context
                .spill
                .write_batches("sort-merge", Arc::clone(schema), merge)?;
            cleanup.add(merged.clone());
            for old in group {
                cleanup.remove(old);
            }
            next.push(merged);
        }
        runs = next;
    }
    Ok(runs)
}

pub(super) struct RunCleanup {
    spill: SpillManager,
    files: Vec<SpillFile>,
}

impl RunCleanup {
    pub(super) fn new(spill: SpillManager) -> Self {
        Self {
            spill,
            files: Vec::new(),
        }
    }

    pub(super) fn add(&mut self, file: SpillFile) {
        self.files.push(file);
    }

    fn remove(&mut self, file: &SpillFile) {
        self.spill.remove_file(file);
        self.files.retain(|candidate| candidate != file);
    }

    pub(super) fn files(&self) -> Vec<SpillFile> {
        self.files.clone()
    }

    pub(super) fn is_empty(&self) -> bool {
        self.files.is_empty()
    }
}

impl Drop for RunCleanup {
    fn drop(&mut self) {
        for file in &self.files {
            self.spill.remove_file(file);
        }
    }
}
