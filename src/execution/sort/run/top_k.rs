use std::sync::Arc;

use arrow::{
    array::Array,
    compute::{concat, interleave_record_batch},
    datatypes::SchemaRef,
    record_batch::RecordBatch,
    row::RowConverter,
};

use super::super::{empty_columns_batch, evaluate_keys};
use crate::{Result, sql::SortExpr};

pub(super) fn select(
    batches: &[RecordBatch],
    expressions: &[SortExpr],
    converter: &RowConverter,
    limit: usize,
    schema: &SchemaRef,
    row_count: usize,
) -> Result<RecordBatch> {
    let batch_keys = batches
        .iter()
        .map(|batch| evaluate_keys(expressions, batch))
        .collect::<Result<Vec<_>>>()?;
    let keys = (0..expressions.len())
        .map(|column| {
            let arrays = batch_keys
                .iter()
                .map(|keys| keys[column].as_ref())
                .collect::<Vec<&dyn Array>>();
            Ok(concat(&arrays)?)
        })
        .collect::<Result<Vec<_>>>()?;
    drop(batch_keys);
    let rows = converter.convert_columns(&keys)?;
    drop(keys);
    let mut indices = (0..row_count as u32).collect::<Vec<_>>();
    let compare =
        |left: &u32, right: &u32| rows.row(*left as usize).cmp(&rows.row(*right as usize));
    indices.select_nth_unstable_by(limit, compare);
    indices.truncate(limit);
    indices.sort_unstable_by(compare);
    drop(rows);
    if schema.fields().is_empty() {
        return empty_columns_batch(Arc::clone(schema), limit);
    }
    let mut offset = 0usize;
    let ends = batches
        .iter()
        .map(|batch| {
            offset += batch.num_rows();
            offset
        })
        .collect::<Vec<_>>();
    let selected = indices
        .into_iter()
        .map(|row| {
            let row = row as usize;
            let batch = ends.partition_point(|end| *end <= row);
            let start = if batch == 0 { 0 } else { ends[batch - 1] };
            (batch, row - start)
        })
        .collect::<Vec<_>>();
    let inputs = batches.iter().collect::<Vec<_>>();
    Ok(interleave_record_batch(&inputs, &selected)?)
}
