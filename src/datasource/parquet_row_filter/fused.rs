use arrow::{
    array::BooleanArray, compute::kernels::boolean, error::ArrowError, record_batch::RecordBatch,
};
use parquet::{
    arrow::{
        ProjectionMask,
        arrow_reader::{ArrowPredicate, ArrowPredicateFn},
    },
    schema::types::SchemaDescriptor,
};
use std::time::Instant;

use super::ColumnPredicate;
use crate::runtime::QueryMetrics;

pub(super) fn build(
    columns: &[ColumnPredicate],
    parquet_schema: &SchemaDescriptor,
    metrics: QueryMetrics,
) -> Box<dyn ArrowPredicate> {
    let mut columns = columns.to_vec();
    columns.sort_unstable_by_key(|column| column.file_column);
    let projection = ProjectionMask::roots(
        parquet_schema,
        columns.iter().map(|column| column.file_column),
    );
    Box::new(ArrowPredicateFn::new(projection, move |batch| {
        let started = Instant::now();
        let rows = batch.num_rows();
        let result = evaluate(&columns, &batch);
        metrics.record_parquet_row_filter_compute(rows, started.elapsed());
        result
    }))
}

pub(super) fn evaluate(
    predicates: &[ColumnPredicate],
    batch: &RecordBatch,
) -> Result<BooleanArray, ArrowError> {
    if batch.num_columns() != predicates.len() {
        return Err(ArrowError::ComputeError(format!(
            "fused Parquet row filter received {} columns for {} predicates",
            batch.num_columns(),
            predicates.len()
        )));
    }
    let mut columns = predicates.iter().zip(batch.columns());
    let Some((first, column)) = columns.next() else {
        return Err(ArrowError::ComputeError(
            "fused Parquet row filter received no predicates".to_owned(),
        ));
    };
    let mut mask = first.evaluate(column)?;
    for (predicate, column) in columns {
        mask = boolean::and(&mask, &predicate.evaluate(column)?)?;
    }
    Ok(mask)
}
