use arrow::{
    array::{ArrayRef, BooleanArray},
    compute::kernels::boolean,
    error::ArrowError,
};
use parquet::{
    arrow::{
        ProjectionMask,
        arrow_reader::{ArrowPredicate, ArrowPredicateFn},
    },
    schema::types::SchemaDescriptor,
};
use std::time::Instant;

use super::{ComparisonOp, PredicateValue, compare_value};
use crate::runtime::QueryMetrics;

#[derive(Clone, Debug)]
pub(super) struct ColumnPredicate {
    pub(super) file_column: usize,
    pub(super) leaves: Vec<LeafPredicate>,
}

#[derive(Clone, Debug)]
pub(super) enum LeafPredicate {
    Comparison {
        file_column: usize,
        op: ComparisonOp,
        value: PredicateValue,
    },
    IsNull {
        file_column: usize,
        negated: bool,
    },
}

impl ColumnPredicate {
    pub(super) fn evaluate(&self, column: &ArrayRef) -> Result<BooleanArray, ArrowError> {
        let mut leaves = self.leaves.iter();
        let Some(first) = leaves.next() else {
            return Err(ArrowError::ComputeError(
                "Parquet row filter received an empty column predicate".to_owned(),
            ));
        };
        let mut mask = first.evaluate(column)?;
        for leaf in leaves {
            mask = boolean::and(&mask, &leaf.evaluate(column)?)?;
        }
        Ok(mask)
    }

    /// Boolean buffers simultaneously live while this predicate is evaluated.
    pub(super) fn mask_buffers(&self) -> usize {
        match self.leaves.len() {
            0 => 0,
            1 => 1,
            _ => 3,
        }
    }
}

pub(super) fn build(
    predicate: ColumnPredicate,
    parquet_schema: &SchemaDescriptor,
    metrics: QueryMetrics,
) -> Box<dyn ArrowPredicate> {
    let projection = ProjectionMask::roots(parquet_schema, [predicate.file_column]);
    Box::new(ArrowPredicateFn::new(projection, move |batch| {
        let started = Instant::now();
        let rows = batch.num_rows();
        let column = batch.columns().first().ok_or_else(|| {
            ArrowError::ComputeError("Parquet row filter received no projected column".to_owned())
        })?;
        let result = predicate.evaluate(column);
        metrics.record_parquet_row_filter_compute(rows, started.elapsed());
        result
    }))
}

impl LeafPredicate {
    fn file_column(&self) -> usize {
        match self {
            Self::Comparison { file_column, .. } | Self::IsNull { file_column, .. } => *file_column,
        }
    }

    fn evaluate(&self, column: &ArrayRef) -> Result<BooleanArray, ArrowError> {
        match self {
            Self::Comparison { op, value, .. } => compare_value(column, *op, value),
            Self::IsNull { negated, .. } => {
                if *negated {
                    boolean::is_not_null(column.as_ref())
                } else {
                    boolean::is_null(column.as_ref())
                }
            }
        }
    }
}

pub(super) fn group_by_file_column(leaves: Vec<LeafPredicate>) -> Vec<ColumnPredicate> {
    let mut columns: Vec<ColumnPredicate> = Vec::new();
    for leaf in leaves {
        let file_column = leaf.file_column();
        if let Some(column) = columns
            .iter_mut()
            .find(|column| column.file_column == file_column)
        {
            column.leaves.push(leaf);
        } else {
            columns.push(ColumnPredicate {
                file_column,
                leaves: vec![leaf],
            });
        }
    }
    columns
}
