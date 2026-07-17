use arrow::{
    array::{
        BooleanArray, Date32Array, Datum, Decimal64Array, Decimal128Array, Float64Array,
        Int64Array, Scalar, TimestampMicrosecondArray, UInt64Array,
    },
    compute::kernels::cmp,
    datatypes::{DataType, Schema, TimeUnit},
    error::ArrowError,
};
use parquet::{arrow::arrow_reader::RowFilter, schema::types::SchemaDescriptor};

use super::{ComparisonOp, PredicateValue, ScanPredicate};
use crate::{
    Error, Result,
    runtime::{QueryMetrics, estimate_array_bytes},
};

mod column;
mod fused;

use column::{ColumnPredicate, LeafPredicate, group_by_file_column};

/// A file-specific, cloneable description of safe Parquet reader predicates.
/// The actual `RowFilter` is rebuilt per row-group reader because it owns
/// mutable predicate closures.
#[derive(Clone, Debug)]
pub(super) struct ParquetRowFilter {
    columns: Vec<ColumnPredicate>,
    fuse_columns: bool,
}

impl ParquetRowFilter {
    pub(super) fn try_new(
        predicate: Option<&ScanPredicate>,
        file_schema: &Schema,
        table_schema: &Schema,
    ) -> Option<Self> {
        Self::try_new_inner(predicate, file_schema, table_schema, false)
    }

    pub(super) fn try_new_strict(
        predicate: Option<&ScanPredicate>,
        file_schema: &Schema,
        table_schema: &Schema,
    ) -> Option<Self> {
        Self::try_new_inner(predicate, file_schema, table_schema, true)
    }

    pub(super) fn try_new_exact(
        predicate: Option<&ScanPredicate>,
        file_schema: &Schema,
        table_schema: &Schema,
    ) -> Result<Self> {
        let predicate = predicate.ok_or_else(|| {
            Error::Internal("exact Parquet scan is missing its predicate".to_owned())
        })?;
        if !super::exact_filter::supported(predicate, table_schema, file_schema) {
            return Err(Error::Internal(
                "exact Parquet predicate is not fully supported by the physical file schema"
                    .to_owned(),
            ));
        }
        Self::try_new_inner(Some(predicate), file_schema, table_schema, true).ok_or_else(|| {
            Error::Internal("exact Parquet row filter could not be constructed".to_owned())
        })
    }

    fn try_new_inner(
        predicate: Option<&ScanPredicate>,
        file_schema: &Schema,
        table_schema: &Schema,
        allow_fusion: bool,
    ) -> Option<Self> {
        let predicate = predicate?;
        let mut leaves = Vec::new();
        collect_file_leaves(predicate, file_schema, table_schema, &mut leaves);
        let columns = group_by_file_column(leaves);
        let fuse_columns = allow_fusion
            && columns.len() >= 3
            && complete_fixed_conjunction(predicate, file_schema, table_schema);
        (!columns.is_empty()).then_some(Self {
            columns,
            fuse_columns,
        })
    }

    pub(super) fn build(
        &self,
        parquet_schema: &SchemaDescriptor,
        metrics: QueryMetrics,
    ) -> RowFilter {
        if self.fuse_columns {
            return RowFilter::new(vec![fused::build(&self.columns, parquet_schema, metrics)]);
        }
        let predicates = self
            .columns
            .iter()
            .cloned()
            .map(|predicate| column::build(predicate, parquet_schema, metrics.clone()))
            .collect();
        RowFilter::new(predicates)
    }

    /// A fragmented selection can still avoid materialising a wide payload.
    /// Keep this hint narrow: the Arrow default remains preferable for other
    /// predicate and projection shapes.
    pub(super) fn has_unfiltered_decimal_payload(
        &self,
        projection: &[usize],
        file_schema: &Schema,
    ) -> bool {
        self.fuse_columns
            && projection.iter().copied().any(|column| {
                !self
                    .columns
                    .iter()
                    .any(|predicate| predicate.file_column == column)
                    && file_schema
                        .fields()
                        .get(column)
                        .is_some_and(|field| matches!(field.data_type(), DataType::Decimal128(..)))
            })
    }

    /// Returns the fixed-width predicate columns Arrow may reuse for the
    /// narrowly admitted strict fused sparse-Decimal scan shape. The final
    /// projection intersection mirrors Arrow's row-group cache projection.
    pub(super) fn sparse_decimal_cache_columns(
        &self,
        projection: &[usize],
        file_schema: &Schema,
    ) -> Option<Vec<usize>> {
        if !self.has_unfiltered_decimal_payload(projection, file_schema) {
            return None;
        }
        let columns = self
            .columns
            .iter()
            .map(|column| column.file_column)
            .filter(|column| projection.contains(column))
            .filter(|column| {
                file_schema
                    .fields()
                    .get(*column)
                    .is_some_and(|field| fixed_filter_type(field.data_type()))
            })
            .collect::<Vec<_>>();
        (!columns.is_empty()).then_some(columns)
    }
}

fn complete_fixed_conjunction(
    predicate: &ScanPredicate,
    file_schema: &Schema,
    table_schema: &Schema,
) -> bool {
    let ScanPredicate::And(predicates) = predicate else {
        return false;
    };
    !predicates.is_empty()
        && predicates
            .iter()
            .all(|predicate| complete_fixed_leaf(predicate, file_schema, table_schema))
}

fn complete_fixed_leaf(
    predicate: &ScanPredicate,
    file_schema: &Schema,
    table_schema: &Schema,
) -> bool {
    match predicate {
        ScanPredicate::And(predicates) => {
            !predicates.is_empty()
                && predicates
                    .iter()
                    .all(|predicate| complete_fixed_leaf(predicate, file_schema, table_schema))
        }
        ScanPredicate::Comparison { column, value, .. } => {
            exact_file_column(*column, file_schema, table_schema)
                .is_some_and(|(_, data_type)| comparison_type_matches(data_type, value))
        }
        ScanPredicate::IsNull { column } | ScanPredicate::IsNotNull { column } => {
            exact_file_column(*column, file_schema, table_schema)
                .is_some_and(|(_, data_type)| fixed_filter_type(data_type))
        }
        ScanPredicate::Or(_) => false,
    }
}

fn collect_file_leaves(
    predicate: &ScanPredicate,
    file_schema: &Schema,
    table_schema: &Schema,
    output: &mut Vec<LeafPredicate>,
) {
    match predicate {
        ScanPredicate::And(predicates) => {
            for predicate in predicates {
                collect_file_leaves(predicate, file_schema, table_schema, output);
            }
        }
        ScanPredicate::Comparison { column, op, value } => {
            let Some((file_column, data_type)) =
                exact_file_column(*column, file_schema, table_schema)
            else {
                return;
            };
            if comparison_type_matches(data_type, value) {
                output.push(LeafPredicate::Comparison {
                    file_column,
                    op: *op,
                    value: value.clone(),
                });
            }
        }
        ScanPredicate::IsNull { column } | ScanPredicate::IsNotNull { column } => {
            let Some((file_column, data_type)) =
                exact_file_column(*column, file_schema, table_schema)
            else {
                return;
            };
            if fixed_filter_type(data_type) {
                output.push(LeafPredicate::IsNull {
                    file_column,
                    negated: matches!(predicate, ScanPredicate::IsNotNull { .. }),
                });
            }
        }
        // A subset of an OR expression is not a safe row-level predicate.
        ScanPredicate::Or(_) => {}
    }
}

fn exact_file_column<'a>(
    table_column: usize,
    file_schema: &'a Schema,
    table_schema: &Schema,
) -> Option<(usize, &'a DataType)> {
    let table_field = table_schema.fields().get(table_column)?;
    let file_column = file_schema.index_of(table_field.name()).ok()?;
    let file_type = file_schema.field(file_column).data_type();
    (file_type == table_field.data_type()).then_some((file_column, file_type))
}

fn comparison_type_matches(data_type: &DataType, value: &PredicateValue) -> bool {
    matches!(
        (data_type, value),
        (DataType::Boolean, PredicateValue::Boolean(_))
            | (DataType::Int64, PredicateValue::Int64(_))
            | (DataType::UInt64, PredicateValue::UInt64(_))
            | (DataType::Float64, PredicateValue::Float64(_))
            | (DataType::Date32, PredicateValue::Date32(_))
            | (
                DataType::Timestamp(TimeUnit::Microsecond, None),
                PredicateValue::TimestampMicros(_)
            )
    ) || matches!(
        (data_type, value),
        (
            DataType::Decimal128(precision, scale),
            PredicateValue::Decimal128 {
                precision: value_precision,
                scale: value_scale,
                ..
            }
        ) if precision == value_precision && scale == value_scale
    )
}

fn fixed_filter_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Boolean
            | DataType::Int64
            | DataType::UInt64
            | DataType::Float64
            | DataType::Date32
            | DataType::Timestamp(TimeUnit::Microsecond, None)
            | DataType::Decimal128(_, _)
    )
}

fn compare_value(
    column: &arrow::array::ArrayRef,
    op: ComparisonOp,
    value: &PredicateValue,
) -> Result<BooleanArray, ArrowError> {
    match value {
        PredicateValue::Boolean(value) => compare(column, &BooleanArray::new_scalar(*value), op),
        PredicateValue::Int64(value) => compare(column, &Int64Array::new_scalar(*value), op),
        PredicateValue::UInt64(value) => compare(column, &UInt64Array::new_scalar(*value), op),
        PredicateValue::Float64(value) => compare(column, &Float64Array::new_scalar(*value), op),
        PredicateValue::Date32(value) => compare(column, &Date32Array::new_scalar(*value), op),
        PredicateValue::TimestampMicros(value) => {
            compare(column, &TimestampMicrosecondArray::new_scalar(*value), op)
        }
        PredicateValue::Decimal128 {
            value,
            precision,
            scale,
        } if matches!(column.data_type(), DataType::Decimal64(_, _)) => {
            if column.data_type() != &DataType::Decimal64(*precision, *scale) {
                return Err(ArrowError::ComputeError(format!(
                    "Parquet narrow Decimal predicate type mismatch: column is {}, predicate is Decimal128({precision}, {scale})",
                    column.data_type(),
                )));
            }
            let value = i64::try_from(*value).map_err(|_| {
                ArrowError::ComputeError(
                    "Parquet narrow Decimal predicate does not fit in 64 bits".to_owned(),
                )
            })?;
            let scalar =
                Decimal64Array::from(vec![value]).with_precision_and_scale(*precision, *scale)?;
            compare(column, &Scalar::new(scalar), op)
        }
        PredicateValue::Decimal128 {
            value,
            precision,
            scale,
        } => {
            let scalar =
                Decimal128Array::from(vec![*value]).with_precision_and_scale(*precision, *scale)?;
            compare(column, &Scalar::new(scalar), op)
        }
        _ => Err(ArrowError::ComputeError(format!(
            "unsupported Parquet row-filter value {value:?}"
        ))),
    }
}

fn compare(
    left: &dyn Datum,
    right: &dyn Datum,
    op: ComparisonOp,
) -> Result<BooleanArray, ArrowError> {
    match op {
        ComparisonOp::Eq => cmp::eq(left, right),
        ComparisonOp::NotEq => cmp::neq(left, right),
        ComparisonOp::Lt => cmp::lt(left, right),
        ComparisonOp::LtEq => cmp::lt_eq(left, right),
        ComparisonOp::Gt => cmp::gt(left, right),
        ComparisonOp::GtEq => cmp::gt_eq(left, right),
    }
}

/// Extra decoder credit held before polling a reader with late materialization.
pub(super) fn workspace_bytes(
    predicate: Option<&ScanPredicate>,
    table_schema: &Schema,
    rows: usize,
) -> usize {
    let Some(predicate) = predicate else {
        return 0;
    };
    let mut leaves = Vec::new();
    collect_workspace_leaves(predicate, table_schema, &mut leaves);
    group_by_file_column(leaves)
        .into_iter()
        .map(|column| {
            estimate_array_bytes(table_schema.field(column.file_column).data_type(), rows)
                .saturating_add(
                    column
                        .mask_buffers()
                        .saturating_mul(estimate_array_bytes(&DataType::Boolean, rows)),
                )
        })
        .fold(0usize, usize::saturating_add)
}

fn collect_workspace_leaves(
    predicate: &ScanPredicate,
    schema: &Schema,
    output: &mut Vec<LeafPredicate>,
) {
    match predicate {
        ScanPredicate::And(predicates) => {
            for predicate in predicates {
                collect_workspace_leaves(predicate, schema, output);
            }
        }
        ScanPredicate::Comparison { column, op, value }
            if schema
                .fields()
                .get(*column)
                .is_some_and(|field| comparison_type_matches(field.data_type(), value)) =>
        {
            output.push(LeafPredicate::Comparison {
                file_column: *column,
                op: *op,
                value: value.clone(),
            });
        }
        ScanPredicate::IsNull { column } | ScanPredicate::IsNotNull { column }
            if schema
                .fields()
                .get(*column)
                .is_some_and(|field| fixed_filter_type(field.data_type())) =>
        {
            output.push(LeafPredicate::IsNull {
                file_column: *column,
                negated: matches!(predicate, ScanPredicate::IsNotNull { .. }),
            });
        }
        _ => {}
    }
}

#[cfg(test)]
#[path = "parquet_row_filter_tests.rs"]
mod tests;
