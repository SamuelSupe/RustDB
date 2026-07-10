use std::cmp::Ordering;

use arrow::datatypes::{DataType, Schema};
use parquet::{
    file::{metadata::ParquetMetaData, statistics::Statistics},
    schema::types::SchemaDescriptor,
};

use super::{ComparisonOp, PredicateValue, ScanPredicate};

pub(super) fn row_groups_for_predicate(
    metadata: &ParquetMetaData,
    parquet_schema: &SchemaDescriptor,
    file_schema: &Schema,
    table_schema: &Schema,
    predicate: Option<&ScanPredicate>,
) -> (Vec<usize>, u64) {
    let Some(predicate) = predicate else {
        return ((0..metadata.num_row_groups()).collect(), 0);
    };

    let mut selected = Vec::with_capacity(metadata.num_row_groups());
    let mut pruned = 0_u64;
    for index in 0..metadata.num_row_groups() {
        if can_prune(
            metadata,
            parquet_schema,
            file_schema,
            table_schema,
            index,
            predicate,
        ) {
            pruned = pruned.saturating_add(1);
        } else {
            selected.push(index);
        }
    }
    (selected, pruned)
}

fn can_prune(
    metadata: &ParquetMetaData,
    parquet_schema: &SchemaDescriptor,
    file_schema: &Schema,
    table_schema: &Schema,
    row_group: usize,
    predicate: &ScanPredicate,
) -> bool {
    match predicate {
        ScanPredicate::And(predicates) => predicates.iter().any(|predicate| {
            can_prune(
                metadata,
                parquet_schema,
                file_schema,
                table_schema,
                row_group,
                predicate,
            )
        }),
        ScanPredicate::Comparison { column, op, value } => column_statistics(
            metadata,
            parquet_schema,
            file_schema,
            table_schema,
            row_group,
            *column,
        )
        .is_some_and(|(statistics, data_type, _)| {
            comparison_excludes_all(statistics, data_type, *op, value)
        }),
        ScanPredicate::IsNull { column } => column_statistics(
            metadata,
            parquet_schema,
            file_schema,
            table_schema,
            row_group,
            *column,
        )
        .is_some_and(|(statistics, _, _)| statistics.null_count_opt() == Some(0)),
        ScanPredicate::IsNotNull { column } => column_statistics(
            metadata,
            parquet_schema,
            file_schema,
            table_schema,
            row_group,
            *column,
        )
        .is_some_and(|(statistics, _, rows)| statistics.null_count_opt() == Some(rows)),
    }
}

fn column_statistics<'a>(
    metadata: &'a ParquetMetaData,
    parquet_schema: &SchemaDescriptor,
    file_schema: &'a Schema,
    table_schema: &'a Schema,
    row_group: usize,
    table_column: usize,
) -> Option<(&'a Statistics, &'a DataType, u64)> {
    let table_field = table_schema.fields().get(table_column)?;
    let file_column = file_schema.index_of(table_field.name()).ok()?;
    let leaves: Vec<_> = (0..parquet_schema.num_columns())
        .filter(|leaf| parquet_schema.get_column_root_idx(*leaf) == file_column)
        .collect();
    let [leaf] = leaves.as_slice() else {
        return None;
    };
    let group = metadata.row_group(row_group);
    let rows = u64::try_from(group.num_rows()).ok()?;
    let statistics = group.column(*leaf).statistics()?;
    Some((statistics, file_schema.field(file_column).data_type(), rows))
}

fn comparison_excludes_all(
    statistics: &Statistics,
    data_type: &DataType,
    op: ComparisonOp,
    value: &PredicateValue,
) -> bool {
    match (statistics, data_type, value) {
        (Statistics::Boolean(stats), DataType::Boolean, PredicateValue::Boolean(value)) => {
            excludes(stats.min_opt(), stats.max_opt(), op, value)
        }
        (Statistics::Int32(stats), DataType::Int32, PredicateValue::Int64(value)) => {
            i32::try_from(*value)
                .ok()
                .is_some_and(|value| excludes(stats.min_opt(), stats.max_opt(), op, &value))
        }
        (Statistics::Int32(stats), DataType::Date32, PredicateValue::Date32(value)) => {
            excludes(stats.min_opt(), stats.max_opt(), op, value)
        }
        (Statistics::Int64(stats), DataType::Int64, PredicateValue::Int64(value)) => {
            excludes(stats.min_opt(), stats.max_opt(), op, value)
        }
        (Statistics::Double(stats), DataType::Float64, PredicateValue::Float64(value)) => {
            value.is_finite() && excludes(stats.min_opt(), stats.max_opt(), op, value)
        }
        (Statistics::ByteArray(stats), DataType::Utf8, PredicateValue::Utf8(value)) => {
            stats.min_is_exact()
                && stats.max_is_exact()
                && excludes_bytes(
                    stats.min_opt().map(|value| value.data()),
                    stats.max_opt().map(|value| value.data()),
                    op,
                    value.as_bytes(),
                )
        }
        _ => false,
    }
}

fn excludes<T: PartialOrd + PartialEq>(
    min: Option<&T>,
    max: Option<&T>,
    op: ComparisonOp,
    value: &T,
) -> bool {
    let (Some(min), Some(max)) = (min, max) else {
        return false;
    };
    excludes_ordering(
        min.partial_cmp(value),
        max.partial_cmp(value),
        min == max,
        op,
    )
}

fn excludes_bytes(min: Option<&[u8]>, max: Option<&[u8]>, op: ComparisonOp, value: &[u8]) -> bool {
    let (Some(min), Some(max)) = (min, max) else {
        return false;
    };
    excludes_ordering(Some(min.cmp(value)), Some(max.cmp(value)), min == max, op)
}

fn excludes_ordering(
    min: Option<Ordering>,
    max: Option<Ordering>,
    min_equals_max: bool,
    op: ComparisonOp,
) -> bool {
    match op {
        ComparisonOp::Eq => {
            matches!(min, Some(Ordering::Greater)) || matches!(max, Some(Ordering::Less))
        }
        ComparisonOp::NotEq => min_equals_max && matches!(min, Some(Ordering::Equal)),
        ComparisonOp::Lt => matches!(min, Some(Ordering::Equal | Ordering::Greater)),
        ComparisonOp::LtEq => matches!(min, Some(Ordering::Greater)),
        ComparisonOp::Gt => matches!(max, Some(Ordering::Equal | Ordering::Less)),
        ComparisonOp::GtEq => matches!(max, Some(Ordering::Less)),
    }
}

#[cfg(test)]
mod tests {
    use parquet::file::statistics::ValueStatistics;

    use super::{ComparisonOp, excludes};

    #[test]
    fn min_max_exclusion_is_conservative() {
        let stats = ValueStatistics::new(Some(10_i64), Some(20_i64), None, Some(0), false);
        assert!(excludes(
            stats.min_opt(),
            stats.max_opt(),
            ComparisonOp::Eq,
            &5
        ));
        assert!(!excludes(
            stats.min_opt(),
            stats.max_opt(),
            ComparisonOp::Eq,
            &15
        ));
        assert!(excludes(
            stats.min_opt(),
            stats.max_opt(),
            ComparisonOp::Gt,
            &20
        ));
        assert!(!excludes(
            stats.min_opt(),
            stats.max_opt(),
            ComparisonOp::GtEq,
            &20
        ));
    }
}
