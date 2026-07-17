use arrow::datatypes::{DataType, Schema, TimeUnit};

use super::{PredicateValue, ScanPredicate};

/// Validates the complete physical contract used by Exact v1. Column indexes
/// are table-schema indexes; every referenced field must exist physically by
/// name with an identical, supported Arrow type.
pub(super) fn supported(
    predicate: &ScanPredicate,
    table_schema: &Schema,
    physical_schema: &Schema,
) -> bool {
    match predicate {
        ScanPredicate::Comparison { column, value, .. } => {
            column_type(*column, table_schema, physical_schema)
                .is_some_and(|data_type| value_matches(data_type, value))
        }
        ScanPredicate::IsNull { column } | ScanPredicate::IsNotNull { column } => {
            column_type(*column, table_schema, physical_schema).is_some_and(exact_type)
        }
        ScanPredicate::And(predicates) => {
            !predicates.is_empty()
                && predicates
                    .iter()
                    .all(|predicate| supported(predicate, table_schema, physical_schema))
        }
        ScanPredicate::Or(_) => false,
    }
}

fn column_type<'a>(
    table_column: usize,
    table_schema: &Schema,
    physical_schema: &'a Schema,
) -> Option<&'a DataType> {
    let table_field = table_schema.fields().get(table_column)?;
    let physical_field = physical_schema.field_with_name(table_field.name()).ok()?;
    (physical_field.data_type() == table_field.data_type()).then_some(physical_field.data_type())
}

fn value_matches(data_type: &DataType, value: &PredicateValue) -> bool {
    matches!(
        (data_type, value),
        (DataType::Boolean, PredicateValue::Boolean(_))
            | (DataType::Int64, PredicateValue::Int64(_))
            | (DataType::UInt64, PredicateValue::UInt64(_))
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

fn exact_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Boolean
            | DataType::Int64
            | DataType::UInt64
            | DataType::Date32
            | DataType::Timestamp(TimeUnit::Microsecond, None)
            | DataType::Decimal128(_, _)
    )
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::{DataType, Field, Schema};

    use super::supported;
    use crate::datasource::{ComparisonOp, PredicateValue, ScanPredicate};

    #[test]
    fn rejects_float_and_missing_physical_columns() {
        let table = Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("score", DataType::Float64, true),
        ]);
        let physical = Schema::new(vec![Field::new("id", DataType::Int64, true)]);
        let float = ScanPredicate::Comparison {
            column: 1,
            op: ComparisonOp::Gt,
            value: PredicateValue::Float64(1.0),
        };
        let missing = ScanPredicate::IsNull { column: 1 };
        assert!(!supported(&float, &table, &table));
        assert!(!supported(&missing, &table, &physical));
    }
}
