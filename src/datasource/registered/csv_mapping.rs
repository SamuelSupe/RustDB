use std::{collections::BTreeMap, sync::Arc};

use arrow::datatypes::{Schema, SchemaRef};

use crate::{Error, Result, datasource::ScanPredicate};

pub(super) fn reorder_schema(current: &SchemaRef, previous: Option<&Schema>) -> Result<SchemaRef> {
    let Some(previous) = previous else {
        return Ok(Arc::clone(current));
    };

    let mut by_name = BTreeMap::new();
    for field in current.fields() {
        if by_name
            .insert(field.name().clone(), Arc::clone(field))
            .is_some()
        {
            return Err(Error::InvalidArgument(format!(
                "CSV refresh schema contains duplicate column '{}'",
                field.name()
            )));
        }
    }

    let mut fields = Vec::with_capacity(by_name.len());
    for field in previous.fields() {
        if let Some(current) = by_name.remove(field.name()) {
            fields.push(current);
        }
    }
    fields.extend(by_name.into_values());
    Ok(Arc::new(Schema::new_with_metadata(
        fields,
        current.metadata().clone(),
    )))
}

pub(super) fn remap_projection(
    logical: &Schema,
    physical: &Schema,
    indices: &[usize],
) -> Result<Vec<usize>> {
    indices
        .iter()
        .map(|index| {
            let field = logical.fields().get(*index).ok_or_else(|| {
                Error::Internal(format!(
                    "registered CSV projection index {index} is out of bounds"
                ))
            })?;
            physical_index(physical, field.name(), field.data_type())
        })
        .collect()
}

pub(super) fn remap_predicate(
    predicate: &ScanPredicate,
    logical: &Schema,
    physical: &Schema,
) -> Result<ScanPredicate> {
    let column = |index: usize| {
        let field = logical.fields().get(index).ok_or_else(|| {
            Error::Internal(format!(
                "registered CSV predicate index {index} is out of bounds"
            ))
        })?;
        physical_index(physical, field.name(), field.data_type())
    };
    match predicate {
        ScanPredicate::Comparison {
            column: index,
            op,
            value,
        } => Ok(ScanPredicate::Comparison {
            column: column(*index)?,
            op: *op,
            value: value.clone(),
        }),
        ScanPredicate::IsNull { column: index } => Ok(ScanPredicate::IsNull {
            column: column(*index)?,
        }),
        ScanPredicate::IsNotNull { column: index } => Ok(ScanPredicate::IsNotNull {
            column: column(*index)?,
        }),
        ScanPredicate::And(predicates) => Ok(ScanPredicate::And(
            predicates
                .iter()
                .map(|predicate| remap_predicate(predicate, logical, physical))
                .collect::<Result<Vec<_>>>()?,
        )),
        ScanPredicate::Or(predicates) => Ok(ScanPredicate::Or(
            predicates
                .iter()
                .map(|predicate| remap_predicate(predicate, logical, physical))
                .collect::<Result<Vec<_>>>()?,
        )),
    }
}

fn physical_index(
    physical: &Schema,
    name: &str,
    expected: &arrow::datatypes::DataType,
) -> Result<usize> {
    let index = physical.index_of(name).map_err(|_| {
        Error::Execution(format!(
            "registered CSV physical schema is missing column '{name}'"
        ))
    })?;
    let actual = physical.field(index).data_type();
    if actual != expected {
        return Err(Error::Execution(format!(
            "registered CSV column '{name}' changed type from {expected:?} to {actual:?}; run REFRESH TABLE"
        )));
    }
    Ok(index)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::datatypes::{DataType, Field, Schema};

    use super::{remap_projection, reorder_schema};

    #[test]
    fn refresh_keeps_survivors_and_sorts_new_columns() {
        let previous = Schema::new(vec![
            Field::new("b", DataType::Int64, false),
            Field::new("a", DataType::Utf8, false),
            Field::new("removed", DataType::Boolean, false),
        ]);
        let current = Arc::new(Schema::new(vec![
            Field::new("z", DataType::Utf8, false),
            Field::new("a", DataType::LargeUtf8, true),
            Field::new("b", DataType::Int64, false),
            Field::new("c", DataType::Boolean, false),
        ]));

        let refreshed = reorder_schema(&current, Some(&previous)).unwrap();
        let names = refreshed
            .fields()
            .iter()
            .map(|field| field.name().as_str())
            .collect::<Vec<_>>();
        assert_eq!(names, ["b", "a", "c", "z"]);
        assert_eq!(refreshed.field(1).data_type(), &DataType::LargeUtf8);
        assert!(refreshed.field(1).is_nullable());
        assert_eq!(
            remap_projection(&refreshed, &current, &[0, 1, 2, 3]).unwrap(),
            [2, 1, 3, 0]
        );
    }
}
