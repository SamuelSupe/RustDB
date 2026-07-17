use std::{collections::BTreeMap, sync::Arc};

use arrow::datatypes::{Schema, SchemaRef};

use crate::{Error, Result, datasource::ScanPredicate};

pub(super) fn remap_projection(
    source: &Schema,
    target: &Schema,
    indices: &[usize],
) -> Result<Vec<usize>> {
    indices
        .iter()
        .filter_map(|index| {
            let field = match source.fields().get(*index) {
                Some(field) => field,
                None => {
                    return Some(Err(Error::Internal(format!(
                        "registered scan projection index {index} is out of bounds"
                    ))));
                }
            };
            target.index_of(field.name()).ok().map(Ok)
        })
        .collect()
}

pub(super) fn remap_predicate(
    predicate: &ScanPredicate,
    source: &Schema,
    target: &Schema,
) -> Option<ScanPredicate> {
    let column = |index: usize| {
        source
            .fields()
            .get(index)
            .and_then(|field| target.index_of(field.name()).ok())
    };
    match predicate {
        ScanPredicate::Comparison {
            column: index,
            op,
            value,
        } => Some(ScanPredicate::Comparison {
            column: column(*index)?,
            op: *op,
            value: value.clone(),
        }),
        ScanPredicate::IsNull { column: index } => Some(ScanPredicate::IsNull {
            column: column(*index)?,
        }),
        ScanPredicate::IsNotNull { column: index } => Some(ScanPredicate::IsNotNull {
            column: column(*index)?,
        }),
        ScanPredicate::And(predicates) => Some(ScanPredicate::And(
            predicates
                .iter()
                .map(|predicate| remap_predicate(predicate, source, target))
                .collect::<Option<Vec<_>>>()?,
        )),
        ScanPredicate::Or(predicates) => Some(ScanPredicate::Or(
            predicates
                .iter()
                .map(|predicate| remap_predicate(predicate, source, target))
                .collect::<Option<Vec<_>>>()?,
        )),
    }
}

pub(super) fn remap_exact_predicate(
    predicate: &ScanPredicate,
    source: &Schema,
    target: &Schema,
) -> Result<ScanPredicate> {
    remap_predicate(predicate, source, target).ok_or_else(|| {
        let column = missing_column(predicate, source, target)
            .and_then(|index| source.fields().get(index))
            .map_or_else(|| "<unknown>".to_owned(), |field| field.name().clone());
        Error::Execution(format!(
            "exact Parquet predicate column '{column}' is absent from the query snapshot"
        ))
    })
}

fn missing_column(predicate: &ScanPredicate, source: &Schema, target: &Schema) -> Option<usize> {
    match predicate {
        ScanPredicate::Comparison { column, .. }
        | ScanPredicate::IsNull { column }
        | ScanPredicate::IsNotNull { column } => source
            .fields()
            .get(*column)
            .filter(|field| target.index_of(field.name()).is_err())
            .map(|_| *column),
        ScanPredicate::And(predicates) | ScanPredicate::Or(predicates) => predicates
            .iter()
            .find_map(|predicate| missing_column(predicate, source, target)),
    }
}

pub(super) fn reorder_schema(current: &SchemaRef, previous: Option<&Schema>) -> SchemaRef {
    let Some(previous) = previous else {
        return Arc::clone(current);
    };
    let by_name = current
        .fields()
        .iter()
        .map(|field| (field.name().clone(), Arc::clone(field)))
        .collect::<BTreeMap<_, _>>();
    let mut fields = Vec::with_capacity(by_name.len());
    for field in previous.fields() {
        if let Some(current) = by_name.get(field.name()) {
            fields.push(Arc::clone(current));
        }
    }
    for (name, field) in by_name {
        if fields.iter().all(|existing| existing.name() != &name) {
            fields.push(field);
        }
    }
    Arc::new(Schema::new_with_metadata(
        fields,
        current.metadata().clone(),
    ))
}

pub(super) fn nullable_schema(schema: &Schema) -> SchemaRef {
    Arc::new(Schema::new_with_metadata(
        schema
            .fields()
            .iter()
            .map(|field| Arc::new(field.as_ref().clone().with_nullable(true)))
            .collect::<Vec<_>>(),
        schema.metadata().clone(),
    ))
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::{DataType, Field, Schema};

    use super::{remap_exact_predicate, reorder_schema};
    use crate::{Error, datasource::ScanPredicate};

    fn schema(names: &[&str]) -> std::sync::Arc<Schema> {
        std::sync::Arc::new(Schema::new(
            names
                .iter()
                .map(|name| Field::new(*name, DataType::Int64, true))
                .collect::<Vec<_>>(),
        ))
    }

    fn names(schema: &Schema) -> Vec<String> {
        schema
            .fields()
            .iter()
            .map(|field| field.name().clone())
            .collect()
    }

    #[test]
    fn initial_order_is_the_merged_first_file_order() {
        let current = schema(&["b", "a"]);
        assert_eq!(names(&reorder_schema(&current, None)), ["b", "a"]);
    }

    #[test]
    fn refresh_keeps_survivors_and_sorts_new_columns() {
        let current = schema(&["b", "d", "a"]);
        let previous = schema(&["c", "b"]);
        assert_eq!(
            names(&reorder_schema(&current, Some(&previous))),
            ["b", "a", "d"]
        );
    }

    #[test]
    fn exact_remap_reports_a_missing_snapshot_column() {
        let registered = schema(&["id", "payload"]);
        let snapshot = schema(&["id"]);
        let error =
            remap_exact_predicate(&ScanPredicate::IsNull { column: 1 }, &registered, &snapshot)
                .unwrap_err();
        assert!(matches!(error, Error::Execution(message) if message.contains("payload")));
    }
}
