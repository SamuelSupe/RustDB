use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;

use crate::{Error, Result};

#[path = "schema_evolution_alignment.rs"]
mod alignment;
#[path = "schema_evolution_types.rs"]
mod types;

pub(crate) use types::{canonical_type, merge_types};

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum ParquetSchemaMode {
    #[default]
    Strict,
    UnionByName,
    SafeWidening,
}

pub(crate) struct SchemaSource<'a> {
    pub(crate) uri: &'a str,
    pub(crate) schema: &'a Schema,
}

pub(crate) fn align_batch_to_schema(
    batch: RecordBatch,
    target: SchemaRef,
    uri: &str,
) -> Result<RecordBatch> {
    alignment::align_batch_to_schema(batch, target, uri)
}

pub(crate) fn align_batch_to_schema_preserving_dictionaries(
    batch: RecordBatch,
    target: SchemaRef,
    uri: &str,
    dictionary_columns: &[usize],
) -> Result<RecordBatch> {
    alignment::align_batch_to_schema_preserving_dictionaries(batch, target, uri, dictionary_columns)
}

pub(crate) fn canonicalize_schema(schema: SchemaRef) -> SchemaRef {
    if schema
        .fields()
        .iter()
        .all(|field| canonical_type(field.data_type()) == *field.data_type())
    {
        return schema;
    }

    Arc::new(Schema::new_with_metadata(
        schema
            .fields()
            .iter()
            .map(|field| {
                Arc::new(
                    Field::new(
                        field.name(),
                        canonical_type(field.data_type()),
                        field.is_nullable(),
                    )
                    .with_metadata(field.metadata().clone()),
                )
            })
            .collect::<Vec<_>>(),
        schema.metadata().clone(),
    ))
}

pub(crate) fn merge_file_schemas(
    sources: &[SchemaSource<'_>],
    mode: ParquetSchemaMode,
    previous: Option<&Schema>,
) -> Result<SchemaRef> {
    if sources.is_empty() {
        return Err(Error::InvalidArgument(
            "cannot merge an empty Parquet schema set".to_owned(),
        ));
    }

    let mut sources: Vec<_> = sources.iter().collect();
    sources.sort_by(|left, right| left.uri.cmp(right.uri));
    validate_sources(&sources)?;

    let mut fields_by_name: BTreeMap<&str, Vec<(&str, &Field)>> = BTreeMap::new();
    for source in &sources {
        for field in source.schema.fields() {
            fields_by_name
                .entry(field.name())
                .or_default()
                .push((source.uri, field));
        }
    }

    if mode != ParquetSchemaMode::UnionByName {
        validate_complete_columns(&sources, &fields_by_name, mode)?;
    }

    let order = field_order(&sources, previous, fields_by_name.keys().copied());
    let mut fields = Vec::with_capacity(order.len());
    for name in order {
        let occurrences = fields_by_name
            .get(name.as_str())
            .ok_or_else(|| Error::Internal(format!("missing merged field {name}")))?;
        let missing = occurrences.len() != sources.len();
        let (first_uri, first_field) = occurrences[0];
        let mut data_type = canonical_type(first_field.data_type());
        for (uri, field) in occurrences.iter().skip(1) {
            let incoming = canonical_type(field.data_type());
            data_type = merge_types(&data_type, &incoming, mode).map_err(|reason| {
                schema_conflict(
                    name.as_str(),
                    first_uri,
                    &data_type,
                    uri,
                    &incoming,
                    &reason,
                )
            })?;
        }

        let nullable = missing
            || occurrences
                .iter()
                .any(|(_, field)| field.is_nullable() || field.data_type() == &DataType::Null);
        let template = previous
            .and_then(|schema| schema.field_with_name(name.as_str()).ok())
            .unwrap_or(first_field);
        fields.push(Arc::new(
            Field::new(name, data_type, nullable).with_metadata(template.metadata().clone()),
        ));
    }

    let metadata = previous
        .map(|schema| schema.metadata().clone())
        .unwrap_or_else(|| sources[0].schema.metadata().clone());
    Ok(Arc::new(Schema::new_with_metadata(fields, metadata)))
}

fn validate_sources(sources: &[&SchemaSource<'_>]) -> Result<()> {
    let mut uris = BTreeSet::new();
    for source in sources {
        if !uris.insert(source.uri) {
            return Err(Error::InvalidArgument(format!(
                "duplicate Parquet schema URI '{}'",
                source.uri
            )));
        }
        let mut names = BTreeSet::new();
        for field in source.schema.fields() {
            if !names.insert(field.name()) {
                return Err(Error::InvalidArgument(format!(
                    "Parquet schema for URI '{}' contains duplicate column '{}'",
                    source.uri,
                    field.name()
                )));
            }
        }
    }
    Ok(())
}

fn validate_complete_columns(
    sources: &[&SchemaSource<'_>],
    fields: &BTreeMap<&str, Vec<(&str, &Field)>>,
    mode: ParquetSchemaMode,
) -> Result<()> {
    for (name, occurrences) in fields {
        if occurrences.len() == sources.len() {
            continue;
        }
        let present_uri = occurrences[0].0;
        let missing_uri = sources
            .iter()
            .find(|source| source.schema.field_with_name(name).is_err())
            .map(|source| source.uri)
            .unwrap_or("<unknown>");
        return Err(Error::InvalidArgument(format!(
            "Parquet schema conflict for column '{name}': URI '{missing_uri}' is missing a column present in URI '{present_uri}' under {mode:?} mode; only UnionByName fills missing columns with NULL"
        )));
    }
    Ok(())
}

fn field_order<'a>(
    sources: &[&'a SchemaSource<'a>],
    previous: Option<&Schema>,
    all_names: impl Iterator<Item = &'a str>,
) -> Vec<String> {
    let all: BTreeSet<_> = all_names.collect();
    let mut order = Vec::with_capacity(all.len());
    let mut seen = BTreeSet::new();
    let preferred = previous.unwrap_or(sources[0].schema);
    for field in preferred.fields() {
        if all.contains(field.name().as_str()) && seen.insert(field.name().as_str()) {
            order.push(field.name().clone());
        }
    }
    for name in all {
        if seen.insert(name) {
            order.push(name.to_owned());
        }
    }
    order
}

fn schema_conflict(
    column: &str,
    left_uri: &str,
    left: &DataType,
    right_uri: &str,
    right: &DataType,
    reason: &str,
) -> Error {
    Error::InvalidArgument(format!(
        "Parquet schema conflict for column '{column}' between URI '{left_uri}' ({left:?}) and URI '{right_uri}' ({right:?}): {reason}"
    ))
}

#[cfg(test)]
#[path = "schema_evolution_alignment_tests.rs"]
mod alignment_tests;
#[cfg(test)]
#[path = "schema_evolution_tests.rs"]
mod tests;
