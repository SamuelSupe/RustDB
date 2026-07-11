use std::{collections::BTreeSet, sync::Arc};

use arrow::{
    array::{ArrayRef, new_null_array},
    compute::{CastOptions, cast_with_options},
    datatypes::{DataType, SchemaRef},
    record_batch::{RecordBatch, RecordBatchOptions},
};

use super::{ParquetSchemaMode, canonical_type, merge_types};
use crate::{Error, Result};

pub(crate) fn align_batch_to_schema(
    batch: RecordBatch,
    target: SchemaRef,
    uri: &str,
) -> Result<RecordBatch> {
    let source_schema = batch.schema();
    let mut target_names = BTreeSet::new();
    for field in target.fields() {
        if !target_names.insert(field.name()) {
            return Err(alignment_error(
                uri,
                field.name(),
                "target schema contains a duplicate column",
            ));
        }
    }

    let mut columns: Vec<ArrayRef> = Vec::with_capacity(target.fields().len());
    for field in target.fields() {
        let mut matches = source_schema
            .fields()
            .iter()
            .enumerate()
            .filter(|(_, source)| source.name() == field.name());
        let matched = matches.next();
        if matches.next().is_some() {
            return Err(alignment_error(
                uri,
                field.name(),
                "source schema contains a duplicate column",
            ));
        }
        let column = match matched {
            Some((index, _)) => {
                let source = batch.column(index);
                if !field.is_nullable() && source.null_count() != 0 {
                    return Err(alignment_error(
                        uri,
                        field.name(),
                        "source contains NULL values for a non-nullable target",
                    ));
                }
                checked_cast(source, field.data_type(), uri, field.name())?
            }
            None if field.is_nullable() => new_null_array(field.data_type(), batch.num_rows()),
            None => {
                return Err(alignment_error(
                    uri,
                    field.name(),
                    "required non-nullable column is missing",
                ));
            }
        };
        columns.push(column);
    }

    let options = RecordBatchOptions::new().with_row_count(Some(batch.num_rows()));
    RecordBatch::try_new_with_options(target, columns, &options).map_err(Error::from)
}

fn checked_cast(array: &ArrayRef, target: &DataType, uri: &str, column: &str) -> Result<ArrayRef> {
    let source = canonical_type(array.data_type());
    if &source == target && array.data_type() == target {
        return Ok(Arc::clone(array));
    }
    if canonical_type(target) != *target {
        return Err(alignment_error(
            uri,
            column,
            "dictionary-encoded target schemas are not canonical",
        ));
    }
    let merged =
        merge_types(&source, target, ParquetSchemaMode::SafeWidening).map_err(|reason| {
            alignment_error(
                uri,
                column,
                &format!("cannot safely align {source:?} to {target:?}: {reason}"),
            )
        })?;
    if &merged != target {
        return Err(alignment_error(
            uri,
            column,
            &format!("conversion from {source:?} to {target:?} is not lossless"),
        ));
    }

    let options = CastOptions {
        safe: true,
        ..CastOptions::default()
    };
    let casted = cast_with_options(array.as_ref(), target, &options).map_err(|error| {
        alignment_error(
            uri,
            column,
            &format!("checked cast from {source:?} to {target:?} failed: {error}"),
        )
    })?;
    if casted.null_count() > array.null_count() {
        return Err(alignment_error(
            uri,
            column,
            &format!(
                "checked cast from {source:?} to {target:?} failed because a value overflowed"
            ),
        ));
    }
    Ok(casted)
}

fn alignment_error(uri: &str, column: &str, reason: &str) -> Error {
    Error::Execution(format!(
        "Parquet schema alignment failed for URI '{uri}', column '{column}': {reason}"
    ))
}
