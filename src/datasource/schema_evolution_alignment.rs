use std::{collections::BTreeSet, sync::Arc};

use arrow::{
    array::{ArrayRef, new_null_array},
    compute::{CastOptions, cast_with_options},
    datatypes::{DataType, SchemaRef},
    record_batch::{RecordBatch, RecordBatchOptions},
};

use super::{ParquetSchemaMode, canonical_type, canonicalize_schema, merge_types};
use crate::{Error, Result};

pub(crate) fn align_batch_to_schema(
    batch: RecordBatch,
    target: SchemaRef,
    uri: &str,
) -> Result<RecordBatch> {
    align_batch_to_schema_preserving_dictionaries(batch, target, uri, &[])
}

pub(crate) fn align_batch_to_schema_preserving_dictionaries(
    batch: RecordBatch,
    target: SchemaRef,
    uri: &str,
    dictionary_columns: &[usize],
) -> Result<RecordBatch> {
    let target = canonicalize_schema(target);
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
    let mut output_fields = target.fields().to_vec();
    let mut preserved = false;
    for (target_index, field) in target.fields().iter().enumerate() {
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
                if dictionary_columns.contains(&target_index)
                    && preservable_dictionary(source.data_type(), field.data_type())
                {
                    output_fields[target_index] = Arc::new(
                        field
                            .as_ref()
                            .clone()
                            .with_data_type(source.data_type().clone()),
                    );
                    preserved = true;
                    Arc::clone(source)
                } else {
                    checked_cast(source, field.data_type(), uri, field.name())?
                }
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

    let output_schema = if preserved {
        Arc::new(arrow::datatypes::Schema::new_with_metadata(
            output_fields,
            target.metadata().clone(),
        ))
    } else {
        target
    };
    let options = RecordBatchOptions::new().with_row_count(Some(batch.num_rows()));
    RecordBatch::try_new_with_options(output_schema, columns, &options).map_err(Error::from)
}

fn preservable_dictionary(source: &DataType, target: &DataType) -> bool {
    matches!(source, DataType::Dictionary(_, value) if value.as_ref() == target)
        && matches!(
            target,
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Binary | DataType::LargeBinary
        )
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
    if !internal_decimal_widening(&source, target) {
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

fn internal_decimal_widening(source: &DataType, target: &DataType) -> bool {
    match (source, target) {
        (
            DataType::Decimal32(source_precision, source_scale),
            DataType::Decimal128(target_precision, target_scale),
        )
        | (
            DataType::Decimal64(source_precision, source_scale),
            DataType::Decimal128(target_precision, target_scale),
        ) => source_scale == target_scale && source_precision <= target_precision,
        _ => false,
    }
}

fn alignment_error(uri: &str, column: &str, reason: &str) -> Error {
    Error::Execution(format!(
        "Parquet schema alignment failed for URI '{uri}', column '{column}': {reason}"
    ))
}
