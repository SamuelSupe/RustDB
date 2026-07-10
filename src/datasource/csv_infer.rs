use std::{collections::HashSet, io::Cursor, sync::Arc};

use arrow::{
    csv::reader::Format,
    datatypes::{DataType, Schema, SchemaRef},
};
use object_store::GetRange;

use crate::{CsvHeader, CsvOptions, Error, Result, runtime::QueryContext, storage::ObjectSource};

const INITIAL_SAMPLE_BYTES: u64 = 1024 * 1024;

pub(super) async fn infer_table_schema(
    files: &[ObjectSource],
    options: &CsvOptions,
    context: Option<&QueryContext>,
) -> Result<(SchemaRef, bool)> {
    let first = files
        .first()
        .ok_or_else(|| Error::InvalidArgument("CSV table requires at least one file".to_owned()))?;
    let first_sample = read_sample(first, options, context).await?;
    let has_header = match options.header {
        CsvHeader::Present => true,
        CsvHeader::Absent => false,
        CsvHeader::Auto => detect_header(&first_sample, options)
            .map_err(|error| csv_schema_error(first.uri(), error))?,
    };

    if let Some(schema) = &options.schema {
        if has_header {
            validate_header_names(&first_sample, schema, options, first.uri())?;
            for file in files.iter().skip(1) {
                let sample = read_sample(file, options, context).await?;
                validate_header_names(&sample, schema, options, file.uri())?;
            }
        }
        return Ok((Arc::clone(schema), has_header));
    }
    if options.sample_size == 0 {
        return Err(Error::InvalidArgument(
            "CSV sample_size must be greater than zero when schema is inferred".to_owned(),
        ));
    }

    let schema = infer(&first_sample, options, has_header)
        .map_err(|error| csv_schema_error(first.uri(), error))?;
    if schema.fields().is_empty() {
        return Err(Error::InvalidArgument(format!(
            "CSV file has no columns: {}",
            first.uri()
        )));
    }
    for file in files.iter().skip(1) {
        let sample = read_sample(file, options, context).await?;
        let actual = infer(&sample, options, has_header)
            .map_err(|error| csv_schema_error(file.uri(), error))?;
        validate_schema(&actual, &schema, file.uri())?;
    }
    Ok((Arc::new(schema), has_header))
}

pub(super) fn format(options: &CsvOptions, has_header: bool) -> Format {
    let mut format = Format::default()
        .with_header(has_header)
        .with_header_validation(has_header)
        .with_delimiter(options.delimiter)
        .with_quote(options.quote)
        .with_truncated_rows(false);
    if let Some(escape) = options.escape {
        format = format.with_escape(escape);
    }
    format
}

async fn read_sample(
    file: &ObjectSource,
    options: &CsvOptions,
    context: Option<&QueryContext>,
) -> Result<Vec<u8>> {
    if file.snapshot().size == 0 {
        return Ok(Vec::new());
    }

    let mut end = file.snapshot().size.min(INITIAL_SAMPLE_BYTES);
    loop {
        if let Some(context) = context {
            context.check_cancelled()?;
            if file.is_s3() {
                context.metrics.add_s3_requests(1);
            }
        }
        let mut get_options = file.get_options_for(file.snapshot());
        get_options.range = Some(GetRange::Bounded(0..end));
        let request = async {
            file.store()
                .get_opts(file.location(), get_options)
                .await?
                .bytes()
                .await
        };
        let bytes = match context {
            Some(context) => tokio::select! {
                _ = context.control.cancelled() => Err(Error::Cancelled),
                result = request => result.map_err(Error::from),
            },
            None => request.await.map_err(Error::from),
        }
        .map_err(|error| csv_sample_error(file.uri(), error))?;
        if file.is_s3()
            && let Some(context) = context
        {
            context
                .metrics
                .add_s3_bytes_transferred(u64::try_from(bytes.len()).unwrap_or(u64::MAX));
        }
        let (complete_end, records) = complete_prefix(
            &bytes,
            options.quote,
            options.escape,
            end == file.snapshot().size,
        );
        if records >= 2 || end == file.snapshot().size {
            return Ok(bytes[..complete_end].to_vec());
        }
        end = end.saturating_mul(2).min(file.snapshot().size);
    }
}

fn infer(sample: &[u8], options: &CsvOptions, has_header: bool) -> Result<Schema> {
    let (schema, _) = format(options, has_header)
        .infer_schema(Cursor::new(sample), Some(options.sample_size.max(1)))?;
    Ok(schema)
}

fn detect_header(sample: &[u8], options: &CsvOptions) -> Result<bool> {
    let without = infer(sample, options, false)?;
    let with = infer(sample, options, true)?;
    if without.fields().len() != with.fields().len() || with.fields().is_empty() {
        return Ok(false);
    }

    let type_signal = without
        .fields()
        .iter()
        .zip(with.fields())
        .any(|(all_rows, data_rows)| {
            all_rows.data_type() == &DataType::Utf8 && data_rows.data_type() != &DataType::Utf8
        });
    if type_signal {
        return Ok(true);
    }

    let mut names = HashSet::new();
    let identifier_signal = with.fields().iter().all(|field| {
        is_identifier(field.name()) && names.insert(field.name().to_ascii_lowercase())
    });
    Ok(identifier_signal)
}

fn validate_header_names(
    sample: &[u8],
    schema: &SchemaRef,
    options: &CsvOptions,
    uri: &str,
) -> Result<()> {
    let inferred = infer(sample, options, true).map_err(|error| csv_schema_error(uri, error))?;
    let names_match = inferred.fields().len() == schema.fields().len()
        && inferred
            .fields()
            .iter()
            .zip(schema.fields())
            .all(|(actual, expected)| actual.name() == expected.name());
    if !names_match {
        return Err(Error::InvalidArgument(format!(
            "CSV header does not match the explicit schema: {uri}"
        )));
    }
    Ok(())
}

fn csv_schema_error(uri: &str, error: Error) -> Error {
    Error::Execution(format!("CSV schema read failed for {uri}: {error}"))
}

fn csv_sample_error(uri: &str, error: Error) -> Error {
    let changed = match &error {
        Error::ObjectStore(source) => matches!(
            source,
            object_store::Error::Precondition { .. } | object_store::Error::NotFound { .. }
        ),
        _ => false,
    };
    if changed {
        Error::Execution(format!(
            "object changed during query preparation: {uri}: {error}"
        ))
    } else {
        Error::Execution(format!("CSV sample read failed for {uri}: {error}"))
    }
}

fn validate_schema(actual: &Schema, expected: &Schema, uri: &str) -> Result<()> {
    if actual.fields().len() != expected.fields().len() {
        return Err(Error::InvalidArgument(format!(
            "CSV schema mismatch in {uri}: expected {} columns, found {}",
            expected.fields().len(),
            actual.fields().len()
        )));
    }
    for (actual, expected) in actual.fields().iter().zip(expected.fields()) {
        if actual.name() != expected.name()
            || (actual.data_type() != expected.data_type() && actual.data_type() != &DataType::Null)
        {
            return Err(Error::InvalidArgument(format!(
                "CSV schema mismatch in {uri} at column {}: expected {:?}, found {} {:?}",
                expected.name(),
                expected.data_type(),
                actual.name(),
                actual.data_type()
            )));
        }
    }
    Ok(())
}

fn complete_prefix(bytes: &[u8], quote: u8, escape: Option<u8>, at_eof: bool) -> (usize, usize) {
    let mut in_quotes = false;
    let mut last_complete = 0;
    let mut records = 0;
    let mut index = 0;
    while index < bytes.len() {
        let byte = bytes[index];
        if in_quotes && escape == Some(byte) && index + 1 < bytes.len() {
            index += 2;
            continue;
        }
        if byte == quote {
            if in_quotes && bytes.get(index + 1) == Some(&quote) {
                index += 2;
                continue;
            }
            in_quotes = !in_quotes;
        } else if byte == b'\n' && !in_quotes {
            records += 1;
            last_complete = index + 1;
        }
        index += 1;
    }

    if at_eof && !in_quotes && last_complete < bytes.len() {
        records += 1;
        last_complete = bytes.len();
    }
    (last_complete, records)
}

fn is_identifier(value: &str) -> bool {
    let mut characters = value.chars();
    characters
        .next()
        .is_some_and(|character| character == '_' || character.is_alphabetic())
        && characters.all(|character| character == '_' || character.is_alphanumeric())
}

#[cfg(test)]
mod tests {
    use super::{complete_prefix, detect_header};
    use crate::CsvOptions;

    #[test]
    fn sample_boundary_ignores_newlines_inside_quotes() {
        let input = b"id,note\n1,\"first\nsecond\"\n2,last";
        let (end, rows) = complete_prefix(input, b'"', None, false);
        assert_eq!(rows, 2);
        assert_eq!(&input[..end], b"id,note\n1,\"first\nsecond\"\n");
    }

    #[test]
    fn header_detection_uses_type_change() {
        let input = b"id,amount\n1,10.5\n2,20.0\n";
        assert!(detect_header(input, &CsvOptions::default()).unwrap());
    }
}
