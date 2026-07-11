use std::{collections::HashSet, io::Cursor, ops::Deref, sync::Arc};

use arrow::{
    csv::reader::Format,
    datatypes::{DataType, Schema, SchemaRef},
};
use bytes::Bytes;
use object_store::GetRange;

use crate::{
    CsvHeader, CsvOptions, Error, Result,
    runtime::{MemoryReservation, QueryContext},
    storage::ObjectSource,
};

const INITIAL_SAMPLE_BYTES: u64 = 1024 * 1024;
const MAX_SAMPLE_BYTES: usize = 64 * 1024 * 1024;

pub(super) fn sample_byte_cap(memory_limit: usize) -> usize {
    (memory_limit / 4).clamp(1, MAX_SAMPLE_BYTES)
}

pub(super) async fn infer_table_schema(
    files: &[ObjectSource],
    options: &CsvOptions,
    sample_byte_cap: usize,
    context: Option<&QueryContext>,
) -> Result<(SchemaRef, bool)> {
    infer_table_schema_impl(files, options, None, sample_byte_cap, context).await
}

pub(super) async fn infer_table_schema_against(
    files: &[ObjectSource],
    options: &CsvOptions,
    expected: &SchemaRef,
    sample_byte_cap: usize,
    context: Option<&QueryContext>,
) -> Result<(SchemaRef, bool)> {
    if options.schema.is_some() {
        return Err(Error::Internal(
            "registered CSV inference options unexpectedly contain a schema".to_owned(),
        ));
    }
    infer_table_schema_impl(files, options, Some(expected), sample_byte_cap, context).await
}

async fn infer_table_schema_impl(
    files: &[ObjectSource],
    options: &CsvOptions,
    expected: Option<&SchemaRef>,
    sample_byte_cap: usize,
    context: Option<&QueryContext>,
) -> Result<(SchemaRef, bool)> {
    let first = files
        .first()
        .ok_or_else(|| Error::InvalidArgument("CSV table requires at least one file".to_owned()))?;

    if let Some(schema) = &options.schema {
        if options.header == CsvHeader::Absent {
            return Ok((Arc::clone(schema), false));
        }

        let required_records = if options.header == CsvHeader::Auto {
            2
        } else {
            1
        };
        let first_sample =
            read_sample(first, options, sample_byte_cap, required_records, context).await?;
        let has_header = match options.header {
            CsvHeader::Present => true,
            CsvHeader::Auto => detect_header(&first_sample, options)
                .map_err(|error| csv_schema_error(first.uri(), error))?,
            CsvHeader::Absent => unreachable!("handled before sampling"),
        };
        if has_header {
            validate_header_names(&first_sample, schema, options, first.uri())?;
            drop(first_sample);
            for file in files.iter().skip(1) {
                let sample = read_sample(file, options, sample_byte_cap, 1, context).await?;
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

    let required_records = match options.header {
        CsvHeader::Absent => 1,
        CsvHeader::Present | CsvHeader::Auto => 2,
    };
    let first_sample =
        read_sample(first, options, sample_byte_cap, required_records, context).await?;
    let has_header = match options.header {
        CsvHeader::Present => true,
        CsvHeader::Absent => false,
        CsvHeader::Auto => detect_header(&first_sample, options)
            .map_err(|error| csv_schema_error(first.uri(), error))?,
    };
    let schema = infer(&first_sample, options, has_header)
        .map_err(|error| csv_schema_error(first.uri(), error))?;
    if schema.fields().is_empty() {
        return Err(Error::InvalidArgument(format!(
            "CSV file has no columns: {}",
            first.uri()
        )));
    }
    if let Some(expected) = expected {
        validate_schema(&schema, expected, first.uri())?;
    }
    drop(first_sample);
    for file in files.iter().skip(1) {
        let required_records = if has_header { 2 } else { 1 };
        let sample = read_sample(file, options, sample_byte_cap, required_records, context).await?;
        let actual = infer(&sample, options, has_header)
            .map_err(|error| csv_schema_error(file.uri(), error))?;
        validate_schema(
            &actual,
            expected.map_or(&schema, |expected| expected.as_ref()),
            file.uri(),
        )?;
    }
    Ok((
        expected.map_or_else(|| Arc::new(schema), Arc::clone),
        has_header,
    ))
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
    sample_byte_cap: usize,
    required_records: usize,
    context: Option<&QueryContext>,
) -> Result<CsvSample> {
    debug_assert!(sample_byte_cap > 0);
    debug_assert!(required_records > 0);
    if file.snapshot().size == 0 {
        return Ok(CsvSample::empty());
    }

    let cap = u64::try_from(sample_byte_cap).unwrap_or(u64::MAX);
    let mut end = file.snapshot().size.min(INITIAL_SAMPLE_BYTES).min(cap);
    loop {
        if let Some(context) = context {
            context.check_cancelled()?;
        }
        let mut memory = match context {
            Some(context) => Some(context.memory.try_reserve(end as usize).map_err(|error| {
                csv_sample_memory_error(file.uri(), end as usize, context, error)
            })?),
            None => None,
        };
        if let Some(context) = context
            && file.is_s3()
        {
            context.metrics.add_s3_requests(1);
        }
        let mut get_options = file.get_options_for(file.snapshot());
        get_options.range = Some(GetRange::Bounded(0..end));
        let request = async {
            let response = file
                .store()
                .get_opts(file.location(), get_options)
                .await
                .map_err(Error::from)?;
            file.snapshot()
                .validate_get_response(file.uri(), &response.meta)?;
            response.bytes().await.map_err(Error::from)
        };
        let bytes = match context {
            Some(context) => tokio::select! {
                _ = context.control.cancelled() => Err(Error::Cancelled),
                result = request => result,
            },
            None => request.await,
        }
        .map_err(|error| csv_sample_error(file.uri(), error))?;
        if let (Some(memory), Some(context)) = (&mut memory, context) {
            memory.try_resize(bytes.len()).map_err(|error| {
                csv_sample_memory_error(file.uri(), bytes.len(), context, error)
            })?;
        }
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
        if records >= required_records || end == file.snapshot().size {
            return Ok(CsvSample {
                bytes,
                complete_end,
                _memory: memory,
            });
        }
        if end >= cap {
            return Err(Error::ResourceExhausted(format!(
                "CSV schema/header sample for {} reached its {sample_byte_cap}-byte limit before finding {required_records} complete record(s); provide an explicit schema/header or increase the engine memory limit",
                file.uri()
            )));
        }
        end = end.saturating_mul(2).min(file.snapshot().size).min(cap);
    }
}

struct CsvSample {
    bytes: Bytes,
    complete_end: usize,
    _memory: Option<MemoryReservation>,
}

impl CsvSample {
    fn empty() -> Self {
        Self {
            bytes: Bytes::new(),
            complete_end: 0,
            _memory: None,
        }
    }
}

impl Deref for CsvSample {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &self.bytes[..self.complete_end]
    }
}

fn csv_sample_memory_error(uri: &str, bytes: usize, context: &QueryContext, error: Error) -> Error {
    Error::ResourceExhausted(format!(
        "CSV schema/header sample for {uri} requires {bytes} bytes (query limit {}, available {}): {error}",
        context.memory.limit(),
        context.memory.available()
    ))
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
    use std::{fs, sync::Arc};

    use arrow::datatypes::{DataType, Field, Schema};
    use tempfile::tempdir;

    use super::{complete_prefix, detect_header, infer_table_schema, sample_byte_cap};
    use crate::{
        CsvHeader, CsvOptions, Error, S3Config,
        runtime::{MemoryPool, QueryContext},
        storage::LocationResolver,
    };

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

    #[test]
    fn sample_cap_is_a_quarter_of_memory_up_to_sixty_four_mib() {
        assert_eq!(sample_byte_cap(1), 1);
        assert_eq!(sample_byte_cap(1024), 256);
        assert_eq!(sample_byte_cap(usize::MAX), 64 * 1024 * 1024);
    }

    #[tokio::test]
    async fn bounded_sample_rejects_a_huge_unterminated_record() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("unterminated.csv");
        let memory_limit = 1024;
        let cap = sample_byte_cap(memory_limit);
        let mut contents = b"id,note\n1,\"".to_vec();
        contents.extend(vec![b'x'; cap * 2]);
        fs::write(&path, contents).unwrap();

        let resolver = LocationResolver::with_memory_limit(S3Config::default(), 1024 * 1024);
        let files = resolver
            .resolve(&[path.to_string_lossy().into_owned()])
            .await
            .unwrap();
        let context = QueryContext::new(MemoryPool::new(4096), directory.path()).unwrap();
        let baseline = context.memory.used();
        let error = infer_table_schema(&files, &CsvOptions::default(), cap, Some(&context))
            .await
            .unwrap_err();

        match error {
            Error::ResourceExhausted(message) => {
                assert!(message.contains(files[0].uri()));
                assert!(message.contains("256-byte limit"));
                assert!(message.contains("2 complete record"));
            }
            other => panic!("expected resource error, got {other:?}"),
        }
        assert_eq!(context.memory.used(), baseline);
        assert!(context.memory.peak() >= cap);
    }

    #[tokio::test]
    async fn explicit_schema_without_a_header_does_not_sample_the_file() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("explicit.csv");
        fs::write(&path, b"1,\"unterminated").unwrap();
        let resolver = LocationResolver::with_memory_limit(S3Config::default(), 1024 * 1024);
        let files = resolver
            .resolve(&[path.to_string_lossy().into_owned()])
            .await
            .unwrap();
        let context = QueryContext::new(MemoryPool::new(4096), directory.path()).unwrap();
        let baseline = context.memory.used();
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("note", DataType::Utf8, true),
        ]));
        let options = CsvOptions {
            schema: Some(Arc::clone(&schema)),
            header: CsvHeader::Absent,
            ..CsvOptions::default()
        };

        let (actual, has_header) = infer_table_schema(&files, &options, 1, Some(&context))
            .await
            .unwrap();

        assert_eq!(actual, schema);
        assert!(!has_header);
        assert_eq!(context.memory.used(), baseline);
        assert_eq!(context.memory.peak(), baseline);
    }
}
