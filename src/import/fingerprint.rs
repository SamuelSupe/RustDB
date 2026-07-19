use std::path::{Component, Path, PathBuf};

use sha2::{Digest, Sha256};
use url::Url;

use super::{NativeImportFormat, NativeImportIntent, NativeImportOptions, PreparedNativeImport};
use crate::{CsvCompression, CsvHeader, CsvOptions, Error, Result};

pub(super) fn prepare(options: NativeImportOptions) -> Result<PreparedNativeImport> {
    super::validate_import_id(&options.import_id)?;
    let table = crate::catalog_name::local(&options.table, "import table")?.to_ascii_lowercase();
    if options.csv.schema.is_some() {
        return Err(Error::InvalidArgument(
            "Native import does not accept an explicit CSV schema; use source inference".to_owned(),
        ));
    }
    if options.format == NativeImportFormat::Parquet && options.csv != CsvOptions::default() {
        return Err(Error::InvalidArgument(
            "CSV options cannot be used with a Parquet import".to_owned(),
        ));
    }
    let location = normalize_location(&options.location)?;
    let request_fingerprint = fingerprint(&table, &location, options.format, &options.csv);
    Ok(PreparedNativeImport {
        intent: NativeImportIntent {
            import_id: options.import_id,
            table,
            request_fingerprint,
        },
        location,
        format: options.format,
        csv: options.csv,
    })
}

fn normalize_location(value: &str) -> Result<String> {
    if value.starts_with("s3://") {
        return normalize_s3(value);
    }
    let path = if value.starts_with("file://") {
        let url =
            Url::parse(value).map_err(|_| Error::InvalidArgument("invalid file URI".to_owned()))?;
        if !url.username().is_empty()
            || url.password().is_some()
            || url.query().is_some()
            || url.fragment().is_some()
        {
            return Err(Error::InvalidArgument(
                "file URI must not contain credentials, a query, or a fragment".to_owned(),
            ));
        }
        url.to_file_path()
            .map_err(|()| Error::InvalidArgument("file URI is not a local path".to_owned()))?
    } else {
        PathBuf::from(value)
    };
    let absolute = if path.is_absolute() {
        path
    } else {
        std::env::current_dir()
            .map_err(|error| Error::io(None, error))?
            .join(path)
    };
    let normalized = lexical_normalize(&absolute);
    normalized
        .to_str()
        .map(str::to_owned)
        .ok_or_else(|| Error::InvalidArgument("local import path is not valid UTF-8".to_owned()))
}

fn normalize_s3(value: &str) -> Result<String> {
    let url = Url::parse(value).map_err(|_| Error::InvalidArgument("invalid S3 URI".to_owned()))?;
    if url.scheme() != "s3"
        || !url.username().is_empty()
        || url.password().is_some()
        || url.port().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err(Error::InvalidArgument(
            "S3 import URI must use s3:// without credentials, port, query, or fragment".to_owned(),
        ));
    }
    let bucket = url
        .host_str()
        .filter(|bucket| !bucket.is_empty())
        .ok_or_else(|| Error::InvalidArgument("S3 import URI has no bucket".to_owned()))?;
    if url.path().trim_start_matches('/').is_empty() {
        return Err(Error::InvalidArgument(
            "S3 import URI must include an object key or pattern".to_owned(),
        ));
    }
    Ok(format!(
        "s3://{}{path}",
        bucket.to_ascii_lowercase(),
        path = url.path()
    ))
}

fn lexical_normalize(path: &Path) -> PathBuf {
    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                normalized.pop();
            }
            component => normalized.push(component.as_os_str()),
        }
    }
    normalized
}

fn fingerprint(
    table: &str,
    location: &str,
    format: NativeImportFormat,
    csv: &CsvOptions,
) -> String {
    let mut digest = Sha256::new();
    for value in [
        "rustdb-native-import-v1".to_owned(),
        table.to_owned(),
        location.to_owned(),
        format_name(format).to_owned(),
        header_name(csv.header).to_owned(),
        csv.delimiter.to_string(),
        csv.quote.to_string(),
        csv.escape
            .map_or_else(|| "none".to_owned(), |value| value.to_string()),
        csv.sample_size.to_string(),
        compression_name(csv.compression).to_owned(),
    ] {
        let length = u64::try_from(value.len()).unwrap_or(u64::MAX);
        digest.update(length.to_le_bytes());
        digest.update(value.as_bytes());
    }
    format!("{:x}", digest.finalize())
}

fn format_name(format: NativeImportFormat) -> &'static str {
    match format {
        NativeImportFormat::Csv => "csv",
        NativeImportFormat::Parquet => "parquet",
    }
}

fn header_name(header: CsvHeader) -> &'static str {
    match header {
        CsvHeader::Auto => "auto",
        CsvHeader::Present => "present",
        CsvHeader::Absent => "absent",
    }
}

fn compression_name(compression: CsvCompression) -> &'static str {
    match compression {
        CsvCompression::Auto => "auto",
        CsvCompression::None => "none",
        CsvCompression::Gzip => "gzip",
        CsvCompression::Zstd => "zstd",
    }
}
