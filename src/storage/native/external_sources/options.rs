use std::path::Path;

use serde::{Deserialize, Serialize};

use super::StoredSchema;
use crate::{
    CsvCompression, CsvHeader, CsvOptions, Error, ParquetOptions, ParquetSchemaMode, Result,
};

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct StoredCsvOptions {
    schema: Option<StoredSchema>,
    header: String,
    delimiter: u8,
    quote: u8,
    escape: Option<u8>,
    sample_size: usize,
    compression: String,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct StoredParquetOptions {
    schema: Option<StoredSchema>,
    union_by_name: bool,
    schema_mode: String,
    hive_partitioning: bool,
}

pub(super) fn encode_csv_options(options: &CsvOptions) -> StoredCsvOptions {
    StoredCsvOptions {
        schema: options.schema.as_deref().map(StoredSchema::from_schema),
        header: match options.header {
            CsvHeader::Auto => "auto",
            CsvHeader::Present => "present",
            CsvHeader::Absent => "absent",
        }
        .to_owned(),
        delimiter: options.delimiter,
        quote: options.quote,
        escape: options.escape,
        sample_size: options.sample_size,
        compression: match options.compression {
            CsvCompression::Auto => "auto",
            CsvCompression::None => "none",
            CsvCompression::Gzip => "gzip",
            CsvCompression::Zstd => "zstd",
        }
        .to_owned(),
    }
}

pub(super) fn decode_csv_options(path: &Path, options: StoredCsvOptions) -> Result<CsvOptions> {
    Ok(CsvOptions {
        schema: options
            .schema
            .map(|schema| schema.decode(path))
            .transpose()?,
        header: match options.header.as_str() {
            "auto" => CsvHeader::Auto,
            "present" => CsvHeader::Present,
            "absent" => CsvHeader::Absent,
            value => return Err(invalid_option(path, "CSV header", value)),
        },
        delimiter: options.delimiter,
        quote: options.quote,
        escape: options.escape,
        sample_size: options.sample_size,
        compression: match options.compression.as_str() {
            "auto" => CsvCompression::Auto,
            "none" => CsvCompression::None,
            "gzip" => CsvCompression::Gzip,
            "zstd" => CsvCompression::Zstd,
            value => return Err(invalid_option(path, "CSV compression", value)),
        },
    })
}

pub(super) fn encode_parquet_options(options: &ParquetOptions) -> StoredParquetOptions {
    StoredParquetOptions {
        schema: options.schema.as_deref().map(StoredSchema::from_schema),
        union_by_name: options.union_by_name,
        schema_mode: match options.schema_mode {
            ParquetSchemaMode::Strict => "strict",
            ParquetSchemaMode::UnionByName => "union_by_name",
            ParquetSchemaMode::SafeWidening => "safe_widening",
        }
        .to_owned(),
        hive_partitioning: options.hive_partitioning,
    }
}

pub(super) fn decode_parquet_options(
    path: &Path,
    options: StoredParquetOptions,
) -> Result<ParquetOptions> {
    Ok(ParquetOptions {
        schema: options
            .schema
            .map(|schema| schema.decode(path))
            .transpose()?,
        union_by_name: options.union_by_name,
        schema_mode: match options.schema_mode.as_str() {
            "strict" => ParquetSchemaMode::Strict,
            "union_by_name" => ParquetSchemaMode::UnionByName,
            "safe_widening" => ParquetSchemaMode::SafeWidening,
            value => return Err(invalid_option(path, "Parquet schema mode", value)),
        },
        hive_partitioning: options.hive_partitioning,
    })
}

fn invalid_option(path: &Path, kind: &str, value: &str) -> Error {
    Error::native_storage(path, format!("invalid {kind} '{value}'"))
}
