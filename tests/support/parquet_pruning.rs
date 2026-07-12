#![allow(dead_code)] // Shared by focused and broad MinIO integration crates.

use std::{fs::File, sync::Arc};

use arrow::{
    array::{Float64Array, Int64Array},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use bytes::Bytes;
use parquet::{
    arrow::ArrowWriter,
    file::{
        metadata::{ParquetMetaDataReader, ParquetMetaDataWriter},
        properties::{EnabledStatistics, WriterProperties},
    },
};
use rustdb::{Error, Result};

pub const ROWS: i64 = 100;
pub const OVERSIZED_BLOOM_LENGTH: i32 = 16 * 1024 * 1024 + 1;

pub fn deep_pruning_bytes() -> Result<Vec<u8>> {
    let temp = tempfile::NamedTempFile::new().map_err(|error| Error::io(None, error))?;
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("score", DataType::Float64, false),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from_iter_values(
                (0_i64..ROWS).map(|value| value * 2),
            )),
            Arc::new(Float64Array::from_iter_values(
                (0_i64..ROWS).map(|value| value as f64 + 0.5),
            )),
        ],
    )?;
    let properties = WriterProperties::builder()
        .set_statistics_enabled(EnabledStatistics::Page)
        .set_data_page_row_count_limit(10)
        .set_write_batch_size(10)
        .set_max_row_group_row_count(Some(ROWS as usize))
        .set_bloom_filter_enabled(true)
        .set_bloom_filter_max_ndv(ROWS as u64)
        .build();
    let file = File::create(temp.path())
        .map_err(|error| Error::io(Some(temp.path().to_path_buf()), error))?;
    let mut writer = ArrowWriter::try_new(file, schema, Some(properties))?;
    writer.write(&batch)?;
    writer.close()?;
    let bytes = std::fs::read(temp.path())
        .map_err(|error| Error::io(Some(temp.path().to_path_buf()), error))?;
    let metadata =
        ParquetMetaDataReader::new().parse_and_finish(&Bytes::copy_from_slice(&bytes))?;
    if (0..2).any(|column| {
        metadata
            .row_group(0)
            .column(column)
            .bloom_filter_offset()
            .is_none()
    }) {
        return Err(Error::InvalidArgument(
            "deep-pruning fixture is missing an advertised Bloom filter".to_owned(),
        ));
    }
    Ok(bytes)
}

pub fn with_bloom_length(mut bytes: Vec<u8>, length: i32) -> Result<Vec<u8>> {
    let footer_start = footer_start(&bytes)?;
    let metadata =
        ParquetMetaDataReader::new().parse_and_finish(&Bytes::copy_from_slice(&bytes))?;
    let mut metadata_builder = metadata.into_builder();
    let mut groups = Vec::new();
    for group in metadata_builder.take_row_groups() {
        let mut group_builder = group.into_builder();
        let mut columns = group_builder.take_columns();
        let column = columns.first_mut().ok_or_else(|| {
            Error::InvalidArgument("deep-pruning fixture has no id column".to_owned())
        })?;
        *column = column
            .clone()
            .into_builder()
            .set_bloom_filter_length(Some(length))
            .build()?;
        groups.push(group_builder.set_column_metadata(columns).build()?);
    }
    let metadata = metadata_builder.set_row_groups(groups).build();
    bytes.truncate(footer_start);
    ParquetMetaDataWriter::new(&mut bytes, &metadata).finish()?;
    Ok(bytes)
}

fn footer_start(bytes: &[u8]) -> Result<usize> {
    let trailer = bytes
        .get(bytes.len().saturating_sub(8)..)
        .filter(|trailer| trailer.len() == 8 && &trailer[4..] == b"PAR1")
        .ok_or_else(|| Error::InvalidArgument("invalid Parquet fixture trailer".to_owned()))?;
    let metadata_len =
        u32::from_le_bytes(trailer[..4].try_into().expect("four-byte footer length"));
    bytes
        .len()
        .checked_sub(metadata_len as usize + 8)
        .ok_or_else(|| Error::InvalidArgument("invalid Parquet fixture footer length".to_owned()))
}
