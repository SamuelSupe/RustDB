#![allow(dead_code)] // Each integration-test crate uses either file or byte fixtures.

use std::{fs::File, path::Path, sync::Arc};

use arrow::{
    array::{ArrayRef, Int32Array, Int64Array, StringArray},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use parquet::arrow::ArrowWriter;
use rustdb::Result;

pub fn write_i32(path: &Path, ids: &[i32], value_name: &str, values: &[&str]) -> Result<()> {
    write(
        path,
        batch(
            DataType::Int32,
            Arc::new(Int32Array::from(ids.to_vec())),
            value_name,
            values,
        )?,
    )
}

pub fn write_i64(path: &Path, ids: &[i64], value_name: &str, values: &[&str]) -> Result<()> {
    write(
        path,
        batch(
            DataType::Int64,
            Arc::new(Int64Array::from(ids.to_vec())),
            value_name,
            values,
        )?,
    )
}

pub fn bytes_i32(ids: &[i32], value_name: &str, values: &[&str]) -> Result<Vec<u8>> {
    bytes(batch(
        DataType::Int32,
        Arc::new(Int32Array::from(ids.to_vec())),
        value_name,
        values,
    )?)
}

pub fn bytes_i64(ids: &[i64], value_name: &str, values: &[&str]) -> Result<Vec<u8>> {
    bytes(batch(
        DataType::Int64,
        Arc::new(Int64Array::from(ids.to_vec())),
        value_name,
        values,
    )?)
}

fn batch(
    id_type: DataType,
    ids: ArrayRef,
    value_name: &str,
    values: &[&str],
) -> Result<RecordBatch> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", id_type, false),
        Field::new(value_name, DataType::Utf8, false),
    ]));
    Ok(RecordBatch::try_new(
        schema,
        vec![ids, Arc::new(StringArray::from(values.to_vec()))],
    )?)
}

fn write(path: &Path, batch: RecordBatch) -> Result<()> {
    let file = File::create(path).map_err(|error| rustdb::Error::io(Some(path.into()), error))?;
    let mut writer = ArrowWriter::try_new(file, batch.schema(), None)?;
    writer.write(&batch)?;
    writer.close()?;
    Ok(())
}

fn bytes(batch: RecordBatch) -> Result<Vec<u8>> {
    let temp = tempfile::NamedTempFile::new().map_err(|error| rustdb::Error::io(None, error))?;
    write(temp.path(), batch)?;
    std::fs::read(temp.path())
        .map_err(|error| rustdb::Error::io(Some(temp.path().to_path_buf()), error))
}
