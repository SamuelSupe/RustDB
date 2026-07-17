mod csv;
mod csv_mapping;
mod parquet;
mod parquet_mapping;

pub(crate) use csv::RegisteredCsvTable;
pub(crate) use parquet::RegisteredParquetTable;
