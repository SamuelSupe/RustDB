#![forbid(unsafe_code)]

mod catalog;
mod command;
mod config;
mod datasource;
mod engine;
mod error;
mod execution;
mod optimizer;
mod runtime;
mod sql;
mod storage;
mod table_function;

pub use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
pub(crate) use catalog::{Catalog, TableEntry};
pub use config::{
    CsvHeader, CsvOptions, EngineConfig, EngineConfigBuilder, ParquetOptions, ParquetPruningMode,
    ParquetScanConfig, S3Config, SpillConfig,
};
pub use datasource::ParquetSchemaMode;
pub use engine::{Engine, QueryCancellation, QueryResult, Session};
pub use error::{Error, Result};
pub use runtime::{QueryMetrics, QueryMetricsSnapshot, RecordBatchStream};

#[doc(hidden)]
pub fn split_sql_statements(sql: &str) -> Result<Vec<String>> {
    crate::sql::split_statement_text(sql)
}
