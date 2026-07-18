#![forbid(unsafe_code)]

mod catalog;
mod catalog_name;
mod command;
mod config;
mod datasource;
mod engine;
mod error;
mod execution;
mod optimizer;
mod prepared;
mod runtime;
mod sql;
mod storage;
mod table_function;

pub use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
pub(crate) use catalog::{Catalog, TableEntry};
pub use config::{
    CsvCompression, CsvHeader, CsvOptions, CsvOptionsBuilder, CsvScanConfig, EngineConfig,
    EngineConfigBuilder, ExecutionConfig, NativeStorageConfig, ParquetOptions, ParquetPruningMode,
    ParquetScanConfig, S3Config, SpillConfig,
};
pub use datasource::ParquetSchemaMode;
pub use engine::{
    CommitInfo, Engine, EngineMemorySnapshot, MigrationInfo, QueryCancellation, QueryResult,
    Session, Transaction, TransactionAccessMode, TransactionOptions, TransactionPreparedStatement,
};
pub use error::{Error, Result};
pub use prepared::{ParameterValue, PreparedStatement};
pub use runtime::{OperatorMetricsSnapshot, QueryMetrics, QueryMetricsSnapshot, RecordBatchStream};

#[doc(hidden)]
pub fn split_sql_statements(sql: &str) -> Result<Vec<String>> {
    crate::sql::split_statement_text(sql)
}
