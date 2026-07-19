use std::time::{Duration, Instant};

use futures::StreamExt;
use sqlparser::ast::{Query, Statement};

use super::Session;
use crate::{
    CsvCompression, CsvHeader, Error, NativeImportFormat, NativeImportOptions, NativeImportResult,
    Result,
    command::{NativeWriteCommand, NativeWriteKind},
};

impl Session {
    /// Imports one CSV or Parquet source into a new persistent Native table.
    ///
    /// `import_id` is durable. Replaying the same request returns the original
    /// receipt without reading the source or appending rows again. Reusing the
    /// id for a different request returns `native.import_conflict`.
    pub async fn import(&self, options: NativeImportOptions) -> Result<NativeImportResult> {
        let prepared = options.prepare()?;
        if self.native_transaction.is_some() {
            return Err(Error::Unsupported(
                "Native import cannot run inside an explicit transaction".to_owned(),
            ));
        }
        let mut sql_transaction = self.sql_transaction.lock().await;
        if sql_transaction
            .as_ref()
            .is_some_and(super::Transaction::can_release_session)
        {
            sql_transaction.take();
        }
        if sql_transaction.is_some() {
            return Err(Error::Unsupported(
                "Native import cannot run inside an SQL transaction".to_owned(),
            ));
        }
        // Keep BEGIN/COMMIT and other work on this Session from racing the
        // transaction check while the import is in progress.
        let _sql_transaction = sql_transaction;

        let _gate = self.engine.inner.import_gate.lock().await;
        self.engine.ensure_native_healthy()?;
        let database = self.engine.inner.database.as_ref().ok_or_else(|| {
            Error::Unsupported("Native import requires Engine::open(path, config)".to_owned())
        })?;
        if let Some(receipt) = database.check_import(&prepared.intent)? {
            return Ok(NativeImportResult::new(receipt, true));
        }
        self.ensure_schema_for(&prepared.intent.table)?;

        let query = import_query(&prepared)?;
        let admission_started = Instant::now();
        let permit = self.acquire_query_permit().await?;
        let command = NativeWriteCommand {
            name: prepared.intent.table.clone(),
            qualifier: crate::catalog_name::full_qualifier(&prepared.intent.table),
            query,
            kind: NativeWriteKind::Import,
            returning: None,
            import: Some(prepared.intent.clone()),
        };
        let mut result = self
            .execute_native_write(command, permit, admission_started.elapsed(), Duration::ZERO)
            .await?;
        while let Some(batch) = result.stream().next().await {
            batch?;
        }

        let receipt = database
            .import_receipt(&prepared.intent.import_id)
            .ok_or_else(|| {
                Error::Internal(format!(
                    "Native import '{}' committed without a durable receipt",
                    prepared.intent.import_id
                ))
            })?;
        if !receipt.matches(&prepared.intent) {
            return Err(Error::Internal(format!(
                "Native import '{}' committed a mismatched receipt",
                prepared.intent.import_id
            )));
        }
        Ok(NativeImportResult::new(receipt, false))
    }
}

fn import_query(prepared: &crate::import::PreparedNativeImport) -> Result<Box<Query>> {
    let location = quote_string(&prepared.location);
    let sql = match prepared.format {
        NativeImportFormat::Parquet => format!("SELECT * FROM read_parquet({location})"),
        NativeImportFormat::Csv => {
            let csv = &prepared.csv;
            let mut arguments = vec![
                format!("header = '{}'", header_name(csv.header)),
                format!("delimiter = {}", quote_byte(csv.delimiter)),
                format!("quote = {}", quote_byte(csv.quote)),
                format!("sample_size = {}", csv.sample_size),
                format!("compression = '{}'", compression_name(csv.compression)),
            ];
            if let Some(escape) = csv.escape {
                arguments.push(format!("escape = {}", quote_byte(escape)));
            }
            format!(
                "SELECT * FROM read_csv({location}, {})",
                arguments.join(", ")
            )
        }
    };
    let mut statements = crate::sql::parse_statements(&sql)?;
    let Statement::Query(query) = statements.remove(0) else {
        return Err(Error::Internal(
            "generated Native import query did not parse".to_owned(),
        ));
    };
    Ok(query)
}

fn quote_string(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

fn quote_byte(value: u8) -> String {
    quote_string(&char::from(value).to_string())
}

fn header_name(value: CsvHeader) -> &'static str {
    match value {
        CsvHeader::Auto => "auto",
        CsvHeader::Present => "present",
        CsvHeader::Absent => "absent",
    }
}

fn compression_name(value: CsvCompression) -> &'static str {
    match value {
        CsvCompression::Auto => "auto",
        CsvCompression::None => "none",
        CsvCompression::Gzip => "gzip",
        CsvCompression::Zstd => "zstd",
    }
}
