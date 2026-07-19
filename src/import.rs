use serde::{Deserialize, Serialize};

use crate::{CsvOptions, Error, Result};

mod fingerprint;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum NativeImportFormat {
    Csv,
    Parquet,
}

#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct NativeImportOptions {
    import_id: String,
    table: String,
    location: String,
    format: NativeImportFormat,
    csv: CsvOptions,
}

impl NativeImportOptions {
    pub fn new(
        import_id: impl Into<String>,
        table: impl Into<String>,
        location: impl Into<String>,
        format: NativeImportFormat,
    ) -> Self {
        Self {
            import_id: import_id.into(),
            table: table.into(),
            location: location.into(),
            format,
            csv: CsvOptions::default(),
        }
    }

    #[must_use]
    pub fn csv_options(mut self, options: CsvOptions) -> Self {
        self.csv = options;
        self
    }

    pub fn import_id(&self) -> &str {
        &self.import_id
    }

    pub fn table(&self) -> &str {
        &self.table
    }

    pub fn location(&self) -> &str {
        &self.location
    }

    pub fn format(&self) -> NativeImportFormat {
        self.format
    }

    pub fn csv(&self) -> &CsvOptions {
        &self.csv
    }

    pub(crate) fn prepare(self) -> Result<PreparedNativeImport> {
        fingerprint::prepare(self)
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct NativeImportReceipt {
    import_id: String,
    table: String,
    request_fingerprint: String,
    catalog_generation: u64,
    transaction_id: String,
    table_id: String,
    table_version: u64,
    snapshot_id: String,
    rows: u64,
}

impl NativeImportReceipt {
    pub fn import_id(&self) -> &str {
        &self.import_id
    }

    pub fn table(&self) -> &str {
        &self.table
    }

    pub fn request_fingerprint(&self) -> &str {
        &self.request_fingerprint
    }

    pub fn catalog_generation(&self) -> u64 {
        self.catalog_generation
    }

    pub fn transaction_id(&self) -> &str {
        &self.transaction_id
    }

    pub fn table_id(&self) -> &str {
        &self.table_id
    }

    pub fn table_version(&self) -> u64 {
        self.table_version
    }

    pub fn snapshot_id(&self) -> &str {
        &self.snapshot_id
    }

    pub fn rows(&self) -> u64 {
        self.rows
    }

    pub(crate) fn committed(
        intent: &NativeImportIntent,
        catalog_generation: u64,
        transaction_id: &str,
        snapshot: &crate::storage::NativeTableSnapshot,
    ) -> Self {
        Self {
            import_id: intent.import_id.clone(),
            table: intent.table.clone(),
            request_fingerprint: intent.request_fingerprint.clone(),
            catalog_generation,
            transaction_id: transaction_id.to_owned(),
            table_id: snapshot.table_id().to_owned(),
            table_version: snapshot.version(),
            snapshot_id: snapshot.snapshot_id().to_owned(),
            rows: snapshot.row_count(),
        }
    }

    pub(crate) fn matches(&self, intent: &NativeImportIntent) -> bool {
        self.import_id == intent.import_id
            && self.table == intent.table
            && self.request_fingerprint == intent.request_fingerprint
    }

    pub(crate) fn validate(&self, key: &str) -> Result<()> {
        validate_import_id(key)?;
        if key != self.import_id {
            return Err(Error::InvalidArgument(
                "Native import receipt key does not match import_id".to_owned(),
            ));
        }
        if self.table.is_empty() || self.table.contains('\0') {
            return Err(Error::InvalidArgument(
                "Native import receipt has an invalid table".to_owned(),
            ));
        }
        if !valid_sha256(&self.request_fingerprint)
            || self.catalog_generation == 0
            || self.table_version == 0
            || uuid::Uuid::parse_str(&self.transaction_id).is_err()
            || uuid::Uuid::parse_str(&self.table_id).is_err()
            || uuid::Uuid::parse_str(&self.snapshot_id).is_err()
        {
            return Err(Error::InvalidArgument(
                "Native import receipt identity is invalid".to_owned(),
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize)]
#[non_exhaustive]
pub struct NativeImportResult {
    receipt: NativeImportReceipt,
    replayed: bool,
}

impl NativeImportResult {
    pub(crate) fn new(receipt: NativeImportReceipt, replayed: bool) -> Self {
        Self { receipt, replayed }
    }

    pub fn receipt(&self) -> &NativeImportReceipt {
        &self.receipt
    }

    pub fn replayed(&self) -> bool {
        self.replayed
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct NativeImportIntent {
    pub(crate) import_id: String,
    pub(crate) table: String,
    pub(crate) request_fingerprint: String,
}

pub(crate) struct PreparedNativeImport {
    pub(crate) intent: NativeImportIntent,
    pub(crate) location: String,
    pub(crate) format: NativeImportFormat,
    pub(crate) csv: CsvOptions,
}

pub(crate) fn validate_import_id(value: &str) -> Result<()> {
    if value.is_empty()
        || value.len() > 128
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b':'))
    {
        return Err(Error::InvalidArgument(
            "import_id must contain 1-128 ASCII letters, digits, '-', '_', '.', or ':'".to_owned(),
        ));
    }
    Ok(())
}

fn valid_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
}
