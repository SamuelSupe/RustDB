use std::path::{Path, PathBuf};

mod code;

pub use code::{ErrorCode, RetryClass};

#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    #[error("I/O error{path}: {source}", path = display_path(.path))]
    Io {
        path: Option<PathBuf>,
        #[source]
        source: std::io::Error,
    },

    #[error("Arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),

    #[error("Parquet error: {0}")]
    Parquet(#[from] parquet::errors::ParquetError),

    #[error("object store error: {0}")]
    ObjectStore(#[from] object_store::Error),

    #[error("SQL parse error: {0}")]
    SqlParse(#[from] sqlparser::parser::ParserError),

    #[error("invalid argument: {0}")]
    InvalidArgument(String),

    #[error("unsupported operation: {0}")]
    Unsupported(String),

    #[error("resource exhausted: {0}")]
    ResourceExhausted(String),

    #[error(
        "native {scope} disk quota exceeded{path}: current {current_bytes} bytes + new {added_bytes} bytes reaches peak {peak_bytes} bytes, limit {limit_bytes} bytes",
        scope = native_quota_scope(.table.as_deref()),
        path = display_required_path(.path)
    )]
    NativeDiskQuotaExceeded {
        path: PathBuf,
        table: Option<String>,
        current_bytes: u64,
        added_bytes: u64,
        peak_bytes: u64,
        limit_bytes: u64,
    },

    #[error("query cancelled")]
    Cancelled,

    #[error("catalog error: {0}")]
    Catalog(String),

    #[error("transaction {transaction_id} is not active: {state}")]
    TransactionClosed {
        transaction_id: String,
        state: &'static str,
    },

    #[error("transaction {transaction_id} conflict: {message}")]
    TransactionConflict {
        transaction_id: String,
        message: String,
    },

    #[error("native storage error{path}: {message}", path = display_required_path(.path))]
    NativeStorage { path: PathBuf, message: String },

    #[error(
        "unsupported native database format{path}: found version {found_version}, current beta version is {current_version}; {reason}",
        path = display_required_path(.path),
        reason = native_format_reason(.alpha)
    )]
    NativeFormatUnsupported {
        path: PathBuf,
        found_version: u32,
        current_version: u32,
        alpha: bool,
    },

    #[error("native repair refused{path}: {message}", path = display_required_path(.path))]
    NativeRepairRefused { path: PathBuf, message: String },

    #[error("native import_id '{import_id}' conflicts with a previously committed request")]
    NativeImportConflict { import_id: String },

    #[error(
        "durable operation outcome is unknown for transaction {transaction_id}{path}: {message}",
        path = display_required_path(.path)
    )]
    CommitOutcomeUnknown {
        path: PathBuf,
        transaction_id: String,
        message: String,
    },

    #[error(
        "native transaction {transaction_id} committed as catalog generation {generation}{path}, but post-commit handling failed: {message}",
        path = display_required_path(.path)
    )]
    NativeCommitPostCommitFailure {
        path: PathBuf,
        transaction_id: String,
        generation: u64,
        message: String,
    },

    #[error(
        "COPY output committed durably{path}, but post-commit handling failed: {message}",
        path = display_required_path(.path)
    )]
    CopyPostCommitFailure { path: PathBuf, message: String },

    #[error("execution error: {0}")]
    Execution(String),

    #[error("internal error: {0}")]
    Internal(String),
}

fn display_path(path: &Option<PathBuf>) -> String {
    path.as_ref()
        .map(|path| format!(" at {}", path.display()))
        .unwrap_or_default()
}

fn display_required_path(path: &Path) -> String {
    format!(" at {}", path.display())
}

fn native_quota_scope(table: Option<&str>) -> String {
    table
        .map(|name| format!("table '{name}'"))
        .unwrap_or_else(|| "engine".to_owned())
}

fn native_format_reason(alpha: &bool) -> &'static str {
    if *alpha {
        "alpha databases are intentionally not migrated; re-import the source CSV or Parquet data"
    } else {
        "this database must be opened by a compatible RustDB version"
    }
}

impl Error {
    /// Returns the stable machine-readable code for this error category.
    pub const fn code(&self) -> ErrorCode {
        match self {
            Self::Io { .. } => ErrorCode::Io,
            Self::Arrow(_) => ErrorCode::Arrow,
            Self::Parquet(_) => ErrorCode::Parquet,
            Self::ObjectStore(_) => ErrorCode::ObjectStore,
            Self::SqlParse(_) => ErrorCode::SqlParse,
            Self::InvalidArgument(_) => ErrorCode::InvalidArgument,
            Self::Unsupported(_) => ErrorCode::Unsupported,
            Self::ResourceExhausted(_) => ErrorCode::ResourceExhausted,
            Self::NativeDiskQuotaExceeded { .. } => ErrorCode::NativeDiskQuotaExceeded,
            Self::Cancelled => ErrorCode::Cancelled,
            Self::Catalog(_) => ErrorCode::Catalog,
            Self::TransactionClosed { .. } => ErrorCode::TransactionClosed,
            Self::TransactionConflict { .. } => ErrorCode::TransactionConflict,
            Self::NativeStorage { .. } => ErrorCode::NativeStorage,
            Self::NativeFormatUnsupported { .. } => ErrorCode::NativeFormatUnsupported,
            Self::NativeRepairRefused { .. } => ErrorCode::NativeRepairRefused,
            Self::NativeImportConflict { .. } => ErrorCode::NativeImportConflict,
            Self::CommitOutcomeUnknown { .. } => ErrorCode::CommitOutcomeUnknown,
            Self::NativeCommitPostCommitFailure { .. } => ErrorCode::NativeCommitPostCommitFailure,
            Self::CopyPostCommitFailure { .. } => ErrorCode::CopyPostCommitFailure,
            Self::Execution(_) => ErrorCode::Execution,
            Self::Internal(_) => ErrorCode::Internal,
        }
    }

    /// Classifies retry safety without requiring callers to parse the message.
    pub const fn retry_class(&self) -> RetryClass {
        match self {
            Self::ResourceExhausted(_) | Self::NativeDiskQuotaExceeded { .. } => RetryClass::Safe,
            Self::TransactionConflict { .. } => RetryClass::ReopenRequired,
            Self::CommitOutcomeUnknown { .. } => RetryClass::OutcomeUnknown,
            Self::Io { .. }
            | Self::Arrow(_)
            | Self::Parquet(_)
            | Self::ObjectStore(_)
            | Self::NativeStorage { .. }
            | Self::NativeCommitPostCommitFailure { .. }
            | Self::CopyPostCommitFailure { .. }
            | Self::Execution(_)
            | Self::Internal(_) => RetryClass::Unknown,
            Self::SqlParse(_)
            | Self::InvalidArgument(_)
            | Self::Unsupported(_)
            | Self::NativeFormatUnsupported { .. }
            | Self::NativeRepairRefused { .. }
            | Self::NativeImportConflict { .. }
            | Self::Cancelled
            | Self::Catalog(_)
            | Self::TransactionClosed { .. } => RetryClass::Never,
        }
    }

    pub fn io(path: impl Into<Option<PathBuf>>, source: std::io::Error) -> Self {
        Self::Io {
            path: path.into(),
            source,
        }
    }

    pub(crate) fn native_storage(path: impl Into<PathBuf>, message: impl Into<String>) -> Self {
        Self::NativeStorage {
            path: path.into(),
            message: message.into(),
        }
    }

    pub(crate) fn native_disk_quota_exceeded(
        path: impl Into<PathBuf>,
        table: Option<String>,
        current_bytes: u64,
        added_bytes: u64,
        peak_bytes: u64,
        limit_bytes: u64,
    ) -> Self {
        Self::NativeDiskQuotaExceeded {
            path: path.into(),
            table,
            current_bytes,
            added_bytes,
            peak_bytes,
            limit_bytes,
        }
    }

    pub(crate) fn native_repair_refused(
        path: impl Into<PathBuf>,
        message: impl Into<String>,
    ) -> Self {
        Self::NativeRepairRefused {
            path: path.into(),
            message: message.into(),
        }
    }

    pub(crate) fn native_import_conflict(import_id: impl Into<String>) -> Self {
        Self::NativeImportConflict {
            import_id: import_id.into(),
        }
    }

    pub(crate) fn commit_outcome_unknown(
        path: impl Into<PathBuf>,
        transaction_id: impl Into<String>,
        message: impl Into<String>,
    ) -> Self {
        Self::CommitOutcomeUnknown {
            path: path.into(),
            transaction_id: transaction_id.into(),
            message: message.into(),
        }
    }

    pub(crate) fn native_commit_post_commit_failure(
        path: impl Into<PathBuf>,
        transaction_id: impl Into<String>,
        generation: u64,
        message: impl Into<String>,
    ) -> Self {
        Self::NativeCommitPostCommitFailure {
            path: path.into(),
            transaction_id: transaction_id.into(),
            generation,
            message: message.into(),
        }
    }

    pub(crate) fn copy_post_commit_failure(
        path: impl Into<PathBuf>,
        message: impl Into<String>,
    ) -> Self {
        Self::CopyPostCommitFailure {
            path: path.into(),
            message: message.into(),
        }
    }
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[cfg(test)]
mod tests {
    use super::{Error, ErrorCode, RetryClass};

    #[test]
    fn exposes_stable_codes_and_retry_classes() {
        let invalid = Error::InvalidArgument("bad input".into());
        assert_eq!(invalid.code(), ErrorCode::InvalidArgument);
        assert_eq!(invalid.code().as_str(), "request.invalid");
        assert_eq!(invalid.retry_class(), RetryClass::Never);

        let exhausted = Error::ResourceExhausted("memory".into());
        assert_eq!(exhausted.code(), ErrorCode::ResourceExhausted);
        assert_eq!(exhausted.retry_class(), RetryClass::Safe);

        let unknown = Error::commit_outcome_unknown("db", "tx-1", "ambiguous publish");
        assert_eq!(unknown.code(), ErrorCode::CommitOutcomeUnknown);
        assert_eq!(unknown.retry_class(), RetryClass::OutcomeUnknown);
    }
}
