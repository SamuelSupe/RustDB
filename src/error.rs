use std::path::{Path, PathBuf};

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

    #[error("query cancelled")]
    Cancelled,

    #[error("catalog error: {0}")]
    Catalog(String),

    #[error("native storage error{path}: {message}", path = display_required_path(.path))]
    NativeStorage { path: PathBuf, message: String },

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

impl Error {
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
}

pub type Result<T, E = Error> = std::result::Result<T, E>;
