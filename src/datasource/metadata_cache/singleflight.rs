use std::{fmt, path::PathBuf, sync::Arc};

use parquet::{arrow::arrow_reader::ArrowReaderMetadata, bloom_filter::Sbbf};

use super::{BloomKey, MetadataCache, MetadataKey};
use crate::Error;

#[derive(Clone)]
pub(super) enum MetadataFlight {
    Loading,
    Ready(std::result::Result<ArrowReaderMetadata, SharedLoadError>),
    Retry,
}

#[derive(Clone)]
pub(super) enum BloomFlight {
    Loading,
    Ready(std::result::Result<Option<Arc<Sbbf>>, SharedLoadError>),
    Retry,
}

pub(crate) struct MetadataLoadGuard {
    cache: MetadataCache,
    key: MetadataKey,
    completed: bool,
}

pub(crate) struct BloomLoadGuard {
    cache: MetadataCache,
    key: BloomKey,
    completed: bool,
}

impl MetadataLoadGuard {
    pub(super) fn new(cache: MetadataCache, key: MetadataKey) -> Self {
        Self {
            cache,
            key,
            completed: false,
        }
    }

    pub(crate) fn succeed(mut self, metadata: ArrowReaderMetadata) {
        self.completed = true;
        self.cache
            .finish_metadata(&self.key, MetadataFlight::Ready(Ok(metadata)));
    }

    pub(crate) fn fail(mut self, error: Error) -> Error {
        self.completed = true;
        let outcome = if is_query_local(&error) {
            MetadataFlight::Retry
        } else {
            MetadataFlight::Ready(Err(SharedLoadError::capture(&error)))
        };
        self.cache.finish_metadata(&self.key, outcome);
        error
    }
}

impl BloomLoadGuard {
    pub(super) fn new(cache: MetadataCache, key: BloomKey) -> Self {
        Self {
            cache,
            key,
            completed: false,
        }
    }

    pub(crate) fn succeed(mut self, filter: Option<Arc<Sbbf>>) {
        self.completed = true;
        self.cache
            .finish_bloom(&self.key, BloomFlight::Ready(Ok(filter)));
    }

    pub(crate) fn fail(mut self, error: Error) -> Error {
        self.completed = true;
        let outcome = if is_query_local(&error) {
            BloomFlight::Retry
        } else {
            BloomFlight::Ready(Err(SharedLoadError::capture(&error)))
        };
        self.cache.finish_bloom(&self.key, outcome);
        error
    }
}

fn is_query_local(error: &Error) -> bool {
    matches!(error, Error::Cancelled | Error::ResourceExhausted(_))
}

impl Drop for MetadataLoadGuard {
    fn drop(&mut self) {
        if !self.completed {
            self.cache.finish_metadata(&self.key, MetadataFlight::Retry);
        }
    }
}

impl Drop for BloomLoadGuard {
    fn drop(&mut self) {
        if !self.completed {
            self.cache.finish_bloom(&self.key, BloomFlight::Retry);
        }
    }
}

#[derive(Clone)]
pub(super) enum SharedLoadError {
    Io {
        path: Option<PathBuf>,
        kind: std::io::ErrorKind,
        message: String,
    },
    Arrow(String),
    Parquet(String),
    ObjectStore(String),
    SqlParse(sqlparser::parser::ParserError),
    InvalidArgument(String),
    Unsupported(String),
    ResourceExhausted(String),
    NativeDiskQuotaExceeded {
        path: PathBuf,
        table: Option<String>,
        current_bytes: u64,
        added_bytes: u64,
        peak_bytes: u64,
        limit_bytes: u64,
    },
    Cancelled,
    Catalog(String),
    TransactionClosed {
        transaction_id: String,
        state: &'static str,
    },
    TransactionConflict {
        transaction_id: String,
        message: String,
    },
    NativeStorage {
        path: PathBuf,
        message: String,
    },
    NativeFormatUnsupported {
        path: PathBuf,
        found_version: u32,
        current_version: u32,
        alpha: bool,
    },
    NativeImportConflict {
        import_id: String,
    },
    CommitOutcomeUnknown {
        path: PathBuf,
        transaction_id: String,
        message: String,
    },
    NativeCommitPostCommitFailure {
        path: PathBuf,
        transaction_id: String,
        generation: u64,
        message: String,
    },
    CopyPostCommitFailure {
        path: PathBuf,
        message: String,
    },
    Execution(String),
    Internal(String),
}

impl SharedLoadError {
    fn capture(error: &Error) -> Self {
        match error {
            Error::Io { path, source } => Self::Io {
                path: path.clone(),
                kind: source.kind(),
                message: source.to_string(),
            },
            Error::Arrow(error) => Self::Arrow(error.to_string()),
            Error::Parquet(error) => Self::Parquet(error.to_string()),
            Error::ObjectStore(error) => Self::ObjectStore(error.to_string()),
            Error::SqlParse(error) => Self::SqlParse(error.clone()),
            Error::InvalidArgument(message) => Self::InvalidArgument(message.clone()),
            Error::Unsupported(message) => Self::Unsupported(message.clone()),
            Error::ResourceExhausted(message) => Self::ResourceExhausted(message.clone()),
            Error::NativeDiskQuotaExceeded {
                path,
                table,
                current_bytes,
                added_bytes,
                peak_bytes,
                limit_bytes,
            } => Self::NativeDiskQuotaExceeded {
                path: path.clone(),
                table: table.clone(),
                current_bytes: *current_bytes,
                added_bytes: *added_bytes,
                peak_bytes: *peak_bytes,
                limit_bytes: *limit_bytes,
            },
            Error::Cancelled => Self::Cancelled,
            Error::Catalog(message) => Self::Catalog(message.clone()),
            Error::TransactionClosed {
                transaction_id,
                state,
            } => Self::TransactionClosed {
                transaction_id: transaction_id.clone(),
                state,
            },
            Error::TransactionConflict {
                transaction_id,
                message,
            } => Self::TransactionConflict {
                transaction_id: transaction_id.clone(),
                message: message.clone(),
            },
            Error::NativeStorage { path, message } => Self::NativeStorage {
                path: path.clone(),
                message: message.clone(),
            },
            Error::NativeFormatUnsupported {
                path,
                found_version,
                current_version,
                alpha,
            } => Self::NativeFormatUnsupported {
                path: path.clone(),
                found_version: *found_version,
                current_version: *current_version,
                alpha: *alpha,
            },
            Error::NativeImportConflict { import_id } => Self::NativeImportConflict {
                import_id: import_id.clone(),
            },
            Error::NativeRepairRefused { .. } => Self::Execution(error.to_string()),
            Error::CommitOutcomeUnknown {
                path,
                transaction_id,
                message,
            } => Self::CommitOutcomeUnknown {
                path: path.clone(),
                transaction_id: transaction_id.clone(),
                message: message.clone(),
            },
            Error::NativeCommitPostCommitFailure {
                path,
                transaction_id,
                generation,
                message,
            } => Self::NativeCommitPostCommitFailure {
                path: path.clone(),
                transaction_id: transaction_id.clone(),
                generation: *generation,
                message: message.clone(),
            },
            Error::CopyPostCommitFailure { path, message } => Self::CopyPostCommitFailure {
                path: path.clone(),
                message: message.clone(),
            },
            Error::Execution(message) => Self::Execution(message.clone()),
            Error::Internal(message) => Self::Internal(message.clone()),
        }
    }

    pub(super) fn into_error(self) -> Error {
        match self {
            Self::Io {
                path,
                kind,
                message,
            } => Error::Io {
                path,
                source: std::io::Error::new(kind, message),
            },
            Self::Arrow(message) => Error::Arrow(arrow::error::ArrowError::ExternalError(
                Box::new(SharedMessage(message)),
            )),
            Self::Parquet(message) => Error::Parquet(parquet::errors::ParquetError::External(
                Box::new(SharedMessage(message)),
            )),
            Self::ObjectStore(message) => Error::ObjectStore(object_store::Error::Generic {
                store: "metadata singleflight",
                source: Box::new(SharedMessage(message)),
            }),
            Self::SqlParse(error) => Error::SqlParse(error),
            Self::InvalidArgument(message) => Error::InvalidArgument(message),
            Self::Unsupported(message) => Error::Unsupported(message),
            Self::ResourceExhausted(message) => Error::ResourceExhausted(message),
            Self::NativeDiskQuotaExceeded {
                path,
                table,
                current_bytes,
                added_bytes,
                peak_bytes,
                limit_bytes,
            } => Error::NativeDiskQuotaExceeded {
                path,
                table,
                current_bytes,
                added_bytes,
                peak_bytes,
                limit_bytes,
            },
            Self::Cancelled => Error::Cancelled,
            Self::Catalog(message) => Error::Catalog(message),
            Self::TransactionClosed {
                transaction_id,
                state,
            } => Error::TransactionClosed {
                transaction_id,
                state,
            },
            Self::TransactionConflict {
                transaction_id,
                message,
            } => Error::TransactionConflict {
                transaction_id,
                message,
            },
            Self::NativeStorage { path, message } => Error::NativeStorage { path, message },
            Self::NativeFormatUnsupported {
                path,
                found_version,
                current_version,
                alpha,
            } => Error::NativeFormatUnsupported {
                path,
                found_version,
                current_version,
                alpha,
            },
            Self::NativeImportConflict { import_id } => Error::NativeImportConflict { import_id },
            Self::CommitOutcomeUnknown {
                path,
                transaction_id,
                message,
            } => Error::CommitOutcomeUnknown {
                path,
                transaction_id,
                message,
            },
            Self::NativeCommitPostCommitFailure {
                path,
                transaction_id,
                generation,
                message,
            } => Error::NativeCommitPostCommitFailure {
                path,
                transaction_id,
                generation,
                message,
            },
            Self::CopyPostCommitFailure { path, message } => {
                Error::CopyPostCommitFailure { path, message }
            }
            Self::Execution(message) => Error::Execution(message),
            Self::Internal(message) => Error::Internal(message),
        }
    }
}

#[derive(Debug)]
struct SharedMessage(String);

impl fmt::Display for SharedMessage {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl std::error::Error for SharedMessage {}
