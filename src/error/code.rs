use std::fmt;

use serde::{Deserialize, Serialize};

/// Stable, machine-readable category for a RustDB error.
///
/// New codes may be added during the beta series. Existing textual values are
/// covered by the public compatibility policy and are not derived from error
/// messages.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[non_exhaustive]
pub enum ErrorCode {
    Io,
    Arrow,
    Parquet,
    ObjectStore,
    SqlParse,
    InvalidArgument,
    Unsupported,
    ResourceExhausted,
    AdmissionQueueFull,
    AdmissionResourceLimit,
    AdmissionUnavailable,
    AuthenticationRequired,
    AuthenticationInvalid,
    AuthorizationForbidden,
    IdempotencyInvalidKey,
    IdempotencyKeyConflict,
    IdempotencyRequired,
    NativeDiskQuotaExceeded,
    Cancelled,
    QueryActive,
    QueryNoResult,
    QueryNotComplete,
    QueryNotFound,
    QueryTimeout,
    QueryServerRestarted,
    QueryResultInvalidated,
    QueryResultUnavailable,
    QueryStateConflict,
    QueryJournalFailed,
    QueryDeleteFailed,
    RequestBodyTooLarge,
    RequestContentType,
    RequestInvalidJson,
    RequestInvalidQuery,
    RequestMethodNotAllowed,
    RequestNotFound,
    RequestTooLarge,
    ServerShuttingDown,
    Catalog,
    TransactionClosed,
    TransactionConflict,
    NativeStorage,
    NativeFormatUnsupported,
    NativeRepairRefused,
    NativeImportConflict,
    CommitOutcomeUnknown,
    NativeCommitPostCommitFailure,
    CopyPostCommitFailure,
    Execution,
    Internal,
}

impl ErrorCode {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Io => "storage.io",
            Self::Arrow => "execution.arrow",
            Self::Parquet => "storage.parquet",
            Self::ObjectStore => "storage.object_store",
            Self::SqlParse => "sql.parse",
            Self::InvalidArgument => "request.invalid",
            Self::Unsupported => "sql.unsupported",
            Self::ResourceExhausted => "query.resource_exhausted",
            Self::AdmissionQueueFull => "admission.queue_full",
            Self::AdmissionResourceLimit => "admission.resource_limit",
            Self::AdmissionUnavailable => "admission.unavailable",
            Self::AuthenticationRequired => "auth.required",
            Self::AuthenticationInvalid => "auth.invalid",
            Self::AuthorizationForbidden => "auth.forbidden",
            Self::IdempotencyInvalidKey => "idempotency.invalid_key",
            Self::IdempotencyKeyConflict => "idempotency.key_conflict",
            Self::IdempotencyRequired => "idempotency.required",
            Self::NativeDiskQuotaExceeded => "native.disk_quota_exceeded",
            Self::Cancelled => "query.cancelled",
            Self::QueryActive => "query.active",
            Self::QueryNoResult => "query.no_result",
            Self::QueryNotComplete => "query.not_complete",
            Self::QueryNotFound => "query.not_found",
            Self::QueryTimeout => "query.timeout",
            Self::QueryServerRestarted => "query.server_restarted",
            Self::QueryResultInvalidated => "query.result_invalidated",
            Self::QueryResultUnavailable => "query.result_unavailable",
            Self::QueryStateConflict => "query.state_conflict",
            Self::QueryJournalFailed => "query.journal_failed",
            Self::QueryDeleteFailed => "query.delete_failed",
            Self::RequestBodyTooLarge => "request.body_too_large",
            Self::RequestContentType => "request.content_type",
            Self::RequestInvalidJson => "request.invalid_json",
            Self::RequestInvalidQuery => "request.invalid_query",
            Self::RequestMethodNotAllowed => "request.method_not_allowed",
            Self::RequestNotFound => "request.not_found",
            Self::RequestTooLarge => "request.too_large",
            Self::ServerShuttingDown => "server.shutting_down",
            Self::Catalog => "sql.catalog",
            Self::TransactionClosed => "transaction.closed",
            Self::TransactionConflict => "transaction.conflict",
            Self::NativeStorage => "native.storage",
            Self::NativeFormatUnsupported => "native.format_unsupported",
            Self::NativeRepairRefused => "native.repair_refused",
            Self::NativeImportConflict => "native.import_conflict",
            Self::CommitOutcomeUnknown => "transaction.outcome_unknown",
            Self::NativeCommitPostCommitFailure => "transaction.post_commit_failure",
            Self::CopyPostCommitFailure => "copy.post_commit_failure",
            Self::Execution => "execution.failed",
            Self::Internal => "server.internal",
        }
    }
}

impl fmt::Display for ErrorCode {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// Whether retrying the same operation is safe without additional recovery.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[non_exhaustive]
#[serde(rename_all = "snake_case")]
pub enum RetryClass {
    Never,
    Safe,
    ReopenRequired,
    OutcomeUnknown,
    #[default]
    Unknown,
}

impl RetryClass {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Never => "never",
            Self::Safe => "safe",
            Self::ReopenRequired => "reopen_required",
            Self::OutcomeUnknown => "outcome_unknown",
            Self::Unknown => "unknown",
        }
    }
}

impl fmt::Display for RetryClass {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}
