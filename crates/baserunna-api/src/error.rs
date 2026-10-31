//! Error responses.

use serde::{Deserialize, Serialize};

/// Body of every error response.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ErrorBody {
    pub error: ApiError,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApiError {
    pub code: ErrorCode,
    #[serde(default)]
    pub message: String,
    /// Whether the same request can succeed if it is sent again later.
    #[serde(default)]
    pub retryable: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub details: Option<Details>,
}

/// Machine-readable error kind. Each code has one HTTP status, except `unavailable`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ErrorCode {
    /// 400: the request is malformed or violates a limit.
    InvalidArgument,
    /// 401: the credentials are missing, expired, or revoked.
    Unauthenticated,
    /// 403
    PermissionDenied,
    /// 404
    NotFound,
    /// 404: the interactive transaction expired, was aborted, or its node changed.
    TxnNotFound,
    /// 409: a concurrent transaction conflicted. A retry can succeed.
    Conflict,
    /// 409
    AlreadyExists,
    /// 409: an earlier attempt with the same idempotency key was aborted. Retry with a new key.
    Aborted,
    /// 412: a `check` or `copy` operation failed; `details.op_index` identifies it.
    PreconditionFailed,
    /// 413
    TooLarge,
    /// 410: the change-feed cursor is older than the retained history.
    CursorExpired,
    /// 507: the storage quota of the database is used.
    QuotaExceeded,
    /// 429: wait `details.retry_after_ms` before the next request.
    RateLimited,
    /// 503: the service cannot complete the request now; `details.outcome` tells whether a
    /// write was applied.
    Unavailable,
    /// 500
    Internal,
    /// A code that this release of the API does not know.
    #[serde(other)]
    Unknown,
}

impl ErrorCode {
    /// Returns the code as it appears in JSON.
    pub fn as_str(self) -> &'static str {
        match self {
            ErrorCode::InvalidArgument => "invalid_argument",
            ErrorCode::Unauthenticated => "unauthenticated",
            ErrorCode::PermissionDenied => "permission_denied",
            ErrorCode::NotFound => "not_found",
            ErrorCode::TxnNotFound => "txn_not_found",
            ErrorCode::Conflict => "conflict",
            ErrorCode::AlreadyExists => "already_exists",
            ErrorCode::Aborted => "aborted",
            ErrorCode::PreconditionFailed => "precondition_failed",
            ErrorCode::TooLarge => "too_large",
            ErrorCode::CursorExpired => "cursor_expired",
            ErrorCode::QuotaExceeded => "quota_exceeded",
            ErrorCode::RateLimited => "rate_limited",
            ErrorCode::Unavailable => "unavailable",
            ErrorCode::Internal => "internal",
            ErrorCode::Unknown => "unknown",
        }
    }
}

/// Additional data of an error. Each field applies to some codes only.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Details {
    /// `precondition_failed`: index of the failed operation in the transaction.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub op_index: Option<usize>,
    /// `unavailable`: node that holds the lease of the database.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lease_holder: Option<String>,
    /// `unavailable`: whether a write of the request was applied.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub outcome: Option<Outcome>,
    /// `rate_limited`: time to wait before the next request.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry_after_ms: Option<u64>,
}

/// Whether a failed request changed data.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Outcome {
    NotApplied,
    /// The request may or may not have been applied.
    Unknown,
}
