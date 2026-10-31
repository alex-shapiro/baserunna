use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ErrorBody {
    pub error: ApiError,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApiError {
    pub code: ErrorCode,
    #[serde(default)]
    pub message: String,
    /// True if repeating the same request later can succeed.
    #[serde(default)]
    pub retryable: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub details: Option<ErrorDetails>,
}

/// Machine-readable error code.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ErrorCode {
    /// 400: the request is malformed or exceeds a limit.
    InvalidArgument,
    /// 401: credentials are missing, expired, or revoked.
    Unauthenticated,
    PermissionDenied,
    NotFound,
    /// 404: the interactive transaction expired, was aborted, or its node changed.
    TxnNotFound,
    /// 409: a concurrent transaction conflicted; a retry can succeed.
    Conflict,
    AlreadyExists,
    /// 409: an earlier attempt with the same idempotency key was aborted; retry with a new key.
    Aborted,
    /// 412: a `check` or `copy` operation failed at `details.op_index`.
    PreconditionFailed,
    TooLarge,
    /// 410: the change-feed cursor is older than the retained history.
    CursorExpired,
    /// 507: the database storage quota is full.
    QuotaExceeded,
    /// 429: wait `details.retry_after_ms` before the next request.
    RateLimited,
    /// 503: the service cannot complete the request now; `details.outcome` indicates whether a
    /// write was applied.
    Unavailable,
    Internal,
    /// Error code from a newer server release.
    #[serde(other)]
    Unknown,
}

impl ErrorCode {
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

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ErrorDetails {
    /// Index of the failed operation in a transaction (`precondition_failed`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub op_index: Option<usize>,
    /// Node that holds the database lease (`unavailable`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lease_holder: Option<String>,
    /// Whether a write was applied before the failure (`unavailable`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub outcome: Option<Outcome>,
    /// Delay in milliseconds before the next request (`rate_limited`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry_after_ms: Option<u64>,
}

/// Whether a failed request modified data.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Outcome {
    NotApplied,
    /// The request might have been applied.
    Unknown,
}
