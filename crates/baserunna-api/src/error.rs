use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;

/// The body of an error response of an API built on [`crate::Endpoint`]: Baserunna's, with its
/// [`ErrorCode`] and [`DbDetails`], or another API's, with its own code and details.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(bound(
    serialize = "C: Serialize, X: Serialize",
    deserialize = "C: Deserialize<'de>, X: Default + Deserialize<'de>"
))]
pub struct ErrorBody<C = ErrorCode, X = DbDetails> {
    pub error: ApiError<C, X>,
}

/// An API error: the `error` object of an error response.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(bound(
    serialize = "C: Serialize, X: Serialize",
    deserialize = "C: Deserialize<'de>, X: Default + Deserialize<'de>"
))]
pub struct ApiError<C = ErrorCode, X = DbDetails> {
    pub code: C,
    #[serde(default)]
    pub message: String,
    /// True if repeating the same request later can succeed.
    #[serde(default)]
    pub retryable: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub details: Option<ErrorDetails<X>>,
}

impl<C: Code, X> ApiError<C, X> {
    /// An error with `code` and `message`, which is retryable as [`Code::transient`] says.
    pub fn new(code: C, message: impl Into<String>) -> ApiError<C, X> {
        ApiError { code, message: message.into(), retryable: code.transient(), details: None }
    }

    /// The details, which an error without them has as the default.
    pub fn details_mut(&mut self) -> &mut ErrorDetails<X>
    where
        X: Default,
    {
        self.details.get_or_insert_with(ErrorDetails::default)
    }

    /// Whether the failed request may have been applied: `details.outcome`, or what
    /// [`Code::outcome`] says of the code.
    pub fn outcome(&self) -> Outcome {
        self.details.as_ref().and_then(|d| d.outcome).unwrap_or(self.code.outcome())
    }
}

/// The code of an API error, which a client reads to decide whether to send a request again.
/// [`ErrorCode`] is Baserunna's; another API built on [`crate::Endpoint`] defines its own.
pub trait Code: Copy + Eq + Debug + Serialize + DeserializeOwned + Send + Sync + 'static {
    /// The name in JSON.
    fn as_str(self) -> &'static str;

    /// Whether a repeat of a request that failed with this code can succeed.
    fn transient(self) -> bool;

    /// Whether a request that failed with this code may have been applied, for an error without
    /// `details.outcome`.
    fn outcome(self) -> Outcome;

    /// Whether this refusal of a request with an idempotency key proves that no earlier attempt
    /// with the key can still apply.
    fn fences_earlier_attempts(self) -> bool;
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

    /// Whether this refusal of a write with an idempotency key proves that no earlier attempt
    /// with the key can still apply. Before it answers `precondition_failed` for a keyed write,
    /// the server replays an earlier attempt that committed or aborts the key, and `aborted`
    /// names an aborted key. Every other error leaves the outcome of an earlier attempt with an
    /// unknown outcome unknown: that attempt may still commit.
    pub const fn fences_earlier_attempts(self) -> bool {
        matches!(self, ErrorCode::PreconditionFailed | ErrorCode::Aborted)
    }
}

impl Code for ErrorCode {
    fn as_str(self) -> &'static str {
        ErrorCode::as_str(self)
    }

    fn transient(self) -> bool {
        matches!(self, ErrorCode::Conflict | ErrorCode::RateLimited | ErrorCode::Unavailable | ErrorCode::Internal)
    }

    /// An `unavailable` error says its outcome in `details.outcome`; one without it was not
    /// applied.
    fn outcome(self) -> Outcome {
        match self {
            ErrorCode::Internal | ErrorCode::Unknown => Outcome::Unknown,
            _ => Outcome::NotApplied,
        }
    }

    fn fences_earlier_attempts(self) -> bool {
        ErrorCode::fences_earlier_attempts(self)
    }
}

/// The details of an error: those that a client of any API built on [`crate::Endpoint`] reads,
/// and the details `X` of one API, in the same JSON object.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(bound(serialize = "X: Serialize", deserialize = "X: Default + Deserialize<'de>"))]
pub struct ErrorDetails<X = DbDetails> {
    /// Whether a write was applied before the failure (`unavailable`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub outcome: Option<Outcome>,
    /// Delay in milliseconds before the next request (`rate_limited`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry_after_ms: Option<u64>,
    #[serde(flatten)]
    pub api: X,
}

/// The details of a Baserunna error that only Baserunna has.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DbDetails {
    /// Index of the failed operation in a transaction (`precondition_failed`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub op_index: Option<usize>,
    /// Node that holds the database lease (`unavailable`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lease_holder: Option<String>,
}

/// Whether a failed request modified data.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Outcome {
    NotApplied,
    /// The request might have been applied.
    Unknown,
}
