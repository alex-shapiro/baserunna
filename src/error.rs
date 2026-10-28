//! Crate-wide error type and HTTP status mapping.

use crate::store::StoreError;
use std::fmt;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Error {
    InvalidArgument(String),
    Unauthenticated(String),
    PermissionDenied(String),
    NotFound(String),
    /// The interactive transaction does not exist on this node or was aborted. Restarting the
    /// transaction can succeed.
    TxnNotFound(String),
    /// Optimistic concurrency conflict; retrying the transaction may succeed.
    Conflict(String),
    AlreadyExists(String),
    /// The commit ID was marked as aborted by a commit status check.
    Aborted(String),
    PreconditionFailed {
        message: String,
        op_index: Option<usize>,
    },
    TooLarge(String),
    /// Change feed cursor predates retained tombstones.
    CursorExpired(String),
    /// Database storage quota exceeded.
    QuotaExceeded(String),
    /// Request rate limit exceeded.
    RateLimited {
        message: String,
        retry_after_ms: u64,
    },
    /// Another node holds this database's lease.
    NotOwner {
        holder: String,
        retry_after_ms: u64,
    },
    Unavailable {
        message: String,
        retry_after_ms: Option<u64>,
        /// A commit may or may not have been applied.
        outcome_unknown: bool,
    },
    Internal(String),
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

impl Error {
    pub fn invalid(m: impl Into<String>) -> Error {
        Error::InvalidArgument(m.into())
    }

    pub fn not_found(m: impl Into<String>) -> Error {
        Error::NotFound(m.into())
    }

    pub fn internal(m: impl Into<String>) -> Error {
        Error::Internal(m.into())
    }

    pub fn txn_not_found() -> Error {
        Error::TxnNotFound("transaction not found (expired, aborted, or its node changed)".into())
    }

    pub fn unavailable(m: impl Into<String>) -> Error {
        Error::Unavailable { message: m.into(), retry_after_ms: None, outcome_unknown: false }
    }

    pub fn code(&self) -> &'static str {
        match self {
            Error::InvalidArgument(_) => "invalid_argument",
            Error::Unauthenticated(_) => "unauthenticated",
            Error::PermissionDenied(_) => "permission_denied",
            Error::NotFound(_) => "not_found",
            Error::TxnNotFound(_) => "txn_not_found",
            Error::Conflict(_) => "conflict",
            Error::AlreadyExists(_) => "already_exists",
            Error::Aborted(_) => "aborted",
            Error::PreconditionFailed { .. } => "precondition_failed",
            Error::TooLarge(_) => "too_large",
            Error::CursorExpired(_) => "cursor_expired",
            Error::QuotaExceeded(_) => "quota_exceeded",
            Error::RateLimited { .. } => "rate_limited",
            Error::NotOwner { .. } | Error::Unavailable { .. } => "unavailable",
            Error::Internal(_) => "internal",
        }
    }

    pub fn status(&self) -> u16 {
        match self {
            Error::InvalidArgument(_) => 400,
            Error::Unauthenticated(_) => 401,
            Error::PermissionDenied(_) => 403,
            Error::NotFound(_) | Error::TxnNotFound(_) => 404,
            Error::Conflict(_) | Error::AlreadyExists(_) | Error::Aborted(_) => 409,
            Error::PreconditionFailed { .. } => 412,
            Error::TooLarge(_) => 413,
            Error::CursorExpired(_) => 410,
            Error::QuotaExceeded(_) => 507,
            Error::RateLimited { .. } => 429,
            Error::NotOwner { .. } | Error::Unavailable { .. } => 503,
            Error::Internal(_) => 500,
        }
    }

    pub fn retryable(&self) -> bool {
        matches!(
            self,
            Error::Conflict(_)
                | Error::TxnNotFound(_)
                | Error::NotOwner { .. }
                | Error::Unavailable { .. }
                | Error::RateLimited { .. }
        )
    }

    pub fn message(&self) -> String {
        match self {
            Error::InvalidArgument(m)
            | Error::Unauthenticated(m)
            | Error::PermissionDenied(m)
            | Error::NotFound(m)
            | Error::TxnNotFound(m)
            | Error::Conflict(m)
            | Error::AlreadyExists(m)
            | Error::Aborted(m)
            | Error::TooLarge(m)
            | Error::CursorExpired(m)
            | Error::QuotaExceeded(m)
            | Error::Internal(m) => m.clone(),
            Error::RateLimited { message, .. } => message.clone(),
            Error::PreconditionFailed { message, .. } => message.clone(),
            Error::NotOwner { holder, .. } => format!("database is leased by node {holder}"),
            Error::Unavailable { message, .. } => message.clone(),
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.code(), self.message())
    }
}

impl std::error::Error for Error {}

impl From<StoreError> for Error {
    fn from(e: StoreError) -> Error {
        match e {
            StoreError::Unavailable(m) => Error::unavailable(format!("object store: {m}")),
            other => Error::Internal(format!("object store: {other}")),
        }
    }
}

impl From<crate::util::codec::DecodeError> for Error {
    fn from(e: crate::util::codec::DecodeError) -> Error {
        Error::Internal(format!("corrupt data: {}", e.0))
    }
}
