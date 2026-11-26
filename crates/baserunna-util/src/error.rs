//! Error type for all baserunna crates, with HTTP status mapping.

use axum_core::response::{IntoResponse, Response};
use baserunna_api::{self as api, ErrorCode};
use http::header::{CONTENT_TYPE, RETRY_AFTER};
use http::{HeaderValue, StatusCode};
use std::fmt;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Error {
    InvalidArgument(String),
    Unauthenticated(String),
    PermissionDenied(String),
    NotFound(String),
    /// Resource not found, with the missing resource kind.
    Missing(baserunna_api::Missing, String),
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

    pub fn missing(kind: baserunna_api::Missing, m: impl Into<String>) -> Error {
        Error::Missing(kind, m.into())
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

    /// Converts the error to the `error` object of an HTTP error response.
    pub fn to_api_error(&self) -> api::ApiError {
        let details = |d: api::ErrorDetails| Some(d);
        let details = match self {
            Error::PreconditionFailed { op_index: Some(i), .. } => {
                details(api::ErrorDetails { op_index: Some(*i), ..api::ErrorDetails::default() })
            }
            Error::NotOwner { holder, .. } => {
                details(api::ErrorDetails { lease_holder: Some(holder.clone()), ..api::ErrorDetails::default() })
            }
            Error::Unavailable { outcome_unknown, .. } => {
                let outcome = if *outcome_unknown { api::Outcome::Unknown } else { api::Outcome::NotApplied };
                details(api::ErrorDetails { outcome: Some(outcome), ..api::ErrorDetails::default() })
            }
            Error::RateLimited { retry_after_ms, .. } => {
                details(api::ErrorDetails { retry_after_ms: Some(*retry_after_ms), ..api::ErrorDetails::default() })
            }
            Error::Missing(kind, _) => {
                details(api::ErrorDetails { missing: Some(*kind), ..api::ErrorDetails::default() })
            }
            _ => None,
        };
        api::ApiError { code: self.api_code(), message: self.message(), retryable: self.retryable(), details }
    }

    pub fn code(&self) -> &'static str {
        self.api_code().as_str()
    }

    pub fn api_code(&self) -> ErrorCode {
        match self {
            Error::InvalidArgument(_) => ErrorCode::InvalidArgument,
            Error::Unauthenticated(_) => ErrorCode::Unauthenticated,
            Error::PermissionDenied(_) => ErrorCode::PermissionDenied,
            Error::NotFound(_) | Error::Missing(..) => ErrorCode::NotFound,
            Error::TxnNotFound(_) => ErrorCode::TxnNotFound,
            Error::Conflict(_) => ErrorCode::Conflict,
            Error::AlreadyExists(_) => ErrorCode::AlreadyExists,
            Error::Aborted(_) => ErrorCode::Aborted,
            Error::PreconditionFailed { .. } => ErrorCode::PreconditionFailed,
            Error::TooLarge(_) => ErrorCode::TooLarge,
            Error::CursorExpired(_) => ErrorCode::CursorExpired,
            Error::QuotaExceeded(_) => ErrorCode::QuotaExceeded,
            Error::RateLimited { .. } => ErrorCode::RateLimited,
            Error::NotOwner { .. } | Error::Unavailable { .. } => ErrorCode::Unavailable,
            Error::Internal(_) => ErrorCode::Internal,
        }
    }

    pub fn status(&self) -> u16 {
        match self {
            Error::InvalidArgument(_) => 400,
            Error::Unauthenticated(_) => 401,
            Error::PermissionDenied(_) => 403,
            Error::NotFound(_) | Error::Missing(..) | Error::TxnNotFound(_) => 404,
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

    /// Returns true if the failed request may have been applied.
    pub fn may_have_been_applied(&self) -> bool {
        matches!(self, Error::Unavailable { outcome_unknown: true, .. } | Error::Internal(_))
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
            Error::Missing(_, m) => m.clone(),
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

impl IntoResponse for Error {
    fn into_response(self) -> Response {
        let status = StatusCode::from_u16(self.status()).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
        let retry_after = match &self {
            Error::NotOwner { retry_after_ms, .. } | Error::RateLimited { retry_after_ms, .. } => Some(*retry_after_ms),
            Error::Unavailable { retry_after_ms, .. } => *retry_after_ms,
            _ => None,
        };
        if let Error::Internal(m) = &self {
            crate::warn!("internal error: {m}");
        }
        let body = serde_json::to_vec(&api::ErrorBody { error: self.to_api_error() }).expect("errors serialize");
        let mut resp = (status, [(CONTENT_TYPE, HeaderValue::from_static("application/json"))], body).into_response();
        if let Some(ms) = retry_after {
            let secs = ms.div_ceil(1000).max(1);
            if let Ok(v) = HeaderValue::from_str(&secs.to_string()) {
                resp.headers_mut().insert(RETRY_AFTER, v);
            }
        }
        resp
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.code(), self.message())
    }
}

impl std::error::Error for Error {}

impl From<crate::codec::DecodeError> for Error {
    fn from(e: crate::codec::DecodeError) -> Error {
        Error::Internal(format!("corrupt data: {}", e.0))
    }
}
