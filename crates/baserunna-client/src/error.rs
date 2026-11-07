use crate::TransportError;
use baserunna_api::{ApiError, Code, DbDetails, ErrorCode, Outcome};
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::fmt;

/// The details of an error that one API adds to [`baserunna_api::ErrorDetails`], such as
/// Baserunna's [`DbDetails`].
pub trait ErrorExtra: Clone + fmt::Debug + Default + Serialize + DeserializeOwned + Send + Sync + 'static {}

impl<X: Clone + fmt::Debug + Default + Serialize + DeserializeOwned + Send + Sync + 'static> ErrorExtra for X {}

/// Request failure after all retry attempts, from an API whose errors have the code `C` and the
/// details `X`: Baserunna's by default.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Error<C = ErrorCode, X = DbDetails> {
    /// The server returned an error response.
    Api {
        status: u16,
        error: ApiError<C, X>,
        /// Delay before the next request, from `details.retry_after_ms` or the `Retry-After`
        /// header.
        retry_after_ms: Option<u64>,
    },
    /// Non-API HTTP response, such as a proxy error page or captive portal.
    Foreign {
        status: u16,
        retry_after_ms: Option<u64>,
        path: String,
    },
    /// JSON response with a 2xx status whose body does not match the expected schema.
    Malformed {
        path: String,
        reason: String,
    },
    Transport(TransportError),
}

impl<C: Code, X> Error<C, X> {
    pub fn code(&self) -> Option<C> {
        match self {
            Error::Api { error, .. } => Some(error.code),
            _ => None,
        }
    }

    /// Returns whether the failed request might have been applied.
    pub fn outcome(&self) -> Outcome {
        let unknown = match self {
            Error::Api { error, .. } => error.outcome() == Outcome::Unknown,
            Error::Foreign { status, .. } => matches!(status, 200..=299 | 408 | 429 | 500..),
            Error::Malformed { .. } => true,
            Error::Transport(t) => !matches!(t, TransportError::NotSent(_)),
        };
        if unknown { Outcome::Unknown } else { Outcome::NotApplied }
    }

    pub fn retry_after_ms(&self) -> Option<u64> {
        match self {
            Error::Api { retry_after_ms, .. } | Error::Foreign { retry_after_ms, .. } => *retry_after_ms,
            _ => None,
        }
    }

    /// Returns true if repeating the request later can succeed.
    ///
    /// If `idempotent` is false, errors with [`Outcome::Unknown`] return false.
    pub fn is_transient(&self, idempotent: bool) -> bool {
        let may_repeat = idempotent || self.outcome() == Outcome::NotApplied;
        match self {
            Error::Api { error, .. } => error.code.transient() && may_repeat,
            Error::Foreign { status, .. } => matches!(status, 200..=299 | 407 | 408 | 429 | 500..) && may_repeat,
            Error::Malformed { .. } => false,
            Error::Transport(TransportError::TimedOut(_)) => false,
            Error::Transport(_) => may_repeat,
        }
    }
}

impl<C: Code, X> fmt::Display for Error<C, X> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Api { status, error, .. } => {
                write!(f, "HTTP {status} {}: {}", error.code.as_str(), error.message)
            }
            Error::Foreign { status, path, .. } => write!(f, "HTTP {status} from {path} without an API response"),
            Error::Malformed { path, reason } => write!(f, "unexpected response from {path}: {reason}"),
            Error::Transport(t) => t.fmt(f),
        }
    }
}

impl<C: Code, X: fmt::Debug> std::error::Error for Error<C, X> {}

impl<C, X> From<TransportError> for Error<C, X> {
    fn from(e: TransportError) -> Error<C, X> {
        Error::Transport(e)
    }
}
