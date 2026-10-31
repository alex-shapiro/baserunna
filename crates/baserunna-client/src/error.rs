use crate::TransportError;
use baserunna_api::{ApiError, ErrorCode, Outcome};
use std::fmt;

/// A request that failed after all attempts.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Error {
    /// The server returned an error response.
    Api {
        status: u16,
        error: ApiError,
        /// Time to wait before the next request, from `details.retry_after_ms` or the
        /// `Retry-After` header.
        retry_after_ms: Option<u64>,
    },
    /// A response that the API did not produce, such as an error page of a proxy or a
    /// captive portal.
    Foreign {
        status: u16,
        retry_after_ms: Option<u64>,
        path: String,
    },
    /// A JSON response with a success status whose body does not match the API.
    Malformed {
        path: String,
        reason: String,
    },
    Transport(TransportError),
}

impl Error {
    /// Returns the error code of an [`Error::Api`].
    pub fn code(&self) -> Option<ErrorCode> {
        match self {
            Error::Api { error, .. } => Some(error.code),
            _ => None,
        }
    }

    /// Checks whether the failed request may have been applied.
    pub fn outcome(&self) -> Outcome {
        let unknown = match self {
            Error::Api { error, .. } => match error.code {
                ErrorCode::Unavailable => error.details.as_ref().and_then(|d| d.outcome) == Some(Outcome::Unknown),
                ErrorCode::Internal | ErrorCode::Unknown => true,
                _ => false,
            },
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

    /// Checks whether the same request can succeed later. An idempotent request can be sent
    /// again after a failure with an unknown outcome.
    pub fn is_transient(&self, idempotent: bool) -> bool {
        let may_repeat = idempotent || self.outcome() == Outcome::NotApplied;
        match self {
            Error::Api { error, .. } => match error.code {
                ErrorCode::Conflict | ErrorCode::RateLimited | ErrorCode::Unavailable | ErrorCode::Internal => {
                    may_repeat
                }
                _ => false,
            },
            Error::Foreign { status, .. } => matches!(status, 200..=299 | 407 | 408 | 429 | 500..) && may_repeat,
            Error::Malformed { .. } => false,
            Error::Transport(TransportError::TimedOut(_)) => false,
            Error::Transport(_) => may_repeat,
        }
    }
}

impl fmt::Display for Error {
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

impl std::error::Error for Error {}

impl From<TransportError> for Error {
    fn from(e: TransportError) -> Error {
        Error::Transport(e)
    }
}
