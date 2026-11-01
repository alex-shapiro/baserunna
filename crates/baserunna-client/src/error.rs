use crate::TransportError;
use baserunna_api::{ApiError, ErrorCode, Outcome};
use std::fmt;

/// Request failure after all retry attempts.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Error {
    /// The server returned an error response.
    Api {
        status: u16,
        error: ApiError,
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
    /// An operation result does not decode as the requested type.
    Decode {
        what: String,
        reason: String,
    },
    /// A request failed with an unknown outcome, and the server confirmed that it was not applied
    /// and never will be.
    NotApplied(Box<Error>),
    Transport(TransportError),
}

impl Error {
    pub fn code(&self) -> Option<ErrorCode> {
        match self {
            Error::Api { error, .. } => Some(error.code),
            Error::NotApplied(e) => e.code(),
            _ => None,
        }
    }

    /// Returns whether the failed request might have been applied.
    pub fn outcome(&self) -> Outcome {
        let unknown = match self {
            Error::Api { error, .. } => match error.code {
                ErrorCode::Unavailable => error.details.as_ref().and_then(|d| d.outcome) == Some(Outcome::Unknown),
                ErrorCode::Internal | ErrorCode::Unknown => true,
                _ => false,
            },
            Error::Foreign { status, .. } => matches!(status, 200..=299 | 408 | 429 | 500..),
            Error::Malformed { .. } => true,
            Error::Decode { .. } | Error::NotApplied(_) => false,
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
            Error::Api { error, .. } => match error.code {
                ErrorCode::Conflict | ErrorCode::RateLimited | ErrorCode::Unavailable | ErrorCode::Internal => {
                    may_repeat
                }
                _ => false,
            },
            Error::Foreign { status, .. } => matches!(status, 200..=299 | 407 | 408 | 429 | 500..) && may_repeat,
            Error::Malformed { .. } | Error::Decode { .. } => false,
            Error::NotApplied(_) => true,
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
            Error::Decode { what, reason } => write!(f, "cannot decode {what}: {reason}"),
            Error::NotApplied(e) => write!(f, "{e} (confirmed not applied)"),
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
