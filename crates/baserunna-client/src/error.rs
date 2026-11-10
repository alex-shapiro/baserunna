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
        /// Retry delay in milliseconds from `details.retry_after_ms` or the `Retry-After` header.
        retry_after_ms: Option<u64>,
    },
    /// Non-API HTTP response.
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
    /// Operation result that cannot be decoded as the requested type.
    Decode {
        target: String,
        reason: String,
    },
    /// Request error whose unknown outcome was resolved as not applied.
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

    /// Returns whether the server may have applied the failed request.
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

    /// Returns the retry delay in milliseconds requested by the server or proxy.
    pub fn retry_after_ms(&self) -> Option<u64> {
        match self {
            Error::Api { retry_after_ms, .. } | Error::Foreign { retry_after_ms, .. } => *retry_after_ms,
            Error::NotApplied(e) => e.retry_after_ms(),
            _ => None,
        }
    }

    /// Returns true if the client can retry the failed request.
    ///
    /// Returns false for [`Outcome::Unknown`] if `idempotent` is false.
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
            Error::NotApplied(e) => e.is_transient(true),
            Error::Transport(TransportError::TimedOut(_)) => false,
            Error::Transport(_) => may_repeat,
        }
    }
}

impl Error {
    /// Returns true if [`Db::retry`](crate::Db::retry) can restart the transaction after this
    /// error.
    pub fn restarts_txn(&self) -> bool {
        let timed_out = matches!(
            self,
            Error::NotApplied(inner) if matches!(**inner, Error::Transport(TransportError::TimedOut(_)))
        );
        matches!(self.code(), Some(ErrorCode::TxnNotFound | ErrorCode::Aborted))
            || self.is_transient(false)
            || timed_out
    }

    /// Shifts `op_index` back by `n` queued operations, or clears it if it points into the first
    /// `n`.
    pub(crate) fn without_ops(self, n: usize) -> Error {
        match self {
            Error::Api { status, mut error, retry_after_ms } => {
                if let Some(d) = &mut error.details {
                    d.op_index = d.op_index.and_then(|i| i.checked_sub(n));
                }
                Error::Api { status, error, retry_after_ms }
            }
            Error::NotApplied(e) => Error::NotApplied(Box::new(e.without_ops(n))),
            other => other,
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
            Error::Decode { target, reason } => write!(f, "cannot decode {target}: {reason}"),
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
