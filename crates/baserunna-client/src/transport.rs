//! The seam between [`Client`](crate::Client) and the network.

use bytes::Bytes;
use std::fmt;
use std::future::Future;
use std::pin::Pin;

pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Sends HTTP requests to a Baserunna server.
///
/// The URI of each request holds only the path and the query string; the transport adds the
/// server address. Tests implement this trait to route requests through a simulated network.
pub trait Transport: Send + Sync {
    fn send(&self, req: http::Request<Bytes>) -> BoxFuture<'_, Result<http::Response<Bytes>, TransportError>>;
}

/// A request that did not receive a complete response.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TransportError {
    /// The request did not reach the server, for example because the connection failed.
    NotSent(String),
    /// The request may have reached the server before the failure.
    Unknown(String),
    /// The request exceeded the deadline of the transport. The client does not send it again,
    /// because the next attempt would wait as long.
    TimedOut(String),
}

impl fmt::Display for TransportError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TransportError::NotSent(m) => write!(f, "not sent: {m}"),
            TransportError::Unknown(m) => write!(f, "failed: {m}"),
            TransportError::TimedOut(m) => write!(f, "timed out: {m}"),
        }
    }
}

impl std::error::Error for TransportError {}

#[cfg(feature = "reqwest")]
pub use http_transport::HttpTransport;

#[cfg(feature = "reqwest")]
mod http_transport {
    use super::{BoxFuture, Transport, TransportError};
    use bytes::Bytes;
    use std::time::Duration;

    /// HTTP and HTTPS transport.
    pub struct HttpTransport {
        base: String,
        client: reqwest::Client,
    }

    impl HttpTransport {
        /// Creates a transport for the server at `base_url`, such as `https://example.com`.
        ///
        /// A request fails with [`TransportError::TimedOut`] after 90 s, which exceeds the
        /// longest long poll of the server.
        pub fn new(base_url: &str) -> HttpTransport {
            let client = reqwest::Client::builder()
                .connect_timeout(Duration::from_secs(10))
                .timeout(Duration::from_secs(90))
                .pool_idle_timeout(Duration::from_secs(50))
                .build()
                .expect("an HTTP client");
            HttpTransport::with_client(base_url, client)
        }

        /// Creates a transport that sends requests with `client`.
        pub fn with_client(base_url: &str, client: reqwest::Client) -> HttpTransport {
            HttpTransport { base: base_url.trim_end_matches('/').to_string(), client }
        }
    }

    impl Transport for HttpTransport {
        fn send(&self, req: http::Request<Bytes>) -> BoxFuture<'_, Result<http::Response<Bytes>, TransportError>> {
            Box::pin(async move {
                let (parts, body) = req.into_parts();
                let url = format!("{}{}", self.base, parts.uri);
                let resp =
                    self.client.request(parts.method, url).headers(parts.headers).body(body).send().await.map_err(
                        |e| {
                            // A connection that timed out sent nothing, so it is not a request timeout.
                            if e.is_connect() || e.is_builder() {
                                TransportError::NotSent(e.to_string())
                            } else if e.is_timeout() {
                                TransportError::TimedOut(e.to_string())
                            } else {
                                TransportError::Unknown(e.to_string())
                            }
                        },
                    )?;
                let mut out = http::Response::builder().status(resp.status());
                if let Some(h) = out.headers_mut() {
                    *h = resp.headers().clone();
                }
                let body = resp.bytes().await.map_err(|e| {
                    if e.is_timeout() {
                        TransportError::TimedOut(e.to_string())
                    } else {
                        TransportError::Unknown(e.to_string())
                    }
                })?;
                out.body(body).map_err(|e| TransportError::Unknown(e.to_string()))
            })
        }
    }
}
