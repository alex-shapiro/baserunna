use bytes::Bytes;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// HTTP transport for sending requests to a Baserunna server.
///
/// Request URIs contain only the path and query string; the transport prepends the server URL.
pub trait Transport: Send + Sync {
    fn send(&self, req: http::Request<Bytes>) -> BoxFuture<'_, Result<http::Response<Bytes>, TransportError>>;
}

impl<T: Transport + ?Sized> Transport for Arc<T> {
    fn send(&self, req: http::Request<Bytes>) -> BoxFuture<'_, Result<http::Response<Bytes>, TransportError>> {
        (**self).send(req)
    }
}

/// Request extension with the route template of the endpoint, such as
/// `/v1/dbs/{db}/transactions/{id}/commit`, for transports that record per-endpoint metrics.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Route(pub &'static str);

/// Transport-level failure before receiving a complete HTTP response.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TransportError {
    /// The request was not sent to the server, for example because the connection failed.
    NotSent(String),
    /// The request might have reached the server before failing.
    Unknown(String),
    /// The request exceeded the transport timeout and is not retried.
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

    /// HTTP and HTTPS transport backed by `reqwest`.
    pub struct HttpTransport {
        base: String,
        client: reqwest::Client,
    }

    impl HttpTransport {
        /// Creates a transport for `base_url` with a 10 s connect timeout and a 90 s request
        /// timeout.
        pub fn new(base_url: &str) -> HttpTransport {
            let client = reqwest::Client::builder()
                .connect_timeout(Duration::from_secs(10))
                .timeout(Duration::from_secs(90))
                .pool_idle_timeout(Duration::from_secs(50))
                .build()
                .expect("an HTTP client");
            HttpTransport::with_client(base_url, client)
        }

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
                            // Check `is_connect` before `is_timeout` because a connect timeout is also a timeout, but nothing was sent.
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
