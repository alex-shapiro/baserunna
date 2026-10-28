//! S3-compatible object stores (Tigris) through the `object_store` crate.

use super::*;
use object_store::path::Path;
use object_store::{
    ClientOptions, GetOptions, GetRange, ObjectStore as Os, PutOptions, PutPayload, RetryConfig, UpdateVersion,
};
use std::task::Poll;

/// Bound on one store request, including the transfer of its body.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

pub struct CloudStore {
    inner: Arc<dyn Os>,
    prefix: String,
}

/// Extracts the HTTP status from an `object_store` error message, which does not expose it.
fn http_status(message: &str) -> Option<u16> {
    let rest = &message[message.find("status code: ")? + "status code: ".len()..];
    rest.get(..3)?.parse().ok()
}

fn map_err(e: object_store::Error) -> StoreError {
    use object_store::Error as E;
    match e {
        E::NotFound { .. } => StoreError::NotFound,
        // S3 answers `409 Conflict` to a conditional write that races another one; it was not applied.
        E::Precondition { .. } | E::AlreadyExists { .. } => StoreError::PreconditionFailed,
        E::NotModified { .. } => StoreError::NotModified,
        E::NotSupported { .. }
        | E::NotImplemented { .. }
        | E::PermissionDenied { .. }
        | E::Unauthenticated { .. }
        | E::InvalidPath { .. }
        | E::UnknownConfigurationKey { .. } => StoreError::Fatal(e.to_string()),
        other => {
            let message = other.to_string();
            match http_status(&message) {
                Some(416) => StoreError::Fatal("invalid range".into()),
                Some(s) if (400..500).contains(&s) && s != 408 && s != 429 => StoreError::Fatal(message),
                _ => StoreError::Unavailable(message),
            }
        }
    }
}

impl CloudStore {
    /// Parses `s3://bucket[/prefix]`. Credentials, region and endpoint are read from the
    /// standard `AWS_*` environment variables, as set by `fly storage create` for Tigris.
    ///
    /// The client does not retry: a timed-out or failed request is reported as
    /// [`StoreError::Unavailable`], so that callers can tell an ambiguous conditional write
    /// from a clean conflict.
    pub fn from_url(url: &str) -> Result<CloudStore, String> {
        CloudStore::from_url_with(url, std::iter::empty::<(String, String)>())
    }

    /// Like [`CloudStore::from_url`], with `object_store` S3 settings (such as
    /// `aws_endpoint`) overriding the environment.
    pub fn from_url_with(
        url: &str,
        settings: impl IntoIterator<Item = (impl AsRef<str>, impl Into<String>)>,
    ) -> Result<CloudStore, String> {
        let rest = url.strip_prefix("s3://").ok_or("store url must be s3://bucket[/prefix]")?;
        let (bucket, prefix) = match rest.split_once('/') {
            Some((b, p)) => (b, p.trim_matches('/')),
            None => (rest, ""),
        };
        if bucket.is_empty() {
            return Err("missing bucket".into());
        }
        let options = ClientOptions::new().with_timeout(REQUEST_TIMEOUT).with_connect_timeout(CONNECT_TIMEOUT);
        let mut builder = object_store::aws::AmazonS3Builder::from_env()
            .with_bucket_name(bucket)
            .with_client_options(options)
            .with_retry(RetryConfig { max_retries: 0, ..Default::default() });
        for (k, v) in settings {
            let key = k.as_ref().parse().map_err(|e: object_store::Error| e.to_string())?;
            builder = builder.with_config(key, v);
        }
        let inner = builder.build().map_err(|e| e.to_string())?;
        Ok(CloudStore { inner: Arc::new(inner), prefix: prefix.to_string() })
    }

    fn path(&self, key: &str) -> Path {
        debug_assert!(
            key.chars().all(|c| c == '/' || key_char(c)),
            "object key {key:?} has characters that S3 paths encode"
        );
        if self.prefix.is_empty() { Path::from(key) } else { Path::from(format!("{}/{}", self.prefix, key)) }
    }

    fn strip(&self, p: &Path) -> String {
        let s = p.as_ref();
        if self.prefix.is_empty() {
            s.to_string()
        } else {
            s.strip_prefix(&self.prefix).map(|r| r.trim_start_matches('/').to_string()).unwrap_or_else(|| s.to_string())
        }
    }
}

impl ObjectStore for CloudStore {
    fn get<'a>(&'a self, key: &'a str, opts: GetOpts) -> BoxFut<'a, SResult<GetResult>> {
        Box::pin(async move {
            if let Some(r) = &opts.range
                && r.start == r.end
            {
                // object_store rejects empty ranges; serve them locally.
                let mut o = self.get(key, GetOpts { range: None, ..opts.clone() }).await?;
                clamp_range(&opts.range, o.size)?;
                o.data = Bytes::new();
                return Ok(o);
            }
            let o = GetOptions {
                range: opts.range.clone().map(GetRange::Bounded),
                if_none_match: opts.if_none_match.clone(),
                ..Default::default()
            };
            let res = self.inner.get_opts(&self.path(key), o).await.map_err(map_err)?;
            let size = res.meta.size;
            let etag = res.meta.e_tag.clone().unwrap_or_default();
            let last_modified_ms = res.meta.last_modified.timestamp_millis().max(0) as u64;
            let data = res.bytes().await.map_err(map_err)?;
            Ok(GetResult { data, etag, size, last_modified_ms })
        })
    }

    fn put<'a>(&'a self, key: &'a str, data: Bytes, mode: PutMode) -> BoxFut<'a, SResult<String>> {
        Box::pin(async move {
            let mode = match mode {
                PutMode::Overwrite => object_store::PutMode::Overwrite,
                PutMode::Create => object_store::PutMode::Create,
                PutMode::IfMatch(e_tag) => {
                    object_store::PutMode::Update(UpdateVersion { e_tag: Some(e_tag), version: None })
                }
            };
            let opts = PutOptions { mode, ..Default::default() };
            let r = self.inner.put_opts(&self.path(key), PutPayload::from(data), opts).await.map_err(map_err)?;
            r.e_tag.ok_or_else(|| StoreError::Fatal("store returned no etag".into()))
        })
    }

    fn delete<'a>(&'a self, key: &'a str) -> BoxFut<'a, SResult<()>> {
        Box::pin(async move {
            use object_store::ObjectStoreExt;
            match self.inner.delete(&self.path(key)).await {
                Ok(()) | Err(object_store::Error::NotFound { .. }) => Ok(()),
                Err(e) => Err(map_err(e)),
            }
        })
    }

    fn list<'a>(
        &'a self,
        prefix: &'a str,
        start_after: Option<&'a str>,
        limit: usize,
    ) -> BoxFut<'a, SResult<ListPage>> {
        Box::pin(async move {
            debug_assert!(prefix.is_empty() || prefix.ends_with('/'), "list prefix {prefix:?} is not a directory");
            let p = self.path(prefix.trim_end_matches('/'));
            let mut stream = match start_after {
                Some(s) => self.inner.list_with_offset(Some(&p), &self.path(s)),
                None => self.inner.list(Some(&p)),
            };
            let limit = limit.max(1);
            let mut objects: Vec<ObjectMeta> = Vec::new();
            let mut next = None;
            loop {
                let item = std::future::poll_fn(|cx| match stream.as_mut().poll_next(cx) {
                    Poll::Ready(x) => Poll::Ready(x),
                    Poll::Pending => Poll::Pending,
                })
                .await;
                let Some(item) = item else { break };
                let m = item.map_err(map_err)?;
                let key = self.strip(&m.location);
                if !key.starts_with(prefix) {
                    continue;
                }
                if objects.len() == limit {
                    next = objects.last().map(|o| o.key.clone());
                    break;
                }
                objects.push(ObjectMeta {
                    key,
                    size: m.size,
                    etag: m.e_tag.unwrap_or_default(),
                    last_modified_ms: m.last_modified.timestamp_millis().max(0) as u64,
                });
            }
            objects.sort_by(|a, b| a.key.cmp(&b.key));
            Ok(ListPage { objects, next })
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn statuses_are_read_from_error_messages() {
        let m = "Error performing GET https://x/b/k in 8ms - Server returned non-2xx status code: 416 Range Not Satisfiable: <Error/>";
        assert_eq!(http_status(m), Some(416));
        assert_eq!(http_status("connection reset"), None);
        assert_eq!(http_status("status code: 5"), None);
    }
}
