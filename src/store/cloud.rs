//! Cloud object stores (S3 and S3-compatible, GCS, Azure Blob) via the
//! `object_store` crate.

use super::*;
use object_store::path::Path;
use object_store::{GetOptions, GetRange, ObjectStore as Os, PutOptions, PutPayload, UpdateVersion};
use std::task::Poll;

pub struct CloudStore {
    inner: Arc<dyn Os>,
    prefix: String,
}

/// Separator between etag and version in our opaque etag string (GCS
/// conditional updates use the object generation, S3 uses the ETag).
const SEP: char = '\u{1f}';

fn map_err(e: object_store::Error) -> StoreError {
    use object_store::Error as E;
    match e {
        E::NotFound { .. } => StoreError::NotFound,
        E::Precondition { .. } | E::AlreadyExists { .. } => StoreError::PreconditionFailed,
        E::NotModified { .. } => StoreError::NotModified,
        E::NotSupported { .. }
        | E::NotImplemented { .. }
        | E::PermissionDenied { .. }
        | E::Unauthenticated { .. }
        | E::InvalidPath { .. }
        | E::UnknownConfigurationKey { .. } => StoreError::Fatal(e.to_string()),
        other => StoreError::Unavailable(other.to_string()),
    }
}

fn join_tag(e_tag: Option<String>, version: Option<String>) -> String {
    format!("{}{SEP}{}", e_tag.unwrap_or_default(), version.unwrap_or_default())
}

fn split_tag(tag: &str) -> (Option<String>, Option<String>) {
    let (a, b) = tag.split_once(SEP).unwrap_or((tag, ""));
    let f = |s: &str| if s.is_empty() { None } else { Some(s.to_string()) };
    (f(a), f(b))
}

impl CloudStore {
    /// Parse `s3://bucket/prefix`, `gs://bucket/prefix` or
    /// `az://container/prefix`. Credentials and endpoints come from the
    /// standard environment variables of each provider (e.g.
    /// `AWS_ACCESS_KEY_ID`, `AWS_ENDPOINT`, `GOOGLE_SERVICE_ACCOUNT`,
    /// `AZURE_STORAGE_ACCOUNT_NAME`).
    pub fn from_url(url: &str) -> Result<CloudStore, String> {
        let (scheme, rest) = url.split_once("://").ok_or("store url must be scheme://bucket[/prefix]")?;
        let (bucket, prefix) = match rest.split_once('/') {
            Some((b, p)) => (b, p.trim_matches('/')),
            None => (rest, ""),
        };
        if bucket.is_empty() {
            return Err("missing bucket".into());
        }
        let inner: Arc<dyn Os> = match scheme {
            "s3" => Arc::new(
                object_store::aws::AmazonS3Builder::from_env()
                    .with_bucket_name(bucket)
                    .build()
                    .map_err(|e| e.to_string())?,
            ),
            "gs" => Arc::new(
                object_store::gcp::GoogleCloudStorageBuilder::from_env()
                    .with_bucket_name(bucket)
                    .build()
                    .map_err(|e| e.to_string())?,
            ),
            "az" => Arc::new(
                object_store::azure::MicrosoftAzureBuilder::from_env()
                    .with_container_name(bucket)
                    .build()
                    .map_err(|e| e.to_string())?,
            ),
            other => return Err(format!("unsupported store scheme {other:?}")),
        };
        Ok(CloudStore { inner, prefix: prefix.to_string() })
    }

    fn path(&self, key: &str) -> Path {
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
                o.data = Bytes::new();
                return Ok(o);
            }
            let o = GetOptions {
                range: opts.range.clone().map(GetRange::Bounded),
                if_none_match: opts.if_none_match.as_deref().map(|t| split_tag(t).0.unwrap_or_default()),
                ..Default::default()
            };
            let res = self.inner.get_opts(&self.path(key), o).await.map_err(map_err)?;
            let size = res.meta.size;
            let etag = join_tag(res.meta.e_tag.clone(), res.meta.version.clone());
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
                PutMode::IfMatch(tag) => {
                    let (e_tag, version) = split_tag(&tag);
                    object_store::PutMode::Update(UpdateVersion { e_tag, version })
                }
            };
            let opts = PutOptions { mode, ..Default::default() };
            let r = self.inner.put_opts(&self.path(key), PutPayload::from(data), opts).await.map_err(map_err)?;
            Ok(join_tag(r.e_tag, r.version))
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
            let dir = prefix.trim_end_matches('/');
            let p = self.path(dir);
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
                    etag: join_tag(m.e_tag, m.version),
                    last_modified_ms: m.last_modified.timestamp_millis().max(0) as u64,
                });
            }
            // Providers list in lexicographic order; enforce it regardless.
            objects.sort_by(|a, b| a.key.cmp(&b.key));
            Ok(ListPage { objects, next })
        })
    }
}
