//! Local filesystem object store for single-node development.
//!
//! Each object is a file prefixed with a 16-byte random etag. Writes rename a
//! temporary file into place under a store-owned mutex, and the store holds an
//! exclusive lock on its root directory so no second `FsStore` can open it.

use super::*;
use parking_lot::Mutex;
use std::path::{Path, PathBuf};

const ETAG_LEN: usize = 16;
const LOCK_FILE: &str = ".lock";

pub struct FsStore {
    root: PathBuf,
    cas_lock: Arc<Mutex<()>>,
    _dir_lock: std::fs::File,
}

impl FsStore {
    /// Open or create a store in `root`. Fails if another `FsStore` holds the directory lock.
    pub fn new(root: impl Into<PathBuf>) -> std::io::Result<FsStore> {
        let root = root.into();
        std::fs::create_dir_all(&root)?;
        let lock = std::fs::OpenOptions::new().create(true).truncate(false).write(true).open(root.join(LOCK_FILE))?;
        match lock.try_lock() {
            Ok(()) => {}
            Err(std::fs::TryLockError::WouldBlock) => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::WouldBlock,
                    format!("data directory {} is in use by another process", root.display()),
                ));
            }
            Err(std::fs::TryLockError::Error(e)) => return Err(e),
        }
        Ok(FsStore { root, cas_lock: Arc::new(Mutex::new(())), _dir_lock: lock })
    }

    fn path(&self, key: &str) -> SResult<PathBuf> {
        if key.is_empty()
            || key.split('/').any(|seg| seg.is_empty() || seg == "." || seg == "..")
            || key.contains('\\')
            || key.contains('\0')
        {
            return Err(StoreError::Fatal(format!("invalid object key {key:?}")));
        }
        Ok(self.root.join(key))
    }

    fn read_obj(path: &Path) -> SResult<(String, Vec<u8>, u64)> {
        let data = match std::fs::read(path) {
            Ok(d) => d,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Err(StoreError::NotFound),
            Err(e) => return Err(StoreError::Unavailable(e.to_string())),
        };
        if data.len() < ETAG_LEN {
            return Err(StoreError::Fatal("corrupt object file".into()));
        }
        let mtime = std::fs::metadata(path)
            .and_then(|m| m.modified())
            .ok()
            .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        let etag = baserunna_api::b64::hex(&data[..ETAG_LEN]);
        Ok((etag, data[ETAG_LEN..].to_vec(), mtime))
    }

    fn current_etag(path: &Path) -> SResult<Option<String>> {
        use std::io::Read;
        match std::fs::File::open(path) {
            Ok(mut f) => {
                let mut b = [0u8; ETAG_LEN];
                f.read_exact(&mut b).map_err(|e| StoreError::Fatal(e.to_string()))?;
                Ok(Some(baserunna_api::b64::hex(&b)))
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(StoreError::Unavailable(e.to_string())),
        }
    }

    fn put_blocking(root: &Path, cas_lock: &Mutex<()>, path: &Path, data: &[u8], mode: &PutMode) -> SResult<String> {
        let mut tag = [0u8; ETAG_LEN];
        aws_lc_rs::rand::fill(&mut tag).map_err(|_| StoreError::Fatal("rng".into()))?;
        let dir = path.parent().unwrap_or(root);
        std::fs::create_dir_all(dir).map_err(|e| StoreError::Unavailable(e.to_string()))?;
        let tmp = dir.join(format!(".tmp-{}", baserunna_api::b64::hex(&tag)));
        let mut buf = Vec::with_capacity(ETAG_LEN + data.len());
        buf.extend_from_slice(&tag);
        buf.extend_from_slice(data);
        std::fs::write(&tmp, &buf).map_err(|e| StoreError::Unavailable(e.to_string()))?;
        let _g = cas_lock.lock();
        let ok = match mode {
            PutMode::Overwrite => true,
            PutMode::Create => Self::current_etag(path)?.is_none(),
            PutMode::IfMatch(e) => Self::current_etag(path)?.as_deref() == Some(e.as_str()),
        };
        if !ok {
            let _ = std::fs::remove_file(&tmp);
            return Err(StoreError::PreconditionFailed);
        }
        std::fs::rename(&tmp, path).map_err(|e| StoreError::Unavailable(e.to_string()))?;
        Ok(baserunna_api::b64::hex(&tag))
    }

    fn list_blocking(root: &Path, prefix: &str, start_after: Option<&str>, limit: usize) -> SResult<ListPage> {
        // Walk from the deepest directory fully contained in the prefix.
        let dir_part = match prefix.rfind('/') {
            Some(i) => &prefix[..i],
            None => "",
        };
        let start_dir = if dir_part.is_empty() { root.to_path_buf() } else { root.join(dir_part) };
        let mut keys = Vec::new();
        let mut stack = vec![start_dir];
        while let Some(d) = stack.pop() {
            let rd = match std::fs::read_dir(&d) {
                Ok(rd) => rd,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
                Err(e) => return Err(StoreError::Unavailable(e.to_string())),
            };
            for ent in rd {
                let ent = ent.map_err(|e| StoreError::Unavailable(e.to_string()))?;
                let p = ent.path();
                let name = ent.file_name().to_string_lossy().to_string();
                if name.starts_with(".tmp-") || name == LOCK_FILE {
                    continue;
                }
                let ft = ent.file_type().map_err(|e| StoreError::Unavailable(e.to_string()))?;
                if ft.is_dir() {
                    stack.push(p);
                } else if let Ok(rel) = p.strip_prefix(root) {
                    let key = rel
                        .components()
                        .map(|c| c.as_os_str().to_string_lossy().to_string())
                        .collect::<Vec<_>>()
                        .join("/");
                    if key.starts_with(prefix) && start_after.is_none_or(|s| key.as_str() > s) {
                        keys.push((key, p));
                    }
                }
            }
        }
        keys.sort();
        let limit = limit.max(1);
        let mut objects = Vec::new();
        let mut next = None;
        for (key, p) in keys {
            if objects.len() == limit {
                next = objects.last().map(|m: &ObjectMeta| m.key.clone());
                break;
            }
            match Self::read_obj(&p) {
                Ok((etag, data, mtime)) => {
                    objects.push(ObjectMeta { key, size: data.len() as u64, etag, last_modified_ms: mtime })
                }
                Err(StoreError::NotFound) => continue,
                Err(e) => return Err(e),
            }
        }
        Ok(ListPage { objects, next })
    }
}

async fn blocking<T: Send + 'static>(f: impl FnOnce() -> SResult<T> + Send + 'static) -> SResult<T> {
    tokio::task::spawn_blocking(f).await.map_err(|e| StoreError::Unavailable(e.to_string()))?
}

impl ObjectStore for FsStore {
    fn get<'a>(&'a self, key: &'a str, opts: GetOpts) -> BoxFut<'a, SResult<GetResult>> {
        Box::pin(async move {
            let path = self.path(key)?;
            let (etag, data, mtime) = blocking(move || Self::read_obj(&path)).await?;
            if opts.if_none_match.as_deref() == Some(etag.as_str()) {
                return Err(StoreError::NotModified);
            }
            let size = data.len() as u64;
            let r = clamp_range(&opts.range, size)?;
            Ok(GetResult {
                data: Bytes::from(data).slice(r.start as usize..r.end as usize),
                etag,
                size,
                last_modified_ms: mtime,
            })
        })
    }

    fn put<'a>(&'a self, key: &'a str, data: Bytes, mode: PutMode) -> BoxFut<'a, SResult<String>> {
        Box::pin(async move {
            let path = self.path(key)?;
            let root = self.root.clone();
            let lock = self.cas_lock.clone();
            blocking(move || Self::put_blocking(&root, &lock, &path, &data, &mode)).await
        })
    }

    fn delete<'a>(&'a self, key: &'a str) -> BoxFut<'a, SResult<()>> {
        Box::pin(async move {
            let path = self.path(key)?;
            blocking(move || match std::fs::remove_file(&path) {
                Ok(()) => Ok(()),
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
                Err(e) => Err(StoreError::Unavailable(e.to_string())),
            })
            .await
        })
    }

    fn list<'a>(
        &'a self,
        prefix: &'a str,
        start_after: Option<&'a str>,
        limit: usize,
    ) -> BoxFut<'a, SResult<ListPage>> {
        Box::pin(async move {
            let root = self.root.clone();
            let prefix = prefix.to_string();
            let start_after = start_after.map(|s| s.to_string());
            blocking(move || Self::list_blocking(&root, &prefix, start_after.as_deref(), limit)).await
        })
    }

    fn time_precision_ms(&self) -> u64 {
        1
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(flavor = "multi_thread")]
    async fn fs_semantics() {
        let dir = std::env::temp_dir().join(format!("baserunna-fs-test-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let s = FsStore::new(&dir).unwrap();
        let e1 = s.put("d/m", Bytes::from_static(b"one"), PutMode::Create).await.unwrap();
        assert_eq!(s.put("d/m", Bytes::from_static(b"x"), PutMode::Create).await, Err(StoreError::PreconditionFailed));
        let e2 = s.put("d/m", Bytes::from_static(b"two"), PutMode::IfMatch(e1.clone())).await.unwrap();
        assert_eq!(
            s.put("d/m", Bytes::from_static(b"x"), PutMode::IfMatch(e1)).await,
            Err(StoreError::PreconditionFailed)
        );
        let g = s.get("d/m", GetOpts::default()).await.unwrap();
        assert_eq!((&g.data[..], g.etag.as_str()), (&b"two"[..], e2.as_str()));
        s.put("d/seg/1", Bytes::from_static(b"s1"), PutMode::Overwrite).await.unwrap();
        s.put("d/seg/2", Bytes::from_static(b"s2"), PutMode::Overwrite).await.unwrap();
        let l = s.list("d/seg/", None, 10).await.unwrap();
        assert_eq!(l.objects.iter().map(|o| o.key.as_str()).collect::<Vec<_>>(), ["d/seg/1", "d/seg/2"]);
        let l = s.list("d/", Some("d/seg/1"), 10).await.unwrap();
        assert_eq!(l.objects.len(), 1);
        s.delete("d/seg/1").await.unwrap();
        s.delete("d/seg/1").await.unwrap();
        assert!(s.path("../x").is_err());
        let all = s.list("", None, 10).await.unwrap();
        assert!(all.objects.iter().all(|o| o.key.starts_with("d/")), "the lock file is not an object");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_directory_has_one_store_at_a_time() {
        let dir = std::env::temp_dir().join(format!("baserunna-fs-lock-test-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let first = FsStore::new(&dir).unwrap();
        let e = FsStore::new(&dir).err().expect("a second store on the same directory must fail");
        assert!(e.to_string().contains("in use by another process"), "{e}");
        drop(first);
        FsStore::new(&dir).expect("the lock is released with the store");
        let _ = std::fs::remove_dir_all(&dir);
    }
}
