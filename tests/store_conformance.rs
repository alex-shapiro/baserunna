//! Object-store conformance tests.
//!
//! Runs `baserunna::store::conformance` against `MemStore`, `FsStore`, `CloudStore` backed by an
//! in-process S3 server, and a live S3 store if `BASERUNNA_TEST_STORE` is set. Also verifies that
//! each conformance check detects its corresponding contract violation.

#[path = "support/fake_s3.rs"]
mod fake_s3;

use axum::http::Method;
use baserunna::store::conformance::{self, Config};
use baserunna::store::*;
use bytes::Bytes;
use fake_s3::{FakeS3, Fault};
use std::sync::{Arc, Mutex};
use std::time::Duration;

fn rt() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread().worker_threads(4).enable_all().build().unwrap()
}

fn mem_store() -> DynStore {
    Arc::new(MemStore::with_system_clock())
}

fn fast_config() -> Config {
    Config { model_ops: 300, racers: 6, race_rounds: 3, ..Config::default() }
}

#[test]
fn mem_store_conforms() {
    let s = mem_store();
    let passed = rt().block_on(conformance::run(s.clone(), s, &fast_config())).unwrap();
    assert_eq!(passed.len(), 12, "{passed:?}");
}

#[test]
fn fs_store_conforms() {
    let dir = std::env::temp_dir().join(format!("baserunna-conformance-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let s: DynStore = Arc::new(FsStore::new(&dir).unwrap());
    rt().block_on(conformance::run(s.clone(), s, &fast_config())).unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn model_sequences_agree_on_mem_and_fs_stores() {
    let dir = std::env::temp_dir().join(format!("baserunna-model-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let fs: DynStore = Arc::new(FsStore::new(&dir).unwrap());
    let rt = rt();
    baserunna::util::prop::check(
        "store_model",
        40,
        |r| r.next_u64(),
        |_| Vec::new(),
        |seed| {
            let cfg = Config { seed: *seed, model_ops: 250, ..Config::default() };
            rt.block_on(async {
                for s in [mem_store(), fs.clone()] {
                    conformance::run_checks(s.clone(), s, &cfg, &["model"]).await?;
                }
                Ok(())
            })
        },
    );
    let _ = std::fs::remove_dir_all(&dir);
}

fn fake_store(s3: &FakeS3, prefix: &str, extra: &[(&str, &str)]) -> DynStore {
    let mut cfg = s3.config();
    cfg.extend(extra.iter().map(|(k, v)| (k.to_string(), v.to_string())));
    Arc::new(CloudStore::from_url_with(&format!("s3://{}/{prefix}", fake_s3::BUCKET), cfg).unwrap())
}

#[test]
fn cloud_store_conforms_against_an_s3_endpoint() {
    rt().block_on(async {
        let s3 = FakeS3::start().await;
        let (a, b) = (fake_store(&s3, "ns", &[]), fake_store(&s3, "ns", &[]));
        conformance::run(a, b, &fast_config()).await.unwrap();
    });
}

#[test]
fn cloud_store_reports_failed_conditional_writes_as_ambiguous_without_retrying() {
    rt().block_on(async {
        let s3 = FakeS3::start().await;
        let s = fake_store(&s3, "", &[]);
        let e1 = s.put("m", Bytes::from_static(b"v1"), PutMode::Create).await.unwrap();

        s3.inject_fault(Method::PUT, Fault::ApplyThenFail(500));
        let before = s3.request_count(&Method::PUT);
        let r = s.put("m", Bytes::from_static(b"v2"), PutMode::IfMatch(e1.clone())).await;
        assert!(matches!(r, Err(StoreError::Unavailable(_))), "an applied write that answered 500 returned {r:?}");
        assert_eq!(s3.request_count(&Method::PUT) - before, 1, "the conditional write was retried");
        assert_eq!(s3.get_data("m").as_deref(), Some(&b"v2"[..]));

        s3.inject_fault(Method::PUT, Fault::Fail(503));
        let r = s.put("m", Bytes::from_static(b"v3"), PutMode::IfMatch(e1)).await;
        assert!(matches!(r, Err(StoreError::Unavailable(_))), "503 returned {r:?}");

        s3.inject_fault(Method::PUT, Fault::Fail(409));
        let cur = s.get("m", GetOpts::default()).await.unwrap().etag;
        let r = s.put("m", Bytes::from_static(b"v4"), PutMode::IfMatch(cur)).await;
        assert_eq!(r, Err(StoreError::PreconditionFailed), "a conflicting conditional write");

        s3.inject_fault(Method::GET, Fault::Fail(500));
        let before = s3.request_count(&Method::GET);
        let r = s.get("m", GetOpts::default()).await.map(|g| g.data);
        assert!(matches!(r, Err(StoreError::Unavailable(_))), "500 on get returned {r:?}");
        assert_eq!(s3.request_count(&Method::GET) - before, 1, "the get was retried");

        s3.inject_fault(Method::GET, Fault::Fail(403));
        let r = s.get("m", GetOpts::default()).await.map(|g| g.data);
        assert!(matches!(r, Err(StoreError::Fatal(_))), "403 returned {r:?}");
        s3.inject_fault(Method::GET, Fault::Fail(400));
        let r = s.get("m", GetOpts::default()).await.map(|g| g.data);
        assert!(matches!(r, Err(StoreError::Fatal(_))), "400 returned {r:?}");
        s3.inject_fault(Method::GET, Fault::Fail(429));
        let r = s.get("m", GetOpts::default()).await.map(|g| g.data);
        assert!(matches!(r, Err(StoreError::Unavailable(_))), "429 returned {r:?}");
    });
}

#[test]
fn cloud_store_requests_time_out() {
    rt().block_on(async {
        let s3 = FakeS3::start().await;
        let s = fake_store(&s3, "", &[("timeout", "300ms")]);
        s.put("k", Bytes::from_static(b"v"), PutMode::Overwrite).await.unwrap();
        s3.inject_fault(Method::GET, Fault::Delay(Duration::from_secs(5)));
        let t = std::time::Instant::now();
        let r = s.get("k", GetOpts::default()).await.map(|g| g.data);
        assert!(matches!(r, Err(StoreError::Unavailable(_))), "a stalled get returned {r:?}");
        assert!(t.elapsed() < Duration::from_secs(2), "the get took {:?}", t.elapsed());
    });
}

fn live_store_pair() -> Option<(DynStore, DynStore)> {
    let url = std::env::var("BASERUNNA_TEST_STORE").ok().filter(|u| !u.is_empty())?;
    let run = format!(
        "{}/conformance-{}-{}",
        url.trim_end_matches('/'),
        std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis(),
        std::process::id()
    );
    let open = || -> DynStore { Arc::new(CloudStore::from_url(&run).expect("BASERUNNA_TEST_STORE")) };
    Some((open(), open()))
}

#[test]
fn live_store_conforms() {
    let Some((a, b)) = live_store_pair() else {
        eprintln!("skipped: BASERUNNA_TEST_STORE is not set");
        return;
    };
    let cfg = Config { racers: 16, race_rounds: 8, ..Config::default() };
    let passed = rt().block_on(conformance::run(a, b, &cfg)).unwrap();
    eprintln!("live store passed {passed:?}");
}

struct FaultyStore {
    inner: MemStore,
    violation: Violation,
    written: Mutex<std::collections::BTreeMap<String, std::time::Instant>>,
}

#[derive(Clone, Copy, Debug)]
enum Violation {
    IgnorePreconditions,
    NonAtomicPreconditions,
    StaleListings,
    SkewedClock,
    MismatchedPutEtag,
    IgnoreByteRanges,
}

impl ObjectStore for FaultyStore {
    fn get<'a>(&'a self, key: &'a str, opts: GetOpts) -> BoxFut<'a, SResult<GetResult>> {
        Box::pin(async move {
            let mut opts = opts;
            if matches!(self.violation, Violation::IgnoreByteRanges) {
                opts.range = None;
            }
            let mut g = self.inner.get_sync(key, &opts)?;
            if matches!(self.violation, Violation::SkewedClock) {
                g.last_modified_ms += 3_600_000;
            }
            Ok(g)
        })
    }

    fn put<'a>(&'a self, key: &'a str, data: Bytes, mode: PutMode) -> BoxFut<'a, SResult<String>> {
        Box::pin(async move {
            self.written.lock().unwrap().insert(key.to_string(), std::time::Instant::now());
            let mode = match (self.violation, mode) {
                (Violation::IgnorePreconditions, _) => PutMode::Overwrite,
                (Violation::NonAtomicPreconditions, mode) => {
                    let cur = self.inner.get_sync(key, &GetOpts::default()).ok().map(|g| g.etag);
                    let ok = match &mode {
                        PutMode::Overwrite => true,
                        PutMode::Create => cur.is_none(),
                        PutMode::IfMatch(e) => cur.as_ref() == Some(e),
                    };
                    tokio::time::sleep(Duration::from_millis(20)).await;
                    if !ok {
                        return Err(StoreError::PreconditionFailed);
                    }
                    PutMode::Overwrite
                }
                (_, mode) => mode,
            };
            let etag = self.inner.put_sync(key, data, &mode)?;
            Ok(match self.violation {
                Violation::MismatchedPutEtag => format!("{etag}-unquoted"),
                _ => etag,
            })
        })
    }

    fn delete<'a>(&'a self, key: &'a str) -> BoxFut<'a, SResult<()>> {
        Box::pin(async move {
            self.inner.delete_sync(key);
            Ok(())
        })
    }

    fn list<'a>(&'a self, prefix: &'a str, after: Option<&'a str>, limit: usize) -> BoxFut<'a, SResult<ListPage>> {
        Box::pin(async move {
            let mut page = self.inner.list_sync(prefix, after, limit);
            if matches!(self.violation, Violation::StaleListings) {
                let w = self.written.lock().unwrap();
                page.objects.retain(|o| w.get(&o.key).is_none_or(|t| t.elapsed() > Duration::from_secs(2)));
            }
            Ok(page)
        })
    }

    fn time_precision_ms(&self) -> u64 {
        1
    }
}

#[test]
fn each_contract_violation_is_caught() {
    let cases = [
        (Violation::IgnorePreconditions, "missing_objects"),
        (Violation::NonAtomicPreconditions, "update_race"),
        (Violation::StaleListings, "etags_identify_writes"),
        (Violation::SkewedClock, "store_clock"),
        (Violation::MismatchedPutEtag, "create_once"),
        (Violation::IgnoreByteRanges, "ranges"),
    ];
    let rt = rt();
    for (violation, check) in cases {
        let s: DynStore = Arc::new(FaultyStore {
            inner: MemStore::with_system_clock(),
            violation,
            written: Mutex::new(Default::default()),
        });
        let err =
            rt.block_on(conformance::run(s.clone(), s, &fast_config())).expect_err(&format!("{violation:?} passed"));
        assert!(err.starts_with(&format!("{check}:")), "{violation:?} failed {err:?}, expected {check}");
    }
}

#[test]
fn preflight_rejects_stores_without_conditional_writes() {
    let rt = rt();
    rt.block_on(conformance::preflight(mem_store())).unwrap();
    let broken: DynStore = Arc::new(FaultyStore {
        inner: MemStore::with_system_clock(),
        violation: Violation::IgnorePreconditions,
        written: Mutex::new(Default::default()),
    });
    let err = rt.block_on(conformance::preflight(broken.clone())).unwrap_err();
    assert!(err.contains("conditional update of a missing object"), "{err}");
    assert!(rt.block_on(broken.list("", None, 10)).unwrap().objects.is_empty(), "preflight left objects behind");
}
