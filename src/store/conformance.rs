//! Verifies that an [`ObjectStore`] satisfies the consistency and conditional-write contract.
//!
//! [`run`] tests conditional writes, cross-client visibility, clock coherence, and random
//! operation sequences against [`MemStore`] using 2 independent clients of the same store.

use super::*;
use crate::engine::clock::StoreClock;
use crate::util::rng::Rng;
use std::collections::HashMap;

#[derive(Clone, Debug)]
pub struct Config {
    pub seed: u64,
    pub model_ops: usize,
    pub racers: usize,
    pub race_rounds: usize,
}

impl Default for Config {
    fn default() -> Config {
        Config { seed: 0, model_ops: 400, racers: 8, race_rounds: 4 }
    }
}

type Check = fn(ClientPair, String, Config) -> BoxFut<'static, Result<(), String>>;

#[derive(Clone)]
struct ClientPair {
    a: DynStore,
    b: DynStore,
}

impl ClientPair {
    fn client(&self, i: usize) -> &DynStore {
        if i.is_multiple_of(2) { &self.a } else { &self.b }
    }
}

const CHECKS: &[(&str, Check)] = &[
    ("missing_objects", |p, d, _| Box::pin(missing_objects(p, d))),
    ("create_once", |p, d, _| Box::pin(create_once(p, d))),
    ("compare_and_swap", |p, d, _| Box::pin(compare_and_swap(p, d))),
    ("conditional_get", |p, d, _| Box::pin(conditional_get(p, d))),
    ("etags_identify_writes", |p, d, _| Box::pin(etags_identify_writes(p, d))),
    ("ranges", |p, d, _| Box::pin(ranges(p, d))),
    ("listing", |p, d, _| Box::pin(listing(p, d))),
    ("writes_are_visible_to_peers", |p, d, _| Box::pin(writes_are_visible_to_peers(p, d))),
    ("update_race", |p, d, c| Box::pin(race(p, d, c, false))),
    ("create_race", |p, d, c| Box::pin(race(p, d, c, true))),
    ("store_clock", |p, d, _| Box::pin(store_clock(p, d))),
    ("model", |p, d, c| Box::pin(model(p, d, c))),
];

/// Runs all conformance checks in the empty namespace shared by `a` and `b`, and returns the
/// names of the checks that passed. Test objects are deleted before returning.
pub async fn run(a: DynStore, b: DynStore, cfg: &Config) -> Result<Vec<&'static str>, String> {
    let all: Vec<&str> = CHECKS.iter().map(|(n, _)| *n).collect();
    run_checks(a, b, cfg, &all).await
}

/// Conditional-write checks executed at node startup.
pub const PREFLIGHT: &[&str] = &["missing_objects", "create_once", "compare_and_swap", "conditional_get"];

/// Runs [`PREFLIGHT`] checks in the empty namespace of `store`.
pub async fn preflight(store: DynStore) -> Result<(), String> {
    run_checks(store.clone(), store, &Config::default(), PREFLIGHT).await.map(drop)
}

/// Runs the checks in `names` against `a` and `b`.
pub async fn run_checks(a: DynStore, b: DynStore, cfg: &Config, names: &[&str]) -> Result<Vec<&'static str>, String> {
    let existing = a.list("", None, 1).await.map_err(|e| format!("listing the namespace failed: {e}"))?;
    if !existing.objects.is_empty() {
        return Err(format!("the store namespace is not empty (found {})", existing.objects[0].key));
    }
    let pair = ClientPair { a: a.clone(), b };
    let mut passed = Vec::new();
    let mut result = Ok(());
    for (name, check) in CHECKS.iter().filter(|(n, _)| names.contains(n)) {
        if let Err(e) = check(pair.clone(), format!("c/{name}/"), cfg.clone()).await {
            result = Err(format!("{name}: {e}"));
            break;
        }
        passed.push(*name);
    }
    let cleanup = clear(&*a).await;
    result?;
    cleanup.map_err(|e| format!("deleting the objects written by the checks failed: {e}"))?;
    Ok(passed)
}

async fn clear(store: &dyn ObjectStore) -> SResult<()> {
    for o in list_all(store, "", &Retry::default()).await? {
        Retry::default().run(|| store.delete(&o.key)).await?;
    }
    Ok(())
}

macro_rules! ensure {
    ($cond:expr, $($fmt:tt)+) => {
        if !$cond {
            return Err(format!($($fmt)+));
        }
    };
}

fn bytes(s: impl Into<String>) -> Bytes {
    Bytes::from(s.into())
}

fn get_opts(range: Option<Range<u64>>, if_none_match: Option<&str>) -> GetOpts {
    GetOpts { range, if_none_match: if_none_match.map(str::to_string) }
}

fn check_step<T>(what: &str, r: SResult<T>) -> Result<T, String> {
    r.map_err(|e| format!("{what} failed: {e}"))
}

async fn missing_objects(p: ClientPair, dir: String) -> Result<(), String> {
    let k = format!("{dir}missing");
    let got = p.a.get(&k, GetOpts::default()).await.map(|g| g.data);
    ensure!(got == Err(StoreError::NotFound), "get of a missing object returned {got:?}");
    let got = p.b.get(&k, get_opts(Some(0..4), None)).await.map(|g| g.data);
    ensure!(got == Err(StoreError::NotFound), "ranged get of a missing object returned {got:?}");
    let put = p.a.put(&k, bytes("x"), PutMode::IfMatch("\"0f0f\"".into())).await;
    ensure!(put == Err(StoreError::PreconditionFailed), "conditional update of a missing object returned {put:?}");
    let got = p.b.get(&k, GetOpts::default()).await.map(|g| g.data);
    ensure!(got == Err(StoreError::NotFound), "a failed conditional update created {got:?}");
    check_step("deleting a missing object", p.a.delete(&k).await)?;
    let page = check_step("listing an empty directory", p.b.list(&dir, None, 10).await)?;
    ensure!(page.objects.is_empty() && page.next.is_none(), "an empty directory listed {page:?}");
    Ok(())
}

async fn create_once(p: ClientPair, dir: String) -> Result<(), String> {
    let k = format!("{dir}k");
    let e1 = check_step("create", p.a.put(&k, bytes("first"), PutMode::Create).await)?;
    let g = check_step("get", p.b.get(&k, GetOpts::default()).await)?;
    ensure!(g.data == "first" && g.etag == e1 && g.size == 5, "read back {:?} (etag {:?})", g.data, g.etag);
    for s in [&p.a, &p.b] {
        let again = s.put(&k, bytes("second"), PutMode::Create).await;
        ensure!(again == Err(StoreError::PreconditionFailed), "creating an existing object returned {again:?}");
    }
    let g = check_step("get", p.a.get(&k, GetOpts::default()).await)?;
    ensure!(g.data == "first" && g.etag == e1, "a refused create replaced the object with {:?}", g.data);
    check_step("delete", p.b.delete(&k).await)?;
    check_step("create after delete", p.a.put(&k, bytes("third"), PutMode::Create).await)?;
    Ok(())
}

async fn compare_and_swap(p: ClientPair, dir: String) -> Result<(), String> {
    let k = format!("{dir}k");
    let e1 = check_step("put", p.a.put(&k, bytes("v1"), PutMode::Overwrite).await)?;
    let e2 = check_step("update with the current etag", p.b.put(&k, bytes("v2"), PutMode::IfMatch(e1.clone())).await)?;
    ensure!(e2 != e1, "an update kept etag {e1:?}");
    for (what, etag) in [("a replaced etag", e1.clone()), ("an unknown etag", "\"0123abcd\"".to_string())] {
        let r = p.a.put(&k, bytes("lost"), PutMode::IfMatch(etag)).await;
        ensure!(r == Err(StoreError::PreconditionFailed), "update against {what} returned {r:?}");
    }
    let g = check_step("get", p.a.get(&k, GetOpts::default()).await)?;
    ensure!(g.data == "v2" && g.etag == e2, "read {:?} with etag {:?} after updates", g.data, g.etag);
    let e3 = check_step("overwrite", p.a.put(&k, bytes("v3"), PutMode::Overwrite).await)?;
    let r = p.b.put(&k, bytes("lost"), PutMode::IfMatch(e2)).await;
    ensure!(r == Err(StoreError::PreconditionFailed), "update against an overwritten etag returned {r:?}");
    check_step("update after an overwrite", p.b.put(&k, bytes("v4"), PutMode::IfMatch(e3.clone())).await)?;
    check_step("delete", p.a.delete(&k).await)?;
    let r = p.b.put(&k, bytes("lost"), PutMode::IfMatch(e3)).await;
    ensure!(r == Err(StoreError::PreconditionFailed), "update of a deleted object returned {r:?}");
    Ok(())
}

async fn conditional_get(p: ClientPair, dir: String) -> Result<(), String> {
    let k = format!("{dir}k");
    let old = check_step("put", p.a.put(&k, bytes("old"), PutMode::Overwrite).await)?;
    let cur = check_step("put", p.a.put(&k, bytes("new"), PutMode::Overwrite).await)?;
    for range in [None, Some(0..2)] {
        let r = p.b.get(&k, get_opts(range.clone(), Some(&cur))).await.map(|g| g.data);
        ensure!(r == Err(StoreError::NotModified), "get with the current etag ({range:?}) returned {r:?}");
    }
    let g = check_step("get with a replaced etag", p.b.get(&k, get_opts(None, Some(&old))).await)?;
    ensure!(g.data == "new" && g.etag == cur, "get with a replaced etag returned {:?}", g.data);
    Ok(())
}

async fn etags_identify_writes(p: ClientPair, dir: String) -> Result<(), String> {
    let mut etags = Vec::new();
    for i in 0..6 {
        let k = format!("{dir}k{}", i % 2);
        let e = check_step("put", p.client(i).put(&k, bytes(format!("body {i}")), PutMode::Overwrite).await)?;
        for s in [&p.a, &p.b] {
            let g = check_step("get", s.get(&k, GetOpts::default()).await)?;
            ensure!(g.etag == e, "put returned etag {e:?} but get returned {:?}", g.etag);
        }
        let listed = check_step("list", p.b.list(&dir, None, 10).await)?;
        let m = listed.objects.iter().find(|o| o.key == k).ok_or_else(|| format!("{k} is not listed"))?;
        ensure!(m.etag == e, "put returned etag {e:?} but list returned {:?}", m.etag);
        etags.push(e);
    }
    let mut distinct = etags.clone();
    distinct.sort();
    distinct.dedup();
    ensure!(distinct.len() == etags.len(), "writes of different bytes shared an etag: {etags:?}");
    Ok(())
}

async fn ranges(p: ClientPair, dir: String) -> Result<(), String> {
    let k = format!("{dir}k");
    let data: Bytes = (0..1000u32).map(|i| (i * 7 % 251) as u8).collect::<Vec<_>>().into();
    check_step("put", p.a.put(&k, data.clone(), PutMode::Overwrite).await)?;
    for r in [0..1000u64, 10..20, 999..1000, 990..2000, 0..0, 500..500, 1000..1000] {
        let g = check_step(&format!("get {r:?}"), p.b.get(&k, get_opts(Some(r.clone()), None)).await)?;
        let want = &data[r.start as usize..r.end.min(1000) as usize];
        ensure!(g.data == want && g.size == 1000, "range {r:?} returned {} bytes of {}", g.data.len(), g.size);
    }
    for r in [1000..1001u64, 2000..2001, 1001..1001] {
        let got = p.b.get(&k, get_opts(Some(r.clone()), None)).await.map(|g| g.data.len());
        ensure!(matches!(got, Err(StoreError::Fatal(_))), "range {r:?} past the end returned {got:?}");
    }
    let empty = format!("{dir}empty");
    check_step("put", p.a.put(&empty, Bytes::new(), PutMode::Overwrite).await)?;
    let g = check_step("get", p.b.get(&empty, GetOpts::default()).await)?;
    ensure!(g.data.is_empty() && g.size == 0, "an empty object read back {} bytes", g.data.len());
    let g = check_step("get 0..0", p.b.get(&empty, get_opts(Some(0..0), None)).await)?;
    ensure!(g.data.is_empty(), "an empty range of an empty object returned {} bytes", g.data.len());
    let got = p.b.get(&empty, get_opts(Some(0..10), None)).await.map(|g| g.data.len());
    ensure!(matches!(got, Err(StoreError::Fatal(_))), "range 0..10 of an empty object returned {got:?}");
    Ok(())
}

async fn listing(p: ClientPair, dir: String) -> Result<(), String> {
    let mut names: Vec<String> = ["a/1", "a/2/x", "b", "b0", "c-d", "c.d", "z"].iter().map(|s| s.to_string()).collect();
    names.extend((0..12).map(|i| format!("n/{i:02}")));
    let base = dir.trim_end_matches('/');
    let outside = [format!("{base}-x/k"), format!("{base}z/k"), format!("{base}.k")];
    for (i, n) in names.iter().enumerate() {
        check_step("put", p.client(i).put(&format!("{dir}{n}"), bytes("x".repeat(i)), PutMode::Overwrite).await)?;
    }
    for k in &outside {
        check_step("put", p.a.put(k, bytes("outside"), PutMode::Overwrite).await)?;
    }
    let mut want: Vec<String> = names.iter().map(|n| format!("{dir}{n}")).collect();
    want.sort();
    for limit in [1, 7, 100] {
        let mut got = Vec::new();
        let mut after: Option<String> = None;
        loop {
            let page = check_step("list", p.b.list(&dir, after.as_deref(), limit).await)?;
            ensure!(page.objects.len() <= limit, "a page of limit {limit} held {}", page.objects.len());
            for o in &page.objects {
                let i = names.iter().position(|n| format!("{dir}{n}") == o.key);
                ensure!(i.is_some_and(|i| o.size == i as u64), "listed {} with size {}", o.key, o.size);
            }
            got.extend(page.objects.iter().map(|o| o.key.clone()));
            match page.next {
                Some(n) => {
                    ensure!(got.last() == Some(&n), "next {n:?} is not the last listed key");
                    after = Some(n);
                }
                None => break,
            }
            ensure!(got.len() <= want.len(), "listing with limit {limit} did not end");
        }
        ensure!(got == want, "listing with limit {limit} returned {got:?}");
    }
    for after in [format!("{dir}b"), format!("{dir}c"), format!("{dir}n/05x"), format!("{dir}zz"), dir.clone()] {
        let page = check_step("list", p.a.list(&dir, Some(&after), 100).await)?;
        let got: Vec<&String> = page.objects.iter().map(|o| &o.key).collect();
        let expected: Vec<&String> = want.iter().filter(|k| **k > after).collect();
        ensure!(got == expected, "listing after {after:?} returned {got:?}");
    }
    Ok(())
}

async fn writes_are_visible_to_peers(p: ClientPair, dir: String) -> Result<(), String> {
    for i in 0..8 {
        let (w, r) = if i % 2 == 0 { (&p.a, &p.b) } else { (&p.b, &p.a) };
        let k = format!("{dir}k{}", i % 3);
        let body = format!("write {i}");
        let e = check_step("put", w.put(&k, bytes(body.clone()), PutMode::Overwrite).await)?;
        let g = check_step("peer get", r.get(&k, GetOpts::default()).await)?;
        ensure!(g.data == body && g.etag == e, "a peer read {:?} right after a write of {body:?}", g.data);
        let listed = check_step("peer list", r.list(&dir, None, 10).await)?;
        ensure!(listed.objects.iter().any(|o| o.key == k && o.etag == e), "a peer list missed a completed write");
        if i % 3 == 2 {
            check_step("delete", w.delete(&k).await)?;
            let got = r.get(&k, GetOpts::default()).await.map(|g| g.data);
            ensure!(got == Err(StoreError::NotFound), "a peer read {got:?} after a delete");
            let listed = check_step("peer list", r.list(&dir, None, 10).await)?;
            ensure!(listed.objects.iter().all(|o| o.key != k), "a peer listed a deleted object");
        }
    }
    Ok(())
}

async fn race(p: ClientPair, dir: String, cfg: Config, create: bool) -> Result<(), String> {
    for round in 0..cfg.race_rounds {
        let k = format!("{dir}r{round}");
        let mode = if create {
            PutMode::Create
        } else {
            PutMode::IfMatch(check_step("put", p.a.put(&k, bytes("base"), PutMode::Overwrite).await)?)
        };
        let start = Arc::new(tokio::sync::Barrier::new(cfg.racers));
        let tasks: Vec<_> = (0..cfg.racers)
            .map(|i| {
                let (s, k, mode, start) = (p.client(i).clone(), k.clone(), mode.clone(), start.clone());
                tokio::spawn(async move {
                    start.wait().await;
                    s.put(&k, bytes(format!("racer {i}")), mode).await
                })
            })
            .collect();
        let (mut won, mut unknown) = (Vec::new(), Vec::new());
        for (i, t) in tasks.into_iter().enumerate() {
            match t.await.map_err(|e| format!("racer panicked: {e}"))? {
                Ok(etag) => won.push((i, etag)),
                Err(StoreError::PreconditionFailed) => {}
                Err(StoreError::Unavailable(_)) => unknown.push(i),
                Err(e) => return Err(format!("racer {i} failed: {e}")),
            }
        }
        ensure!(won.len() <= 1, "{} conditional writes succeeded with the same precondition: {won:?}", won.len());
        let g = check_step("get", p.a.get(&k, GetOpts::default()).await)?;
        match won.first() {
            Some((i, etag)) => ensure!(
                g.data == format!("racer {i}") && g.etag == *etag,
                "racer {i} won, but the object holds {:?}",
                g.data
            ),
            None => ensure!(
                unknown.iter().any(|i| g.data == format!("racer {i}")),
                "0 conditional writes succeeded, and the object holds {:?}",
                g.data
            ),
        }
    }
    Ok(())
}

async fn store_clock(p: ClientPair, dir: String) -> Result<(), String> {
    let clock = StoreClock::new();
    let age = Duration::from_secs(3600);
    clock
        .sync(&*p.a, &Retry::default(), Duration::ZERO, 5_000)
        .await
        .map_err(|e| format!("measuring the store clock failed: {e}"))?;
    let bounds = || {
        let now = clock.now_ms(age).unwrap_or(0);
        let err = now - clock.earliest_now_ms(age).unwrap_or(0);
        (now.saturating_sub(err), now + err)
    };
    let precision = p.a.time_precision_ms();
    let mut last = None;
    for i in 0..3 {
        let k = format!("{dir}t{i}");
        let (lo, _) = bounds();
        check_step("put", p.client(i).put(&k, bytes("tick"), PutMode::Overwrite).await)?;
        let (_, hi) = bounds();
        let g = check_step("get", p.b.get(&k, GetOpts::default()).await)?;
        let lm = g.last_modified_ms;
        ensure!(
            lm + precision > lo && lm <= hi + 1,
            "last_modified {lm} is outside the measured window {lo}..={hi} (precision {precision} ms)"
        );
        let listed = check_step("list", p.a.list(&dir, None, 10).await)?;
        let m = listed.objects.iter().find(|o| o.key == k).ok_or_else(|| format!("{k} is not listed"))?;
        ensure!(m.last_modified_ms == lm, "get reported last_modified {lm} but list reported {}", m.last_modified_ms);
        ensure!(last.is_none_or(|l| lm > l), "last_modified went from {last:?} to {lm} across objects");
        last = Some(lm);
        tokio::time::sleep(Duration::from_millis(precision + 100)).await;
    }
    Ok(())
}

const POSITION_CHARS: [u8; 9] = *b"-.09Ad_kz";

async fn model(p: ClientPair, dir: String, cfg: Config) -> Result<(), String> {
    let model = MemStore::new(Box::new(|| 0));
    let mut rng = Rng::unbranched(cfg.seed);
    let keys: Vec<String> = ["k0", "k1", "d/k2", "d/k3"].iter().map(|k| format!("{dir}{k}")).collect();
    let mut to_model: HashMap<String, String> = HashMap::new();
    let mut seen: Vec<String> = Vec::new();
    let pick_etag = |rng: &mut Rng, seen: &Vec<String>| match seen.is_empty() || rng.chance(0.15) {
        true => "\"0bad\"".to_string(),
        false => seen[seen.len() - 1 - rng.usize(seen.len().min(4))].clone(),
    };
    for step in 0..cfg.model_ops {
        let s = p.client(rng.usize(2));
        let k = rng.pick(&keys).clone();
        let what = |op: &str| format!("step {step}: {op} {k}");
        match rng.below(10) {
            0..=3 => {
                let (mode, model_mode) = match rng.below(3) {
                    0 => (PutMode::Overwrite, PutMode::Overwrite),
                    1 => (PutMode::Create, PutMode::Create),
                    _ => {
                        let e = pick_etag(&mut rng, &seen);
                        let m = to_model.get(&e).cloned().unwrap_or_else(|| e.clone());
                        (PutMode::IfMatch(e), PutMode::IfMatch(m))
                    }
                };
                let body = Bytes::from(format!("{}-{step}-{}", cfg.seed, "x".repeat(rng.usize(48))));
                let got = s.put(&k, body.clone(), mode.clone()).await;
                let want = model.put_sync(&k, body, &model_mode);
                match (got, want) {
                    (Ok(e), Ok(m)) => {
                        to_model.insert(e.clone(), m);
                        seen.push(e);
                    }
                    (Err(StoreError::Unavailable(_)), _) => resync_key(&p, &model, &k, &mut to_model).await?,
                    (got, want) => ensure!(
                        error_kind(&got) == error_kind(&want),
                        "{}: {mode:?} returned {got:?}, the model {want:?}",
                        what("put")
                    ),
                }
            }
            4..=6 => {
                let inm = rng.chance(0.3).then(|| pick_etag(&mut rng, &seen));
                let range = rng.chance(0.5).then(|| {
                    let start = rng.below(80);
                    start..start + rng.below(80)
                });
                let got = s.get(&k, get_opts(range.clone(), inm.as_deref())).await;
                let model_inm = inm.as_ref().map(|e| to_model.get(e).cloned().unwrap_or_else(|| e.clone()));
                let want = model.get_sync(&k, &get_opts(range.clone(), model_inm.as_deref()));
                match (&got, &want) {
                    (Ok(g), Ok(m)) => ensure!(
                        g.data == m.data && g.size == m.size && to_model.get(&g.etag) == Some(&m.etag),
                        "{} (range {range:?}): read {:?}, the model {:?}",
                        what("get"),
                        g.data,
                        m.data
                    ),
                    (Err(StoreError::Unavailable(_)), _) => {}
                    _ => ensure!(
                        error_kind(&got) == error_kind(&want),
                        "{} (range {range:?}, if_none_match {inm:?}): returned {:?}, the model {:?}",
                        what("get"),
                        got.as_ref().map(|g| &g.data),
                        want.as_ref().map(|g| &g.data)
                    ),
                }
            }
            7 => match s.delete(&k).await {
                Ok(()) => model.delete_sync(&k),
                Err(StoreError::Unavailable(_)) => resync_key(&p, &model, &k, &mut to_model).await?,
                Err(e) => return Err(format!("{}: {e}", what("delete"))),
            },
            _ => {
                let after = match rng.below(3) {
                    0 => None,
                    1 => Some(rng.pick(&keys).clone()),
                    _ => {
                        let segment: String =
                            (0..1 + rng.usize(3)).map(|_| *rng.pick(&POSITION_CHARS) as char).collect();
                        Some(format!("{dir}{segment}"))
                    }
                };
                let limit = 1 + rng.usize(4);
                let got = match s.list(&dir, after.as_deref(), limit).await {
                    Ok(page) => page,
                    Err(StoreError::Unavailable(_)) => continue,
                    Err(e) => return Err(format!("step {step}: list failed: {e}")),
                };
                let want = model.list_sync(&dir, after.as_deref(), limit);
                let got_view: Vec<_> =
                    got.objects.iter().map(|o| (o.key.clone(), o.size, to_model.get(&o.etag).cloned())).collect();
                let want_view: Vec<_> =
                    want.objects.iter().map(|o| (o.key.clone(), o.size, Some(o.etag.clone()))).collect();
                ensure!(
                    got_view == want_view && got.next == want.next,
                    "step {step}: list after {after:?} limit {limit} returned {got_view:?} next {:?}, the model {want_view:?} next {:?}",
                    got.next,
                    want.next
                );
            }
        }
    }
    Ok(())
}

fn error_kind<T>(r: &SResult<T>) -> Option<std::mem::Discriminant<StoreError>> {
    r.as_ref().err().map(std::mem::discriminant)
}

async fn resync_key(
    p: &ClientPair,
    model: &MemStore,
    k: &str,
    to_model: &mut HashMap<String, String>,
) -> Result<(), String> {
    match Retry::default().run(|| p.a.get(k, GetOpts::default())).await {
        Ok(g) => {
            if to_model.contains_key(&g.etag) && model.get_sync(k, &GetOpts::default()).is_ok_and(|m| m.data == g.data)
            {
                return Ok(());
            }
            let m = model.put_sync(k, g.data, &PutMode::Overwrite).map_err(|e| e.to_string())?;
            to_model.insert(g.etag, m);
        }
        Err(StoreError::NotFound) => model.delete_sync(k),
        Err(e) => return Err(format!("reading {k} after an ambiguous operation failed: {e}")),
    }
    Ok(())
}
