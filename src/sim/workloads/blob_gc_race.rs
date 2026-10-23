//! Blob reads racing GC with grace periods shorter than a download. Writers
//! overwrite and delete blobs while readers stream full and ranged reads by key
//! and by ID. Reads that lose the race must fail with a broken stream, 404, or
//! 503—never wrong bytes—and a final GC cycle must preserve every referenced blob.

use super::super::client::{Client, ZOMBIE_WAIT_MS};
use super::super::{Sim, SimConfig};
use crate::util::{b64, sha256};
use bytes::Bytes;
use serde_json::json;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

const BASE: &str = "/v1/dbs/gc";
const KEYS: u64 = 3;
/// Minimum size of `f*` blobs so readers can pick byte ranges without checking size first.
const MIN_SIZE: usize = 20 * 1024;
/// Size bounds for `s*` blobs: large enough to store out-of-band, small enough to return inline in JSON results.
const SMALL: (usize, usize) = (600, 4_000);

/// Sets GC grace periods shorter than a multi-chunk download.
pub fn config(base: SimConfig) -> SimConfig {
    let mut c = base;
    c.engine.txn_max_lifetime_ms = 1_000;
    c.engine.gc_grace_ms = 1_500;
    c.engine.blob_stage_ttl_ms = 4_000;
    c.engine.orphan_grace_ms = 8_000;
    c.engine.sweep_interval_ms = 1_000;
    c.node.engine = c.engine.clone();
    c
}

#[derive(Default)]
struct Written {
    /// Content indexed by hex SHA-256.
    by_sha: BTreeMap<String, Arc<Vec<u8>>>,
    /// Content of acknowledged writes by `(key, version)`.
    by_version: BTreeMap<(u64, u64), Arc<Vec<u8>>>,
    /// All content ever written to a key, including unacknowledged writes.
    by_key: BTreeMap<u64, Vec<Arc<Vec<u8>>>>,
    /// Keys each key may have been copied from, including by unacknowledged copies.
    copied_from: BTreeMap<u64, BTreeSet<u64>>,
}

impl Written {
    /// All content key `k` may hold: content written to it or, transitively, to any key
    /// copied to it. A copy reads its source when it commits, so content written to the
    /// source after the copy was sent counts too.
    fn candidates(&self, k: u64) -> Vec<Arc<Vec<u8>>> {
        let mut keys = BTreeSet::from([k]);
        let mut todo = vec![k];
        while let Some(k) = todo.pop() {
            for f in self.copied_from.get(&k).into_iter().flatten() {
                if keys.insert(*f) {
                    todo.push(*f);
                }
            }
        }
        keys.iter().flat_map(|k| self.by_key.get(k).into_iter().flatten().cloned()).collect()
    }
}

#[derive(Default)]
struct Stats {
    ok: AtomicU64,
    broken: AtomicU64,
}

const WRITE_BUDGET_MS: u64 = 30 * 60_000;

async fn writer(c: Client, rounds: usize, written: Arc<Mutex<Written>>) -> Result<(), String> {
    let mut rng = c.sim.fork_rng();
    let deadline = c.sim.now_ms() + WRITE_BUDGET_MS;
    for _ in 0..rounds {
        if c.sim.now_ms() > deadline {
            break;
        }
        c.pause(100 + rng.below(400)).await;
        let small = rng.chance(0.3);
        let k = rng.below(KEYS) + if small { KEYS } else { 0 };
        let path = format!("{BASE}/kv/{}", key_name(k));
        if rng.chance(0.2) {
            c.call_settled("DELETE", &path, None).await;
            continue;
        }
        if rng.chance(0.2) {
            copy(&c, rng.below(KEYS) + if small { KEYS } else { 0 }, k, &written).await?;
            continue;
        }
        let size =
            if small { rng.range(SMALL.0 as u64, SMALL.1 as u64) as usize } else { MIN_SIZE + rng.usize(2 * MIN_SIZE) };
        let data = Arc::new(rng.bytes(size));
        {
            let mut w = written.lock().unwrap_or_else(|e| e.into_inner());
            w.by_sha.insert(b64::hex(&sha256(&data)), data.clone());
            w.by_key.entry(k).or_default().push(data.clone());
        }
        while c.sim.now_ms() <= deadline {
            match c.send("PUT", &path, &[], Bytes::from(data.to_vec())).await {
                Ok(r) if r.status == 200 => {
                    let v: serde_json::Value = serde_json::from_slice(&r.body).unwrap_or_default();
                    let v = v["version"].as_u64().ok_or("put: missing version")?;
                    written.lock().unwrap_or_else(|e| e.into_inner()).by_version.insert((k, v), data.clone());
                    break;
                }
                Ok(r) if r.status == 503 && !String::from_utf8_lossy(&r.body).contains("\"unknown\"") => {
                    c.pause(100).await;
                }
                // Abort this write if the upload exceeded the shortened staging TTL.
                Ok(r) if r.status == 400 && String::from_utf8_lossy(&r.body).contains("expired") => break,
                Ok(r) if r.status != 503 => return Err(format!("put {path}: {}", r.status)),
                _ => c.pause(ZOMBIE_WAIT_MS).await,
            }
        }
    }
    Ok(())
}

/// Copies key `from` to key `to`, sharing its blob, which must outlive `from`'s deletion.
async fn copy(c: &Client, from: u64, to: u64, written: &Mutex<Written>) -> Result<(), String> {
    written.lock().unwrap_or_else(|e| e.into_inner()).copied_from.entry(to).or_default().insert(from);
    let body = json!({"ops": [{"op": "copy", "from": key_name(from), "to": key_name(to)}]});
    let r = c.call_settled("POST", &format!("{BASE}/txn"), Some(body)).await;
    match r.status {
        200 | 412 => Ok(()),
        s => Err(format!("copy {} to {}: {s} {}", key_name(from), key_name(to), r.json)),
    }
}

/// Keys `0..KEYS` are large blobs; `KEYS..2 * KEYS` are small blobs.
fn key_name(k: u64) -> String {
    if k < KEYS { format!("f{k}") } else { format!("s{}", k - KEYS) }
}

fn header<'a>(r: &'a super::super::SimResponse, name: &str) -> Option<&'a str> {
    r.headers.get(name).and_then(|v| v.to_str().ok())
}

/// Verifies a 200/206 response body against `candidates` and its `Content-Range` header.
fn check_body(r: &super::super::SimResponse, candidates: &[Arc<Vec<u8>>], what: &str) -> Result<(), String> {
    let (start, end) = match header(r, "content-range") {
        Some(cr) => {
            let span = cr.strip_prefix("bytes ").and_then(|s| s.split('/').next()).ok_or("bad content-range")?;
            let (a, b) = span.split_once('-').ok_or("bad content-range")?;
            let (a, b): (usize, usize) = (a.parse().map_err(|_| "bad range")?, b.parse().map_err(|_| "bad range")?);
            (a, b + 1)
        }
        None => (0, r.body.len()),
    };
    let matches = |c: &Arc<Vec<u8>>| {
        (r.status == 206 || c.len() == r.body.len()) && c.get(start..end).is_some_and(|s| s == r.body.as_ref())
    };
    if candidates.iter().any(matches) {
        return Ok(());
    }
    Err(format!("{what}: {} bytes [{start}, {end}) do not match any written content", r.body.len()))
}

async fn reader(
    c: Client,
    written: Arc<Mutex<Written>>,
    stats: Arc<Stats>,
    done: Arc<AtomicBool>,
) -> Result<(), String> {
    let mut rng = c.sim.fork_rng();
    while !done.load(Ordering::SeqCst) {
        c.pause(rng.below(200)).await;
        if rng.chance(0.25) {
            small_read(&c, KEYS + rng.below(KEYS), &written, &stats).await?;
            continue;
        }
        let k = rng.below(KEYS);
        let range = rng.chance(0.6).then(|| {
            let a = rng.usize(MIN_SIZE - 1);
            (a, a + rng.usize(MIN_SIZE - a))
        });
        let hdr: Vec<(&str, String)> = range.iter().map(|(a, b)| ("range", format!("bytes={a}-{b}"))).collect();
        let by_id = rng.chance(0.4);
        let (path, what) = if by_id {
            let r = c.get(&format!("{BASE}/kv?prefix=f{k}&keys_only=true&limit=1")).await;
            let Ok(r) = r else { continue };
            let Some(item) = r.json["items"].as_array().and_then(|a| a.first()).cloned() else { continue };
            let Some(id) = item["blob"]["blob_id"].as_str() else { continue };
            (format!("{BASE}/blobs/{id}"), format!("blob {id}"))
        } else {
            (format!("{BASE}/kv/f{k}"), format!("key f{k}"))
        };
        let r = match c.send("GET", &path, &hdr, Bytes::new()).await {
            Ok(r) => r,
            Err(_) => {
                stats.broken.fetch_add(1, Ordering::Relaxed);
                continue;
            }
        };
        match r.status {
            200 | 206 => {
                let w = written.lock().unwrap_or_else(|e| e.into_inner());
                let version = header(&r, "x-baserunna-version").and_then(|v| v.parse::<u64>().ok());
                let candidates: Vec<Arc<Vec<u8>>> = match (by_id, version) {
                    (false, Some(v)) => match w.by_version.get(&(k, v)) {
                        Some(c) => vec![c.clone()],
                        None => w.candidates(k),
                    },
                    (false, None) => return Err(format!("{what}: missing version header")),
                    (true, _) => w.by_sha.values().cloned().collect(),
                };
                drop(w);
                check_body(&r, &candidates, &what)?;
                stats.ok.fetch_add(1, Ordering::Relaxed);
            }
            404 | 503 => {}
            s => return Err(format!("{what}: unexpected status {s}: {}", String::from_utf8_lossy(&r.body))),
        }
    }
    Ok(())
}

async fn small_read(c: &Client, k: u64, written: &Mutex<Written>, stats: &Stats) -> Result<(), String> {
    let key = key_name(k);
    let body = json!({"ops": [{"op": "get", "key": b64::encode(key.as_bytes())}], "encoding": "base64"});
    let Ok(r) = c.post(&format!("{BASE}/txn"), body).await else { return Ok(()) };
    match r.status {
        200 => {
            let g = &r.json["results"][0];
            if g["found"] != true {
                return Ok(());
            }
            let got = g["value"].as_str().and_then(b64::decode).ok_or(format!("{key}: missing inline value: {g}"))?;
            let w = written.lock().unwrap_or_else(|e| e.into_inner());
            let want = g["version"].as_u64().and_then(|v| w.by_version.get(&(k, v)));
            let ok = match want {
                Some(c) => **c == got,
                None => w.candidates(k).iter().any(|c| **c == got),
            };
            if !ok {
                return Err(format!("{key}: inline value of {} bytes does not match any written value", got.len()));
            }
            stats.ok.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }
        404 | 503 => Ok(()),
        s => Err(format!("{key}: transaction get: {s} {}", r.json)),
    }
}

pub async fn run(sim: Arc<Sim>, writers: usize, readers: usize, rounds: usize, chaos: bool) -> Result<(), String> {
    let owner = Client::signup(&sim, "gc", "gc@gc.test", "password-123").await?;
    owner.create_db("gc").await?;
    let token = owner.token.clone().unwrap_or_default();
    let written = Arc::new(Mutex::new(Written::default()));
    let stats = Arc::new(Stats::default());
    let done = Arc::new(AtomicBool::new(false));
    let phase = chaos.then(|| super::chaos_phase(&sim));
    let rs: Vec<_> = (0..readers)
        .map(|i| {
            let c = Client::with_token(&sim, &format!("gcreader{i}"), &token);
            tokio::spawn(reader(c, written.clone(), stats.clone(), done.clone()))
        })
        .collect();
    let ws: Vec<_> = (0..writers)
        .map(|i| {
            tokio::spawn(writer(Client::with_token(&sim, &format!("gcwriter{i}"), &token), rounds, written.clone()))
        })
        .collect();
    let mut err = None;
    for h in ws {
        if let Err(e) = h.await.map_err(|e| e.to_string()).and_then(|r| r) {
            err = err.or(Some(e));
        }
    }
    done.store(true, Ordering::SeqCst);
    for h in rs {
        if let Err(e) = h.await.map_err(|e| e.to_string()).and_then(|r| r) {
            err = err.or(Some(e));
        }
    }
    if let Some(e) = err {
        return Err(e);
    }
    if std::env::var("SIM_VERBOSE").is_ok() {
        eprintln!(
            "blob_gc_race: {} verified reads, {} broken by GC or the network",
            stats.ok.load(Ordering::Relaxed),
            stats.broken.load(Ordering::Relaxed)
        );
    }
    super::settle(&sim, phase).await;
    let e = &sim.cfg.engine;
    sim.sleep(e.orphan_grace_ms + e.gc_grace_ms + 2 * e.sweep_interval_ms).await;
    let body = json!({"ops": [{"op": "put", "key": "touch", "value": "1"}], "idempotency_key": "gc-touch"});
    owner.call_settled("POST", &format!("{BASE}/txn"), Some(body)).await;
    sim.sleep(e.gc_grace_ms + 5_000).await;
    for k in 0..2 * KEYS {
        let path = format!("{BASE}/kv/{}", key_name(k));
        let r = loop {
            match owner.send("GET", &path, &[], Bytes::new()).await {
                Ok(r) if r.status != 503 => break r,
                _ => owner.pause(200).await,
            }
        };
        match r.status {
            404 => {}
            200 => {
                let w = written.lock().unwrap_or_else(|e| e.into_inner());
                check_body(&r, &w.candidates(k), &path)?;
            }
            s => return Err(format!("final read of {path}: {s}")),
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sim::SimResponse;

    fn resp(status: u16, range: Option<&str>, body: &[u8]) -> SimResponse {
        let mut headers = http::HeaderMap::new();
        if let Some(r) = range {
            headers.insert("content-range", r.parse().unwrap());
        }
        SimResponse { status, headers, body: Bytes::copy_from_slice(body) }
    }

    #[test]
    fn body_check_rejects_foreign_bytes() {
        let a = Arc::new(b"0123456789".to_vec());
        let b = Arc::new(b"abcdefghij".to_vec());
        let both = [a.clone(), b.clone()];
        check_body(&resp(200, None, b"0123456789"), &both, "full").unwrap();
        check_body(&resp(206, Some("bytes 2-4/10"), b"234"), &both, "range").unwrap();
        check_body(&resp(206, Some("bytes 2-4/10"), b"cde"), &both, "range").unwrap();
        assert!(check_body(&resp(200, None, b"0123456789"), std::slice::from_ref(&b), "wrong").is_err());
        assert!(check_body(&resp(206, Some("bytes 2-4/10"), b"235"), &both, "torn").is_err());
        assert!(check_body(&resp(206, Some("bytes 3-5/10"), b"234"), &both, "shifted").is_err());
        assert!(check_body(&resp(200, None, b"01234"), &both, "truncated").is_err());
    }
}
