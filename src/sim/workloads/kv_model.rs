//! `kv_model`: one client per database issues random KV operations through
//! every API form under faults, and every response must match an exact
//! model. Ambiguous outcomes are resolved as a careful client would: with
//! idempotency keys, fenced commit-status queries or fenced read-backs.

use super::super::Sim;
use super::super::client::{Client, Resp, url_escape};
use super::chaos_step;
use crate::util::b64;
use crate::util::rng::Rng;
use serde_json::{Value as Json, json};
use std::collections::BTreeMap;
use std::sync::Arc;

type Key = Vec<u8>;
type Expect = Box<dyn Fn(&Json) -> Result<(), String> + Send>;

#[derive(Clone, Debug, Default)]
struct Model {
    data: BTreeMap<Key, (Vec<u8>, u64)>,
}

/// Transaction simulator over the model with read-your-writes semantics.
struct TxnSim {
    base: Model,
    writes: BTreeMap<Key, Option<Vec<u8>>>,
    ranges: Vec<(Key, Option<Key>)>,
}

fn in_range(k: &[u8], r: &(Key, Option<Key>)) -> bool {
    k >= r.0.as_slice() && r.1.as_ref().is_none_or(|e| k < e.as_slice())
}

fn prefix_range(p: &[u8]) -> (Key, Option<Key>) {
    (p.to_vec(), crate::util::prefix_end(p))
}

impl TxnSim {
    fn new(base: Model) -> TxnSim {
        TxnSim { base, writes: BTreeMap::new(), ranges: Vec::new() }
    }

    fn get(&self, k: &[u8]) -> Option<(Vec<u8>, Option<u64>)> {
        if let Some(w) = self.writes.get(k) {
            return w.clone().map(|v| (v, None));
        }
        if self.ranges.iter().any(|r| in_range(k, r)) {
            return None;
        }
        self.base.data.get(k).map(|(v, ver)| (v.clone(), Some(*ver)))
    }

    fn scan(&self, r: &(Key, Option<Key>), reverse: bool) -> Vec<(Key, Vec<u8>, Option<u64>)> {
        let mut m: BTreeMap<Key, (Vec<u8>, Option<u64>)> = BTreeMap::new();
        for (k, (v, ver)) in &self.base.data {
            if in_range(k, r) && !self.writes.contains_key(k) && !self.ranges.iter().any(|x| in_range(k, x)) {
                m.insert(k.clone(), (v.clone(), Some(*ver)));
            }
        }
        for (k, w) in &self.writes {
            if let (true, Some(v)) = (in_range(k, r), w) {
                m.insert(k.clone(), (v.clone(), None));
            }
        }
        let mut out: Vec<_> = m.into_iter().map(|(k, (v, ver))| (k, v, ver)).collect();
        if reverse {
            out.reverse();
        }
        out
    }

    fn put(&mut self, k: Key, v: Vec<u8>) {
        self.writes.insert(k, Some(v));
    }

    fn delete(&mut self, k: Key) {
        self.writes.insert(k, None);
    }

    fn delete_range(&mut self, r: (Key, Option<Key>)) {
        self.writes.retain(|k, _| !in_range(k, &r));
        self.ranges.push(r);
    }

    fn is_write(&self) -> bool {
        !self.writes.is_empty() || !self.ranges.is_empty()
    }

    fn apply(self, base: &mut Model, version: u64) {
        for r in &self.ranges {
            base.data.retain(|k, _| !in_range(k, r) || self.writes.contains_key(k));
        }
        for (k, w) in self.writes {
            match w {
                Some(v) => {
                    base.data.insert(k, (v, version));
                }
                None => {
                    base.data.remove(&k);
                }
            }
        }
    }
}

const JSON_VALUE_MAX: usize = 4096;
const INLINE_MAX: usize = 512;

fn pick_key(r: &mut Rng, spaces: &[&str]) -> Key {
    let sp = spaces[r.usize(spaces.len())];
    gen_key(r, sp)
}

fn gen_key(r: &mut Rng, space: &str) -> Key {
    let mut k = space.as_bytes().to_vec();
    k.push(b'/');
    for _ in 0..1 + r.usize(2) {
        k.push(b"xyz"[r.usize(3)]);
    }
    k
}

/// Generates keys in the `d/` keyspace to exercise range tombstones on prefix deletion.
fn bulk_key(r: &mut Rng) -> Key {
    format!("d/{}{:02}", ["p", "q", "r"][r.usize(3)], r.usize(80)).into_bytes()
}

fn gen_value(r: &mut Rng) -> Vec<u8> {
    let n = match r.below(20) {
        0 => 600 + r.usize(800),
        1 => 4000 + r.usize(3000),
        _ => r.usize(24),
    };
    (0..n).map(|_| b'a' + r.below(26) as u8).collect()
}

fn s(b: &[u8]) -> String {
    String::from_utf8_lossy(b).into_owned()
}

fn check_value(got: &Json, want: &[u8], ctx: &str) -> Result<(), String> {
    if want.len() > INLINE_MAX {
        let size = got["blob"]["size"].as_u64();
        if size != Some(want.len() as u64) {
            return Err(format!("{ctx}: blob size {size:?}, want {}", want.len()));
        }
    }
    if want.len() <= JSON_VALUE_MAX {
        let v = got["value"].as_str().map(|x| x.as_bytes().to_vec());
        if v.as_deref() != Some(want) {
            return Err(format!("{ctx}: value {:?}, want {:?}", got["value"], s(want)));
        }
    } else if got.get("value").is_some() {
        return Err(format!("{ctx}: large value should not be inlined"));
    }
    Ok(())
}

fn check_version(got: &Json, want: Option<u64>, ctx: &str) -> Result<(), String> {
    if got["version"].as_u64() != want {
        return Err(format!("{ctx}: version {:?}, want {want:?}", got["version"]));
    }
    Ok(())
}

pub struct KvModel {
    c: Client,
    db: String,
    model: Model,
    rng: Rng,
    step: u64,
    feed: Option<Follower>,
}

/// Cursor and state reconstructed by a follower of a database's change feed.
type Follower = (String, BTreeMap<Key, (Json, u64)>);

impl KvModel {
    pub async fn new(sim: &Arc<Sim>, user: &str, db: &str) -> Result<KvModel, String> {
        let c = Client::signup(sim, user, &format!("{user}@kv.test"), "password-123").await?;
        c.create_db(db).await?;
        Ok(KvModel { rng: sim.fork_rng(), c, db: db.to_string(), model: Model::default(), step: 0, feed: None })
    }

    fn path(&self, rest: &str) -> String {
        format!("/v1/dbs/{}{rest}", self.db)
    }

    /// Waits out any zombie request, then forces a manifest write so no
    /// in-flight attempt can commit afterwards.
    async fn fence(&self) {
        self.c.pause(super::super::client::ZOMBIE_WAIT_MS).await;
        let id = format!("{:016x}", self.step + (1u64 << 62));
        let r = self.c.call_retry("GET", &self.path(&format!("/commits/{id}")), None).await;
        assert_eq!(r.json["status"], "not_committed");
    }

    pub async fn step(&mut self) -> Result<(), String> {
        self.step += 1;
        match self.rng.below(12) {
            0..=4 => self.one_shot().await,
            5..=6 => self.interactive().await,
            7..=8 => self.raw().await,
            9 | 10 => self.follow().await,
            _ => self.full_check().await,
        }
    }

    async fn one_shot(&mut self) -> Result<(), String> {
        let mut sim = TxnSim::new(self.model.clone());
        let mut ops = Vec::new();
        let mut expect: Vec<Expect> = Vec::new();
        let mut fail_at: Option<usize> = None;
        let n = 1 + self.rng.usize(6);
        for i in 0..n {
            if fail_at.is_some() {
                break;
            }
            let ctx = format!("step {} op {i}", self.step);
            match self.rng.below(11) {
                0 | 1 => {
                    let k = match self.rng.chance(0.3) {
                        true => bulk_key(&mut self.rng),
                        false => pick_key(&mut self.rng, &["a", "b", "c"]),
                    };
                    ops.push(json!({"op": "get", "key": s(&k)}));
                    let want = sim.get(&k);
                    expect.push(Box::new(move |g| {
                        match &want {
                            None => {
                                if g["found"] != false {
                                    return Err(format!("{ctx}: expected not found, got {g}"));
                                }
                            }
                            Some((v, ver)) => {
                                check_value(g, v, &ctx)?;
                                check_version(g, *ver, &ctx)?;
                            }
                        }
                        Ok(())
                    }));
                }
                2 | 3 => {
                    let k = pick_key(&mut self.rng, &["a", "b"]);
                    let v = gen_value(&mut self.rng);
                    ops.push(json!({"op": "put", "key": s(&k), "value": s(&v)}));
                    sim.put(k, v);
                    expect.push(Box::new(|_| Ok(())));
                }
                4 => {
                    let k = pick_key(&mut self.rng, &["a", "b", "c"]);
                    ops.push(json!({"op": "delete", "key": s(&k)}));
                    sim.delete(k);
                    expect.push(Box::new(|_| Ok(())));
                }
                5 => {
                    let p = ["a/x", "b/", "c/", "a/", "b/y", "d/", "d/q"][self.rng.usize(7)];
                    let reverse = self.rng.chance(0.3);
                    let limit = 1 + self.rng.usize(5);
                    ops.push(json!({"op": "scan", "prefix": p, "limit": limit, "reverse": reverse}));
                    let all = sim.scan(&prefix_range(p.as_bytes()), reverse);
                    expect.push(Box::new(move |g| {
                        let items = g["items"].as_array().ok_or(format!("{ctx}: no items"))?;
                        if items.len() != all.len().min(limit) {
                            return Err(format!(
                                "{ctx}: scan {p} got {} items want {}",
                                items.len(),
                                all.len().min(limit)
                            ));
                        }
                        for (it, (k, v, ver)) in items.iter().zip(&all) {
                            if it["key"].as_str() != Some(&s(k)) {
                                return Err(format!("{ctx}: scan key {} want {}", it["key"], s(k)));
                            }
                            check_value(it, v, &ctx)?;
                            check_version(it, *ver, &ctx)?;
                        }
                        let more = all.len() > limit;
                        if g["next_cursor"].is_null() == more {
                            return Err(format!("{ctx}: next_cursor presence wrong (more={more})"));
                        }
                        Ok(())
                    }));
                }
                6 => {
                    let p = ["a/x", "b/", "c/z", "a/y"][self.rng.usize(4)];
                    ops.push(json!({"op": "delete_range", "prefix": p}));
                    sim.delete_range(prefix_range(p.as_bytes()));
                    expect.push(Box::new(|_| Ok(())));
                }
                9 => {
                    let n = 5 + self.rng.usize(40);
                    for _ in 0..n {
                        let k = bulk_key(&mut self.rng);
                        let v: Vec<u8> =
                            (0..40 + self.rng.usize(160)).map(|_| b'a' + self.rng.below(26) as u8).collect();
                        ops.push(json!({"op": "put", "key": s(&k), "value": s(&v)}));
                        sim.put(k, v);
                        expect.push(Box::new(|_| Ok(())));
                    }
                }
                10 => {
                    let p = ["d/", "d/p", "d/q", "d/r", "d/q1"][self.rng.usize(5)];
                    ops.push(json!({"op": "delete_range", "prefix": p}));
                    sim.delete_range(prefix_range(p.as_bytes()));
                    expect.push(Box::new(|_| Ok(())));
                }
                7 => {
                    let k = gen_key(&mut self.rng, "c");
                    let by = self.rng.range(0, 20) as i64 - 5;
                    ops.push(json!({"op": "increment", "key": s(&k), "by": by}));
                    let cur: i64 = sim.get(&k).map(|(v, _)| s(&v).parse().unwrap_or(0)).unwrap_or(0);
                    let n = cur + by;
                    sim.put(k, n.to_string().into_bytes());
                    expect.push(Box::new(move |g| {
                        if g["value"].as_i64() != Some(n) {
                            return Err(format!("{ctx}: increment got {} want {n}", g["value"]));
                        }
                        Ok(())
                    }));
                }
                _ => {
                    let k = pick_key(&mut self.rng, &["a", "b", "c"]);
                    let cur = sim.get(&k);
                    let truthful = self.rng.chance(0.85);
                    let (op, holds) = match self.rng.below(3) {
                        0 => {
                            let e = if truthful { cur.is_some() } else { cur.is_none() };
                            (json!({"op": "check", "key": s(&k), "exists": e}), truthful)
                        }
                        1 => match &cur {
                            Some((_, Some(ver))) => {
                                let v = if truthful { *ver } else { *ver + 1000 };
                                (json!({"op": "check", "key": s(&k), "version": v}), truthful)
                            }
                            _ => (json!({"op": "check", "key": s(&k), "version": 1_000_000}), false),
                        },
                        _ => match &cur {
                            Some((v, _)) if truthful => (json!({"op": "check", "key": s(&k), "value": s(v)}), true),
                            _ => (json!({"op": "check", "key": s(&k), "value": "not-the-value"}), false),
                        },
                    };
                    ops.push(op);
                    if !holds {
                        fail_at = Some(ops.len() - 1);
                    }
                    expect.push(Box::new(|_| Ok(())));
                }
            }
        }
        let writes = sim.is_write();
        let key = format!("step-{}-{}", self.c.name, self.step);
        let mut body = json!({"ops": ops});
        if writes {
            body["idempotency_key"] = json!(key);
        }
        let path = self.path("/txn");
        loop {
            let r = self.c.call("POST", &path, Some(body.clone())).await;
            let r = match r {
                Ok(r) if r.outcome_unknown() => {
                    self.c.pause(super::super::client::ZOMBIE_WAIT_MS).await;
                    continue;
                }
                Ok(r) => r,
                Err(_) => {
                    // Idempotent retry once any zombie attempt has expired.
                    self.c.pause(super::super::client::ZOMBIE_WAIT_MS).await;
                    continue;
                }
            };
            match r.status {
                200 => {
                    if let Some(i) = fail_at {
                        return Err(format!(
                            "step {}: expected precondition failure at op {i}, got {}",
                            self.step, r.json
                        ));
                    }
                    let version = r.json["version"].as_u64().ok_or("no version")?;
                    if r.json["replayed"] == true {
                        if !writes {
                            return Err("read-only replay".into());
                        }
                    } else {
                        let results = r.json["results"].as_array().ok_or("no results")?;
                        if results.len() != expect.len() {
                            return Err("result count mismatch".into());
                        }
                        for (res, e) in results.iter().zip(&expect) {
                            e(res)?;
                        }
                    }
                    if writes {
                        if r.json["committed"] != true {
                            return Err("writes not committed".into());
                        }
                        sim.apply(&mut self.model, version);
                    }
                    return Ok(());
                }
                412 => {
                    let i = r.json["error"]["details"]["op_index"].as_u64();
                    if fail_at.map(|x| x as u64) != i {
                        return Err(format!(
                            "step {}: unexpected precondition failure {} (expected {fail_at:?})",
                            self.step, r.json
                        ));
                    }
                    return Ok(());
                }
                503 | 409 => {
                    // Safe to retry with the same idempotency key (409 indicates an OCC conflict with a zombie).
                    self.c.pause(100).await;
                }
                // Rejected due to storage quota; deletions are never rejected.
                507 if sim.writes.values().any(|w| w.is_some()) => return Ok(()),
                _ => return Err(format!("step {}: unexpected response {} {}", self.step, r.status, r.json)),
            }
        }
    }

    async fn interactive(&mut self) -> Result<(), String> {
        let begin = self.c.call("POST", &self.path("/transactions"), Some(json!({}))).await;
        let Ok(begin) = begin else { return Ok(()) };
        if begin.status != 201 {
            if begin.status == 503 {
                return Ok(());
            }
            return Err(format!("begin failed: {} {}", begin.status, begin.json));
        }
        let id = begin.json["txn_id"].as_str().unwrap_or_default().to_string();
        let commit_id = begin.json["commit_id"].as_str().unwrap_or_default().to_string();
        let read_version = begin.json["read_version"].as_u64().unwrap_or(0);
        let mut sim = TxnSim::new(self.model.clone());
        let rounds = 1 + self.rng.usize(3);
        for _ in 0..rounds {
            let k = pick_key(&mut self.rng, &["a", "b"]);
            let v = gen_value(&mut self.rng);
            let k2 = pick_key(&mut self.rng, &["a", "b"]);
            let want = sim.get(&k2);
            let ops = json!({"ops": [
                {"op": "put", "key": s(&k), "value": s(&v)},
                {"op": "get", "key": s(&k2)},
            ]});
            sim.put(k.clone(), v.clone());
            // The read of k2 happens after the put of k.
            let want = if k2 == k { Some((v.clone(), None)) } else { want };
            let r = self.c.call("POST", &self.path(&format!("/transactions/{id}/ops")), Some(ops)).await;
            match r {
                Ok(r) if r.status == 200 => {
                    let g = &r.json["results"][1];
                    match &want {
                        None => {
                            if g["found"] != false {
                                return Err(format!("interactive get: expected missing, got {g}"));
                            }
                        }
                        Some((v, ver)) => {
                            check_value(g, v, "interactive get")?;
                            check_version(g, *ver, "interactive get")?;
                        }
                    }
                }
                _ => {
                    let _ = self.c.call("DELETE", &self.path(&format!("/transactions/{id}")), None).await;
                    return Ok(());
                }
            }
        }
        let r = self.c.call("POST", &self.path(&format!("/transactions/{id}/commit")), Some(json!({}))).await;
        let committed_version = match r {
            Ok(r) if r.status == 200 => Some(r.json["version"].as_u64().ok_or("no version")?),
            Ok(r) if r.status == 404 || r.status == 507 || r.retryable_not_applied() => None,
            Ok(r) if r.status == 409 => return Err(format!("single-client interactive txn conflicted: {}", r.json)),
            Ok(r) if !r.outcome_unknown() && r.status != 503 => return Err(format!("commit: {} {}", r.status, r.json)),
            _ => {
                // Ambiguous: resolve with a fenced status query.
                self.c.pause(super::super::client::ZOMBIE_WAIT_MS).await;
                let st = self.c.call_retry("GET", &self.path(&format!("/commits/{commit_id}")), None).await;
                match st.json["status"].as_str() {
                    Some("committed") => st.json["version"].as_u64(),
                    Some("not_committed") => None,
                    _ => return Err(format!("bad status {}", st.json)),
                }
            }
        };
        if let Some(v) = committed_version {
            if v <= read_version {
                return Err(format!("commit version {v} not after read version {read_version}"));
            }
            sim.apply(&mut self.model, v);
        }
        Ok(())
    }

    async fn raw(&mut self) -> Result<(), String> {
        let k = pick_key(&mut self.rng, &["a", "b"]);
        let kpath = self.path(&format!("/kv/{}", url_escape(&k)));
        match self.rng.below(4) {
            0 | 1 => {
                let v = gen_value(&mut self.rng);
                let cur = self.model.data.get(&k).cloned();
                let mut headers: Vec<(&str, String)> = Vec::new();
                let mut expect_ok = true;
                match self.rng.below(4) {
                    0 => {
                        headers.push(("if-none-match", "*".into()));
                        expect_ok = cur.is_none();
                    }
                    1 => {
                        let ver = cur.as_ref().map(|c| c.1).unwrap_or(999_999);
                        let ver = if self.rng.chance(0.8) { ver } else { ver + 7 };
                        headers.push(("if-match", format!("\"{ver}\"")));
                        expect_ok = cur.as_ref().is_some_and(|c| c.1 == ver);
                    }
                    _ => {}
                }
                headers.push(("content-type", "text/plain".into()));
                loop {
                    match self.c.send("PUT", &kpath, &headers, bytes::Bytes::from(v.clone())).await {
                        Ok(r) if r.status == 200 => {
                            if !expect_ok {
                                return Err(format!("conditional put should have failed: {k:?}"));
                            }
                            let j: Json = serde_json::from_slice(&r.body).unwrap_or(Json::Null);
                            let ver = j["version"].as_u64().ok_or("no version")?;
                            self.model.data.insert(k, (v, ver));
                            return Ok(());
                        }
                        Ok(r) if r.status == 412 => {
                            if expect_ok {
                                return Err(format!("unexpected 412 for {k:?}"));
                            }
                            return Ok(());
                        }
                        Ok(r) if r.status == 507 => return Ok(()),
                        Ok(r) if r.status == 503 && !String::from_utf8_lossy(&r.body).contains("\"unknown\"") => {
                            continue;
                        }
                        _ => {
                            // Ambiguous: fence, then read back.
                            self.fence().await;
                            let g = self.c.call_retry("GET", &kpath, None).await;
                            let now =
                                if g.status == 200 { Some((g.raw.body.to_vec(), header_version(&g))) } else { None };
                            match now {
                                Some((b, ver)) if b == v && cur.as_ref().is_none_or(|c| c.1 != ver) => {
                                    self.model.data.insert(k, (b, ver));
                                }
                                _ => {
                                    if now.as_ref().map(|x| (&x.0, x.1)) != cur.as_ref().map(|c| (&c.0, c.1)) {
                                        return Err(format!("ambiguous put left unexpected state for {k:?}"));
                                    }
                                }
                            }
                            return Ok(());
                        }
                    }
                }
            }
            2 => {
                let cur = self.model.data.get(&k).cloned();
                let (range, hdr) = match (&cur, self.rng.chance(0.4)) {
                    (Some((v, _)), true) if !v.is_empty() => {
                        let a = self.rng.usize(v.len());
                        let b = a + self.rng.usize(v.len() - a);
                        (Some((a, b)), vec![("range", format!("bytes={a}-{b}"))])
                    }
                    _ => (None, vec![]),
                };
                let r = loop {
                    match self.c.send("GET", &kpath, &hdr, bytes::Bytes::new()).await {
                        Ok(r) if r.status != 503 => break r,
                        _ => self.c.pause(100).await,
                    }
                };
                match (&cur, r.status) {
                    (None, 404) => Ok(()),
                    (Some((v, ver)), 200) => {
                        if r.body.as_ref() != v.as_slice() {
                            return Err(format!("raw get {k:?}: body mismatch"));
                        }
                        let got = r
                            .headers
                            .get("x-baserunna-version")
                            .and_then(|h| h.to_str().ok())
                            .and_then(|h| h.parse::<u64>().ok());
                        if got != Some(*ver) {
                            return Err(format!("raw get {k:?}: version {got:?} want {ver}"));
                        }
                        Ok(())
                    }
                    (Some((v, _)), 206) => {
                        let (a, b) = range.ok_or("unexpected 206")?;
                        if r.body.as_ref() != &v[a..=b.min(v.len() - 1)] {
                            return Err(format!("range {a}-{b} of {k:?}: mismatch"));
                        }
                        Ok(())
                    }
                    (c, st) => Err(format!("raw get {k:?}: status {st}, model has {}", c.is_some())),
                }
            }
            _ => {
                let cur = self.model.data.get(&k).cloned();
                loop {
                    match self.c.send("DELETE", &kpath, &[], bytes::Bytes::new()).await {
                        Ok(r) if r.status == 200 => {
                            let j: Json = serde_json::from_slice(&r.body).unwrap_or(Json::Null);
                            if j["existed"] != cur.is_some() {
                                return Err(format!("delete {k:?}: existed={} model={}", j["existed"], cur.is_some()));
                            }
                            self.model.data.remove(&k);
                            return Ok(());
                        }
                        Ok(r) if r.status == 503 && !String::from_utf8_lossy(&r.body).contains("\"unknown\"") => {
                            continue;
                        }
                        _ => {
                            self.fence().await;
                            let g = self.c.call_retry("GET", &kpath, None).await;
                            if g.status == 404 {
                                self.model.data.remove(&k);
                            } else if cur.as_ref().map(|c| c.1) != Some(header_version(&g)) {
                                return Err(format!("ambiguous delete left unexpected state for {k:?}"));
                            }
                            return Ok(());
                        }
                    }
                }
            }
        }
    }

    /// Reads the change feed from the follower's cursor and verifies that
    /// applying the changes reproduces the model. Resets from a full scan if
    /// the cursor has expired.
    async fn follow(&mut self) -> Result<(), String> {
        let (mut cursor, mut mirror) = match self.feed.take() {
            Some(f) => f,
            None => {
                let r = self.c.call_retry("GET", &self.path("/kv?limit=1"), None).await;
                let v = r.json["version"].as_u64().ok_or("no version")?;
                let mirror = self.model.data.iter().map(|(k, (_, ver))| (k.clone(), (Json::Null, *ver))).collect();
                (v.to_string(), mirror)
            }
        };
        loop {
            let limit = 1 + self.rng.usize(6);
            let wait = if self.rng.chance(0.2) { 50 + self.rng.below(300) } else { 0 };
            let p = self.path(&format!("/changes?after={cursor}&limit={limit}&wait_ms={wait}"));
            let r = self.c.call_retry("GET", &p, None).await;
            match r.status {
                200 => {}
                410 if r.code() == "cursor_expired" => return Ok(()),
                _ => return Err(format!("step {}: feed: {} {}", self.step, r.status, r.json)),
            }
            let changes = r.json["changes"].as_array().ok_or("no changes")?;
            for ch in changes {
                let version = ch["version"].as_u64().ok_or("change without a version")?;
                let ctx = format!("step {} feed change {ch}", self.step);
                if let Some(range) = ch.get("deleted_range") {
                    let start = range["start"].as_str().ok_or("no start")?.as_bytes().to_vec();
                    let end = range["end"].as_str().map(|e| e.as_bytes().to_vec());
                    mirror.retain(|k, _| !in_range(k, &(start.clone(), end.clone())));
                    continue;
                }
                let k = ch["key"].as_str().ok_or("no key")?.as_bytes().to_vec();
                match (ch["deleted"] == true, self.model.data.get(&k)) {
                    (true, None) => {
                        mirror.remove(&k);
                    }
                    (false, Some((v, ver))) if *ver == version => {
                        check_value(ch, v, &ctx)?;
                        mirror.insert(k, (ch.clone(), version));
                    }
                    (_, m) => return Err(format!("{ctx}: the model has {:?}", m.map(|(_, v)| v))),
                }
            }
            cursor = r.json["next_cursor"].as_str().ok_or("no next_cursor")?.to_string();
            if r.json["more"] != true {
                break;
            }
        }
        let got: Vec<(&Key, u64)> = mirror.iter().map(|(k, (_, v))| (k, *v)).collect();
        let want: Vec<(&Key, u64)> = self.model.data.iter().map(|(k, (_, v))| (k, *v)).collect();
        if got != want {
            return Err(format!("step {}: the feed's state {got:?} differs from the model {want:?}", self.step));
        }
        self.feed = Some((cursor, mirror));
        Ok(())
    }

    /// Runs a paginated full scan against the model and validates storage quota bounds.
    pub async fn full_check(&mut self) -> Result<(), String> {
        let info = self.c.call_retry("GET", &self.path(""), None).await;
        if let (Some(size), Some(quota)) =
            (info.json["stats"]["size_bytes"].as_u64(), info.json["stats"]["quota_bytes"].as_u64())
        {
            // Allow headroom for segment formatting and compaction overhead beyond the raw quota.
            if size > quota + quota / 4 + 8 * 1024 {
                return Err(format!("step {}: {size} bytes stored with a quota of {quota}", self.step));
            }
        }
        let mut got: BTreeMap<Key, (Json, u64)> = BTreeMap::new();
        let mut cursor: Option<String> = None;
        loop {
            let mut p = self.path("/kv?limit=7");
            if let Some(c) = &cursor {
                p.push_str(&format!("&cursor={c}"));
            }
            let r = self.c.call_retry("GET", &p, None).await;
            if r.status != 200 {
                return Err(format!("scan failed: {} {}", r.status, r.json));
            }
            for it in r.json["items"].as_array().ok_or("no items")? {
                let k = it["key"].as_str().ok_or("no key")?.as_bytes().to_vec();
                got.insert(k, (it.clone(), it["version"].as_u64().unwrap_or(0)));
            }
            match r.json["next_cursor"].as_str() {
                Some(c) => cursor = Some(c.to_string()),
                None => break,
            }
        }
        if got.len() != self.model.data.len() || !got.keys().eq(self.model.data.keys()) {
            let gk: Vec<String> = got.keys().map(|k| s(k)).collect();
            let mk: Vec<String> = self.model.data.keys().map(|k| s(k)).collect();
            return Err(format!("step {}: key set mismatch\n got {gk:?}\nwant {mk:?}", self.step));
        }
        for (k, (v, ver)) in &self.model.data {
            let (j, gv) = &got[k];
            check_value(j, v, &format!("full check {}", s(k)))?;
            if gv != ver {
                return Err(format!("full check {}: version {gv} want {ver}", s(k)));
            }
        }
        let r = self.c.call_retry("GET", &self.path("/kv?encoding=base64&keys_only=true&limit=1000"), None).await;
        let n = r.json["items"].as_array().map(|a| a.len()).unwrap_or(0);
        if n != self.model.data.len() {
            return Err(format!("base64 scan count {n} != {}", self.model.data.len()));
        }
        for it in r.json["items"].as_array().into_iter().flatten() {
            let k = it["key"].as_str().and_then(b64::decode).ok_or("bad base64 key")?;
            if !self.model.data.contains_key(&k) {
                return Err("base64 scan returned unknown key".into());
            }
        }
        Ok(())
    }
}

fn header_version(r: &Resp) -> u64 {
    r.raw.headers.get("x-baserunna-version").and_then(|h| h.to_str().ok()).and_then(|h| h.parse().ok()).unwrap_or(0)
}

pub async fn run(sim: Arc<Sim>, clients: usize, steps: usize, chaos: bool) -> Result<(), String> {
    let mut handles = Vec::new();
    for i in 0..clients {
        let sim = sim.clone();
        handles.push(tokio::spawn(async move {
            let mut w = KvModel::new(&sim, &format!("kvuser{i}"), &format!("kv{i}")).await?;
            for _ in 0..steps {
                w.step().await?;
            }
            Ok::<KvModel, String>(w)
        }));
    }
    let chaos_task = if chaos {
        let sim = sim.clone();
        Some(tokio::spawn(async move {
            loop {
                chaos_step(&sim).await;
            }
        }))
    } else {
        None
    };
    let mut models = Vec::new();
    let mut err = None;
    for h in handles {
        match h.await {
            Ok(Ok(w)) => models.push(w),
            Ok(Err(e)) => err = err.or(Some(e)),
            Err(e) => err = err.or(Some(format!("client task panicked: {e}"))),
        }
    }
    if let Some(t) = chaos_task {
        t.abort();
    }
    if let Some(e) = err {
        return Err(e);
    }
    super::recover(&sim).await;
    for w in &mut models {
        w.full_check().await?;
    }
    // Wait for transaction horizons to expire so range tombstones retire during subsequent writes.
    for _ in 0..2 {
        sim.sleep(sim.cfg.engine.txn_max_lifetime_ms + sim.cfg.engine.version_sample_ms).await;
        for w in &mut models {
            for _ in 0..10 {
                w.step().await?;
            }
            w.full_check().await?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn value_checks_reject_mismatches() {
        check_value(&json!({"value": "abc"}), b"abc", "t").unwrap();
        assert!(check_value(&json!({"value": "abd"}), b"abc", "t").is_err());
        assert!(check_value(&json!({}), b"abc", "t").is_err());
        check_value(&json!({"value": s(&[b'x'; INLINE_MAX])}), &[b'x'; INLINE_MAX], "inline at the limit").unwrap();
        let big = vec![b'x'; INLINE_MAX + 1];
        check_value(&json!({"value": s(&big), "blob": {"size": big.len()}}), &big, "t").unwrap();
        assert!(check_value(&json!({"value": s(&big), "blob": {"size": 3}}), &big, "t").is_err());
        let huge = vec![b'x'; JSON_VALUE_MAX + 1];
        assert!(check_value(&json!({"value": s(&huge), "blob": {"size": huge.len()}}), &huge, "t").is_err());
        check_version(&json!({"version": 4}), Some(4), "t").unwrap();
        assert!(check_version(&json!({"version": 4}), Some(5), "t").is_err());
        assert!(check_version(&json!({}), Some(5), "t").is_err());
    }

    #[test]
    fn txn_model_has_read_your_writes() {
        let mut base = Model::default();
        base.data.insert(b"a/x".to_vec(), (b"1".to_vec(), 3));
        base.data.insert(b"a/y".to_vec(), (b"2".to_vec(), 4));
        let mut t = TxnSim::new(base.clone());
        assert_eq!(t.get(b"a/x"), Some((b"1".to_vec(), Some(3))));
        t.put(b"a/z".to_vec(), b"3".to_vec());
        t.delete_range(prefix_range(b"a/x"));
        assert_eq!(t.get(b"a/x"), None);
        assert_eq!(t.get(b"a/z"), Some((b"3".to_vec(), None)));
        let keys: Vec<Key> = t.scan(&prefix_range(b"a/"), false).into_iter().map(|e| e.0).collect();
        assert_eq!(keys, vec![b"a/y".to_vec(), b"a/z".to_vec()]);
        assert!(t.is_write());
        t.apply(&mut base, 9);
        assert_eq!(base.data.keys().cloned().collect::<Vec<_>>(), vec![b"a/y".to_vec(), b"a/z".to_vec()]);
        assert_eq!(base.data[&b"a/z".to_vec()].1, 9);
    }
}
