//! `ttl`: verifies key-value and row TTL expiration and distributed lock
//! renewal under faults.
//!
//! Each client tracks the expiration window `[t0 + ttl - eps, t1 + ttl + eps]`
//! for every value and row it writes, where `[t0, t1]` is the commit request
//! interval and `eps` bounds clock error. Reads must observe a value before
//! its window and must not observe it after.
//!
//! Clients also contend on a TTL-backed lock key, renewing it before expiration.
//! In version order, each acquisition must be committed at or after the previous
//! lock's expiration, and each renewal must be committed before the lock it read
//! expired.

use super::super::Sim;
use super::super::client::{Client, ZOMBIE_WAIT_MS, url_escape};
use crate::util::rng::Rng;
use serde_json::{Value as Json, json};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

/// Expected state of a value: data, commit version, and expiration window in
/// simulation milliseconds.
#[derive(Clone, Debug)]
struct Val {
    data: String,
    version: u64,
    expires: Option<(u64, u64)>,
    /// True when the commit record expired before retries finished, leaving
    /// the write outcome unknown until the next definitive write or `expires`.
    uncertain: bool,
}

impl Val {
    fn uncertain() -> Val {
        Val { data: String::new(), version: 0, expires: None, uncertain: true }
    }
}

#[derive(Debug, PartialEq)]
enum Seen {
    Present,
    Absent,
    Either,
}

/// Expected visibility of `v` for a read spanning `[r0, r1]`.
fn expect(v: Option<&Val>, r0: u64, r1: u64, eps: u64) -> Seen {
    match v.map(|v| v.expires) {
        Some(Some((_, hi))) if r0.saturating_sub(eps) >= hi => Seen::Absent,
        _ if v.is_some_and(|v| v.uncertain) => Seen::Either,
        None => Seen::Absent,
        Some(None) => Seen::Present,
        Some(Some((lo, _))) if r1 + eps < lo => Seen::Present,
        Some(Some(_)) => Seen::Either,
    }
}

fn check_read(
    ctx: &str,
    v: Option<&Val>,
    got: Option<(&str, u64, Option<u64>)>,
    r0: u64,
    r1: u64,
    eps: u64,
) -> Result<(), String> {
    let seen = expect(v, r0, r1, eps);
    if v.is_some_and(|v| v.uncertain) && seen == Seen::Either {
        return Ok(());
    }
    match (got, v) {
        (None, _) if seen != Seen::Present => Ok(()),
        (Some((data, version, expires)), Some(m)) if seen != Seen::Absent => {
            if data != m.data || version != m.version {
                return Err(format!(
                    "{ctx}: read {data:?} v{version}, the newest write is {:?} v{}",
                    m.data, m.version
                ));
            }
            match (m.expires, expires) {
                (None, None) => Ok(()),
                (Some((lo, hi)), Some(e)) if (lo..=hi).contains(&e) => Ok(()),
                (want, e) => Err(format!("{ctx}: expires_at {e:?}, want within {want:?}")),
            }
        }
        (got, _) => Err(format!("{ctx}: read {got:?} between {r0} and {r1}, model {v:?} ({seen:?})")),
    }
}

struct Owner {
    c: Client,
    db: String,
    model: BTreeMap<String, Val>,
    rows: BTreeMap<i64, (String, Val)>,
    rng: Rng,
    step: u64,
    eps: u64,
}

const KEYS: &[&str] = &["k/a", "k/b", "k/c", "k/d", "k/e"];
const TOKENS: &[&str] = &["t1", "t2", "t3"];

impl Owner {
    fn path(&self, rest: &str) -> String {
        format!("/v1/dbs/{}{rest}", self.db)
    }

    fn ttl(&mut self) -> Option<u64> {
        (!self.rng.chance(0.3)).then(|| 200 + self.rng.below(4_000))
    }

    /// Commits `ops` with an idempotency key and returns the response and start
    /// timestamp, or `None` if the commit retention window elapsed before a
    /// definitive response was received.
    async fn commit(&mut self, ops: Json) -> Result<Option<(super::super::client::Resp, u64)>, String> {
        self.step += 1;
        let body = json!({"ops": ops, "idempotency_key": format!("{}-{}", self.c.name, self.step)});
        let t0 = self.c.sim.now_ms();
        let mut ambiguous = false;
        loop {
            if ambiguous && !self.c.dedups(t0) {
                return Ok(None);
            }
            match self.c.call("POST", &self.path("/txn"), Some(body.clone())).await {
                Ok(r) if r.code() == "conflict" || r.retryable_not_applied() => self.c.pause(50).await,
                Ok(r) if r.status == 200 || (400..500).contains(&r.status) && r.status != 429 => {
                    return Ok(Some((r, t0)));
                }
                _ => {
                    ambiguous = true;
                    self.c.pause(ZOMBIE_WAIT_MS).await;
                }
            }
        }
    }

    async fn put(&mut self) -> Result<(), String> {
        let k = KEYS[self.rng.usize(KEYS.len())].to_string();
        let data = format!("v{}", self.step);
        let ttl = self.ttl();
        let mut op = json!({"op": "put", "key": k, "value": data});
        if let Some(t) = ttl {
            op["ttl_ms"] = json!(t);
        }
        let Some((r, t0)) = self.commit(json!([op])).await? else {
            self.model.insert(k, Val::uncertain());
            return Ok(());
        };
        if r.status != 200 {
            return Err(format!("put: {} {}", r.status, r.json));
        }
        let (t1, version) = (self.c.sim.now_ms(), r.json["version"].as_u64().ok_or("missing version")?);
        let expires = ttl.map(|t| (t0 + t - self.eps, t1 + t + self.eps));
        self.model.insert(k, Val { data, version, expires, uncertain: false });
        Ok(())
    }

    async fn delete(&mut self) -> Result<(), String> {
        let k = KEYS[self.rng.usize(KEYS.len())].to_string();
        let Some((r, _)) = self.commit(json!([{"op": "delete", "key": k}])).await? else {
            self.model.insert(k, Val::uncertain());
            return Ok(());
        };
        if r.status != 200 {
            return Err(format!("delete: {} {}", r.status, r.json));
        }
        self.model.remove(&k);
        Ok(())
    }

    async fn read(&self, ops: Json) -> Result<Option<(Json, u64, u64)>, String> {
        let r0 = self.c.sim.now_ms();
        match self.c.call("POST", &self.path("/txn"), Some(json!({"ops": ops}))).await {
            Ok(r) if r.status == 200 => Ok(Some((r.json, r0, self.c.sim.now_ms()))),
            Ok(r) if r.status >= 500 || r.status == 429 => Ok(None),
            Ok(r) => Err(format!("read: {} {}", r.status, r.json)),
            Err(_) => Ok(None),
        }
    }

    async fn get(&mut self) -> Result<(), String> {
        let k = KEYS[self.rng.usize(KEYS.len())];
        let Some((j, r0, r1)) = self.read(json!([{"op": "get", "key": k}])).await? else { return Ok(()) };
        let g = &j["results"][0];
        let got = (g["found"] == true).then(|| {
            (g["value"].as_str().unwrap_or_default(), g["version"].as_u64().unwrap_or(0), g["expires_at"].as_u64())
        });
        check_read(&format!("get {k}"), self.model.get(k), got, r0, r1, self.eps)
    }

    async fn scan(&mut self) -> Result<(), String> {
        let Some((j, r0, r1)) = self.read(json!([{"op": "scan", "prefix": "k/"}])).await? else { return Ok(()) };
        let items = j["results"][0]["items"].as_array().ok_or("missing items")?;
        let got: BTreeMap<&str, &Json> = items.iter().map(|it| (it["key"].as_str().unwrap_or_default(), it)).collect();
        for k in KEYS {
            let it = got.get(k).map(|g| {
                (g["value"].as_str().unwrap_or_default(), g["version"].as_u64().unwrap_or(0), g["expires_at"].as_u64())
            });
            check_read(&format!("scan {k}"), self.model.get(*k), it, r0, r1, self.eps)?;
        }
        Ok(())
    }

    async fn raw_get(&mut self) -> Result<(), String> {
        let k = KEYS[self.rng.usize(KEYS.len())];
        let r0 = self.c.sim.now_ms();
        let Ok(r) =
            self.c.send("GET", &self.path(&format!("/kv/{}", url_escape(k.as_bytes()))), &[], Default::default()).await
        else {
            return Ok(());
        };
        let r1 = self.c.sim.now_ms();
        let header = |h: &str| r.headers.get(h).and_then(|v| v.to_str().ok()).and_then(|v| v.parse::<u64>().ok());
        let got = match r.status {
            200 => Some((String::from_utf8_lossy(&r.body).into_owned(), header("x-baserunna-version").unwrap_or(0))),
            404 => None,
            _ => return Ok(()),
        };
        let got = got.as_ref().map(|(d, v)| (d.as_str(), *v, header("x-baserunna-expires-at")));
        check_read(&format!("raw get {k}"), self.model.get(k), got, r0, r1, self.eps)
    }

    /// Inserts a row with a unique token and verifies that it is rejected only
    /// while another non-expired row holds that token.
    async fn insert_row(&mut self) -> Result<(), String> {
        let id = self.step as i64;
        let token = TOKENS[self.rng.usize(TOKENS.len())].to_string();
        let ttl = 200 + self.rng.below(3_000);
        let holders: Vec<(i64, Val)> =
            self.rows.iter().filter(|(_, (t, _))| *t == token).map(|(id, (_, v))| (*id, v.clone())).collect();
        let op = json!({"op": "insert_row", "table": "sess", "row": {"id": id, "token": token}, "ttl_ms": ttl});
        let Some((r, t0)) = self.commit(json!([op])).await? else {
            let max_expiry = self.c.sim.now_ms() + ttl + self.eps;
            self.rows.insert(id, (token, Val { expires: Some((0, max_expiry)), ..Val::uncertain() }));
            return Ok(());
        };
        let t1 = self.c.sim.now_ms();
        let seen =
            holders.iter().map(|(_, v)| expect(Some(v), t0, t1, self.eps)).fold(Seen::Absent, |a, b| match (a, b) {
                (Seen::Present, _) | (_, Seen::Present) => Seen::Present,
                (Seen::Either, _) | (_, Seen::Either) => Seen::Either,
                _ => Seen::Absent,
            });
        match (r.status, seen) {
            (200, Seen::Absent | Seen::Either) => {
                self.rows.retain(|_, (t, _)| *t != token);
                let version = r.json["version"].as_u64().ok_or("missing version")?;
                let val = Val {
                    data: token.clone(),
                    version,
                    expires: Some((t0 + ttl - self.eps, t1 + ttl + self.eps)),
                    uncertain: false,
                };
                self.rows.insert(id, (token, val));
                Ok(())
            }
            (409, Seen::Present | Seen::Either) => Ok(()),
            (s, seen) => Err(format!("insert {token}: {s} {}, holders {holders:?} ({seen:?})", r.json)),
        }
    }

    async fn get_row(&mut self) -> Result<(), String> {
        let Some((&id, _)) = self.rows.iter().nth(self.rng.usize(self.rows.len().max(1))) else { return Ok(()) };
        let Some((j, r0, r1)) = self.read(json!([{"op": "get_row", "table": "sess", "pk": id}])).await? else {
            return Ok(());
        };
        let g = &j["results"][0];
        let row = self.rows.get(&id).map(|(_, v)| v);
        let token = g["row"]["token"].as_str().unwrap_or_default();
        let got = (g["found"] == true).then(|| (token, row.map_or(0, |v| v.version), g["expires_at"].as_u64()));
        check_read(&format!("row {id}"), row, got, r0, r1, self.eps)
    }

    /// Writes a batch of expiring values so whole compacted segments expire below level 0.
    async fn bulk(&mut self) -> Result<(), String> {
        let ttl = 3_000 + self.rng.below(5_000);
        let ops: Vec<Json> = (0..30)
            .map(|i| json!({"op": "put", "key": format!("b/{}/{i:02}", self.step), "value": "x".repeat(150), "ttl_ms": ttl}))
            .collect();
        match self.commit(Json::Array(ops)).await? {
            Some((r, _)) if r.status != 200 => Err(format!("bulk: {} {}", r.status, r.json)),
            _ => Ok(()),
        }
    }

    async fn step(&mut self) -> Result<(), String> {
        match self.rng.below(20) {
            0..=6 => self.put().await?,
            7 => self.delete().await?,
            8..=11 => self.get().await?,
            12 | 13 => self.scan().await?,
            14 => self.raw_get().await?,
            15 | 16 => self.insert_row().await?,
            17 => self.get_row().await?,
            _ => self.bulk().await?,
        }
        let pause = self.rng.below(1_500);
        self.c.pause(pause).await;
        Ok(())
    }

    async fn settle(&mut self) -> Result<(), String> {
        let last =
            self.model.values().chain(self.rows.values().map(|(_, v)| v)).filter_map(|v| v.expires).map(|e| e.1).max();
        if let Some(hi) = last {
            let wait = hi.saturating_sub(self.c.sim.now_ms()) + 2 * self.eps;
            self.c.pause(wait).await;
        }
        self.model.retain(|_, v| v.expires.is_none());
        self.rows.clear();
        let r = self.c.call_retry("GET", &self.path("/kv?prefix=k/&limit=100"), None).await;
        let keys: Vec<&str> =
            r.json["items"].as_array().into_iter().flatten().filter_map(|it| it["key"].as_str()).collect();
        let sure: Vec<&str> = self.model.iter().filter(|(_, v)| !v.uncertain).map(|(k, _)| k.as_str()).collect();
        let explained = |k: &&str| self.model.contains_key(*k);
        if !sure.iter().all(|k| keys.contains(k)) || !keys.iter().all(explained) {
            return Err(format!("after every expiry: keys {keys:?}, want {sure:?}"));
        }
        let q = self.c.call_retry("POST", &self.path("/tables/sess/rows/query"), Some(json!({}))).await;
        if q.json["rows"].as_array().is_none_or(|r| !r.is_empty()) {
            return Err(format!("expired rows still read: {}", q.json));
        }
        // Trigger compaction over the expired data.
        for _ in 0..8 {
            self.bulk().await?;
            self.c.pause(2_000).await;
        }
        Ok(())
    }
}

/// Recorded lock acquisition or renewal: commit version, expiry timestamp, and
/// for renewals, the expiry of the previous lock if the response was not replayed.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct LockWrite {
    version: u64,
    expires: Option<u64>,
    renewal: Option<Option<u64>>,
}

const LOCK_TTL_MS: u64 = 1_500;

/// Commits `ops` with idempotency key `key`, retrying until it is committed
/// or fails a check (`Err` with the check's op index).
async fn lock_txn(c: &Client, ops: Json, key: &str) -> Result<Result<(u64, Json), u64>, String> {
    let body = json!({"ops": ops, "idempotency_key": key});
    loop {
        match c.call("POST", "/v1/dbs/locks/txn", Some(body.clone())).await {
            Ok(r) if r.status == 200 => return Ok(Ok((r.json["version"].as_u64().ok_or("missing version")?, r.json))),
            Ok(r) if r.status == 412 => {
                return Ok(Err(r.json["error"]["details"]["op_index"].as_u64().unwrap_or(u64::MAX)));
            }
            Ok(r) if r.code() == "conflict" || r.retryable_not_applied() => c.pause(30).await,
            Ok(r) if r.status != 503 && r.status != 429 => return Err(format!("lock: {} {}", r.status, r.json)),
            _ => c.pause(ZOMBIE_WAIT_MS).await,
        }
    }
}

async fn lock_expiry(c: &Client, version: u64) -> Option<u64> {
    let r = c.call("POST", "/v1/dbs/locks/txn", Some(json!({"ops": [{"op": "get", "key": "lock"}]}))).await;
    match r {
        Ok(r) if r.status == 200 && r.json["results"][0]["version"] == version => {
            r.json["results"][0]["expires_at"].as_u64()
        }
        _ => None,
    }
}

/// Acquires the lock if free and attempts to renew it near its expiry.
async fn take_and_renew(c: &Client, me: &str, out: &Mutex<Vec<LockWrite>>) -> Result<(), String> {
    let held = format!("held/{me}");
    let acquire = json!([
        {"op": "check", "key": held, "exists": false},
        {"op": "check", "key": "lock", "exists": false},
        {"op": "put", "key": "lock", "value": me, "ttl_ms": LOCK_TTL_MS},
        {"op": "put", "key": held, "value": ""},
        {"op": "increment", "key": "taken"},
    ]);
    let version = match lock_txn(c, acquire, me).await? {
        Ok((version, _)) => version,
        Err(0) => {
            let r = c.call_retry("POST", "/v1/dbs/locks/txn", Some(json!({"ops": [{"op": "get", "key": held}]}))).await;
            r.json["results"][0]["version"].as_u64().ok_or_else(|| format!("{held}: {}", r.json))?
        }
        Err(_) => return Ok(()),
    };
    let expires = lock_expiry(c, version).await;
    out.lock().unwrap_or_else(|e| e.into_inner()).push(LockWrite { version, expires, renewal: None });
    let d = LOCK_TTL_MS - 400 + c.rng().below(500);
    c.pause(d).await;
    let renew = json!([
        {"op": "get", "key": "lock"},
        {"op": "check", "key": "lock", "value": me},
        {"op": "put", "key": "lock", "value": me, "ttl_ms": LOCK_TTL_MS},
    ]);
    if let Ok((version, r)) = lock_txn(c, renew, &format!("{me}/renew")).await? {
        let seen = r["results"].get(0).and_then(|g| g["expires_at"].as_u64());
        let expires = lock_expiry(c, version).await;
        out.lock().unwrap_or_else(|e| e.into_inner()).push(LockWrite { version, expires, renewal: Some(seen) });
    }
    Ok(())
}

/// Verifies that in version order, each acquisition is committed at or after
/// the previous lock's expiry, and each renewal is committed before the lock it read
/// expired.
fn check_locks(writes: &[LockWrite]) -> Result<(), String> {
    let mut sorted = writes.to_vec();
    sorted.sort();
    for (i, w) in sorted.iter().enumerate() {
        let committed = w.expires.map(|e| e - LOCK_TTL_MS);
        if let Some(next) = sorted.get(i + 1)
            && next.version == w.version
        {
            return Err(format!("two lock writes committed as version {}", w.version));
        }
        let prev = i.checked_sub(1).map(|p| sorted[p]);
        let prev_expires = prev.and_then(|p| p.expires);
        match (w.renewal, committed) {
            (Some(Some(seen)), Some(at)) if at >= seen => {
                return Err(format!(
                    "renewal v{} committed at {at}, once the lock it read expired at {seen}",
                    w.version
                ));
            }
            (Some(Some(seen)), _) if prev_expires.is_some_and(|e| e != seen) => {
                return Err(format!(
                    "renewal v{} read a lock expiring at {seen}, not the latest lock {prev:?}",
                    w.version
                ));
            }
            (Some(None), Some(at)) if prev_expires.is_some_and(|e| at >= e) => {
                return Err(format!("renewal v{} committed at {at}, once the lock {prev:?} expired", w.version));
            }
            (None, Some(at)) if prev_expires.is_some_and(|e| at < e) => {
                return Err(format!("lock taken at v{} ({at}) before the lock {prev:?} expired", w.version));
            }
            _ => {}
        }
    }
    Ok(())
}

pub async fn run(sim: Arc<Sim>, owners: usize, steps: usize, chaos: bool) -> Result<(), String> {
    let eps = 2 * sim.cfg.engine.clock_sync_max_error_ms;
    let mut handles = Vec::new();
    for i in 0..owners {
        let sim = sim.clone();
        handles.push(tokio::spawn(async move {
            let c = Client::signup(&sim, &format!("ttl{i}"), &format!("ttl{i}@ttl.test"), "password-123").await?;
            let db = format!("ttl{i}");
            c.create_db(&db).await?;
            let def = json!({"name": "sess", "primary_key": ["id"],
                "columns": [{"name": "id", "type": "int", "nullable": false}, {"name": "token", "type": "string"}],
                "indexes": [{"name": "by_token", "columns": ["token"], "unique": true}]});
            let r = c.call_settled("POST", &format!("/v1/dbs/{db}/tables"), Some(def)).await;
            if r.status != 201 && r.json["error"]["code"] != "already_exists" {
                return Err(format!("create table: {} {}", r.status, r.json));
            }
            let mut o =
                Owner { rng: sim.fork_rng(), c, db, model: BTreeMap::new(), rows: BTreeMap::new(), step: 0, eps };
            for _ in 0..steps {
                o.step().await?;
            }
            Ok::<Owner, String>(o)
        }));
    }
    let lockers = Client::signup(&sim, "locker", "locker@ttl.test", "password-123").await?;
    lockers.create_db("locks").await?;
    let token = lockers.token.clone().unwrap_or_default();
    let lock_writes: Arc<Mutex<Vec<LockWrite>>> = Arc::default();
    let mut lock_handles = Vec::new();
    for i in 0..3 {
        let (sim, token, lock_writes) = (sim.clone(), token.clone(), lock_writes.clone());
        lock_handles.push(tokio::spawn(async move {
            let c = Client::with_token(&sim, &format!("locker{i}"), &token);
            for n in 0..steps / 2 {
                take_and_renew(&c, &format!("l{i}-{n}"), &lock_writes).await?;
                let d = c.rng().below(LOCK_TTL_MS);
                c.pause(d).await;
            }
            Ok::<(), String>(())
        }));
    }
    let phase = chaos.then(|| super::chaos_phase(&sim));
    let mut owners_done = Vec::new();
    let mut err = None;
    for h in handles {
        match h.await {
            Ok(Ok(o)) => owners_done.push(o),
            Ok(Err(e)) => err = err.or(Some(e)),
            Err(e) => err = err.or(Some(format!("owner panicked: {e}"))),
        }
    }
    for h in lock_handles {
        match h.await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => err = err.or(Some(e)),
            Err(e) => err = err.or(Some(format!("locker panicked: {e}"))),
        }
    }
    if let Some(e) = err {
        return Err(e);
    }
    super::settle(&sim, phase).await;
    let writes = lock_writes.lock().unwrap_or_else(|e| e.into_inner()).clone();
    check_locks(&writes)?;
    let acquired = writes.iter().filter(|w| w.renewal.is_none()).count();
    let r =
        lockers.call_retry("POST", "/v1/dbs/locks/txn", Some(json!({"ops": [{"op": "get", "key": "taken"}]}))).await;
    let taken: usize = r.json["results"][0]["value"].as_str().and_then(|v| v.parse().ok()).unwrap_or(0);
    if taken != acquired {
        return Err(format!("the lock was taken {taken} times, {acquired} acquisitions were acknowledged"));
    }
    for o in &mut owners_done {
        o.settle().await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn val(expires: Option<(u64, u64)>) -> Val {
        Val { data: "d".into(), version: 3, expires, uncertain: false }
    }

    #[test]
    fn reads_are_checked_against_expiry_windows() {
        let v = val(Some((1_000, 1_100)));
        assert!(check_read("t", Some(&v), Some(("d", 3, Some(1_050))), 800, 850, 10).is_ok());
        assert!(check_read("t", Some(&v), None, 800, 850, 10).is_err());
        assert!(check_read("t", Some(&v), Some(("d", 3, Some(1_050))), 1_200, 1_250, 10).is_err());
        assert!(check_read("t", Some(&v), None, 1_200, 1_250, 10).is_ok());
        assert!(check_read("t", Some(&v), None, 1_000, 1_050, 10).is_ok());
        assert!(check_read("t", Some(&v), Some(("d", 3, Some(1_050))), 1_000, 1_050, 10).is_ok());
        assert!(check_read("t", Some(&v), Some(("old", 2, None)), 800, 850, 10).is_err());
        assert!(check_read("t", Some(&v), Some(("d", 3, Some(1_500))), 800, 850, 10).is_err());
        assert!(check_read("t", Some(&val(None)), Some(("d", 3, None)), 0, 1, 10).is_ok());
        assert!(check_read("t", Some(&val(None)), Some(("d", 3, Some(5))), 0, 1, 10).is_err());
        assert!(check_read("t", None, Some(("d", 3, None)), 0, 1, 10).is_err());
        let unsure = Val { expires: Some((0, 1_000)), ..Val::uncertain() };
        assert!(check_read("t", Some(&unsure), Some(("t1", 0, Some(990))), 900, 950, 10).is_ok());
        assert!(check_read("t", Some(&unsure), None, 900, 950, 10).is_ok());
        assert!(check_read("t", Some(&unsure), Some(("t1", 0, Some(990))), 5_000, 5_100, 10).is_err());
    }

    #[test]
    fn lock_checks_reject_overlapping_holders() {
        let w = |version, expires: Option<u64>, renewal| LockWrite { version, expires, renewal };
        let good = [
            w(5, Some(10_000), None),
            w(7, Some(11_000), Some(Some(10_000))),
            w(8, Some(12_000), Some(None)),
            w(9, Some(13_500), None),
            w(12, None, None),
        ];
        check_locks(&good).unwrap();
        assert!(check_locks(&[w(5, Some(10_000), None), w(9, Some(11_499), None)]).is_err());
        assert!(check_locks(&[w(9, Some(11_000), None), w(5, Some(10_000), None)]).is_err());
        assert!(check_locks(&[w(5, None, None), w(5, None, None)]).is_err());
        let late = [w(5, Some(10_000), None), w(7, Some(11_500), Some(Some(10_000)))];
        assert!(check_locks(&late).is_err());
        let replayed = [w(5, Some(10_000), None), w(7, Some(11_500), Some(None))];
        assert!(check_locks(&replayed).is_err());
        let stale = [w(5, Some(10_000), None), w(6, Some(10_500), None), w(7, Some(11_000), Some(Some(10_000)))];
        assert!(check_locks(&stale).is_err());
    }
}
